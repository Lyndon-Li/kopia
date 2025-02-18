package content

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/edsrzf/mmap-go"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/cache"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content/index"
	"github.com/kopia/kopia/repo/logging"
)

const (
	simpleIndexSuffix = ".sndx"
)

type diskCommittedContentIndexCache struct {
	dirname              string
	timeNow              func() time.Time
	v1PerContentOverhead func() int
	log                  logging.Logger
	minSweepAge          time.Duration
}

type memMapInfo struct {
	handle *os.File
	mapped mmap.MMap
}

type memMapStore struct {
	fullpath string
	mapped   unsafe.Pointer
	refCount int64
	lock     sync.Mutex
	log      logging.Logger
}

func (c *diskCommittedContentIndexCache) indexBlobPath(indexBlobID blob.ID) string {
	return filepath.Join(c.dirname, string(indexBlobID)+simpleIndexSuffix)
}

func (c *diskCommittedContentIndexCache) openIndex(ctx context.Context, indexBlobID blob.ID) (index.Index, error) {
	fullpath := c.indexBlobPath(indexBlobID)

	ndx, err := index.OpenWithStore(&memMapStore{
		fullpath: fullpath,
		log:      c.log,
	}, c.v1PerContentOverhead)
	if err != nil {
		return nil, errors.Wrapf(err, "error opening index from %v", indexBlobID)
	}

	return ndx, nil
}

func (s *memMapStore) Load() error {
	ref := atomic.AddInt64(&s.refCount, 1)
	if ref > 1 && atomic.LoadPointer(&s.mapped) != nil {
		return nil
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	if atomic.LoadPointer(&s.mapped) != nil {
		return nil
	}

	s.log.Infof("-------mapping index file %s", s.fullpath)

	m, err := mmapOpenWithRetry(s.fullpath, s.log)
	if err != nil {
		atomic.AddInt64(&s.refCount, -1)
		return err
	}

	atomic.StorePointer(&s.mapped, unsafe.Pointer(m))

	return nil
}

func (s *memMapStore) Get() []byte {
	p := atomic.LoadPointer(&s.mapped)
	return (*memMapInfo)(p).mapped
}

func (s *memMapStore) Release() {
	ref := atomic.AddInt64(&s.refCount, -1)
	if ref > 0 {
		return
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	if atomic.LoadInt64(&s.refCount) > 0 {
		return
	}

	s.log.Infof("-------unmapping index file %s", s.fullpath)

	p := (*memMapInfo)(atomic.SwapPointer(&s.mapped, nil))
	if p != nil {
		if err := p.mapped.Unmap(); err != nil {
			s.log.Warnf("failed to unmap index %v, err: %v", s.fullpath, err)
		}

		if err := p.handle.Close(); err != nil {
			s.log.Warnf("failed to close index %v, err: %v", s.fullpath)
		}
	}
}

func (s *memMapStore) Close() error {
	return nil
}

// mmapOpenWithRetry attempts mmap.Open() with exponential back-off to work around rare issue specific to Windows where
// we can't open the file right after it has been written.
func mmapOpenWithRetry(path string, log logging.Logger) (*memMapInfo, error) {
	const (
		maxRetries    = 8
		startingDelay = 10 * time.Millisecond
	)

	// retry milliseconds: 10, 20, 40, 80, 160, 320, 640, 1280, total ~2.5s
	f, err := os.Open(path) //nolint:gosec
	nextDelay := startingDelay

	retryCount := 0
	for err != nil && retryCount < maxRetries {
		retryCount++
		log.Debugf("retry #%v unable to mmap.Open(): %v", retryCount, err)
		time.Sleep(nextDelay)
		nextDelay *= 2

		f, err = os.Open(path) //nolint:gosec
	}

	if err != nil {
		return nil, errors.Wrap(err, "unable to open file despite retries")
	}

	mm, err := mmap.Map(f, mmap.RDONLY, 0)
	if err != nil {
		f.Close() //nolint:errcheck

		return nil, errors.Wrap(err, "mmap error")
	}

	return &memMapInfo{f, mm}, nil
}

func (c *diskCommittedContentIndexCache) hasIndexBlobID(ctx context.Context, indexBlobID blob.ID) (bool, error) {
	_, err := os.Stat(c.indexBlobPath(indexBlobID))
	if err == nil {
		return true, nil
	}

	if os.IsNotExist(err) {
		return false, nil
	}

	return false, errors.Wrapf(err, "error checking %v", indexBlobID)
}

func (c *diskCommittedContentIndexCache) addContentToCache(ctx context.Context, indexBlobID blob.ID, data gather.Bytes) error {
	exists, err := c.hasIndexBlobID(ctx, indexBlobID)
	if err != nil {
		return err
	}

	if exists {
		return nil
	}

	tmpFile, err := writeTempFileAtomic(c.dirname, data.ToByteSlice())
	if err != nil {
		return err
	}

	// rename() is atomic, so one process will succeed, but the other will fail
	if err := os.Rename(tmpFile, c.indexBlobPath(indexBlobID)); err != nil {
		// verify that the content exists
		exists, err := c.hasIndexBlobID(ctx, indexBlobID)
		if err != nil {
			return err
		}

		if !exists {
			return errors.Errorf("unsuccessful index write of content %q", indexBlobID)
		}
	}

	return nil
}

func writeTempFileAtomic(dirname string, data []byte) (string, error) {
	// write to a temp file to avoid race where two processes are writing at the same time.
	tf, err := os.CreateTemp(dirname, "tmp")
	if err != nil {
		if os.IsNotExist(err) {
			os.MkdirAll(dirname, cache.DirMode) //nolint:errcheck
			tf, err = os.CreateTemp(dirname, "tmp")
		}
	}

	if err != nil {
		return "", errors.Wrap(err, "can't create tmp file")
	}

	if _, err := tf.Write(data); err != nil {
		return "", errors.Wrap(err, "can't write to temp file")
	}

	if err := tf.Close(); err != nil {
		return "", errors.New("can't close tmp file")
	}

	return tf.Name(), nil
}

func (c *diskCommittedContentIndexCache) expireUnused(ctx context.Context, used []blob.ID) error {
	c.log.Debugw("expireUnused",
		"except", used,
		"minSweepAge", c.minSweepAge)

	entries, err := os.ReadDir(c.dirname)
	if err != nil {
		return errors.Wrap(err, "can't list cache")
	}

	remaining := map[blob.ID]os.FileInfo{}

	for _, ent := range entries {
		fi, err := ent.Info()
		if os.IsNotExist(err) {
			// we lost the race, the file was deleted since it was listed.
			continue
		}

		if err != nil {
			return errors.Wrap(err, "failed to read file info")
		}

		if strings.HasSuffix(ent.Name(), simpleIndexSuffix) {
			n := strings.TrimSuffix(ent.Name(), simpleIndexSuffix)
			remaining[blob.ID(n)] = fi
		}
	}

	for _, u := range used {
		delete(remaining, u)
	}

	for _, rem := range remaining {
		if c.timeNow().Sub(rem.ModTime()) > c.minSweepAge {
			c.log.Debugw("removing unused",
				"name", rem.Name(),
				"mtime", rem.ModTime())

			if err := os.Remove(filepath.Join(c.dirname, rem.Name())); err != nil {
				c.log.Errorf("unable to remove unused index file: %v", err)
			}
		} else {
			c.log.Debugw("keeping unused index because it's too new",
				"name", rem.Name(),
				"mtime", rem.ModTime(),
				"threshold", c.minSweepAge)
		}
	}

	return nil
}
