package index

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"hash/fnv"
	"io"
	"os"

	"github.com/edsrzf/mmap-go"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/tempfile"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/logging"
	"github.com/pkg/errors"
)

const (
	defaultSegCapacity = 64 << 10
	defaultSegFileSize = 3 << 20
)

type LargeBuilder struct {
	store       map[*ID]infoLoc
	segWrite    mmap.MMap
	writeOffset int
	writeSegNum int
	segRead     mmap.MMap
	segCapacity int
	totalSegs   int
	instanceID  string
	mapFiles    map[string]*os.File
	baseTimestamp          int64
	packBlobIDs map[blob.ID]int
	indexBlobIDs map[blob.ID]int

}

type infoLoc struct {
	segment int16
	pos     int16
	
}

type pair struct {
	key   *ID
	value infoLoc
}

var log = logging.Module("idxbuilder")

func NewLargeBuilder() *LargeBuilder {
	return &LargeBuilder{
		store:       make(map[*ID]infoLoc),
		segCapacity: defaultSegCapacity,
	}
}

// Add adds a new entry to the builder or conditionally replaces it if the timestamp is greater.
func (b *LargeBuilder) Add(i Info) error {
	loc, err := b.storeInfo(&i)
	if err != nil {
		return errors.Wrap(err, "error to store index")
	}
	
	binary.Write()
	binary.Read()

	cid := &i.ContentID

	oldLoc, found := b.store[cid]
	if !found {
		b.store[cid] = loc
		return nil
	}

	old, err := b.getInfoFromLoc(oldLoc)
	if err != nil {
		return errors.Wrapf(err, "error to load index at loc %v", oldLoc)
	}

	if contentInfoGreaterThanStruct(i, *old) {
		b.store[cid] = loc
	}

	return nil
}

func (b *LargeBuilder) EndAdd() {
	b.closeWrite()
}

func (b *LargeBuilder) Close() {
	b.closeWrite()
	b.closeReadMap()
	b.closeMapFiles()
}

func (b *LargeBuilder) closeWrite() {
	if b.segWrite != nil {
		if err := b.segWrite.Unmap(); err != nil {
			log(ctx).Warn("unable to close write segment")
		}

		b.segWrite = nil
	}
}

func (b *LargeBuilder) closeReadMap() {
	if b.segRead != nil {
		if err := b.segRead.Unmap(); err != nil {
			log(ctx).Warn("unable to close read segment")
		}

		b.segRead = nil
	}
}

func (b *LargeBuilder) closeMapFiles() {
	for k, v := range b.mapFiles {
		if err := v.Close(); err != nil {
			log(ctx).Warnf("unable to close map file %s, for segment %s", v.Name(), k)
		}
	}

	clear(b.mapFiles)
}

func (b *LargeBuilder) shard(maxShardSize int) [][]pair {
	if len(b.store) == 0 {
		return [][]pair{}
	}

	numShards := (len(b.store) + maxShardSize - 1) / maxShardSize
	result := make([][]pair, numShards)

	if numShards <= 1 {
		for k, v := range b.store {
			result[0] = append(result[0], pair{k, v})
		}
	} else {
		for k, v := range b.store {
			h := fnv.New32a()
			io.WriteString(h, k.String()) //nolint:errcheck

			shard := h.Sum32() % uint32(numShards)

			result[shard] = append(result[shard], pair{k, v})
		}
	}

	return result
}

// BuildShards builds the set of index shards ensuring no more than the provided number of contents are in each index.
// Returns shard bytes and function to clean up after the shards have been written.
func (b *LargeBuilder) BuildShards(indexVersion int, stable bool, shardSize int) ([]gather.Bytes, func(), error) {
	if shardSize == 0 {
		return nil, nil, errors.Errorf("invalid shard size")
	}

	var (
		shards        = b.shard(shardSize)
		dataShardsBuf []*gather.WriteBuffer
		dataShards    []gather.Bytes
		randomSuffix  [32]byte
	)

	closeShards := func() {
		for _, ds := range dataShardsBuf {
			ds.Close()
		}
	}

	for _, s := range shards {
		builder := b.makeBuilder(s)

		buf := gather.NewWriteBuffer()

		dataShardsBuf = append(dataShardsBuf, buf)

		if err := builder.BuildStable(buf, indexVersion); err != nil {
			closeShards()

			return nil, nil, errors.Wrap(err, "error building index shard")
		}

		if !stable {
			if _, err := rand.Read(randomSuffix[:]); err != nil {
				closeShards()

				return nil, nil, errors.Wrap(err, "error getting random bytes for suffix")
			}

			if _, err := buf.Write(randomSuffix[:]); err != nil {
				closeShards()

				return nil, nil, errors.Wrap(err, "error writing extra random suffix to ensure indexes are always globally unique")
			}
		}

		dataShards = append(dataShards, buf.Bytes())
	}

	return dataShards, closeShards, nil
}

func (b *LargeBuilder) makeBuilder(shard []pair) Builder {
	length := len(shard)
	builder := make(map[ID]Info, length)
	for i := 0; i < length; i++ {
		info := b.getInfoFromLoc(shard[i].value)
		builder[*shard[i].key] = *info
	}

	return builder
}

func (b *LargeBuilder) storeInfo(i *Info) infoLoc {

}

func (b *LargeBuilder) getInfoFromLoc(loc infoLoc) *Info {
}

func (b *LargeBuilder) mayMapWriteSegment(ctx context.Context, segName string) (mmap.MMap, error) {
	flags := 0
	var f *os.File
	var err error

	if b.writeOffset 

	if _, found := b.mapFiles[segName]; found {
		f = b.mapFiles[segName]
	}

	if f == nil {
		if read {
			return nil, errors.Errorf("there is no map file for segment %s", segName)
		}

		f, err = createMappedFile(ctx, defaultSegFileSize)
		if err != nil {
			return nil, err
		}

		b.mapFiles[segName] = f
	}

	s, err := mmap.MapRegion(f, m.opts.FileSegmentSize, mmap.RDWR, flags, 0)
	if err != nil {
		return nil, errors.Wrap(err, "unable to map region")
	}

	return s[:0], nil
}

func (b *LargeBuilder) mapSegment(ctx context.Context, read bool, segName string) (mmap.MMap, error) {
	flags := 0
	var f *os.File
	var err error



	if _, found := b.mapFiles[segName]; found {
		f = b.mapFiles[segName]
	}

	if f == nil {
		if read {
			return nil, errors.Errorf("there is no map file for segment %s", segName)
		}

		f, err = createMappedFile(ctx, defaultSegFileSize)
		if err != nil {
			return nil, err
		}

		b.mapFiles[segName] = f
	}

	s, err := mmap.MapRegion(f, m.opts.FileSegmentSize, mmap.RDWR, flags, 0)
	if err != nil {
		return nil, errors.Wrap(err, "unable to map region")
	}

	return s[:0], nil
}

func createMappedFile(ctx context.Context, size int) (*os.File, error) {
	f, err := tempfile.Create("")
	if err != nil {
		return nil, errors.Wrap(err, "unable to create memory-mapped file")
	}

	if err := f.Truncate(int64(size)); err != nil {
		closeFile(ctx, f)

		return nil, errors.Wrap(err, "unable to truncate memory-mapped file")
	}

	return f, nil
}

func closeFile(ctx context.Context, f *os.File) {
	if err := f.Close(); err != nil {
		log(ctx).Warnf("unable to close segment file: %v", err)
	}
}
