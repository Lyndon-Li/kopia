package snapshotgc

import (
	"bufio"
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"io"
	"os"
	"sort"
	"sync"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/tempfile"
)

// SortedSet is a set of byte slices that spills to disk when it gets too large.
// It is optimized for write-heavy phases followed by a sequential read phase (sorted).
type SortedSet struct {
	mu            sync.Mutex
	buffer        map[string]struct{}
	bufferSize    int
	maxBufferSize int

	tempFiles []*os.File
	iter      *mergeIterator
}

// NewSortedSet creates a new SortedSet with the given memory buffer limit.
func NewSortedSet(maxBufferSize int) *SortedSet {
	return &SortedSet{
		buffer:        make(map[string]struct{}),
		maxBufferSize: maxBufferSize,
	}
}

// Put adds a key to the set.
func (s *SortedSet) Put(ctx context.Context, key []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	k := string(key)
	if _, ok := s.buffer[k]; ok {
		return nil
	}

	s.buffer[k] = struct{}{}
	s.bufferSize += len(k)

	if s.bufferSize >= s.maxBufferSize {
		return s.flushLocked()
	}

	return nil
}

func (s *SortedSet) flushLocked() error {
	if len(s.buffer) == 0 {
		return nil
	}

	f, err := tempfile.CreateAutoDelete()
	if err != nil {
		return errors.Wrap(err, "unable to create temp file")
	}

	keys := make([]string, 0, len(s.buffer))
	for k := range s.buffer {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	w := bufio.NewWriter(f)
	lenBuf := make([]byte, 4)

	for _, k := range keys {
		binary.BigEndian.PutUint32(lenBuf, uint32(len(k)))
		if _, err := w.Write(lenBuf); err != nil {
			f.Close() //nolint:errcheck
			return err
		}
		if _, err := w.WriteString(k); err != nil {
			f.Close() //nolint:errcheck
			return err
		}
	}

	if err := w.Flush(); err != nil {
		f.Close() //nolint:errcheck
		return err
	}

	if _, err := f.Seek(0, 0); err != nil {
		f.Close() //nolint:errcheck
		return err
	}

	s.tempFiles = append(s.tempFiles, f)
	s.buffer = make(map[string]struct{})
	s.bufferSize = 0

	return nil
}

// Finish prepares the set for reading. It flushes any remaining items in memory.
func (s *SortedSet) Finish(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.flushLocked(); err != nil {
		return err
	}

	// Create iterator
	iter, err := newMergeIterator(s.tempFiles)
	if err != nil {
		return err
	}
	s.iter = iter
	return nil
}

// Contains checks if the key exists in the set.
// IMPORTANT: This method assumes keys are queried in increasing order (lexicographically).
// It maintains state and advances the iterator.
func (s *SortedSet) Contains(key []byte) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.iter == nil {
		return false
	}

	return s.iter.contains(key)
}

// Close closes all temporary files.
func (s *SortedSet) Close(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, f := range s.tempFiles {
		f.Close() //nolint:errcheck
	}
	s.tempFiles = nil
	s.buffer = nil
}

// mergeIterator merges multiple sorted files.
type mergeIterator struct {
	pq       priorityQueue
	nextItem []byte
	hasItem  bool
	err      error
}

func newMergeIterator(files []*os.File) (*mergeIterator, error) {
	pq := make(priorityQueue, 0, len(files))
	for _, f := range files {
		r := &fileReader{r: bufio.NewReader(f), f: f}
		if err := r.advance(); err != nil {
			if err == io.EOF {
				continue
			}
			return nil, err
		}
		heap.Push(&pq, r)
	}

	it := &mergeIterator{pq: pq}
	it.advance()
	return it, nil
}

func (it *mergeIterator) advance() {
	if it.pq.Len() == 0 {
		it.hasItem = false
		return
	}

	// Get smallest item
	top := it.pq[0]
	it.nextItem = append([]byte(nil), top.current...)
	it.hasItem = true

	// Advance the reader that provided this item
	if err := top.advance(); err != nil {
		if err == io.EOF {
			heap.Pop(&it.pq)
		} else {
			it.err = err
			it.hasItem = false
		}
	} else {
		heap.Fix(&it.pq, 0)
	}

	// Skip duplicates
	for it.pq.Len() > 0 {
		top = it.pq[0]
		if bytes.Equal(top.current, it.nextItem) {
			if err := top.advance(); err != nil {
				if err == io.EOF {
					heap.Pop(&it.pq)
				} else {
					it.err = err
				}
			} else {
				heap.Fix(&it.pq, 0)
			}
		} else {
			break
		}
	}
}

func (it *mergeIterator) contains(key []byte) bool {
	// Advance until nextItem >= key
	for it.hasItem && bytes.Compare(it.nextItem, key) < 0 {
		it.advance()
	}

	if !it.hasItem {
		return false
	}

	return bytes.Equal(it.nextItem, key)
}

type fileReader struct {
	r       *bufio.Reader
	f       *os.File
	current []byte
}

func (fr *fileReader) advance() error {
	var lenBuf [4]byte
	if _, err := io.ReadFull(fr.r, lenBuf[:]); err != nil {
		return err
	}
	l := binary.BigEndian.Uint32(lenBuf[:])
	buf := make([]byte, l)
	if _, err := io.ReadFull(fr.r, buf); err != nil {
		return err
	}
	fr.current = buf
	return nil
}

type priorityQueue []*fileReader

func (pq priorityQueue) Len() int { return len(pq) }
func (pq priorityQueue) Less(i, j int) bool {
	return bytes.Compare(pq[i].current, pq[j].current) < 0
}
func (pq priorityQueue) Swap(i, j int) { pq[i], pq[j] = pq[j], pq[i] }
func (pq *priorityQueue) Push(x interface{}) {
	*pq = append(*pq, x.(*fileReader))
}
func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}
