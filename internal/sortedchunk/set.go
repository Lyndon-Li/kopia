package snapshotgc

import (
	"bufio"
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"io"
	"os"
	"sync"

	"github.com/petar/GoLLRB/llrb"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/tempfile"
)

// List is a sorted list and flush data to disk when it increases too large
type List interface {
	// Put pushes a key to List
	Put([]byte) error

	// Finalize flushes the keys to disk
	Finalize() error

	// GetIterator returns an Iterator to read from List
	GetIterator() (Iterator, error)

	// Close cleans up the List in both memory and disk
	Close()
}

// Iterator defines methods to read from List
type Iterator interface {
	// Contain checks if the list contains the key
	Contain([]byte) bool
}

type sortedList struct {
	mu            sync.Mutex
	store         *llrb.LLRB
	maxCacheItems int

	chunkFiles []*os.File
}

type mergeIterator struct {
	pq       priorityQueue
	nextItem []byte
	hasItem  bool
	err      error
}

// NewList creates a new List
func NewList(maxCacheItems int) List {
	return &sortedList{
		store:         llrb.New(),
		maxCacheItems: maxCacheItems,
	}
}

type storeItem struct {
	entry []byte
}

func (i *storeItem) Less(other llrb.Item) bool {
	return bytes.Compare(i.entry, other.(*storeItem).entry) == -1
}

func (s *sortedList) Put(ctx context.Context, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.store.Len() > s.maxCacheItems {
		if err := s.flushLocked(); err != nil {
			return errors.Wrap(err, "error triggering chunk flush")
		}
	}

	s.store.ReplaceOrInsert(&storeItem{entry: data})

	return nil
}

func (s *sortedList) flushLocked() error {
	if s.store.Len() == 0 {
		return nil
	}

	f, err := tempfile.CreateAutoDelete()
	if err != nil {
		return errors.Wrap(err, "unable to create temp file")
	}
	defer f.Close() //nolint:errcheck

	w := bufio.NewWriter(f)
	lenBuf := make([]byte, 4)

	for s.store.Len() > 0 {
		item := s.store.DeleteMin()
		entry := item.(*storeItem).entry

		binary.BigEndian.PutUint32(lenBuf, uint32(len(entry)))
		if _, err := w.Write(lenBuf); err != nil {
			return err
		}
		if _, err := w.Write(entry); err != nil {
			return err
		}
	}

	if err := w.Flush(); err != nil {
		return err
	}

	if _, err := f.Seek(0, 0); err != nil {
		return err
	}

	s.chunkFiles = append(s.chunkFiles, f)

	return nil
}

func (s *sortedList) Finalize() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.flushLocked(); err != nil {
		return err
	}

	return nil
}

func (s *sortedList) GetIterator() (Iterator, error) {
	return newMergeIterator(s.chunkFiles)
}

func (s *sortedList) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, f := range s.chunkFiles {
		f.Close() //nolint:errcheck
	}
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

	top := it.pq[0]
	it.nextItem = append([]byte(nil), top.current...)
	it.hasItem = true

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

func (it *mergeIterator) Contains(key []byte) bool {
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

func (pq priorityQueue) Len() int {
	return len(pq)
}

func (pq priorityQueue) Less(i, j int) bool {
	return bytes.Compare(pq[i].current, pq[j].current) < 0
}

func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

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
