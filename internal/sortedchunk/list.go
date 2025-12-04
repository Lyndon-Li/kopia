package sortedchunk

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/petar/GoLLRB/llrb"
	"github.com/pkg/errors"
	"golang.org/x/sync/semaphore"
)

// List is a sorted and deduplicated list based on sorted chunks
type List interface {
	// Append appends an element to List
	Append([]byte) error

	// Finalize completely sorts and flushes the elements to disk
	Finalize() error

	// GetIterator returns an Iterator to read from List
	GetIterator() Iterator

	// Close cleans up the List in both memory and disk
	Close()
}

// Iterator defines methods to read from List
type Iterator interface {
	// Next returns the next list element
	Next() ([]byte, error)
}

type iterator struct {
	ctx          context.Context
	dir          string
	entrySize    int
	chunkFiles   []string
	totalEntries int

	currentChunkIndex int
	currentEntryIndex int
	currentData       []byte
	currentPos        int
}

type flushResult struct {
	flushErr   error
	chunkFiles []string
}

type mergeResult struct {
	chunkFiles   []string
	totalEntries int
}

type listWriter struct {
	ctx       context.Context
	dir       string
	entrySize int
	cacheSize int
	chunkSize int

	writerLock      sync.Mutex
	flushSem        *semaphore.Weighted
	flushWg         sync.WaitGroup
	flushResultLock sync.Mutex

	flushResult flushResult
	mergeResult mergeResult

	flushChunkIndex int
	mergeChunkIndex int

	store *llrb.LLRB
}

// NewList creates an instance of List
func NewList(ctx context.Context, dir string, entrySize, cacheSize, chunkSize int) List {
	return &listWriter{
		ctx:       ctx,
		dir:       dir,
		entrySize: entrySize,
		cacheSize: cacheSize,
		chunkSize: chunkSize,
		store:     llrb.New(),
		flushSem:  semaphore.NewWeighted(1),
	}
}

func (r *iterator) Next() ([]byte, error) {
	for {
		if r.currentPos < len(r.currentData) {
			if r.currentPos+r.entrySize > len(r.currentData) {
				return nil, errors.Errorf("unexpected EOF of chunk data %v", len(r.currentData))
			}

			entry := r.currentData[r.currentPos : r.currentPos+r.entrySize]
			r.currentPos += r.entrySize

			return entry, nil
		}

		r.currentData = nil

		if r.currentEntryIndex == r.totalEntries {
			return nil, nil
		}

		if r.currentChunkIndex == len(r.chunkFiles) {
			return nil, errors.New("no more chunks")
		}

		data, err := os.ReadFile(r.chunkFiles[r.currentChunkIndex])
		if err != nil {
			return nil, errors.Wrapf(err, "error reading chunk data from %s", r.chunkFiles[r.currentChunkIndex])
		}

		r.currentData = data
		r.currentPos = 0
		r.currentChunkIndex++
	}
}

type storeItem struct {
	entry []byte
}

func (i *storeItem) Less(other llrb.Item) bool {
	return bytes.Compare(i.entry, other.(*storeItem).entry) == -1
}

func (w *listWriter) Append(data []byte) error {
	w.writerLock.Lock()
	defer w.writerLock.Unlock()

	if err := w.checkFlushError(); err != nil {
		return err
	}

	if w.store.Len()*w.entrySize > w.cacheSize {
		if err := w.flush(); err != nil {
			return errors.Wrap(err, "error triggering chunk flush")
		}
	}

	w.store.ReplaceOrInsert(&storeItem{entry: data})

	return nil
}

func (w *listWriter) GetIterator() Iterator {
	return &iterator{
		ctx:          w.ctx,
		dir:          w.dir,
		entrySize:    w.entrySize,
		chunkFiles:   w.mergeResult.chunkFiles,
		totalEntries: w.mergeResult.totalEntries,
	}
}

func (w *listWriter) Close() {
	w.flushWg.Wait()

	for _, path := range w.flushResult.chunkFiles {
		if path != "" {
			os.Remove(path)
		}
	}

	for _, path := range w.mergeResult.chunkFiles {
		if path != "" {
			os.Remove(path)
		}
	}
}

func (w *listWriter) flush() error {
	if w.store.Len() == 0 {
		return nil
	}

	if err := w.flushSem.Acquire(w.ctx, 1); err != nil {
		return errors.Wrap(err, "error accquiring flush semaphore")
	}

	buffer := make([]byte, w.store.Len()*w.entrySize)
	for w.store.Len() > 0 {
		item := w.store.DeleteMin()
		buffer = append(buffer, item.(*storeItem).entry...)
	}

	w.flushWg.Add(1)

	go func(buf []byte, index int) {
		defer func() {
			w.flushSem.Release(1)
			w.flushWg.Done()
		}()

		var flushErr error
		var flushChunk string
		for {
			chunkPath := filepath.Join(w.dir, fmt.Sprintf("chunk_%03d.bin", index))
			chunkFile, err := os.Create(chunkPath)
			if err != nil {
				flushErr = errors.Wrapf(err, "error creating chunk file %s", chunkPath)
				break
			}
			defer chunkFile.Close()

			if _, err := chunkFile.Write(buf); err != nil {
				flushErr = errors.Wrapf(err, "error writing chunk file %s", chunkPath)
				break
			}

			flushChunk = chunkPath
		}

		w.flushResultLock.Lock()
		defer w.flushResultLock.Unlock()

		if flushErr != nil {
			w.flushResult.flushErr = flushErr
		} else {
			w.flushResult.chunkFiles = append(w.flushResult.chunkFiles, flushChunk)
		}
	}(buffer, w.flushChunkIndex)

	w.flushChunkIndex++

	return nil
}

func (w *listWriter) checkFlushError() error {
	w.flushResultLock.Lock()
	defer w.flushResultLock.Unlock()

	return errors.Wrap(w.flushResult.flushErr, "error flushing chunk")
}

type mergeInput struct {
	path   string
	file   *os.File
	reader *bufio.Reader
	buffer []byte
	eof    bool
}

func (w *listWriter) getMergeInputs() ([]*mergeInput, error) {
	w.flushWg.Wait()

	w.flushResultLock.Lock()
	defer w.flushResultLock.Unlock()

	if w.flushResult.flushErr != nil {
		return nil, errors.Wrap(w.flushResult.flushErr, "error flushing chunk files")
	}

	if len(w.flushResult.chunkFiles) == 0 {
		return nil, errors.New("No chunk file to merge")
	}

	inputs := make([]*mergeInput, len(w.flushResult.chunkFiles))
	for i, chunkPath := range w.flushResult.chunkFiles {
		f, err := os.Open(chunkPath)
		if err != nil {
			return nil, errors.Wrapf(err, "error opening chunk %s", chunkPath)
		}

		input := &mergeInput{
			path:   chunkPath,
			file:   f,
			reader: bufio.NewReaderSize(f, 1<<20),
			buffer: make([]byte, w.entrySize),
		}

		read, err := io.ReadFull(input.reader, input.buffer)
		if err != nil {
			return nil, errors.Wrapf(err, "error reading chunk %s", chunkPath)
		}

		if read != w.entrySize {
			return nil, errors.Errorf("invalid data size %v reading from chunk %s", read, chunkPath)
		}

		inputs[i] = input
	}

	return inputs, nil
}

func (w *listWriter) openNextMergeChunk(curWriter *bufio.Writer, curFile *os.File) (*bufio.Writer, *os.File, error) {
	if err := completeMergeChunk(curWriter, curFile); err != nil {
		return nil, nil, err
	}

	chunkPath := filepath.Join(w.dir, fmt.Sprintf("chunk_%03d.bin", w.mergeChunkIndex))
	f, err := os.Create(chunkPath)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "error creating output chunk %s", chunkPath)
	}

	w.mergeChunkIndex++
	w.mergeResult.chunkFiles = append(w.mergeResult.chunkFiles, chunkPath)

	return bufio.NewWriterSize(f, 1<<20), f, nil
}

func completeMergeChunk(curWriter *bufio.Writer, curFile *os.File) error {
	if curWriter != nil {
		err := curWriter.Flush()
		curFile.Close()

		if err != nil {
			return errors.Wrapf(err, "error flushing merge file %s", curFile.Name())
		}
	}

	return nil
}

func completeMergeInput(input *mergeInput) {
	if input == nil {
		return
	}

	if input.file != nil {
		input.file.Close()
	}

	if input.path != "" {
		os.Remove(input.path)
	}

	input.path = ""
	input.reader = nil
	input.eof = true
}

func (w *listWriter) Finalize() error {
	inputs, err := w.getMergeInputs()
	defer func() {
		for _, input := range inputs {
			completeMergeInput(input)
		}
	}()

	if err != nil {
		return errors.Wrap(err, "error get merge inputs")
	}

	var (
		curFile   *os.File
		curWriter *bufio.Writer
		curSize   int64
		prevEntry []byte
		minEntry  []byte
		minIdx    = -1
	)

	curWriter, curFile, err = w.openNextMergeChunk(curWriter, curFile)
	if err != nil {
		return errors.Wrap(err, "error opening merge chunk")
	}

	defer func() {
		completeMergeChunk(curWriter, curFile)
	}()

	for {
		for i, input := range inputs {
			if input.eof {
				continue
			}

			if minIdx == -1 || bytes.Compare(input.buffer, minEntry) < 0 {
				minEntry = input.buffer
				minIdx = i
			}
		}

		if minIdx == -1 {
			break
		}

		if prevEntry == nil || !bytes.Equal(prevEntry, minEntry) {
			if _, err := curWriter.Write(minEntry); err != nil {
				return errors.Wrap(err, "error writing to output chunk")
			}

			curSize += int64(w.entrySize)
			w.mergeResult.totalEntries++

			prevEntry = append([]byte(nil), minEntry...)

			if curSize >= int64(w.chunkSize) {
				curWriter, curFile, err = w.openNextMergeChunk(curWriter, curFile)
				if err != nil {
					return errors.Wrap(err, "error opening merge chunk")
				}
			}
		}

		input := inputs[minIdx]
		if read, err := io.ReadFull(input.reader, input.buffer); err == io.EOF {
			completeMergeInput(input)
		} else if err != nil {
			return errors.Wrapf(err, "error reading input chunk file %s", input.path)
		} else if read != w.entrySize {
			return errors.Errorf("invalid data size %v reading from chunk %s", read, input.path)
		}
	}

	return nil
}
