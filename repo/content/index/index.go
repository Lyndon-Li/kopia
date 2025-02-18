// Package index manages content indices.
package index

import (
	"io"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/hashing"
)

const (
	maxContentIDSize = hashing.MaxHashSize + 1
	unknownKeySize   = 255
)

// Index is a read-only index of packed contents.
type Index interface {
	io.Closer
	ApproximateCount() int
	GetInfo(contentID ID, result *Info) (bool, error)

	// invoked the provided callback for all entries such that entry.ID >= startID and entry.ID < endID
	Iterate(r IDRange, cb func(Info) error) error
}

type Store interface {
	Load() error
	Get() []byte
	Release()
	Close() error
}

type directMemoryStore struct {
	data []byte
}

func (s *directMemoryStore) Load() error {
	return nil
}

func (s *directMemoryStore) Get() []byte {
	return s.data
}

func (s *directMemoryStore) Release() {
}

func (s *directMemoryStore) Close() error {
	return nil
}

// Open reads an Index from a given reader. The caller must call Close() when the index is no longer used.
func Open(data []byte, closer func() error, v1PerContentOverhead func() int) (Index, error) {
	h, err := v1ReadHeader(data)
	if err != nil {
		return nil, errors.Wrap(err, "invalid header")
	}

	switch h.version {
	case Version1:
		return openV1PackIndex(h, &directMemoryStore{data}, uint32(v1PerContentOverhead())) //nolint:gosec

	case Version2:
		return openV2PackIndex(&directMemoryStore{data})

	default:
		return nil, errors.Errorf("invalid header format: %v", h.version)
	}
}

// OpenWithStore opens an Index from a given store. The caller must call Close() when the index is no longer used.
func OpenWithStore(store Store, v1PerContentOverhead func() int) (Index, error) {
	err := store.Load()
	if err != nil {
		return nil, errors.Wrap(err, "error to allocate index data")
	}

	defer store.Release()

	data := store.Get()

	h, err := v1ReadHeader(data)
	if err != nil {
		return nil, errors.Wrap(err, "invalid header")
	}

	var idx Index
	switch h.version {
	case Version1:
		idx, err = openV1PackIndex(h, store, uint32(v1PerContentOverhead())) //nolint:gosec

	case Version2:
		idx, err = openV2PackIndex(store)

	default:
		err = errors.Errorf("invalid header format: %v", h.version)
	}

	return idx, err
}

func safeSlice(data []byte, offset int64, length int) (v []byte, err error) {
	defer func() {
		if recover() != nil {
			v = nil
			err = errors.Errorf("invalid slice offset=%v, length=%v, data=%v bytes", offset, length, len(data))
		}
	}()

	return data[offset : offset+int64(length)], nil
}

func safeSliceString(data []byte, offset int64, length int) (s string, err error) {
	defer func() {
		if recover() != nil {
			s = ""
			err = errors.Errorf("invalid slice offset=%v, length=%v, data=%v bytes", offset, length, len(data))
		}
	}()

	return string(data[offset : offset+int64(length)]), nil
}
