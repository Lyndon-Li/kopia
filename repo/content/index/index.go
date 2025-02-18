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

type Allocator interface {
	Allocate() (BufferCloser, error)
}

type BufferCloser interface {
	Get() []byte
	Close() error
}

// Open reads an Index from a given reader. The caller must call Close() when the index is no longer used.
func Open(data []byte, closer func() error, v1PerContentOverhead func() int) (Index, error) {
	h, err := v1ReadHeader(data)
	if err != nil {
		return nil, errors.Wrap(err, "invalid header")
	}

	switch h.version {
	case Version1:
		return openV1PackIndex(h, data, nil, uint32(v1PerContentOverhead())) //nolint:gosec

	case Version2:
		return openV2PackIndex(data, nil)

	default:
		return nil, errors.Errorf("invalid header format: %v", h.version)
	}
}

// Open reads an Index from a given allocator. The caller must call Close() when the index is no longer used.
func OpenWithAllocator(allocator Allocator, v1PerContentOverhead func() int) (Index, error) {
	data, err := allocator.Allocate()
	if err != nil {
		return nil, errors.Wrap(err, "error to allocate index data")
	}

	h, err := v1ReadHeader(data.Get())
	if err != nil {
		return nil, errors.Wrap(err, "invalid header")
	}

	var idx Index
	switch h.version {
	case Version1:
		idx, err = openV1PackIndex(h, data.Get(), allocator, uint32(v1PerContentOverhead())) //nolint:gosec

	case Version2:
		idx, err = openV2PackIndex(data.Get(), allocator)

	default:
		err = errors.Errorf("invalid header format: %v", h.version)
	}

	data.Close()

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
