package index

import (
	"crypto/rand"
	"hash/fnv"
	"io"
	"runtime"
	"sort"
	"sync"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/gather"
)

const randomSuffixSize = 32 // number of random bytes to append at the end to make the index blob unique

// Builder prepares and writes content index.
type Builder interface {
	Add(Info)
	AddRaw(BuilderItem)
	Clone() Builder
	Length() int
	Build(io.Writer, int) error
	BuildStable(io.Writer, int) error
	BuildShards(int, bool, int) ([]gather.Bytes, func(), error)
	Find(ID) (Info, bool)
	Iterate(func(ID, Info))
	IterateRaw(func(ID, BuilderItem))
	Delete(ID)
	SortedContents() []BuilderItem
}

type normalBuilder struct {
	store map[ID]Info
}

func NewNormalBuilder() Builder {
	return newNormalBuilder()
}

func newNormalBuilder() *normalBuilder {
	return &normalBuilder{
		store: make(map[ID]Info),
	}
}

// Clone returns a deep Clone of the Builder.
func (b *normalBuilder) Clone() Builder {
	if b == nil {
		return nil
	}

	r := newNormalBuilder()

	for k, v := range b.store {
		r.store[k] = v
	}

	return r
}

// Add adds a new entry to the builder or conditionally replaces it if the timestamp is greater.
func (b *normalBuilder) Add(i Info) {
	cid := i.GetContentID()

	old, found := b.store[cid]
	if !found || contentInfoGreaterThanStruct(i, old) {
		b.store[cid] = i
	}
}

func (b *normalBuilder) AddRaw(i BuilderItem) {
	cid := i.GetContentID()

	old, found := b.store[cid]
	if !found || contentInfoGreaterThanStruct(i, old) {
		b.store[cid] = i.(Info)
	}
}

func (b *normalBuilder) Length() int {
	return len(b.store)
}

func (b *normalBuilder) Find(cid ID) (Info, bool) {
	i, found := b.store[cid]
	return i, found
}

func (b *normalBuilder) IterateRaw(callback func(cid ID, i BuilderItem)) {
	for k, v := range b.store {
		callback(k, v)
	}
}

func (b *normalBuilder) Iterate(callback func(cid ID, i Info)) {
	for k, v := range b.store {
		callback(k, v)
	}
}

func (b *normalBuilder) Delete(cid ID) {
	delete(b.store, cid)
}

// base36Value stores a base-36 reverse lookup such that ASCII character corresponds to its
// base-36 value ('0'=0..'9'=9, 'a'=10, 'b'=11, .., 'z'=35).
//
//nolint:gochecknoglobals
var base36Value [256]byte

func init() {
	for i := range 10 {
		base36Value['0'+i] = byte(i)
	}

	for i := range 26 {
		base36Value['a'+i] = byte(i + 10) //nolint:gomnd
		base36Value['A'+i] = byte(i + 10) //nolint:gomnd
	}
}

// sortedContents returns the list of []BuilderItem sorted lexicographically using bucket sort
// sorting is optimized based on the format of content IDs (optional single-character
// alphanumeric prefix (0-9a-z), followed by hexadecimal digits (0-9a-f).
func (b *normalBuilder) SortedContents() []BuilderItem {
	var buckets [36 * 16][]BuilderItem

	// phase 1 - bucketize into 576 (36 *16) separate lists
	// by first [0-9a-z] and second character [0-9a-f].
	for cid, v := range b.store {
		first := int(base36Value[cid.prefix])
		second := int(cid.data[0] >> 4) //nolint:gomnd

		// first: 0..35, second: 0..15
		buck := first<<4 + second //nolint:gomnd

		buckets[buck] = append(buckets[buck], v)
	}

	// phase 2 - sort each non-empty bucket in parallel using goroutines
	// this is much faster than sorting one giant list.
	var wg sync.WaitGroup

	numWorkers := runtime.NumCPU()
	for worker := range numWorkers {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for i := range buckets {
				if i%numWorkers == worker {
					buck := buckets[i]

					sort.Slice(buck, func(i, j int) bool {
						return buck[i].GetContentID().less(buck[j].GetContentID())
					})
				}
			}
		}()
	}

	wg.Wait()

	// Phase 3 - merge results from all buckets.
	result := make([]BuilderItem, 0, b.Length())

	for i := range len(buckets) {
		result = append(result, buckets[i]...)
	}

	return result
}

// Build writes the pack index to the provided output.
func (b *normalBuilder) Build(output io.Writer, version int) error {
	if err := b.BuildStable(output, version); err != nil {
		return err
	}

	randomSuffix := make([]byte, randomSuffixSize)

	if _, err := rand.Read(randomSuffix); err != nil {
		return errors.Wrap(err, "error getting random bytes for suffix")
	}

	if _, err := output.Write(randomSuffix); err != nil {
		return errors.Wrap(err, "error writing extra random suffix to ensure indexes are always globally unique")
	}

	return nil
}

// BuildStable writes the pack index to the provided output.
func (b *normalBuilder) BuildStable(output io.Writer, version int) error {
	switch version {
	case Version1:
		return buildV1(b, output)

	case Version2:
		return buildV2(b, output)

	default:
		return errors.Errorf("unsupported index version: %v", version)
	}
}

func (b *normalBuilder) shard(maxShardSize int) []Builder {
	numShards := (b.Length() + maxShardSize - 1) / maxShardSize
	if numShards <= 1 {
		if b.Length() == 0 {
			return []Builder{}
		}

		return []Builder{b}
	}

	result := make([]Builder, numShards)
	for i := range result {
		result[i] = newNormalBuilder()
	}

	for k, v := range b.store {
		h := fnv.New32a()
		io.WriteString(h, k.String()) //nolint:errcheck

		shard := h.Sum32() % uint32(numShards)

		nb := result[shard]
		nb.(*normalBuilder).store[k] = v
	}

	var nonEmpty []Builder

	for _, r := range result {
		if r.Length() > 0 {
			nonEmpty = append(nonEmpty, r)
		}
	}

	return nonEmpty
}

// BuildShards builds the set of index shards ensuring no more than the provided number of contents are in each index.
// Returns shard bytes and function to clean up after the shards have been written.
func (b *normalBuilder) BuildShards(indexVersion int, stable bool, shardSize int) ([]gather.Bytes, func(), error) {
	if shardSize == 0 {
		return nil, nil, errors.Errorf("invalid shard size")
	}

	var (
		shardedBuilders = b.shard(shardSize)
		dataShardsBuf   []*gather.WriteBuffer
		dataShards      []gather.Bytes
		randomSuffix    [32]byte
	)

	closeShards := func() {
		for _, ds := range dataShardsBuf {
			ds.Close()
		}
	}

	for _, s := range shardedBuilders {
		buf := gather.NewWriteBuffer()

		dataShardsBuf = append(dataShardsBuf, buf)

		if err := s.BuildStable(buf, indexVersion); err != nil {
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
