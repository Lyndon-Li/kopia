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
	"github.com/kopia/kopia/internal/searchtree"
	"github.com/kopia/kopia/repo/blob"
)

const randomSuffixSize = 32 // number of random bytes to append at the end to make the index blob unique

// Builder prepares and writes content index.
type Builder interface {
	Add(Info)
	AddCompacted(*InfoCompact)
	Clone() Builder
	Length() int
	Build(io.Writer, int) error
	BuildStable(io.Writer, int) error
	BuildShards(int, bool, int) ([]gather.Bytes, func(), error)
	Find(ID) (Info, bool)
	Iterate(func(ID, Info))
	IterateCompacted(func(*ID, *InfoCompact))
	Delete(*ID)
}

type LargeBuilder struct {
	indexStore  *searchtree.AddressableMap[*ID, *InfoCompact]
	packBlobIDs *searchtree.AddressableSet[*blob.ID]
}

func NewLargeBuilder() Builder {
	return newLargeBuilder()
}

func newLargeBuilder() *LargeBuilder {
	return &LargeBuilder{
		indexStore:  searchtree.NewAddressableMap[*ID, *InfoCompact](lessID, equalID),
		packBlobIDs: searchtree.NewAddressableSet[*blob.ID](lessPackBlobID, equalPackBlobID),
	}
}

func lessPackBlobID(a *blob.ID, b *blob.ID) bool {
	return *a < *b
}

func equalPackBlobID(a *blob.ID, b *blob.ID) bool {
	return *a == *b
}

func lessID(a *ID, b *ID) bool {
	return a.String() < b.String()
}

func equalID(a *ID, b *ID) bool {
	return a.String() == b.String()
}

// Clone returns a deep Clone of the Builder.
func (b *LargeBuilder) Clone() Builder {
	if b == nil {
		return nil
	}

	r := &LargeBuilder{
		indexStore:  searchtree.NewAddressableMap[*ID, *InfoCompact](lessID, equalID),
		packBlobIDs: searchtree.NewAddressableSet[*blob.ID](lessPackBlobID, equalPackBlobID),
	}

	r.indexStore.Clone(b.indexStore)
	r.packBlobIDs.Clone(b.packBlobIDs)

	return r
}

// Add adds a new entry to the builder or conditionally replaces it if the timestamp is greater.
func (b *LargeBuilder) Add(i Info) {
	cid := i.ContentID

	found, old := b.indexStore.Search(&cid)
	if !found || contentInfoGreaterThanStruct(&i, &Info{
		PackBlobID:       *old.PackBlobID,
		TimestampSeconds: old.TimestampSeconds,
		Deleted:          old.Deleted,
	}) {
		packBlobID := i.PackBlobID
		b.packBlobIDs.Insert(&packBlobID)

		b.indexStore.Insert(&cid, CompactInfo(i))
	}
}

func (b *LargeBuilder) AddCompacted(ic *InfoCompact) {
	found, old := b.indexStore.Search(ic.ContentID)
	if !found || contentInfoGreaterThanStruct(&Info{
		PackBlobID:       *ic.PackBlobID,
		TimestampSeconds: ic.TimestampSeconds,
		Deleted:          ic.Deleted,
	}, &Info{
		PackBlobID:       *old.PackBlobID,
		TimestampSeconds: old.TimestampSeconds,
		Deleted:          old.Deleted,
	}) {
		b.indexStore.Insert(ic.ContentID, ic)
	}
}

func (b *LargeBuilder) Length() int {
	return b.indexStore.Length()
}

func (b *LargeBuilder) Find(cid ID) (Info, bool) {
	found, infoCompact := b.indexStore.Search(&cid)
	return FlatenInfo(infoCompact), found
}

func (b *LargeBuilder) Iterate(func(cid ID, i Info)) {

}

func (b *LargeBuilder) IterateCompacted(func(cid *ID, i *InfoCompact)) {

}

func (b *LargeBuilder) Delete(cid *ID) {

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

// sortedContents returns the list of []Info sorted lexicographically using bucket sort
// sorting is optimized based on the format of content IDs (optional single-character
// alphanumeric prefix (0-9a-z), followed by hexadecimal digits (0-9a-f).
func (b *LargeBuilder) sortedContents() []*InfoCompact {
	var buckets [36 * 16][]*InfoCompact

	// phase 1 - bucketize into 576 (36 *16) separate lists
	// by first [0-9a-z] and second character [0-9a-f].
	b.indexStore.Interate(func(cid *ID, value *InfoCompact) bool {
		first := int(base36Value[cid.prefix])
		second := int(cid.data[0] >> 4) //nolint:gomnd

		// first: 0..35, second: 0..15
		buck := first<<4 + second //nolint:gomnd

		buckets[buck] = append(buckets[buck], value)

		return false
	})

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
						return buck[i].ContentID.less(*buck[j].ContentID)
					})
				}
			}
		}()
	}

	wg.Wait()

	// Phase 3 - merge results from all buckets.
	result := make([]*InfoCompact, 0, b.Length())

	for i := range len(buckets) {
		result = append(result, buckets[i]...)
	}

	return result
}

// Build writes the pack index to the provided output.
func (b *LargeBuilder) Build(output io.Writer, version int) error {
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
func (b *LargeBuilder) BuildStable(output io.Writer, version int) error {
	switch version {
	case Version1:
		return b.buildV1(output)

	case Version2:
		return b.buildV2(output)

	default:
		return errors.Errorf("unsupported index version: %v", version)
	}
}

func (b *LargeBuilder) shard(maxShardSize int) []*LargeBuilder {
	numShards := (b.Length() + maxShardSize - 1) / maxShardSize
	if numShards <= 1 {
		if b.Length() == 0 {
			return []*LargeBuilder{}
		}

		return []*LargeBuilder{b}
	}

	result := make([]*LargeBuilder, numShards)
	for i := range result {
		result[i] = &LargeBuilder{
			indexStore:  searchtree.NewAddressableMap[*ID, *InfoCompact](lessID, equalID),
			packBlobIDs: searchtree.NewAddressableSet[*blob.ID](lessPackBlobID, equalPackBlobID),
		}
	}

	b.indexStore.Interate(func(k *ID, v *InfoCompact) bool {
		h := fnv.New32a()
		io.WriteString(h, k.String()) //nolint:errcheck

		shard := h.Sum32() % uint32(numShards)

		result[shard].indexStore.Insert(k, v)

		return true
	})
	var nonEmpty []*LargeBuilder

	for _, r := range result {
		if r.Length() > 0 {
			nonEmpty = append(nonEmpty, r)
		}
	}

	return nonEmpty
}

// BuildShards builds the set of index shards ensuring no more than the provided number of contents are in each index.
// Returns shard bytes and function to clean up after the shards have been written.
func (b *LargeBuilder) BuildShards(indexVersion int, stable bool, shardSize int) ([]gather.Bytes, func(), error) {
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
