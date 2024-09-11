package index

import (
	"crypto/rand"
	"hash/fnv"
	"io"

	"github.com/pkg/errors"

	"github.com/google/btree"
	"github.com/kopia/kopia/internal/gather"
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

func (ic *InfoCompact) Less(other btree.Item) bool {
	return ic.ContentID.String() < other.(*InfoCompact).ContentID.String()
}

type blobIDWrap struct {
	id *blob.ID
}

func (b blobIDWrap) Less(other btree.Item) bool {
	return *b.id < *other.(blobIDWrap).id
}

type LargeBuilder struct {
	indexStore  *btree.BTree
	packBlobIDs *btree.BTree
}

func NewLargeBuilder() Builder {
	return newLargeBuilder()
}

func newLargeBuilder() *LargeBuilder {
	return &LargeBuilder{
		indexStore:  btree.New(2),
		packBlobIDs: btree.New(2),
	}
}

// Clone returns a deep Clone of the Builder.
func (b *LargeBuilder) Clone() Builder {
	if b == nil {
		return nil
	}

	r := newLargeBuilder()
	r.indexStore = b.indexStore.Clone()
	r.packBlobIDs = b.packBlobIDs.Clone()

	return r
}

// Add adds a new entry to the builder or conditionally replaces it if the timestamp is greater.
func (b *LargeBuilder) Add(i Info) {
	cid := i.ContentID

	found := b.indexStore.Get(&InfoCompact{ContentID: &cid})
	if found == nil || contentInfoGreaterThanStruct(&i, &Info{
		PackBlobID:       *found.(*InfoCompact).PackBlobID,
		TimestampSeconds: found.(*InfoCompact).TimestampSeconds,
		Deleted:          found.(*InfoCompact).Deleted,
	}) {
		var id btree.Item
		if id = b.packBlobIDs.Get(blobIDWrap{&i.PackBlobID}); id == nil {
			packBlobID := i.PackBlobID
			id = b.packBlobIDs.ReplaceOrInsert(blobIDWrap{&packBlobID})
		}

		_ = b.indexStore.ReplaceOrInsert(&InfoCompact{
			PackBlobID:          id.(blobIDWrap).id,
			ContentID:           &cid,
			TimestampSeconds:    i.TimestampSeconds,
			OriginalLength:      i.OriginalLength,
			PackedLength:        i.PackedLength,
			PackOffset:          i.PackOffset,
			CompressionHeaderID: i.CompressionHeaderID,
			Deleted:             i.Deleted,
			FormatVersion:       i.FormatVersion,
			EncryptionKeyID:     i.EncryptionKeyID,
		})
	}
}

func (b *LargeBuilder) AddCompacted(ic *InfoCompact) {
	found := b.indexStore.Get(&InfoCompact{ContentID: ic.ContentID})
	if found == nil || contentInfoGreaterThanStruct(&Info{
		PackBlobID:       *ic.PackBlobID,
		TimestampSeconds: ic.TimestampSeconds,
		Deleted:          ic.Deleted,
	}, &Info{
		PackBlobID:       *found.(*InfoCompact).PackBlobID,
		TimestampSeconds: found.(*InfoCompact).TimestampSeconds,
		Deleted:          found.(*InfoCompact).Deleted,
	}) {
		_ = b.indexStore.ReplaceOrInsert(ic)
	}
}

func (b *LargeBuilder) Length() int {
	return b.indexStore.Len()
}

func (b *LargeBuilder) Find(cid ID) (Info, bool) {
	found := b.indexStore.Get(&InfoCompact{ContentID: &cid})
	if found == nil {
		return Info{}, false
	} else {
		return FlatenInfo(found.(*InfoCompact)), true
	}
}

func (b *LargeBuilder) Iterate(callback func(cid ID, i Info)) {
	b.indexStore.Ascend(func(v btree.Item) bool {
		callback(*v.(*InfoCompact).ContentID, FlatenInfo(v.(*InfoCompact)))
		return true
	})
}

func (b *LargeBuilder) IterateCompacted(callback func(cid *ID, i *InfoCompact)) {
	b.indexStore.Ascend(func(v btree.Item) bool {
		callback(v.(*InfoCompact).ContentID, v.(*InfoCompact))
		return true
	})
}

func (b *LargeBuilder) Delete(cid *ID) {
	b.indexStore.Delete(&InfoCompact{ContentID: cid})
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
	b.indexStore.Ascend(func(v btree.Item) bool {
		first := int(base36Value[v.(*InfoCompact).ContentID.prefix])
		second := int(v.(*InfoCompact).ContentID.data[0] >> 4) //nolint:gomnd

		// first: 0..35, second: 0..15
		buck := first<<4 + second //nolint:gomnd

		buckets[buck] = append(buckets[buck], v.(*InfoCompact))

		return true
	})

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
		result[i] = newLargeBuilder()
	}

	b.indexStore.Ascend(func(v btree.Item) bool {
		h := fnv.New32a()
		io.WriteString(h, v.(*InfoCompact).ContentID.String()) //nolint:errcheck

		shard := h.Sum32() % uint32(numShards)

		result[shard].indexStore.ReplaceOrInsert(v)

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
