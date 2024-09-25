package index

import (
	"crypto/rand"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"runtime"
	"runtime/pprof"

	"github.com/google/uuid"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
	"github.com/petar/GoLLRB/llrb"
)

var minInfoCompact = &InfoCompact{}

func (ic *InfoCompact) Less(other llrb.Item) bool {
	if other.(*InfoCompact) == minInfoCompact {
		return false
	}
	return ic.ContentID.less(other.(*InfoCompact).ContentID)
}

type blobIDWrap struct {
	id *blob.ID
}

func (b blobIDWrap) Less(other llrb.Item) bool {
	return *b.id < *other.(blobIDWrap).id
}

type largeBuilder struct {
	indexStore  *llrb.LLRB
	packBlobIDs *llrb.LLRB
	iterating   bool
}

func NewLargeBuilder() Builder {
	return newLargeBuilder()
}

func newLargeBuilder() *largeBuilder {
	return &largeBuilder{
		indexStore:  llrb.New(),
		packBlobIDs: llrb.New(),
	}
}

// Large builder doesn't support Clone.
func (b *largeBuilder) Clone() Builder {
	panic("not supported")
}

// Add adds a new entry to the builder or conditionally replaces it if the timestamp is greater.
func (b *largeBuilder) Add(i Info) {
	cid := i.ContentID

	found := b.indexStore.Get(&InfoCompact{ContentID: cid})
	if found == nil || contentInfoGreaterThanStruct(&i, &Info{
		PackBlobID:       *found.(*InfoCompact).PackBlobID,
		TimestampSeconds: found.(*InfoCompact).TimestampSeconds,
		Deleted:          found.(*InfoCompact).Deleted,
	}) {
		id := new(blob.ID)
		if item := b.packBlobIDs.Get(blobIDWrap{&i.PackBlobID}); item == nil {
			*id = i.PackBlobID
			_ = b.packBlobIDs.ReplaceOrInsert(blobIDWrap{id})
		} else {
			id = item.(blobIDWrap).id
		}

		_ = b.indexStore.ReplaceOrInsert(&InfoCompact{
			PackBlobID:          id,
			ContentID:           cid,
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

func (b *largeBuilder) AddRaw(i BuilderItem) {
	ic := i.(*InfoCompact)

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

func (b *largeBuilder) Length() int {
	return b.indexStore.Len()
}

func (b *largeBuilder) Find(cid ID) (Info, bool) {
	found := b.indexStore.Get(&InfoCompact{ContentID: cid})
	if found == nil {
		return Info{}, false
	} else {
		return FlatenInfo(found.(*InfoCompact)), true
	}
}

func (b *largeBuilder) IterateRaw(callback func(cid ID, i BuilderItem)) {
	b.iterating = true
	b.indexStore.AscendGreaterOrEqual(minInfoCompact, func(v llrb.Item) bool {
		callback(v.(*InfoCompact).ContentID, v.(*InfoCompact))
		return true
	})
	b.iterating = false
}

func (b *largeBuilder) Iterate(callback func(cid ID, i Info)) {
	b.iterating = true
	b.indexStore.AscendGreaterOrEqual(minInfoCompact, func(v llrb.Item) bool {
		callback(v.(*InfoCompact).ContentID, FlatenInfo(v.(*InfoCompact)))
		return true
	})
	b.iterating = false
}

func (b *largeBuilder) Delete(cid ID) {
	if b.iterating {
		panic("modification during iteration is not supported")
	}

	b.indexStore.Delete(&InfoCompact{ContentID: cid})
}

func (b *largeBuilder) sortedContents() []BuilderItem {
	result := make([]BuilderItem, 0, b.Length())

	b.indexStore.AscendGreaterOrEqual(minInfoCompact, func(v llrb.Item) bool {
		result = append(result, v.(*InfoCompact))

		return true
	})

	return result
}

// Build writes the pack index to the provided output.
func (b *largeBuilder) Build(output io.Writer, version int) error {
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
func (b *largeBuilder) BuildStable(output io.Writer, version int) error {
	return buildSortedContents(b.sortedContents(), output, version)
}

func (b *largeBuilder) shard(maxShardSize int) [][]BuilderItem {
	numShards := (b.Length() + maxShardSize - 1) / maxShardSize
	if numShards <= 1 {
		if b.Length() == 0 {
			return [][]BuilderItem{}
		}

		return [][]BuilderItem{b.sortedContents()}
	}

	result := make([][]BuilderItem, numShards)

	b.indexStore.AscendGreaterOrEqual(minInfoCompact, func(v llrb.Item) bool {
		h := fnv.New32a()
		io.WriteString(h, v.(*InfoCompact).ContentID.String()) //nolint:errcheck

		shard := h.Sum32() % uint32(numShards)

		result[shard] = append(result[shard], v.(*InfoCompact))

		return true
	})

	var nonEmpty [][]BuilderItem

	for _, r := range result {
		if len(r) > 0 {
			nonEmpty = append(nonEmpty, r)
		}
	}

	return nonEmpty
}

// BuildShards builds the set of index shards ensuring no more than the provided number of contents are in each index.
// Returns shard bytes and function to clean up after the shards have been written.
func (b *largeBuilder) BuildShards(indexVersion int, stable bool, shardSize int) ([]gather.Bytes, func(), error) {
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

		if err := buildSortedContents(s, buf, indexVersion); err != nil {
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

	fmt.Printf("%v", b.Length())
	for _, s := range shardedBuilders {
		for _, it := range s {
			if it.GetPackBlobID() == "fake-abc" {
				fmt.Printf("keep sorted %v, %v", it.GetContentID(), it.GetPackBlobID())
			}
		}
	}

	return dataShards, closeShards, nil
}

func genProfile(n string) {
	if n == "" {
		n = uuid.NewString()
	}

	name := fmt.Sprintf("/tmp/profile-%s.pb.gz", n)
	f, _ := os.Create(name)
	defer f.Close()
	runtime.GC()
	pprof.WriteHeapProfile(f)
}
