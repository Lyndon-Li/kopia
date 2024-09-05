package index

import (
	"crypto/rand"
	"hash/fnv"
	"io"
	"math"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/compression"
	"github.com/kopia/kopia/repo/logging"
	"github.com/pkg/errors"
)

type LargeBuilder struct {
	storeIdx      map[uint32]int
	packBlobIDIdx map[uint32]uint32
	packBlobIDs   []blob.ID
	store         []*infoCompact
}

type infoCompact struct {
	packBlobIDIndex     uint32
	originalLength      uint32
	packedLength        uint32
	packOffset          uint32
	timeStamp           int64
	compressionHeaderID compression.HeaderID
	contentID           ID
	deleted             bool
	formatVersion       byte
	encryptionKeyID     byte
}

var log = logging.Module("idxbuilder")

func NewLargeBuilder() *LargeBuilder {
	return &LargeBuilder{
		storeIdx:      make(map[uint32]int),
		packBlobIDIdx: make(map[uint32]uint32),
	}
}

// Add adds a new entry to the builder or conditionally replaces it if the timestamp is greater.
func (b *LargeBuilder) Add(i *Info) error {
	packBlobIDIdx, err := b.storePackBlobID(i.PackBlobID)
	if err != nil {
		return err
	}

	storeID, err := b.getValidStoreID(i.ContentID)
	if err != nil {
		return err
	}

	storeIdx, found := b.storeIdx[storeID]
	if !found || contentInfoGreaterThanStruct(i, &Info{
		TimestampSeconds: b.store[storeIdx].timeStamp,
		Deleted:          b.store[storeIdx].deleted,
		PackBlobID:       b.packBlobIDs[b.store[storeIdx].packBlobIDIndex],
	}) {
		idx := len(b.store)
		b.store = append(b.store, &infoCompact{
			contentID:           i.ContentID,
			packBlobIDIndex:     packBlobIDIdx,
			originalLength:      i.OriginalLength,
			packedLength:        i.PackedLength,
			packOffset:          i.PackOffset,
			timeStamp:           i.TimestampSeconds,
			compressionHeaderID: i.CompressionHeaderID,
			deleted:             i.Deleted,
			formatVersion:       i.FormatVersion,
			encryptionKeyID:     i.EncryptionKeyID,
		})

		b.storeIdx[storeID] = idx
	}

	return nil
}

func (b *LargeBuilder) storePackBlobID(id blob.ID) (uint32, error) {
	for {
		h := fnv.New32a()
		io.WriteString(h, string(id)) //nolint:errcheck
		sum := h.Sum32()

		if idx, found := b.packBlobIDIdx[sum]; found {
			if uint32(len(b.packBlobIDs)) <= idx {
				return math.MaxUint32, errors.New("packBlobIDs overflow")
			}

			if b.packBlobIDs[idx] == id {
				return idx, nil
			}
		} else {
			idx = uint32(len(b.packBlobIDs))
			b.packBlobIDs = append(b.packBlobIDs, id)
			b.packBlobIDIdx[sum] = idx
			return idx, nil
		}
	}
}

func (b *LargeBuilder) getValidStoreID(id ID) (uint32, error) {
	for {
		h := fnv.New32a()
		io.WriteString(h, id.String()) //nolint:errcheck
		sum := h.Sum32()

		if idx, found := b.storeIdx[sum]; found {
			if len(b.store) <= idx {
				return math.MaxUint32, errors.New("store overflow")
			}

			if b.store[idx].contentID == id {
				return sum, nil
			}
		} else {
			return sum, nil
		}
	}
}

func (b *LargeBuilder) shard(maxShardSize int) [][]*infoCompact {
	if len(b.store) == 0 {
		return [][]*infoCompact{}
	}

	numShards := (len(b.store) + maxShardSize - 1) / maxShardSize
	result := make([][]*infoCompact, numShards)

	if numShards <= 1 {
		for _, v := range b.store {
			result[0] = append(result[0], v)
		}
	} else {
		for _, v := range b.store {
			h := fnv.New32a()
			io.WriteString(h, v.contentID.String()) //nolint:errcheck

			shard := h.Sum32() % uint32(numShards)

			result[shard] = append(result[shard], v)
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
		builder, err := b.makeBuilder(s)
		if err != nil {
			closeShards()

			return nil, nil, errors.Wrap(err, "error making builder")
		}

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

func (b *LargeBuilder) makeBuilder(shard []*infoCompact) (Builder, error) {
	length := len(shard)
	builder := make(map[ID]Info, length)
	for i := 0; i < length; i++ {
		packBlobID, err := b.getPackBlobID(shard[i].packBlobIDIndex)
		if err != nil {
			return Builder{}, err
		}

		builder[shard[i].contentID] = Info{
			PackBlobID:          packBlobID,
			TimestampSeconds:    shard[i].timeStamp,
			OriginalLength:      shard[i].originalLength,
			PackedLength:        shard[i].packedLength,
			PackOffset:          shard[i].packOffset,
			CompressionHeaderID: shard[i].compressionHeaderID,
			ContentID:           shard[i].contentID,
			Deleted:             shard[i].deleted,
			FormatVersion:       shard[i].formatVersion,
			EncryptionKeyID:     shard[i].encryptionKeyID,
		}
	}

	return builder, nil
}

func (b *LargeBuilder) getPackBlobID(idxKey uint32) (blob.ID, error) {
	if idx, found := b.packBlobIDIdx[idxKey]; !found {
		return blob.ID(""), errors.New("not found")
	} else {
		if uint32(len(b.packBlobIDs)) <= idx {
			return blob.ID(""), errors.New("packBlobIDs overflow")
		}

		return b.packBlobIDs[idx], nil
	}
}
