package index

import (
	"time"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/compression"
)

// Info is an implementation of Info based on a structure.
type Info struct {
	PackBlobID          blob.ID              `json:"packFile,omitempty"`
	ContentID           ID                   `json:"contentID"`
	TimestampSeconds    int64                `json:"time"`
	OriginalLength      uint32               `json:"originalLength"`
	PackedLength        uint32               `json:"length"`
	PackOffset          uint32               `json:"packOffset,omitempty"`
	CompressionHeaderID compression.HeaderID `json:"compression,omitempty"`
	Deleted             bool                 `json:"deleted"`
	FormatVersion       byte                 `json:"formatVersion"`
	EncryptionKeyID     byte                 `json:"encryptionKeyID,omitempty"`
}

type InfoCompact struct {
	PackBlobID          *blob.ID
	ContentID           *ID
	TimestampSeconds    int64
	OriginalLength      uint32
	PackedLength        uint32
	PackOffset          uint32
	CompressionHeaderID compression.HeaderID
	Deleted             bool
	FormatVersion       byte
	EncryptionKeyID     byte
}

type BuilderItem interface {
	GetPackBlobID() blob.ID
	GetPPackBlobID() *blob.ID
	GetContentID() ID
	GetTimestampSeconds() int64
	GetOriginalLength() uint32
	GetPackedLength() uint32
	GetPackOffset() uint32
	GetCompressionHeaderID() compression.HeaderID
	IsDeleted() bool
	GetFormatVersion() byte
	GetEncryptionKeyID() byte
	Timestamp() time.Time
}

// Timestamp implements the Info interface.
func (i Info) Timestamp() time.Time {
	return time.Unix(i.TimestampSeconds, 0)
}

func (i Info) GetPackBlobID() blob.ID {
	return i.PackBlobID
}

func (i Info) GetPPackBlobID() *blob.ID {
	packBlobID := i.PackBlobID
	return &packBlobID
}

func (i Info) GetContentID() ID {
	return i.ContentID
}

func (i Info) GetTimestampSeconds() int64 {
	return i.TimestampSeconds
}

func (i Info) GetOriginalLength() uint32 {
	return i.OriginalLength
}

func (i Info) GetPackedLength() uint32 {
	return i.PackedLength
}

func (i Info) GetPackOffset() uint32 {
	return i.PackOffset
}

func (i Info) GetCompressionHeaderID() compression.HeaderID {
	return i.CompressionHeaderID
}

func (i Info) IsDeleted() bool {
	return i.Deleted
}

func (i Info) GetFormatVersion() byte {
	return i.FormatVersion
}

func (i Info) GetEncryptionKeyID() byte {
	return i.EncryptionKeyID
}

// Timestamp implements the Info interface.
func (i InfoCompact) Timestamp() time.Time {
	return time.Unix(i.TimestampSeconds, 0)
}

func (i *InfoCompact) GetPackBlobID() blob.ID {
	return *i.PackBlobID
}

func (i *InfoCompact) GetPPackBlobID() *blob.ID {
	return i.PackBlobID
}

func (i *InfoCompact) GetContentID() ID {
	return *i.ContentID
}

func (i *InfoCompact) GetTimestampSeconds() int64 {
	return i.TimestampSeconds
}

func (i *InfoCompact) GetOriginalLength() uint32 {
	return i.OriginalLength
}

func (i *InfoCompact) GetPackedLength() uint32 {
	return i.PackedLength
}

func (i InfoCompact) GetPackOffset() uint32 {
	return i.PackOffset
}

func (i *InfoCompact) GetCompressionHeaderID() compression.HeaderID {
	return i.CompressionHeaderID
}

func (i *InfoCompact) IsDeleted() bool {
	return i.Deleted
}

func (i *InfoCompact) GetFormatVersion() byte {
	return i.FormatVersion
}

func (i *InfoCompact) GetEncryptionKeyID() byte {
	return i.EncryptionKeyID
}

func FlatenInfo(ic *InfoCompact) Info {
	return Info{
		PackBlobID:          *ic.PackBlobID,
		ContentID:           *ic.ContentID,
		TimestampSeconds:    ic.TimestampSeconds,
		OriginalLength:      ic.OriginalLength,
		PackedLength:        ic.PackedLength,
		PackOffset:          ic.PackOffset,
		CompressionHeaderID: ic.CompressionHeaderID,
		Deleted:             ic.Deleted,
		FormatVersion:       ic.FormatVersion,
		EncryptionKeyID:     ic.EncryptionKeyID,
	}
}
