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

// Timestamp implements the Info interface.
func (i Info) Timestamp() time.Time {
	return time.Unix(i.TimestampSeconds, 0)
}

// Timestamp implements the Info interface.
func (i InfoCompact) Timestamp() time.Time {
	return time.Unix(i.TimestampSeconds, 0)
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
