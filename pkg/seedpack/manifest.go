package seedpack

import (
	"encoding/binary"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

// FormatVersion is the seed-package manifest format version. Bump on any
// breaking change to the layout or the canonical element/chunking it implies.
const FormatVersion uint32 = 1

const (
	manifestHeaderLen = 4 + 4 + 32 + 32 + 4 + 4 + 8 + 4 // version, height, blockHash, setHash, chunkMin, chunkMax, chunkMask, chunkCount
	chunkRefLen       = 32 + 4                          // hash, size
)

// ChunkRef identifies one content-addressed chunk and its byte length.
type ChunkRef struct {
	Hash [32]byte
	Size uint32
}

// Manifest binds an ordered list of chunks to the committed set state.
//
// ChunkMin, ChunkMax, and ChunkMask record the content-defined-chunking
// parameters the producer used. They are informational only — reassembly
// follows the chunk list, not re-chunking — but recording them lets a producer
// reproduce identical boundaries (and therefore cross-snapshot dedup).
type Manifest struct {
	FormatVersion uint32
	Height        uint32
	BlockHash     chainhash.Hash
	SetHash       [32]byte
	ChunkMin      uint32
	ChunkMax      uint32
	ChunkMask     uint64
	Chunks        []ChunkRef
}

// Serialize encodes the manifest as:
//
//	version(4 LE) | height(4 LE) | blockHash(32) | setHash(32) | chunkMin(4 LE) | chunkMax(4 LE) | chunkMask(8 LE) | chunkCount(4 LE) | chunkCount*(hash(32) | size(4 LE))
func (m Manifest) Serialize() []byte {
	out := make([]byte, 0, manifestHeaderLen+len(m.Chunks)*chunkRefLen)
	out = binary.LittleEndian.AppendUint32(out, m.FormatVersion)
	out = binary.LittleEndian.AppendUint32(out, m.Height)
	out = append(out, m.BlockHash[:]...)
	out = append(out, m.SetHash[:]...)
	out = binary.LittleEndian.AppendUint32(out, m.ChunkMin)
	out = binary.LittleEndian.AppendUint32(out, m.ChunkMax)
	out = binary.LittleEndian.AppendUint64(out, m.ChunkMask)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(m.Chunks)))

	for _, c := range m.Chunks {
		out = append(out, c.Hash[:]...)
		out = binary.LittleEndian.AppendUint32(out, c.Size)
	}

	return out
}

// ParseManifest decodes a manifest, validating the version, structure, and length.
func ParseManifest(b []byte) (Manifest, error) {
	var m Manifest

	if len(b) < manifestHeaderLen {
		return m, errors.NewProcessingError("manifest too short: %d bytes", len(b))
	}

	m.FormatVersion = binary.LittleEndian.Uint32(b[0:4])
	if m.FormatVersion != FormatVersion {
		return m, errors.NewProcessingError("unsupported manifest version %d", m.FormatVersion)
	}

	m.Height = binary.LittleEndian.Uint32(b[4:8])
	copy(m.BlockHash[:], b[8:40])
	copy(m.SetHash[:], b[40:72])
	m.ChunkMin = binary.LittleEndian.Uint32(b[72:76])
	m.ChunkMax = binary.LittleEndian.Uint32(b[76:80])
	m.ChunkMask = binary.LittleEndian.Uint64(b[80:88])

	count := binary.LittleEndian.Uint32(b[88:92])

	// Bound count by what the buffer could possibly hold before any arithmetic
	// or allocation, so a crafted manifest cannot overflow the size calculation
	// or trigger a huge allocation (manifests come from untrusted seed packages).
	if maxCount := uint32((len(b) - manifestHeaderLen) / chunkRefLen); count > maxCount {
		return m, errors.NewProcessingError("manifest claims %d chunks, buffer holds at most %d", count, maxCount)
	}

	want := manifestHeaderLen + int(count)*chunkRefLen
	if len(b) != want {
		return m, errors.NewProcessingError("manifest length %d, expected %d for %d chunks", len(b), want, count)
	}

	m.Chunks = make([]ChunkRef, count)

	off := manifestHeaderLen
	for i := range m.Chunks {
		copy(m.Chunks[i].Hash[:], b[off:off+32])
		m.Chunks[i].Size = binary.LittleEndian.Uint32(b[off+32 : off+36])
		off += chunkRefLen
	}

	return m, nil
}
