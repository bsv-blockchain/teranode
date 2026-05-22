package model

import (
	"encoding/binary"
	"time"

	safe "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/bsv-blockchain/teranode/errors"
)

type BlockHeaderMeta struct {
	ID             uint32     `json:"id"`               // ID of the block in the internal blockchain DB.
	Height         uint32     `json:"height"`           // Height of the block in the blockchain.
	TxCount        uint64     `json:"tx_count"`         // Number of transactions in the block.
	SizeInBytes    uint64     `json:"size_in_bytes"`    // Size of the block in bytes.
	Miner          string     `json:"miner"`            // Miner
	PeerID         string     `json:"peer_id"`          // Peer ID of the miner that mined the block.
	BlockTime      uint32     `json:"block_time"`       // Time of the block.
	Timestamp      uint32     `json:"timestamp"`        // Timestamp of creation of the block in the db.
	MedianTimePast uint32     `json:"median_time_past"` // Median Time Past (MTP) - median of last 11 block timestamps (BIP113)
	ChainWork      []byte     `json:"chainwork"`        // ChainWork of the block.
	MinedSet       bool       `json:"mined_set"`        // Whether the block is in the mined set.
	SubtreesSet    bool       `json:"subtrees_set"`     // Whether the block has its subtrees set.
	Invalid        bool       `json:"invalid"`          // Whether the block is marked as invalid.
	ProcessedAt    *time.Time `json:"processed_at"`     // Timestamp when the block was processed (nullable).
}

func (m *BlockHeaderMeta) Bytes() []byte {
	capacity := 1 + 4 + 4 + 8 + 8 + 4 + 4 + 4 + 4 + len(m.ChainWork) + 4 + len(m.Miner) + 4 + len(m.PeerID)
	if m.ProcessedAt != nil {
		capacity += 8
	}
	b := make([]byte, 0, capacity)

	// write the flags (minedSet, subtreesSet, invalid, processedAt) as the first byte
	flags := byte(0)
	if m.MinedSet {
		flags |= 1 << 0
	}
	if m.SubtreesSet {
		flags |= 1 << 1
	}
	if m.Invalid {
		flags |= 1 << 2
	}
	if m.ProcessedAt != nil {
		flags |= 1 << 3
	}

	b = append(b, flags)

	b32 := make([]byte, 4)
	binary.LittleEndian.PutUint32(b32, m.ID)
	b = append(b, b32...)

	binary.LittleEndian.PutUint32(b32, m.Height)
	b = append(b, b32...)

	b64 := make([]byte, 8)
	binary.LittleEndian.PutUint64(b64, m.TxCount)
	b = append(b, b64...)

	binary.LittleEndian.PutUint64(b64, m.SizeInBytes)
	b = append(b, b64...)

	binary.LittleEndian.PutUint32(b32, m.BlockTime)
	b = append(b, b32...)

	binary.LittleEndian.PutUint32(b32, m.Timestamp)
	b = append(b, b32...)

	binary.LittleEndian.PutUint32(b32, m.MedianTimePast)
	b = append(b, b32...)

	len32, _ := safe.IntToUint32(len(m.ChainWork))
	binary.LittleEndian.PutUint32(b32, len32)
	b = append(b, b32...)
	b = append(b, m.ChainWork...)

	// append the miner string
	len32, _ = safe.IntToUint32(len(m.Miner))
	binary.LittleEndian.PutUint32(b32, len32)
	b = append(b, b32...)
	b = append(b, []byte(m.Miner)...)

	// append the peer ID
	len32, _ = safe.IntToUint32(len(m.PeerID))
	binary.LittleEndian.PutUint32(b32, len32)
	b = append(b, b32...)
	b = append(b, []byte(m.PeerID)...)

	// append the processed_at timestamp if present
	if m.ProcessedAt != nil {
		binary.LittleEndian.PutUint64(b64, uint64(m.ProcessedAt.Unix()))
		b = append(b, b64...)
	}

	return b
}

func NewBlockHeaderMetaFromBytes(b []byte) (*BlockHeaderMeta, error) {
	// 4 for ID, Height; 8 for TxCount, SizeInBytes; 4 for BlockTime, Timestamp, MedianTimePast; 4 for ChainWork length
	if len(b) < 4+4+8+8+4+4+4+4 {
		return nil, errors.NewProcessingError("invalid length for BlockHeaderMeta: %d", len(b))
	}

	m := &BlockHeaderMeta{}
	// read in the flags
	flags := b[0]
	m.MinedSet = (flags & (1 << 0)) != 0
	m.SubtreesSet = (flags & (1 << 1)) != 0
	m.Invalid = (flags & (1 << 2)) != 0
	hasProcessedAt := (flags & (1 << 3)) != 0

	// remove the flags byte from the slice
	b = b[1:]

	// read in the fixed-size fields
	m.ID = binary.LittleEndian.Uint32(b[:4])
	m.Height = binary.LittleEndian.Uint32(b[4:8])
	m.TxCount = binary.LittleEndian.Uint64(b[8:16])
	m.SizeInBytes = binary.LittleEndian.Uint64(b[16:24])
	m.BlockTime = binary.LittleEndian.Uint32(b[24:28])
	m.Timestamp = binary.LittleEndian.Uint32(b[28:32])
	m.MedianTimePast = binary.LittleEndian.Uint32(b[32:36])

	// read in the chainwork length
	len32 := binary.LittleEndian.Uint32(b[36:40])

	if len32 > 0 {
		m.ChainWork = make([]byte, len32)
		copy(m.ChainWork, b[40:40+len32])
	}

	offset := 40 + len32

	// read in the miner string
	if len(b) < int(offset+4) {
		return nil, errors.NewProcessingError("invalid length for miner string in BlockHeaderMeta: %d", len(b))
	}

	len32 = binary.LittleEndian.Uint32(b[offset : offset+4])
	if len32 > 0 {
		if len(b) < int(offset+4+len32) {
			return nil, errors.NewProcessingError("invalid length for miner string in BlockHeaderMeta: %d", len(b))
		}
		m.Miner = string(b[offset+4 : offset+4+len32])
		offset += 4 + len32
	} else {
		m.Miner = ""
	}

	// read in the peer ID
	if len(b) < int(offset+4) {
		return nil, errors.NewProcessingError("invalid length for peer ID in BlockHeaderMeta: %d", len(b))
	}

	len32 = binary.LittleEndian.Uint32(b[offset : offset+4]) //nolint:gosec // bounds already checked above
	if len32 > 0 {
		if len(b) < int(offset+4+len32) {
			return nil, errors.NewProcessingError("invalid length for peer ID in BlockHeaderMeta: %d", len(b))
		}
		m.PeerID = string(b[offset+4 : offset+4+len32]) //nolint:gosec // bounds checked on line above
		offset += 4 + len32
	} else {
		m.PeerID = ""
		offset += 4
	}

	// read the processed_at timestamp if present
	if hasProcessedAt {
		if len(b) < int(offset+8) {
			return nil, errors.NewProcessingError("invalid length for processed_at in BlockHeaderMeta: %d", len(b))
		}
		processedAt := binary.LittleEndian.Uint64(b[offset : offset+8]) // #nosec G602 - bounds checked above
		p := time.Unix(int64(processedAt), 0)
		m.ProcessedAt = &p
	}

	return m, nil
}
