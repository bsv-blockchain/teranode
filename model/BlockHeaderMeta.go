package model

import (
	"encoding/binary"

	"github.com/bitcoin-sv/teranode/errors"
	safe "github.com/bsv-blockchain/go-safe-conversion"
)

type BlockHeaderMeta struct {
	ID          uint32 `json:"id"`            // ID of the block in the internal blockchain DB.
	Height      uint32 `json:"height"`        // Height of the block in the blockchain.
	TxCount     uint64 `json:"tx_count"`      // Number of transactions in the block.
	SizeInBytes uint64 `json:"size_in_bytes"` // Size of the block in bytes.
	Miner       string `json:"miner"`         // Miner
	PeerID      string `json:"peer_id"`       // Peer ID of the miner that mined the block.
	BlockTime   uint32 `json:"block_time"`    // Time of the block.
	Timestamp   uint32 `json:"timestamp"`     // Timestamp of creation of the block in the db.
	ChainWork   []byte `json:"chainwork"`     // ChainWork of the block.
}

func (m *BlockHeaderMeta) Bytes() []byte {
	b := make([]byte, 0, 4+4+8+8+len(m.Miner))

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

	return b
}

func NewBlockHeaderMetaFromBytes(b []byte) (*BlockHeaderMeta, error) {
	// 4 for ID, Height; 8 for TxCount, SizeInBytes; 4 for BlockTime, Timestamp; 4 for ChainWork length
	if len(b) < 4+4+8+8+4+4+4 {
		return nil, errors.NewProcessingError("invalid length for BlockHeaderMeta: %d", len(b))
	}

	m := &BlockHeaderMeta{}
	m.ID = binary.LittleEndian.Uint32(b[:4])
	m.Height = binary.LittleEndian.Uint32(b[4:8])
	m.TxCount = binary.LittleEndian.Uint64(b[8:16])
	m.SizeInBytes = binary.LittleEndian.Uint64(b[16:24])
	m.BlockTime = binary.LittleEndian.Uint32(b[24:28])
	m.Timestamp = binary.LittleEndian.Uint32(b[28:32])

	// read in the chainwork length
	len32 := binary.LittleEndian.Uint32(b[32:36])

	if len32 > 0 {
		m.ChainWork = make([]byte, len32)
		copy(m.ChainWork, b[36:36+len32])
	}

	offset := 36 + len32

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

	len32 = binary.LittleEndian.Uint32(b[offset : offset+4])
	if len32 > 0 {
		if len(b) < int(offset+4+len32) {
			return nil, errors.NewProcessingError("invalid length for peer ID in BlockHeaderMeta: %d", len(b))
		}
		m.PeerID = string(b[offset+4 : offset+4+len32])
	} else {
		m.PeerID = ""
	}

	return m, nil
}
