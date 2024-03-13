package model

import (
	"encoding/binary"
	"fmt"
)

type BlockHeaderMeta struct {
	ID          uint32 `json:"id"`            // ID of the block in the internal blockchain DB.
	Height      uint32 `json:"height"`        // Height of the block in the blockchain.
	TxCount     uint64 `json:"tx_count"`      // Number of transactions in the block.
	SizeInBytes uint64 `json:"size_in_bytes"` // Size of the block in bytes.
	Miner       string `json:"miner"`         // Miner
	BlockTime   uint32 `json:"block_time"`    // Time of the block.
	Timestamp   uint32 `json:"timestamp"`     // Timestamp of the block.
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

	b = append(b, []byte(m.Miner)...)

	return b
}

func NewBlockHeaderMetaFromBytes(b []byte) (*BlockHeaderMeta, error) {
	if len(b) < 4+4+8+8 {
		return nil, fmt.Errorf("invalid length for BlockHeaderMeta: %d", len(b))
	}

	m := &BlockHeaderMeta{}
	m.ID = binary.LittleEndian.Uint32(b[:4])
	m.Height = binary.LittleEndian.Uint32(b[4:8])
	m.TxCount = binary.LittleEndian.Uint64(b[8:16])
	m.SizeInBytes = binary.LittleEndian.Uint64(b[16:24])
	m.BlockTime = binary.LittleEndian.Uint32(b[24:28])
	m.Timestamp = binary.LittleEndian.Uint32(b[28:32])

	m.Miner = string(b[32:])

	return m, nil
}
