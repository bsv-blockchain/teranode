// Package utxo provides serialization utilities for UnminedTransaction.
package utxo

import (
	"encoding/binary"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
)

// ErrInvalidSerializedData indicates the serialized data is too short or malformed.
var ErrInvalidSerializedData = errors.NewProcessingError("invalid serialized unmined transaction data")

// SerializeUnminedTransaction serializes an UnminedTransaction to bytes.
// Format:
//   - Hash: 32 bytes
//   - Fee: 8 bytes (uint64, little-endian)
//   - SizeInBytes: 8 bytes (uint64, little-endian)
//   - CreatedAt: 4 bytes (int32, little-endian)
//   - Flags: 1 byte (bit 0: Locked, bit 1: Skip)
//   - UnminedSince: 4 bytes (int32, little-endian)
//   - BlockIDs count: 4 bytes (uint32, little-endian)
//   - BlockIDs: N * 4 bytes (uint32, little-endian)
//   - TxInpoints: variable (using existing Serialize)
func SerializeUnminedTransaction(tx *UnminedTransaction) ([]byte, error) {
	// Calculate size estimate
	blockIDsSize := 4 + len(tx.BlockIDs)*4
	estimatedSize := 32 + 8 + 8 + 4 + 1 + 4 + blockIDsSize + 256 // 256 for TxInpoints estimate

	buf := make([]byte, 0, estimatedSize)

	// Hash (32 bytes)
	buf = append(buf, tx.Node.Hash[:]...)

	// Fee (8 bytes)
	b8 := make([]byte, 8)
	binary.LittleEndian.PutUint64(b8, tx.Node.Fee)
	buf = append(buf, b8...)

	// SizeInBytes (8 bytes)
	binary.LittleEndian.PutUint64(b8, tx.Node.SizeInBytes)
	buf = append(buf, b8...)

	// CreatedAt (4 bytes)
	b4 := make([]byte, 4)
	binary.LittleEndian.PutUint32(b4, uint32(tx.CreatedAt))
	buf = append(buf, b4...)

	// Flags (1 byte)
	var flags byte
	if tx.Locked {
		flags |= 0x01
	}
	if tx.Skip {
		flags |= 0x02
	}
	buf = append(buf, flags)

	// UnminedSince (4 bytes)
	binary.LittleEndian.PutUint32(b4, uint32(tx.UnminedSince))
	buf = append(buf, b4...)

	// BlockIDs count (4 bytes)
	binary.LittleEndian.PutUint32(b4, uint32(len(tx.BlockIDs)))
	buf = append(buf, b4...)

	// BlockIDs (N * 4 bytes)
	for _, blockID := range tx.BlockIDs {
		binary.LittleEndian.PutUint32(b4, blockID)
		buf = append(buf, b4...)
	}

	// TxInpoints (variable)
	if tx.TxInpoints != nil {
		inpointsBytes, err := tx.TxInpoints.Serialize()
		if err != nil {
			return nil, err
		}
		buf = append(buf, inpointsBytes...)
	}

	return buf, nil
}

// DeserializeUnminedTransaction deserializes bytes into an UnminedTransaction.
func DeserializeUnminedTransaction(data []byte) (*UnminedTransaction, error) {
	if len(data) < 61 { // Minimum: 32+8+8+4+1+4+4 = 61 bytes
		return nil, ErrInvalidSerializedData
	}

	offset := 0

	// Hash (32 bytes)
	var hash chainhash.Hash
	copy(hash[:], data[offset:offset+32])
	offset += 32

	// Fee (8 bytes)
	fee := binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	// SizeInBytes (8 bytes)
	sizeInBytes := binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	// CreatedAt (4 bytes)
	createdAt := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
	offset += 4

	// Flags (1 byte)
	flags := data[offset]
	offset++
	locked := flags&0x01 != 0
	skip := flags&0x02 != 0

	// UnminedSince (4 bytes)
	unminedSince := int(binary.LittleEndian.Uint32(data[offset : offset+4]))
	offset += 4

	// BlockIDs count (4 bytes)
	blockIDsCount := binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4

	// BlockIDs (N * 4 bytes)
	blockIDs := make([]uint32, blockIDsCount)
	for i := uint32(0); i < blockIDsCount; i++ {
		blockIDs[i] = binary.LittleEndian.Uint32(data[offset : offset+4])
		offset += 4
	}

	// TxInpoints (variable)
	var txInpoints *subtree.TxInpoints
	if offset < len(data) {
		inpoints, err := subtree.NewTxInpointsFromBytes(data[offset:])
		if err != nil {
			return nil, err
		}
		txInpoints = &inpoints
	}

	return &UnminedTransaction{
		Node: &subtree.Node{
			Hash:        hash,
			Fee:         fee,
			SizeInBytes: sizeInBytes,
		},
		TxInpoints:   txInpoints,
		CreatedAt:    createdAt,
		Locked:       locked,
		Skip:         skip,
		UnminedSince: unminedSince,
		BlockIDs:     blockIDs,
	}, nil
}
