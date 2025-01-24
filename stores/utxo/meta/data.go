// Package meta provides types and utilities for handling Bitcoin SV transaction metadata
// in the UTXO store. It includes serialization and deserialization of transaction data
// along with associated metadata like parent transactions, block references, and fees.
package meta

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

// Data represents transaction metadata including the transaction itself,
// its parent transactions, block references, and other metadata.
// The struct fields are ordered for optimal memory layout.
// IMPORTANT - Do not change order, it has been optimized for size: https://golangprojectstructure.com/how-to-make-go-structs-more-efficient/
type Data struct {
	// Tx is the full transaction data
	Tx *bt.Tx `json:"tx"`

	// ParentTxHashes contains the transaction IDs of all parent transactions
	ParentTxHashes []chainhash.Hash `json:"parentTxHashes"`

	// BlockIDs contains the block heights where this transaction appears
	BlockIDs []uint32 `json:"blockIDs"`

	// Fee is the total transaction fee in satoshis
	Fee uint64 `json:"fee"`

	// SizeInBytes is the serialized size of the transaction
	SizeInBytes uint64 `json:"sizeInBytes"`

	// IsCoinbase indicates if this is a coinbase transaction
	IsCoinbase bool `json:"isCoinbase"`

	// LockTime is the block height or timestamp until which this transaction is locked.
	// This can differ from the transaction's own locktime, especially for coinbase transactions.
	LockTime uint32 `json:"lockTime"`

	// Frozen is a flag indicating if the transaction is frozen
	Frozen bool `json:"frozen"`

	// Conflicting is a flag indicating if the transaction is conflicting
	Conflicting bool `json:"conflicting"`

	// Unspendable is a flag indicating if the transaction is not spendable
	Unspendable bool `json:"unspendable"`
}

// PreviousOutput represents an input's previous output information.
// It's used when decorating transaction inputs with their source output data.
type PreviousOutput struct {
	// PreviousTxID is the transaction ID containing the output
	PreviousTxID chainhash.Hash

	// Vout is the output index in the previous transaction
	Vout uint32

	// Idx is the input index in the spending transaction
	Idx int

	// LockingScript is the output's locking script
	LockingScript []byte

	// Satoshis is the amount in satoshis
	Satoshis uint64
}

// NewMetaDataFromBytes creates a new Data object from a byte slice
// the byte slice should be in the format:
//
//	8 bytes for Fee
//	8 bytes for SizeInBytes
//	1 byte for IsCoinbase
//	8 bytes for the number of ParentTxHashes
//	32 bytes for each ParentTxHash
func NewMetaDataFromBytes(dataBytes *[]byte, d *Data) {
	// read the numbers
	d.Fee = binary.LittleEndian.Uint64((*dataBytes)[:8])
	d.SizeInBytes = binary.LittleEndian.Uint64((*dataBytes)[8:16])

	d.IsCoinbase = ((*dataBytes)[16] & 0b01) == 0b01
	d.Frozen = ((*dataBytes)[16] & 0b10) == 0b10
	d.Conflicting = ((*dataBytes)[16] & 0b100) == 0b100
	d.Unspendable = ((*dataBytes)[16] & 0b1000) == 0b1000

	parentTxHashesLen := binary.LittleEndian.Uint64((*dataBytes)[17:25])
	d.ParentTxHashes = make([]chainhash.Hash, parentTxHashesLen)

	for i := uint64(0); i < parentTxHashesLen; i++ {
		d.ParentTxHashes[i] = chainhash.Hash((*dataBytes)[25+i*32 : 25+(i+1)*32])
	}
}

// NewDataFromBytes creates a new Data object from a byte slice
// the byte slice should be in the format:
// 8 bytes for Fee
// 8 bytes for SizeInBytes
// 1 byte for IsCoinbase
// 8 bytes for the number of ParentTxHashes
// 32 bytes for each ParentTxHash
// the transaction data as a serialized byte slice
// the remainder of the byte slice is the block hashes as 4 byte integers
func NewDataFromBytes(dataBytes []byte) (*Data, error) {
	if len(dataBytes) < 24 {
		return nil, errors.NewProcessingError("dataBytes too short, expected at least 24 bytes, got %d", len(dataBytes))
	}

	d := &Data{}

	// read the numbers
	d.Fee = binary.LittleEndian.Uint64(dataBytes[:8])
	d.SizeInBytes = binary.LittleEndian.Uint64(dataBytes[8:16])

	d.IsCoinbase = dataBytes[16]&0b01 == 0b01
	d.Frozen = dataBytes[16]&0b10 == 0b10
	d.Conflicting = dataBytes[16]&0b100 == 0b100
	d.Unspendable = dataBytes[16]&0b1000 == 0b1000

	parentTxHashesLen := binary.LittleEndian.Uint64(dataBytes[17:25])
	buf := bytes.NewReader(dataBytes[25:])

	// read the parent tx hashes
	var hashBytes [32]byte

	d.ParentTxHashes = make([]chainhash.Hash, parentTxHashesLen)

	for i := uint64(0); i < parentTxHashesLen; i++ {
		_, err := io.ReadFull(buf, hashBytes[:])
		if err != nil {
			return nil, errors.NewProcessingError("could not read hash bytes", err)
		}

		d.ParentTxHashes[i] = chainhash.Hash(hashBytes[:])
	}

	// read the tx
	d.Tx = &bt.Tx{}

	_, err := d.Tx.ReadFrom(buf)
	if err != nil {
		return nil, errors.NewProcessingError("could not read transaction", err)
	}

	// read the block hashes as the remainder data
	var blockBytes [4]byte

	d.BlockIDs = make([]uint32, 0)

	for {
		_, err = io.ReadFull(buf, blockBytes[:])
		if err != nil {
			if err == io.EOF {
				break
			}

			return nil, errors.NewProcessingError("could not read block bytes", err)
		}

		d.BlockIDs = append(d.BlockIDs, binary.LittleEndian.Uint32(blockBytes[:]))
	}

	return d, nil
}

// Bytes returns the Data object as a byte slice
// the byte slice is in the format:
// 8 bytes for Fee
// 8 bytes for SizeInBytes
// 1 byte for IsCoinbase
// 8 bytes for the number of ParentTxHashes
// 32 bytes for each ParentTxHash
// the transaction data as a serialized byte slice
// the remainder of the byte slice is the block hashes as 4 byte integers
func (d *Data) Bytes() []byte {
	buf := make([]byte, 25, 1024) // 8 for Fee, 8 for SizeInBytes

	binary.LittleEndian.PutUint64(buf[:8], d.Fee)
	binary.LittleEndian.PutUint64(buf[8:16], d.SizeInBytes)

	if d.IsCoinbase {
		buf[16] |= 0b01
	}

	if d.Frozen {
		buf[16] |= 0b10
	}

	if d.Conflicting {
		buf[16] |= 0b100
	}

	if d.Unspendable {
		buf[16] |= 0b1000
	}

	binary.LittleEndian.PutUint64(buf[17:25], uint64(len(d.ParentTxHashes)))

	for _, parentTxHash := range d.ParentTxHashes {
		buf = append(buf, parentTxHash.CloneBytes()...)
	}

	// write the tx data
	if d.Tx != nil {
		buf = append(buf, d.Tx.ExtendedBytes()...)
	}

	// write a varint for the length and then all the block hashes
	var blockBytes [4]byte
	for _, blockID := range d.BlockIDs {
		binary.LittleEndian.PutUint32(blockBytes[:], blockID)
		buf = append(buf, blockBytes[:]...)
	}

	return buf
}

// MetaBytes returns the Data object as a byte slice
// the byte slice is in the format:
// 8 bytes for Fee
// 8 bytes for SizeInBytes
// 1 byte for IsCoinbase
// 8 bytes for the number of ParentTxHashes
// 32 bytes for each ParentTxHash
func (d *Data) MetaBytes() []byte {
	buf := make([]byte, 25, 1024) // 8 for Fee, 8 for SizeInBytes

	binary.LittleEndian.PutUint64(buf[:8], d.Fee)
	binary.LittleEndian.PutUint64(buf[8:16], d.SizeInBytes)

	if d.IsCoinbase {
		buf[16] |= 0b01
	}

	if d.Frozen {
		buf[16] |= 0b10
	}

	if d.Conflicting {
		buf[16] |= 0b100
	}

	if d.Unspendable {
		buf[16] |= 0b1000
	}

	binary.LittleEndian.PutUint64(buf[17:25], uint64(len(d.ParentTxHashes)))

	for _, parentTxHash := range d.ParentTxHashes {
		buf = append(buf, parentTxHash.CloneBytes()...)
	}

	return buf
}

// String returns a human-readable representation of the Data object.
// The format includes all metadata fields and the transaction ID if available.
// Returns "nil" if the Data object is nil.
func (d *Data) String() string {
	if d == nil {
		return "nil"
	}

	return fmt.Sprintf("{TxID: %s, ParentTxHashes: %v, BlockIDs: %v, Fee: %d, SizeInBytes: %d, IsCoinbase: %t, IsFrozen: %t, IsConflicting: %t, IsUnspendable: %t, LockTime: %d}",
		func() string {
			if d.Tx == nil {
				return "nil"
			}

			return d.Tx.TxIDChainHash().String()
		}(),
		d.ParentTxHashes,
		d.BlockIDs,
		d.Fee,
		d.SizeInBytes,
		d.IsCoinbase,
		d.Frozen,
		d.Conflicting,
		d.Unspendable,
		d.LockTime)
}
