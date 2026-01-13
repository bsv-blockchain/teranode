// Package meta provides types and utilities for handling Bitcoin SV transaction metadata
// in the UTXO store. It includes serialization and deserialization of transaction data
// along with associated metadata like parent transactions, block references, and fees.
//
// The meta package is a core component of the UTXO store, providing:
//   - Structured representation of transaction metadata
//   - Binary serialization/deserialization for efficient storage
//   - Support for advanced Bitcoin features like conflicting transaction tracking
//   - Methods for retrieving and manipulating transaction metadata
//
// Transaction metadata is essential for efficient blockchain validation, allowing
// quick access to critical information without requiring full transaction parsing.
package meta

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
)

// Data represents transaction metadata including the transaction itself,
// its parent transactions, block references, and other metadata.
// The struct fields are ordered for optimal memory layout.
// IMPORTANT - Do not change order, it has been optimized for size: https://golangprojectstructure.com/how-to-make-go-structs-more-efficient/
type Data struct {
	// Tx is the full transaction data
	Tx *bt.Tx `json:"tx"`

	// TxInpoints contains the transaction IDs and indexes of all parent transactions
	TxInpoints subtree.TxInpoints `json:"txInpoints"`

	// BlockIDs contains the block heights where this transaction appears
	BlockIDs []uint32 `json:"blockIDs"`

	// BlockHeights contains the block heights where this transaction appears
	BlockHeights []uint32 `json:"blockHeights"`

	// SubtreeIdxs contains the subtree indexes where this transaction appears
	SubtreeIdxs []int `json:"subtreeIdxs"`

	// Fee is the total transaction fee in satoshis
	Fee uint64 `json:"fee"`

	// SizeInBytes is the serialized size of the transaction
	SizeInBytes uint64 `json:"sizeInBytes"`

	// IsCoinbase indicates if this is a coinbase transaction
	IsCoinbase bool `json:"isCoinbase"`

	// LockTime is the block height or timestamp until which this transaction is locked.
	// This can differ from the transaction's own locktime, especially for coinbase transactions.
	LockTime uint32 `json:"lockTime"`

	// UnminedSince is the block height when an unmined transaction was first stored.
	// When set to a block height value, it indicates the transaction is unmined on the longest chain.
	// When 0, it indicates the transaction has been mined on the longest chain.
	UnminedSince uint32 `json:"unminedSince"`

	// Frozen is a flag indicating if the transaction is frozen
	Frozen bool `json:"frozen"`

	// Conflicting is a flag indicating if the transaction is conflicting
	Conflicting bool `json:"conflicting"`

	// ConflictingChildren contains the transaction IDs of all transactions that tried to spend this conflicting transaction
	ConflictingChildren []chainhash.Hash `json:"conflictingChildren"`

	// Locked is a flag indicating if the transaction is locked and not spendable
	Locked bool `json:"locked"`

	// SpendingDatas is the transaction ID of the transaction that spent the given tx output idx
	SpendingDatas []*spendpkg.SpendingData `json:"spendingDatas"`
}

// NewMetaDataFromBytes creates a new Data object from a byte slice.
// This function populates an existing Data object with metadata extracted from the binary format.
// It's optimized for performance by avoiding allocations and operating directly on the provided slice.
//
// The binary format is as follows:
//   - [0:8]   - 8 bytes for Fee (uint64, little-endian)
//   - [8:16]  - 8 bytes for SizeInBytes (uint64, little-endian)
//   - [16]    - 1 byte for flags (bit 0: IsCoinbase, bit 1: Frozen, bit 2: Conflicting, bit 3: Locked)
//   - [17:25] - 8 bytes for number of ParentTxHashes (uint64, little-endian)
//   - [25+]   - 32 bytes for each ParentTxHash
//
// Parameters:
//   - dataBytes: Pointer to a byte slice containing the serialized metadata
//   - d: Pointer to a Data object to populate
//
// Note: This function assumes the byte slice is valid and contains enough data.
func NewMetaDataFromBytes(dataBytes []byte, d *Data) (err error) {
	if len(dataBytes) < 17 {
		return errors.NewProcessingError("dataBytes too short, expected at least 17 bytes, got %d", len(dataBytes))
	}

	// read the numbers
	d.Fee = binary.LittleEndian.Uint64(dataBytes[:8])
	d.SizeInBytes = binary.LittleEndian.Uint64(dataBytes[8:16])

	d.IsCoinbase = (dataBytes[16] & 0b01) == 0b01
	d.Frozen = (dataBytes[16] & 0b10) == 0b10
	d.Conflicting = (dataBytes[16] & 0b100) == 0b100
	d.Locked = (dataBytes[16] & 0b1000) == 0b1000

	d.TxInpoints, err = subtree.NewTxInpointsFromBytes(dataBytes[17:])

	return err
}

// NewDataFromBytes creates a new Data object from a byte slice.
// This function allocates and returns a new Data object containing transaction metadata
// and the transaction itself.
//
// The binary format is as follows:
//   - [0:8]   - 8 bytes for Fee (uint64, little-endian)
//   - [8:16]  - 8 bytes for SizeInBytes (uint64, little-endian)
//   - [16]    - 1 byte for flags (bit 0: IsCoinbase, bit 1: Frozen, bit 2: Conflicting, bit 3: Locked)
//   - [17:25] - 8 bytes for number of ParentTxHashes (uint64, little-endian)
//   - [25+]   - 32 bytes for each ParentTxHash
//   - [...]   - Variable-length transaction data (full transaction)
//   - [...]   - Remainder: block IDs as 4-byte integers (uint32, little-endian)
//
// Unlike NewMetaDataFromBytes, this function:
//   - Performs error checking on the input data
//   - Allocates and returns a new Data object
//   - Parses the full transaction data
//   - Extracts block IDs
//
// Parameters:
//   - dataBytes: Byte slice containing the serialized data
//
// Returns:
//   - Pointer to a new Data object
//   - Error if parsing fails or input data is invalid
func NewDataFromBytes(dataBytes []byte) (d *Data, err error) {
	if len(dataBytes) < 24 {
		return nil, errors.NewProcessingError("dataBytes too short, expected at least 24 bytes, got %d", len(dataBytes))
	}

	d = &Data{}

	// read the numbers
	d.Fee = binary.LittleEndian.Uint64(dataBytes[:8])
	d.SizeInBytes = binary.LittleEndian.Uint64(dataBytes[8:16])

	d.IsCoinbase = dataBytes[16]&0b01 == 0b01
	d.Frozen = dataBytes[16]&0b10 == 0b10
	d.Conflicting = dataBytes[16]&0b100 == 0b100
	d.Locked = dataBytes[16]&0b1000 == 0b1000

	buf := bytes.NewReader(dataBytes[17:])

	// read the number of parent tx inpoints
	d.TxInpoints, err = subtree.NewTxInpointsFromReader(buf)
	if err != nil {
		return nil, errors.NewProcessingError("could not deserialize tx inpoints", err)
	}

	// read the tx
	d.Tx = &bt.Tx{}

	if _, err = d.Tx.ReadFrom(buf); err != nil {
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

// Bytes returns the Data object as a byte slice.
// This method provides a binary serialization of the complete Data object,
// including the transaction data and all metadata. It's the inverse operation
// of NewDataFromBytes.
//
// The binary format is as follows:
//   - [0:8]   - 8 bytes for Fee (uint64, little-endian)
//   - [8:16]  - 8 bytes for SizeInBytes (uint64, little-endian)
//   - [16]    - 1 byte for flags (bit 0: IsCoinbase, bit 1: Frozen, bit 2: Conflicting, bit 3: Locked)
//   - [17:25] - 8 bytes for number of ParentTxHashes (uint64, little-endian)
//   - [25+]   - 32 bytes for each ParentTxHash
//   - [...]   - Variable-length transaction data (full transaction)
//   - [...]   - Remainder: block IDs as 4-byte integers (uint32, little-endian)
//
// Returns:
//   - Byte slice containing the serialized Data object
//
// Note: This method allocates a new byte slice with an initial capacity of 1024 bytes
// and grows it as needed to accommodate the full serialized data.
func (d *Data) Bytes() ([]byte, error) {
	buf := make([]byte, 17, 1024) // 8 for Fee, 8 for SizeInBytes

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

	if d.Locked {
		buf[16] |= 0b1000
	}

	txInpointsBytes, err := d.TxInpoints.Serialize()
	if err != nil {
		return nil, err
	}

	buf = append(buf, txInpointsBytes...)

	// write the tx data
	if d.Tx != nil {
		buf = append(buf, d.Tx.ExtendedBytes()...)
	}

	// write a varint for the length and then all the block IDs
	var blockBytes [4]byte
	for _, blockID := range d.BlockIDs {
		binary.LittleEndian.PutUint32(blockBytes[:], blockID)
		buf = append(buf, blockBytes[:]...)
	}

	return buf, nil
}

// MetaBytes returns the Data object as a byte slice containing only metadata.
// Unlike Bytes(), this method does not include the transaction data or block IDs,
// making it more efficient when only metadata is needed.
//
// The binary format is as follows:
//   - [0:8]   - 8 bytes for Fee (uint64, little-endian)
//   - [8:16]  - 8 bytes for SizeInBytes (uint64, little-endian)
//   - [16]    - 1 byte for flags (bit 0: IsCoinbase, bit 1: Frozen, bit 2: Conflicting, bit 3: Locked)
//   - [17:25] - 8 bytes for number of ParentTxHashes (uint64, little-endian)
//   - [25+]   - 32 bytes for each ParentTxHash
//
// Returns:
//   - Byte slice containing the serialized metadata (without transaction or block IDs)
//
// Use this method when you need to efficiently store or transmit just the
// metadata portion of a transaction record.
func (d *Data) MetaBytes() ([]byte, error) {
	buf := make([]byte, 17, 1024) // 8 for Fee, 8 for SizeInBytes, 1 for flags

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

	if d.Locked {
		buf[16] |= 0b1000
	}

	txInpointsBytes, err := d.TxInpoints.Serialize()
	if err != nil {
		return nil, err
	}

	buf = append(buf, txInpointsBytes...)

	return buf, nil
}

// String returns a human-readable representation of the Data object.
// This method implements the Stringer interface, providing a formatted
// string representation of the Data object's contents for debugging
// and logging purposes.
//
// The string includes all key metadata fields such as:
//   - Transaction ID (if transaction is available)
//   - Fee amount
//   - Size in bytes
//   - IsCoinbase flag
//   - BlockIDs where the transaction appears
//   - Parent transaction hashes
//   - Status flags (Frozen, Conflicting, Locked)
//
// Returns "nil" if the Data object is nil.
func (d *Data) String() string {
	if d == nil {
		return "nil"
	}

	return fmt.Sprintf("{TxID: %s, ParentTxHashes: %v, BlockIDs: %v, Fee: %d, SizeInBytes: %d, IsCoinbase: %t, IsFrozen: %t, IsConflicting: %t, IsLocked: %t, LockTime: %d}",
		func() string {
			if d.Tx == nil {
				return "nil"
			}

			return d.Tx.TxIDChainHash().String()
		}(),
		d.TxInpoints,
		d.BlockIDs,
		d.Fee,
		d.SizeInBytes,
		d.IsCoinbase,
		d.Frozen,
		d.Conflicting,
		d.Locked,
		d.LockTime)
}
