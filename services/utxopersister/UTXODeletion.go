// Package utxopersister creates and maintains up-to-date Unspent Transaction Output (UTXO) file sets
// for each block in the Teranode blockchain. Its primary function is to process the output of the
// Block Persister service (utxo-additions and utxo-deletions) and generate complete UTXO set files.
// The resulting UTXO set files can be exported and used to initialize the UTXO store in new Teranode instances.
//
// UTXODeletion.go implements the data structures and methods required for tracking spent transaction
// outputs. When an input references a previous output, that output is no longer unspent and must be
// removed from the UTXO set. This file provides the mechanisms to record and serialize those deletions
// efficiently, supporting the overall UTXO management process.
package utxopersister

import (
	"bytes"
	"fmt"
	"io"

	"github.com/libsv/go-bt/v2/chainhash"
)

// UTXODeletion represents a deletion of an Unspent Transaction Output.
// It tracks a specific output that has been spent by storing its transaction ID and output index.
// This structure is used to record spent outputs when processing transactions.
//
// UTXODeletion is a compact representation that uniquely identifies a specific output
// by its transaction ID (txid) and output index (vout). When an input references this
// specific output, it is considered spent and must be removed from the UTXO set.
// The structure is designed to be space-efficient while maintaining the ability
// to precisely reference any output in the blockchain.
type UTXODeletion struct {
	// TxID contains the transaction ID
	TxID chainhash.Hash

	// Index represents the output index in the transaction
	Index uint32
}

// NewUTXODeletionFromReader creates a new UTXODeletion from the provided reader.
// It deserializes a UTXODeletion by reading the transaction ID and output index.
// The function also checks for EOF markers to detect the end of a stream.
// It returns the UTXODeletion and any error encountered during reading.
// Binary format is:
// 32 bytes - txID
// 4 bytes - index
//
// Parameters:
// - r: io.Reader from which to read the serialized UTXODeletion data
//
// Returns:
// - *UTXODeletion: Deserialized UTXODeletion, or empty UTXODeletion if EOF marker is encountered
// - error: io.EOF if EOF marker is encountered, or any error during deserialization
//
// This method reads a fixed-size buffer of 36 bytes (32 for txID, 4 for index) and parses it
// to create a UTXODeletion instance. If the transaction ID matches the EOF marker (32 zero bytes),
// it returns an empty UTXODeletion with io.EOF error to signal the end of the stream.
// This function is used when reading deletion records from persistent storage to reconstruct
// the set of spent outputs that need to be excluded from the UTXO set.
func NewUTXODeletionFromReader(r io.Reader) (*UTXODeletion, error) {
	// Read all the fixed size fields
	b := make([]byte, 32+4)

	_, err := io.ReadFull(r, b)
	if err != nil {
		return nil, err
	}

	// Check if all the bytes are zero
	if bytes.Equal(b[:32], EOFMarker) {
		// We return an empty UTXODeletion and io.EOF to signal the end of the stream
		// The empty UTXODeletion indicates an EOF where the eofMarker was written
		return &UTXODeletion{}, io.EOF
	}

	u := &UTXODeletion{
		Index: uint32(b[32]) | uint32(b[33])<<8 | uint32(b[34])<<16 | uint32(b[35])<<24,
	}

	copy(u.TxID[:], b[:32])

	return u, nil
}

// DeletionBytes returns the byte representation of the UTXODeletion.
// It serializes the transaction ID and output index into a byte slice.
// This is used for writing deletion records to persistent storage.
//
// Returns:
// - []byte: Serialized binary representation of the UTXODeletion
//
// The serialization format is as follows:
// - Bytes 0-31: Transaction ID (32 bytes)
// - Bytes 32-35: Output index (4 bytes, little-endian)
//
// This compact, fixed-size format ensures efficient storage and retrieval of deletion records.
// The method pre-allocates the required byte slice capacity for optimal performance.
// This serialized form is written to the deletions file to track spent outputs.
func (u *UTXODeletion) DeletionBytes() []byte {
	b := make([]byte, 0, 32+4)

	b = append(b, u.TxID[:]...)
	// Append little-endian index
	b = append(b, byte(u.Index), byte(u.Index>>8), byte(u.Index>>16), byte(u.Index>>24))

	return b
}

// String returns a string representation of the UTXODeletion.
// The format is "txid:index" which concisely identifies the specific output being spent.
// This is useful for debugging, logging, and human-readable representations.
//
// Returns:
// - string: Human-readable representation of the UTXODeletion in "txid:index" format
//
// This method provides a standardized string representation that matches conventional Bitcoin
// notation for referencing specific outputs. The transaction ID is displayed in its hexadecimal
// form, followed by a colon and the numeric index. This format is widely used in blockchain
// explorers and debugging tools, making it easy to cross-reference with other systems.
func (u *UTXODeletion) String() string {
	return fmt.Sprintf("%s:%d", u.TxID.String(), u.Index)
}
