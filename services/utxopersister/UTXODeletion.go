// Package utxopersister creates and maintains up-to-date Unspent Transaction Output (UTXO) file sets
// for each block in the Teranode blockchain. Its primary function is to process the output of the
// Block Persister service (utxo-additions and utxo-deletions) and generate complete UTXO set files.
// The resulting UTXO set files can be exported and used to initialize the UTXO store in new Teranode instances.
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
func (u *UTXODeletion) String() string {
	return fmt.Sprintf("%s:%d", u.TxID.String(), u.Index)
}
