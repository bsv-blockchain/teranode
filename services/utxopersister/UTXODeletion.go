// Package utxopersister provides functionality for managing UTXO (Unspent Transaction Output) persistence.
package utxopersister

import (
	"bytes"
	"fmt"
	"io"

	"github.com/libsv/go-bt/v2/chainhash"
)

// UTXODeletion represents a deletion of an Unspent Transaction Output.
type UTXODeletion struct {
	// TxID contains the transaction ID
	TxID chainhash.Hash

	// Index represents the output index in the transaction
	Index uint32
}

// NewUTXODeletionFromReader creates a new UTXODeletion from the provided reader.
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
func (u *UTXODeletion) DeletionBytes() []byte {
	b := make([]byte, 0, 32+4)

	b = append(b, u.TxID[:]...)
	// Append little-endian index
	b = append(b, byte(u.Index), byte(u.Index>>8), byte(u.Index>>16), byte(u.Index>>24))

	return b
}

// String returns a string representation of the UTXODeletion.
func (u *UTXODeletion) String() string {
	return fmt.Sprintf("%s:%d", u.TxID.String(), u.Index)
}
