// Package utxopersister creates and maintains up-to-date Unspent Transaction Output (UTXO) file sets
// for each block in the Teranode blockchain. Its primary function is to process the output of the
// Block Persister service (utxo-additions and utxo-deletions) and generate complete UTXO set files.
// The resulting UTXO set files can be exported and used to initialize the UTXO store in new Teranode instances.
//
// This file defines the core data structures for representing UTXOs and their serialization format.
// It provides methods for converting between the in-memory representation and binary formats used for
// persistence. The serialization format is optimized for storage efficiency while maintaining
// all necessary information for UTXO validation and transaction processing.
package utxopersister

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
)

// UTXOWrapper wraps transaction outputs with additional metadata.
// It encapsulates a transaction ID, block height, coinbase flag, and a collection of UTXOs
// that belong to a single transaction.
//
// UTXOWrapper provides an efficient way to store and retrieve multiple outputs from a single
// transaction without duplicating common data such as the transaction ID and block height.
// It is designed to minimize storage requirements while maintaining all information required
// for validation and transaction processing. The wrapper approach also improves performance
// when multiple outputs from the same transaction are processed together.
type UTXOWrapper struct {
	// TxID contains the transaction ID
	TxID chainhash.Hash

	// Height represents the block height
	Height uint32

	// Coinbase indicates if this is a coinbase transaction
	Coinbase bool

	// UTXOs contains the unspent transaction outputs
	UTXOs []*UTXO
}

// UTXO represents an Unspent Transaction Output.
// It contains the essential components of a Bitcoin transaction output: index, value, and script.
//
// The UTXO struct encapsulates the minimal information required to validate spending of an output:
// - Index: Position of the output within the transaction (vout)
// - Value: Amount of satoshis stored in this output
// - Script: Locking script (scriptPubKey) that must be satisfied to spend this output
//
// This representation balances storage efficiency with quick access to critical validation data.
// The parent transaction information (txid, block height) is stored in the UTXOWrapper to avoid
// redundancy when multiple outputs from the same transaction are present.
type UTXO struct {
	// Index represents the output index in the transaction
	Index uint32

	// Value represents the amount in satoshis
	Value uint64

	// Script contains the locking script
	Script []byte
}

// Bytes returns the byte representation of the UTXOWrapper.
// The serialized format includes the transaction ID, encoded height/coinbase flag,
// number of UTXOs, and the serialized UTXOs themselves.
// This is used for persistent storage of UTXOs.
//
// Returns:
// - []byte: Serialized binary representation of the UTXOWrapper
//
// The serialization format is as follows:
// - Bytes 0-31: Transaction ID (32 bytes)
// - Bytes 32-35: Encoded height and coinbase flag (4 bytes)
//   - The height is shifted left by 1 bit to leave space for the coinbase flag
//   - The least significant bit indicates whether this is a coinbase transaction
//
// - Bytes 36-39: Number of UTXOs (4 bytes)
// - Bytes 40+: Serialized UTXOs (variable length)
//
// This format is space-efficient and enables quick parsing of the relevant information
// when reading from storage. The encoding of height and coinbase flag into a single
// 4-byte value optimizes storage usage.
func (uw *UTXOWrapper) Bytes() []byte {
	size := 32 + 4 + 4 // TXID + encoded height / coinbase + len(UTXOs)
	for _, u := range uw.UTXOs {
		size += 4 + 8 + 4 + len(u.Script) // index + value + script length + script
	}

	b := make([]byte, 0, size)

	b = append(b, uw.TxID[:]...)

	// To store the height and coinbase flag in a single uint32:
	// 1.	Shift the height left by 1 bit to leave space for the flag.
	// 2.	Set the flag as the least significant bit.

	var flag uint32
	if uw.Coinbase {
		flag = 1
	}

	encodedValue := (uw.Height << 1) | flag

	// Append the encoded height/coinbase
	b = append(b, byte(encodedValue), byte(encodedValue>>8), byte(encodedValue>>16), byte(encodedValue>>24))

	// Append the number of UTXOs
	b = append(b, byte(len(uw.UTXOs)), byte(len(uw.UTXOs)>>8), byte(len(uw.UTXOs)>>16), byte(len(uw.UTXOs)>>24))

	for _, u := range uw.UTXOs {
		b = append(b, u.Bytes()...)
	}

	return b
}

// DeletionBytes returns the byte representation for deletion of a specific output.
// It creates a fixed-size array containing the transaction ID and the output index.
// This is used when marking a UTXO as spent.
//
// Parameters:
// - index: The output index within the transaction to be marked as spent
//
// Returns:
// - [36]byte: Fixed-size array with deletion information
//   - Bytes 0-31: Transaction ID (32 bytes)
//   - Bytes 32-35: Output index (4 bytes)
//
// This format provides a compact, fixed-size representation for UTXO deletions,
// which is essential for efficiently tracking spent outputs. The fixed size enables
// optimized processing when applying deletions to a UTXO set.
func (uw *UTXOWrapper) DeletionBytes(index uint32) [36]byte {
	var b [36]byte

	copy(b[:], uw.TxID[:])
	b[32] = byte(index)
	b[33] = byte(index >> 8)
	b[34] = byte(index >> 16)
	b[35] = byte(index >> 24)

	return b
}

// NewUTXOWrapperFromReader creates a new UTXOWrapper from the provided reader.
// It deserializes the UTXOWrapper data from a byte stream, checking for EOF markers
// and properly decoding the height, coinbase flag, and UTXOs.
// Returns the UTXOWrapper and any error encountered during deserialization.
//
// Parameters:
// - ctx: Context for controlling the deserialization process, allowing cancellation
// - r: io.Reader from which to read the serialized UTXOWrapper data
//
// Returns:
// - *UTXOWrapper: Deserialized UTXOWrapper, or empty wrapper if EOF marker is encountered
// - error: io.EOF if EOF marker is encountered, or any error during deserialization
//
// This method implements the inverse of the Bytes() serialization method.
// It reads the transaction ID, encoded height/coinbase flag, number of UTXOs,
// and each UTXO in sequence. If the transaction ID matches the EOF marker (32 zero bytes),
// it returns an empty UTXOWrapper with io.EOF error to signal the end of the stream.
// This supports reading a continuous stream of UTXOWrappers until EOF is reached.
//
// The method handles context cancellation, checking if the context is done
// before proceeding with potentially blocking read operations.
func NewUTXOWrapperFromReader(ctx context.Context, r io.Reader) (*UTXOWrapper, error) {
	uw := &UTXOWrapper{}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		n, err := io.ReadFull(r, uw.TxID[:])
		if err != nil {
			if err == io.EOF {
				return nil, io.EOF
			}

			return nil, errors.NewStorageError("failed to read txid, expected 32 bytes got %d", n, err)
		}

		// Read the encoded height/coinbase + number of UTXOs
		var b [8]byte
		if n, err := io.ReadFull(r, b[:]); err != nil || n != 8 {
			return nil, errors.NewStorageError("failed to read height and number of utxos, expected 8 bytes got %d", n, err)
		}

		encodedHeight := uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
		numUTXOs := uint32(b[4]) | uint32(b[5])<<8 | uint32(b[6])<<16 | uint32(b[7])<<24

		uw.Height = encodedHeight >> 1
		uw.Coinbase = (encodedHeight & 1) == 1
		uw.UTXOs = make([]*UTXO, numUTXOs)

		for i := uint32(0); i < numUTXOs; i++ {
			if uw.UTXOs[i], err = NewUTXOFromReader(r); err != nil {
				return nil, err
			}
		}
	}

	return uw, nil
}

// NewUTXOWrapperFromBytes creates a new UTXOWrapper from the provided bytes.
// It's a convenience wrapper around NewUTXOWrapperFromReader that uses a bytes.Reader.
// Returns the UTXOWrapper and any error encountered during deserialization.
//
// Parameters:
// - b: Byte slice containing the serialized UTXOWrapper data
//
// Returns:
// - *UTXOWrapper: Deserialized UTXOWrapper, or empty wrapper if EOF marker is encountered
// - error: Any error encountered during deserialization
//
// This is a convenience method that creates a bytes.Reader from the provided byte slice
// and calls NewUTXOWrapperFromReader with a background context. It's useful for
// deserializing UTXOWrapper data from memory or smaller in-memory buffers rather than
// reading directly from storage or network streams.
func NewUTXOWrapperFromBytes(b []byte) (*UTXOWrapper, error) {
	return NewUTXOWrapperFromReader(context.Background(), bytes.NewReader(b))
}

// String returns a string representation of the UTXOWrapper.
// The string includes the transaction ID, height, coinbase status, number of outputs,
// and a formatted representation of each UTXO in the wrapper.
// This is useful for debugging and logging purposes.
//
// Returns:
// - string: Human-readable representation of the UTXOWrapper
//
// The formatted string contains:
// - Transaction ID in hexadecimal format
// - Block height
// - Coinbase flag (if true)
// - Number of outputs
// - Indented list of each UTXO's string representation
//
// This method is primarily used for debugging, logging, and providing human-readable
// displays of UTXO data during development or troubleshooting.
func (uw *UTXOWrapper) String() string {
	s := strings.Builder{}

	if uw.Coinbase {
		s.WriteString(fmt.Sprintf("%s - (height %d coinbase) - %d output(s):\n", uw.TxID.String(), uw.Height, len(uw.UTXOs)))
	} else {
		s.WriteString(fmt.Sprintf("%s - (height %d) - %d output(s):\n", uw.TxID.String(), uw.Height, len(uw.UTXOs)))
	}

	for _, u := range uw.UTXOs {
		s.WriteString(fmt.Sprintf("\t%v\n", u))
	}

	return s.String()
}

// NewUTXOFromReader creates a new UTXO from the provided reader.
// It deserializes a UTXO by reading the index, value, script length, and script bytes.
// Returns the UTXO and any error encountered during deserialization.
//
// Parameters:
// - r: io.Reader from which to read the serialized UTXO data
//
// Returns:
// - *UTXO: Deserialized UTXO instance
// - error: Any error encountered during deserialization
//
// The deserialization format is as follows:
// - Bytes 0-3: Output index (4 bytes)
// - Bytes 4-11: Value in satoshis (8 bytes)
// - Bytes 12-15: Script length (4 bytes)
// - Bytes 16+: Script bytes (variable length based on script length)
//
// This method implements the inverse of the UTXO.Bytes() serialization method.
// All integers are decoded using little-endian byte order.
func NewUTXOFromReader(r io.Reader) (*UTXO, error) {
	// Read all the fixed size fields
	var b [16]byte // index + value + length of script

	if _, err := io.ReadFull(r, b[:]); err != nil {
		return nil, err
	}

	u := &UTXO{
		Index: uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24,
		Value: uint64(b[4]) | uint64(b[5])<<8 | uint64(b[6])<<16 | uint64(b[7])<<24 | uint64(b[8])<<32 | uint64(b[9])<<40 | uint64(b[10])<<48 | uint64(b[11])<<56,
	}

	// Read the script length
	l := uint32(b[12]) | uint32(b[13])<<8 | uint32(b[14])<<16 | uint32(b[15])<<24

	// Read the script
	u.Script = make([]byte, l)

	if _, err := io.ReadFull(r, u.Script); err != nil {
		return nil, err
	}

	return u, nil
}

// func NewUTXOFromBytes(b []byte) (*UTXO, error) {
// 	return NewUTXOFromReader(bytes.NewReader(b))
// }

// Bytes returns the byte representation of the UTXO.
// The serialized format includes the index (4 bytes), value (8 bytes), script length (4 bytes),
// and the script itself. All integers are serialized in little-endian format.
// This is used for persistent storage of UTXOs.
//
// Returns:
// - []byte: Serialized binary representation of the UTXO
//
// The serialization format is as follows:
// - Bytes 0-3: Output index (4 bytes)
// - Bytes 4-11: Value in satoshis (8 bytes)
// - Bytes 12-15: Script length (4 bytes)
// - Bytes 16+: Script bytes (variable length)
//
// This format is designed for storage efficiency while maintaining all necessary
// information to validate and spend the output. The method pre-allocates the
// required byte slice capacity for optimal performance.
func (u *UTXO) Bytes() []byte {
	b := make([]byte, 0, 4+8+4+len(u.Script)) // index + value + length of script + script

	// Append little-endian index
	b = append(b, byte(u.Index), byte(u.Index>>8), byte(u.Index>>16), byte(u.Index>>24))
	// Append little-endian value
	b = append(b, byte(u.Value), byte(u.Value>>8), byte(u.Value>>16), byte(u.Value>>24), byte(u.Value>>32), byte(u.Value>>40), byte(u.Value>>48), byte(u.Value>>56))

	// Append little-endian script length

	// TODO: consider logging or returning an error
	scriptLen, err := safeconversion.IntToInt32(len(u.Script))
	if err != nil {
		return nil
	}

	b = append(b, byte(scriptLen), byte(scriptLen>>8), byte(scriptLen>>16), byte(scriptLen>>24))

	// Append script
	b = append(b, u.Script...)

	return b
}

// String returns a string representation of the UTXO.
// It includes the output index, value in satoshis, and a hexadecimal representation of the script.
// This is useful for debugging and logging purposes.
func (u *UTXO) String() string {
	return fmt.Sprintf("%d: %d - %x", u.Index, u.Value, u.Script)
}
