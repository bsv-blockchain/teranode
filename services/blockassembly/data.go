// Package blockassembly provides functionality for assembling Bitcoin blocks in Teranode.
package blockassembly

import (
	"encoding/binary"

	"github.com/bitcoin-sv/teranode/pkg/go-subtree"
	"github.com/libsv/go-bt/v2/chainhash"
)

// Data represents transaction metadata used in block assembly.
// This structure encapsulates essential transaction information needed for the block assembly process,
// including the transaction ID, fee amount, and size. It provides methods for serialization
// and deserialization to enable efficient storage and transmission between components.

type Data struct {
	// TxIDChainHash is the transaction ID hash
	TxIDChainHash chainhash.Hash

	// Fee represents the transaction fee
	Fee uint64

	// Size represents the transaction size in bytes
	Size uint64

	// Parents is a list of parent transaction hashes
	TxInpoints subtree.TxInpoints
}

// NewFromBytes deserializes a byte array into a Data structure.
//
// Parameters:
//   - bytes: Byte array containing serialized transaction metadata (at least 48 bytes)
//
// Returns:
//   - *Data: Parsed transaction metadata
//   - error: Any error encountered during deserialization
func NewFromBytes(bytes []byte) (*Data, error) {
	d := &Data{}

	// read first 32 bytes as txIDChainHash
	d.TxIDChainHash = chainhash.Hash(bytes[:32])

	// read next 8 bytes as fee
	d.Fee = binary.LittleEndian.Uint64(bytes[32:40])

	// read next 8 bytes as size
	d.Size = binary.LittleEndian.Uint64(bytes[40:48])

	// read remaining bytes as parents
	txInpoints, err := subtree.NewTxInpointsFromBytes(bytes[48:])
	if err != nil {
		return nil, err
	}

	d.TxInpoints = txInpoints

	return d, nil
}

// Bytes serializes the Data structure into a byte array.
// The resulting byte array contains the transaction ID hash (32 bytes),
// fee (8 bytes), and size (8 bytes) in little-endian format.
//
// Returns:
//   - []byte: Serialized transaction metadata
func (d *Data) Bytes() []byte {
	bytes := make([]byte, 0, 256)

	// write txIDChainHash
	bytes = append(bytes, d.TxIDChainHash[:]...)

	// write 8 bytes for fee
	b64 := make([]byte, 8)
	binary.LittleEndian.PutUint64(b64, d.Fee)
	bytes = append(bytes, b64...)

	// write 8 bytes for size
	binary.LittleEndian.PutUint64(b64, d.Size)
	bytes = append(bytes, b64...)

	// write txInpoints
	txInpointsBytes, err := d.TxInpoints.Serialize()
	if err != nil {
		return nil
	}

	bytes = append(bytes, txInpointsBytes...)

	return bytes
}
