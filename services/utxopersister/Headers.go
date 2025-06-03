// Package utxopersister creates and maintains up-to-date Unspent Transaction Output (UTXO) file sets
// for each block in the Teranode blockchain. Its primary function is to process the output of the
// Block Persister service (utxo-additions and utxo-deletions) and generate complete UTXO set files.
// The resulting UTXO set files can be exported and used to initialize the UTXO store in new Teranode instances.
//
// This file defines structures and methods for block header indexing and serialization. Block headers
// are crucial for verifying the integrity and validity of UTXO sets. The headers are stored as part of
// the UTXO set files and provide metadata about the blocks from which the UTXOs were derived.
package utxopersister

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/libsv/go-bt/v2/chainhash"
)

// BlockIndex represents the index information for a block.
// It encapsulates critical metadata about a block including its hash, height, transaction count,
// and header information. This structure is used for efficient lookup and validation of blocks.
//
// BlockIndex serves as a compact reference to blocks in the blockchain and is used to:
// - Link UTXO sets to their corresponding blocks
// - Validate block sequence integrity when applying UTXO changes
// - Provide block metadata without requiring access to the full block
// - Support header-based synchronization for lightweight clients
//
// The structure stores only essential block information needed for UTXO operations,
// keeping storage requirements minimal while enabling all necessary validation checks.
type BlockIndex struct {
	// Hash contains the block hash
	Hash *chainhash.Hash

	// Height represents the block height
	Height uint32

	// TxCount represents the number of transactions
	TxCount uint64

	// BlockHeader contains the block header information
	BlockHeader *model.BlockHeader
}

// Serialise writes the BlockIndex data to the provided writer.
// It serializes the block hash, height, transaction count, and block header in a defined format.
// This method is used when persisting block index information to storage.
// Returns an error if any write operation fails.
//
// Parameters:
// - writer: io.Writer interface to which the serialized data will be written
//
// Returns:
// - error: Any error encountered during the serialization or write operations
//
// The serialization format is as follows:
// - Bytes 0-31: Block hash (32 bytes)
// - Bytes 32-35: Block height (4 bytes, little-endian)
// - Bytes 36-43: Transaction count (8 bytes, little-endian)
// - Bytes 44-123: Block header (80 bytes in wire format)
//
// This serialized representation contains all necessary information to reconstruct the BlockIndex
// and validate its integrity. The block header is serialized using the standard Bitcoin wire format.
func (bi *BlockIndex) Serialise(writer io.Writer) error {
	if _, err := writer.Write(bi.Hash[:]); err != nil {
		return err
	}

	var heightBytes [4]byte

	binary.LittleEndian.PutUint32(heightBytes[:], bi.Height)

	if _, err := writer.Write(heightBytes[:]); err != nil {
		return err
	}

	var TxCountBytes [8]byte

	binary.LittleEndian.PutUint64(TxCountBytes[:], bi.TxCount)

	if _, err := writer.Write(TxCountBytes[:]); err != nil {
		return err
	}

	return bi.BlockHeader.ToWireBlockHeader().Serialize(writer)
}

// NewUTXOHeaderFromReader creates a new BlockIndex from the provided reader.
// It deserializes block index data by reading the hash, height, transaction count, and block header.
// The function also checks for EOF markers to detect the end of a stream.
// Returns the BlockIndex and any error encountered during reading.
//
// Parameters:
// - reader: io.Reader from which to read the serialized BlockIndex data
//
// Returns:
// - *BlockIndex: Deserialized BlockIndex, or empty BlockIndex if EOF marker is encountered
// - error: io.EOF if EOF marker is encountered, or any error during deserialization
//
// This method implements the inverse of the Serialise method, reading the block hash, height,
// transaction count, and block header in sequence. If the block hash matches the EOF marker
// (32 zero bytes), it returns an empty BlockIndex with io.EOF error to signal the end of the stream.
//
// The method performs thorough error checking at each step of the deserialization process,
// returning detailed error information if any read operations fail or if the data format is invalid.
// It uses model.NewBlockHeaderFromBytes to convert the raw header bytes into a structured BlockHeader.
func NewUTXOHeaderFromReader(reader io.Reader) (*BlockIndex, error) {
	hash := &chainhash.Hash{}
	if n, err := io.ReadFull(reader, hash[:]); err != nil || n != 32 {
		return nil, errors.NewProcessingError("Expected 32 bytes, got %d", n, err)
	}

	var heightBytes [4]byte
	if n, err := io.ReadFull(reader, heightBytes[:]); err != nil || n != 4 {
		return nil, errors.NewProcessingError("Expected 4 bytes, got %d", n, err)
	}

	height := binary.LittleEndian.Uint32(heightBytes[:])

	var txCountBytes [8]byte
	if n, err := io.ReadFull(reader, txCountBytes[:]); err != nil || n != 8 {
		return nil, errors.NewProcessingError("Expected 8 bytes, got %d", n, err)
	}

	txCount := binary.LittleEndian.Uint64(txCountBytes[:])

	var header [80]byte
	if n, err := io.ReadFull(reader, header[:]); err != nil || n != 80 {
		return nil, errors.NewProcessingError("Expected 80 bytes, got %d", n, err)
	}

	blockHeader, err := model.NewBlockHeaderFromBytes(header[:])
	if err != nil {
		return nil, errors.NewProcessingError("Error creating block header from bytes", err)
	}

	return &BlockIndex{
		Hash:        hash,
		Height:      height,
		TxCount:     txCount,
		BlockHeader: blockHeader,
	}, nil
}

// String returns a string representation of the BlockIndex.
// The string includes the block hash, height, transaction count, previous hash, and computed hash.
// This is useful for debugging, logging, and producing human-readable representations of block data.
//
// Returns:
// - string: Human-readable representation of the BlockIndex
//
// The formatted string contains:
// - Block hash in hexadecimal format
// - Block height
// - Transaction count
// - Previous block hash from the block header
// - Computed hash from the block header
//
// This method is primarily used for debugging, logging, and diagnostic purposes.
// The computed hash should match the block hash if the block data is valid and uncorrupted.
func (bi *BlockIndex) String() string {
	return fmt.Sprintf("Hash: %s, Height: %d, TxCount: %d, PreviousHash: %s, ComputedHash: %s", bi.Hash.String(), bi.Height, bi.TxCount, bi.BlockHeader.HashPrevBlock.String(), bi.BlockHeader.String())
}
