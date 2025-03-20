// Package utxopersister creates and maintains up-to-date Unspent Transaction Output (UTXO) file sets
// for each block in the Teranode blockchain. Its primary function is to process the output of the
// Block Persister service (utxo-additions and utxo-deletions) and generate complete UTXO set files.
// The resulting UTXO set files can be exported and used to initialize the UTXO store in new Teranode instances.
package utxopersister

import (
	"bytes"
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
func NewUTXOHeaderFromReader(reader io.Reader) (*BlockIndex, error) {
	hash := &chainhash.Hash{}
	if n, err := io.ReadFull(reader, hash[:]); err != nil || n != 32 {
		return nil, errors.NewProcessingError("Expected 32 bytes, got %d", n, err)
	}

	if bytes.Equal(hash[:], EOFMarker) {
		// We return an empty BlockIndex and io.EOF to signal the end of the stream
		// The empty BlockIndex indicates an EOF where the eofMarker was written
		return &BlockIndex{}, io.EOF
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
func (bi *BlockIndex) String() string {
	return fmt.Sprintf("Hash: %s, Height: %d, TxCount: %d, PreviousHash: %s, ComputedHash: %s", bi.Hash.String(), bi.Height, bi.TxCount, bi.BlockHeader.HashPrevBlock.String(), bi.BlockHeader.String())
}
