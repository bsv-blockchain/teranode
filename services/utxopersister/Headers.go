// Package utxopersister provides functionality for managing UTXO (Unspent Transaction Output) persistence.
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
// It returns an error if the write operation fails.
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
// It returns the BlockIndex and any error encountered during reading.
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
func (bi *BlockIndex) String() string {
	return fmt.Sprintf("Hash: %s, Height: %d, TxCount: %d, PreviousHash: %s, ComputedHash: %s", bi.Hash.String(), bi.Height, bi.TxCount, bi.BlockHeader.HashPrevBlock.String(), bi.BlockHeader.String())
}
