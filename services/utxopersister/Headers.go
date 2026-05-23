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

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
)

// BlockIndex represents the index information for a block.
// It encapsulates critical metadata about a block including its hash, height, transaction count,
// header information, and optionally the coinbase transaction. This structure is used for efficient
// lookup and validation of blocks.
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

	// CoinbaseTx contains the coinbase transaction (optional, may be nil for legacy data)
	CoinbaseTx *bt.Tx
}

// Serialise writes the BlockIndex data to the provided writer.
// It serializes the block hash, height, transaction count, block header, and coinbase transaction
// in a defined format. This method is used when persisting block index information to storage.
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
// - Bytes 124-127: Coinbase tx length (4 bytes, little-endian) - 0 if no coinbase tx
// - Bytes 128+: Coinbase tx bytes (variable length, only if length > 0)
//
// This serialized representation contains all necessary information to reconstruct the BlockIndex
// and validate its integrity. The block header is serialized using the standard Bitcoin wire format.
// The coinbase transaction is optional and stored in extended format for forward compatibility.
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

	if err := bi.BlockHeader.ToWireBlockHeader().Serialize(writer); err != nil {
		return err
	}

	// Write coinbase transaction length and data
	var coinbaseTxBytes []byte
	if bi.CoinbaseTx != nil {
		coinbaseTxBytes = bi.CoinbaseTx.Bytes()
	}

	var coinbaseLenBytes [4]byte
	binary.LittleEndian.PutUint32(coinbaseLenBytes[:], uint32(len(coinbaseTxBytes)))

	if _, err := writer.Write(coinbaseLenBytes[:]); err != nil {
		return err
	}

	if len(coinbaseTxBytes) > 0 {
		if _, err := writer.Write(coinbaseTxBytes); err != nil {
			return err
		}
	}

	return nil
}

// NewUTXOHeaderFromReader creates a new BlockIndex from the provided reader.
// It deserializes block index data by reading the hash, height, transaction count, block header,
// and optionally the coinbase transaction. The function also checks for EOF markers to detect
// the end of a stream. Returns the BlockIndex and any error encountered during reading.
//
// Parameters:
// - reader: io.Reader from which to read the serialized BlockIndex data
// - isV1: true if reading V1 format (without coinbase), false for V2 format (with coinbase)
//
// Returns:
// - *BlockIndex: Deserialized BlockIndex, or empty BlockIndex if EOF marker is encountered
// - error: io.EOF if EOF marker is encountered, or any error during deserialization
//
// This method implements the inverse of the Serialise method, reading the block hash, height,
// transaction count, block header, and optionally coinbase transaction in sequence based on version.
//
// The method performs thorough error checking at each step of the deserialization process,
// returning detailed error information if any read operations fail or if the data format is invalid.
// It uses model.NewBlockHeaderFromBytes to convert the raw header bytes into a structured BlockHeader.
func NewUTXOHeaderFromReader(reader io.Reader, isV1 bool) (*BlockIndex, error) {
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

	// Read coinbase transaction if this is V2 format
	var coinbaseTx *bt.Tx
	if !isV1 {
		// V2 format includes coinbase transaction
		var coinbaseLenBytes [4]byte
		n, err := io.ReadFull(reader, coinbaseLenBytes[:])
		if err != nil {
			return nil, errors.NewProcessingError("Error reading coinbase length", err)
		}
		if n != 4 {
			return nil, errors.NewProcessingError("Expected 4 bytes for coinbase length, got %d", n)
		}

		coinbaseLen := binary.LittleEndian.Uint32(coinbaseLenBytes[:])
		if coinbaseLen > 0 {
			coinbaseTxBytes := make([]byte, coinbaseLen)
			n, err := io.ReadFull(reader, coinbaseTxBytes)
			if err != nil {
				return nil, errors.NewProcessingError("Error reading coinbase tx bytes", err)
			}
			if n != int(coinbaseLen) {
				return nil, errors.NewProcessingError("Expected %d coinbase tx bytes, got %d", coinbaseLen, n)
			}

			coinbaseTx, err = bt.NewTxFromBytes(coinbaseTxBytes)
			if err != nil {
				return nil, errors.NewProcessingError("Error parsing coinbase transaction", err)
			}
		}
	}

	return &BlockIndex{
		Hash:        hash,
		Height:      height,
		TxCount:     txCount,
		BlockHeader: blockHeader,
		CoinbaseTx:  coinbaseTx,
	}, nil
}

// String returns a string representation of the BlockIndex.
// The string includes the block hash, height, transaction count, previous hash, computed hash,
// and coinbase transaction information. This is useful for debugging, logging, and producing
// human-readable representations of block data.
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
// - Coinbase transaction ID (if present) or "none" if not available
//
// This method is primarily used for debugging, logging, and diagnostic purposes.
// The computed hash should match the block hash if the block data is valid and uncorrupted.
func (bi *BlockIndex) String() string {
	coinbaseInfo := "none"
	if bi.CoinbaseTx != nil {
		coinbaseInfo = bi.CoinbaseTx.String()
	}
	return fmt.Sprintf("Hash: %s, Height: %d, TxCount: %d, PreviousHash: %s, ComputedHash: %s, CoinbaseTx: %s", bi.Hash.String(), bi.Height, bi.TxCount, bi.BlockHeader.HashPrevBlock.String(), bi.BlockHeader.String(), coinbaseInfo)
}
