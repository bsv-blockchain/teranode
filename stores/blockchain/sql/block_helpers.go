// Package sql implements the blockchain.Store interface using SQL database backends.
// It provides concrete SQL-based implementations for all blockchain operations
// defined in the interface, with support for different SQL engines.
//
// This file contains helper functions for processing block-related database operations.
// These utilities handle common tasks such as scanning database rows, converting between
// database representations and model objects, and processing query results efficiently.
package sql

import (
	"database/sql"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/model/time"
	"github.com/bsv-blockchain/teranode/util"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// processBlockRows processes SQL query results for blocks and converts them to model.BlockInfo objects.
// If includeHash is true, it expects the hash field to be included in the query results.
func (s *SQL) processBlockRows(rows *sql.Rows) ([]*model.BlockInfo, error) {
	blockInfos := make([]*model.BlockInfo, 0)

	for rows.Next() {
		info, err := s.scanBlockRow(rows)
		if err != nil {
			return nil, err
		}

		blockInfos = append(blockInfos, info)
	}

	return blockInfos, nil
}

// scanBlockRow scans a single row from the SQL result set and converts it to a BlockInfo object.
func (s *SQL) scanBlockRow(rows *sql.Rows) (*model.BlockInfo, error) {
	var (
		hashPrevBlock  []byte
		hashMerkleRoot []byte
		coinbaseBytes  []byte
		nBits          []byte
		seenAt         time.CustomTime
	)

	header := &model.BlockHeader{}
	info := &model.BlockInfo{}

	err := rows.Scan(
		&header.Version,
		&header.Timestamp,
		&nBits,
		&header.Nonce,
		&hashPrevBlock,
		&hashMerkleRoot,
		&info.TransactionCount,
		&info.Size,
		&coinbaseBytes,
		&info.Height,
		&seenAt,
	)

	if err != nil {
		return nil, errors.NewStorageError("failed to scan row", err)
	}

	// Process nBits
	bits, _ := model.NewNBitFromSlice(nBits)
	header.Bits = *bits

	// Process previous block hash
	header.HashPrevBlock, err = chainhash.NewHash(hashPrevBlock)
	if err != nil {
		return nil, errors.NewProcessingError("failed to convert hashPrevBlock", err)
	}

	// Process merkle root hash
	header.HashMerkleRoot, err = chainhash.NewHash(hashMerkleRoot)
	if err != nil {
		return nil, errors.NewProcessingError("failed to convert hashMerkleRoot", err)
	}

	// Set the block header bytes
	info.BlockHeader = header.Bytes()

	// Process coinbase transaction if available
	if len(coinbaseBytes) > 0 {
		// Parse coinbase transaction
		coinbaseTx, err := bt.NewTxFromBytes(coinbaseBytes)
		if err != nil {
			return nil, errors.NewProcessingError("failed to convert coinbaseTx", err)
		}

		// Calculate coinbase value
		info.CoinbaseValue = 0
		for _, output := range coinbaseTx.Outputs {
			info.CoinbaseValue += output.Satoshis
		}

		// Extract miner information - handle errors gracefully for invalid blocks
		info.Miner, err = util.ExtractCoinbaseMiner(coinbaseTx)
		if err != nil {
			// For invalid blocks, the coinbase may be malformed, so we just log and continue
			s.logger.Debugf("failed to extract miner (block may be invalid): %v", err)
			info.Miner = ""
		}
	}

	// Set the timestamp
	info.SeenAt = timestamppb.New(seenAt.Time)

	return info, nil
}

// processFullBlockRows processes SQL query results and converts them to model.Block objects.
// It iterates through all rows and constructs complete Block objects with all fields populated.
func (s *SQL) processFullBlockRows(rows *sql.Rows) ([]*model.Block, error) {
	blocks := make([]*model.Block, 0)

	for rows.Next() {
		block, err := s.scanFullBlockRow(rows)
		if err != nil {
			return nil, err
		}
		blocks = append(blocks, block)
	}

	return blocks, nil
}

// scanFullBlockRow scans a single row and constructs a complete Block object.
// Expects columns in order: ID, version, block_time, n_bits, nonce, previous_hash, merkle_root,
//
//	tx_count, size_in_bytes, coinbase_tx, subtree_count, subtrees, height
func (s *SQL) scanFullBlockRow(rows *sql.Rows) (*model.Block, error) {
	var (
		subtreeCount     uint64
		transactionCount uint64
		sizeInBytes      uint64
		subtreeBytes     []byte
		hashPrevBlock    []byte
		hashMerkleRoot   []byte
		coinbaseTx       []byte
		height           uint32
		nBits            []byte
	)

	block := &model.Block{
		Header: &model.BlockHeader{},
	}

	err := rows.Scan(
		&block.ID,
		&block.Header.Version,
		&block.Header.Timestamp,
		&nBits,
		&block.Header.Nonce,
		&hashPrevBlock,
		&hashMerkleRoot,
		&transactionCount,
		&sizeInBytes,
		&coinbaseTx,
		&subtreeCount,
		&subtreeBytes,
		&height,
	)
	if err != nil {
		return nil, errors.NewStorageError("failed to scan row", err)
	}

	bits, _ := model.NewNBitFromSlice(nBits)
	block.Header.Bits = *bits

	block.Header.HashPrevBlock, err = chainhash.NewHash(hashPrevBlock)
	if err != nil {
		return nil, errors.NewStorageError("failed to convert hashPrevBlock", err)
	}

	block.Header.HashMerkleRoot, err = chainhash.NewHash(hashMerkleRoot)
	if err != nil {
		return nil, errors.NewStorageError("failed to convert hashMerkleRoot", err)
	}

	block.TransactionCount = transactionCount
	block.SizeInBytes = sizeInBytes

	if len(coinbaseTx) > 0 {
		block.CoinbaseTx, err = bt.NewTxFromBytes(coinbaseTx)
		if err != nil {
			return nil, errors.NewProcessingError("failed to convert coinbaseTx", err)
		}
	}

	err = block.SubTreesFromBytes(subtreeBytes)
	if err != nil {
		return nil, errors.NewProcessingError("failed to convert subtrees", err)
	}

	block.Height = height

	return block, nil
}
