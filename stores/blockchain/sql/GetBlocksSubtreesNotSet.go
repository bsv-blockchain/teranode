// Package sql implements the blockchain.Store interface using SQL database backends.
// It provides concrete SQL-based implementations for all blockchain operations
// defined in the interface, with support for different SQL engines.
//
// This file implements the GetBlocksSubtreesNotSet method, which retrieves blocks that
// have been stored in the blockchain database but have not yet had their subtrees properly
// processed and set. In Teranode's architecture for BSV, subtrees are a critical data
// structure that replaces the traditional mempool concept, organizing transactions into
// hierarchical structures for efficient validation and mining. The implementation uses a
// straightforward SQL query to identify blocks with the subtrees_set flag set to false,
// which typically indicates blocks that require additional processing to extract and
// organize their transaction data into the subtree format. This method supports Teranode's
// high-throughput transaction processing by ensuring all blocks are properly integrated
// into the subtree-based transaction management system.
package sql

import (
	"context"
	"database/sql"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/util/tracing"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

// GetBlocksSubtreesNotSet retrieves blocks whose subtrees have not been properly processed.
// This implements a specialized blockchain query method not directly defined in the Store interface.
//
// In Teranode's architecture for BSV, subtrees are a critical data structure that replaces
// the traditional mempool concept, organizing transactions into hierarchical structures for
// efficient validation and mining. This method identifies blocks that have been stored in
// the blockchain database but have not yet had their transactions properly organized into
// the subtree format (subtrees_set flag is false).
//
// This functionality serves several important purposes:
//   - Supports post-processing of blocks to extract and organize transaction data
//   - Ensures all blocks are properly integrated into the subtree-based transaction management
//   - Facilitates efficient transaction validation and mining operations
//   - Helps maintain consistency between block storage and transaction processing systems
//
// The implementation uses a straightforward SQL query to retrieve blocks with subtrees_set=false,
// ordered by height to process them in chronological order. It leverages the shared
// getBlocksWithQuery helper method to handle the common block reconstruction logic used
// across multiple query methods in the package.
//
// Parameters:
//   - ctx: Context for the database operation, allowing for cancellation and timeouts
//
// Returns:
//   - []*model.Block: An array of complete block objects whose subtrees need to be processed
//   - error: Any error encountered during retrieval, specifically:
//     - StorageError for database errors or processing failures
func (s *SQL) GetBlocksSubtreesNotSet(ctx context.Context) ([]*model.Block, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "sql:GetBlocksSubtreesNotSet")
	defer deferFn()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	q := `
		SELECT
		 b.ID
	  ,b.version
		,b.block_time
		,b.n_bits
		,b.nonce
		,b.previous_hash
		,b.merkle_root
	  ,b.tx_count
		,b.size_in_bytes
		,b.coinbase_tx
		,b.subtree_count
		,b.subtrees
		,b.height
		FROM blocks b
		WHERE subtrees_set = false
		ORDER BY height ASC
	`

	return s.getBlocksWithQuery(ctx, q)
}

// getBlocksWithQuery is a shared helper method that executes a SQL query and reconstructs
// block objects from the results. This method centralizes the common block reconstruction
// logic used across multiple query methods in the package.
//
// This helper method significantly reduces code duplication by providing a reusable
// implementation for converting database rows into fully populated block objects. It handles
// all the complex conversions between raw database types and the structured blockchain types,
// including binary-to-hash conversions, difficulty bits parsing, and subtree deserialization.
//
// The implementation follows a systematic approach to block reconstruction:
// 1. Executes the provided SQL query against the database
// 2. Iterates through the result rows, scanning each into appropriate variables
// 3. Converts raw binary data into structured blockchain types (hashes, transactions, etc.)
// 4. Handles special fields like difficulty bits and subtrees with appropriate conversions
// 5. Builds a complete array of block objects with all fields properly populated
//
// This centralized approach ensures consistent error handling and data conversion across
// all block retrieval methods, improving code maintainability and reducing the risk of
// inconsistencies between different query implementations.
//
// Parameters:
//   - ctx: Context for the database operation, allowing for cancellation and timeouts
//   - q: The SQL query string to execute, which must select all required block fields
//
// Returns:
//   - []*model.Block: An array of complete block objects reconstructed from the query results
//   - error: Any error encountered during query execution or block reconstruction, specifically:
//     - Database errors from query execution
//     - ProcessingError for data conversion failures
func (s *SQL) getBlocksWithQuery(ctx context.Context, q string) ([]*model.Block, error) {
	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	blocks := make([]*model.Block, 0)

	for rows.Next() {
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

		if err = rows.Scan(
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
		); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return blocks, nil
			}

			return nil, err
		}

		bits, _ := model.NewNBitFromSlice(nBits)
		block.Header.Bits = *bits

		block.Header.HashPrevBlock, err = chainhash.NewHash(hashPrevBlock)
		if err != nil {
			return nil, errors.NewProcessingError("failed to convert hashPrevBlock", err)
		}

		block.Header.HashMerkleRoot, err = chainhash.NewHash(hashMerkleRoot)
		if err != nil {
			return nil, errors.NewProcessingError("failed to convert hashMerkleRoot", err)
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

		blocks = append(blocks, block)
	}

	return blocks, nil
}
