// Package sql implements the blockchain.Store interface using SQL database backends.
// It provides concrete SQL-based implementations for all blockchain operations
// defined in the interface, with support for different SQL engines.
//
// This file implements the GetBlocksByHeight method, which retrieves full blocks
// within a specified height range. The implementation uses an optimized SQL query
// to retrieve all block data including headers, subtrees, transactions, and metadata
// in a single database operation. This is particularly useful for efficient block
// range processing and subtree search operations where multiple consecutive blocks
// need to be examined.
package sql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/util/tracing"
)

// GetBlocksByHeight retrieves full blocks within a specified height range.
// This implements the blockchain.Store.GetBlocksByHeight interface method.
//
// This method efficiently retrieves complete block data including headers, subtrees,
// and transaction metadata for all blocks within the specified height range. Unlike
// GetBlockHeadersByHeight which only returns header data, this method provides the
// full block information needed for operations like subtree searching and detailed
// block analysis.
//
// The implementation executes a single optimized SQL query to retrieve all blocks
// within the specified range, filtering out invalid blocks to ensure only valid chain
// data is returned. The results are ordered by ascending height, providing a chronological
// view of the blockchain within the requested range.
//
// For each block, the method constructs complete Block objects containing:
// - Block headers with consensus-critical fields
// - Subtree hashes for merkle tree operations
// - Transaction count and size metadata
// - Coinbase transaction data
// - Block height and internal identifiers
//
// This method is particularly optimized for:
// - Subtree searching across block ranges
// - Bulk block processing operations
// - Chain analysis requiring full block data
// - Reducing database round trips for range operations
//
// Parameters:
//   - ctx: Context for the database operation, allowing for cancellation and timeouts
//   - startHeight: The lower bound of the height range (inclusive)
//   - endHeight: The upper bound of the height range (inclusive)
//
// Returns:
//   - []*model.Block: Slice of complete blocks within the specified height range
//   - error: Any error encountered during retrieval, specifically:
//   - StorageError for database access or query execution errors
//   - ProcessingError for data deserialization errors
//   - nil if the operation was successful (even if no blocks were found)
func (s *SQL) GetBlocksByHeight(ctx context.Context, startHeight, endHeight uint32) ([]*model.Block, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "sql:GetBlocksByHeight")
	defer deferFn()

	// Try to get from response cache using derived cache key
	// Use operation-prefixed key to be consistent with other operations
	cacheID := chainhash.HashH([]byte(fmt.Sprintf("GetBlocksByHeight-%d-%d", startHeight, endHeight)))

	cached := s.responseCache.Get(cacheID)
	if cached != nil {
		if blocks, ok := cached.Value().([]*model.Block); ok {
			return blocks, nil
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	capacity := max(1, endHeight-startHeight+1)
	blocks := make([]*model.Block, 0, capacity)

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
		WHERE id IN (
			SELECT id FROM blocks
			WHERE id IN (
				WITH RECURSIVE ChainBlocks AS (
					SELECT id, parent_id, height
					FROM blocks
					WHERE invalid = false
					AND hash = (
						SELECT b.hash
						FROM blocks b
						WHERE b.invalid = false
						ORDER BY chain_work DESC, peer_id ASC, id ASC
						LIMIT 1
					)
					UNION ALL
					SELECT bb.id, bb.parent_id, bb.height
					FROM blocks bb
					JOIN ChainBlocks cb ON bb.id = cb.parent_id
					WHERE bb.id != cb.id
					  AND bb.invalid = false
				)
				SELECT id FROM ChainBlocks
				WHERE height >= $1 AND height <= $2
				  AND invalid = FALSE
				ORDER BY height ASC
			)
		)
	`

	rows, err := s.db.QueryContext(ctx, q, startHeight, endHeight)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return blocks, nil
		}

		return nil, errors.NewStorageError("failed to get blocks", err)
	}

	defer rows.Close()

	blocks, err = s.processFullBlockRows(rows)
	if err != nil {
		return nil, err
	}

	// Cache the result in response cache
	s.responseCache.Set(cacheID, blocks, s.cacheTTL)

	return blocks, nil
}
