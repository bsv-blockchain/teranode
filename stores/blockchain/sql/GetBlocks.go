// Package sql implements the blockchain.Store interface using SQL database backends.
// It provides concrete SQL-based implementations for all blockchain operations
// defined in the interface, with support for different SQL engines.
//
// This file implements the GetBlocks method, which retrieves a sequence of consecutive
// blocks from the blockchain starting from a specified block hash. The implementation
// uses a recursive Common Table Expression (CTE) in SQL to efficiently traverse the
// blockchain structure by following the parent-child relationships between blocks.
// This method is particularly useful for blockchain synchronization, chain analysis,
// and block validation processes that require examining sequences of blocks.
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

// GetBlocks retrieves a sequence of consecutive blocks from the blockchain.
// This implements the blockchain.Store.GetBlocks interface method.
//
// The method retrieves a specified number of blocks starting from a given block hash
// and traversing backward through the blockchain (from newer to older blocks). It uses
// a recursive SQL query to efficiently navigate the blockchain structure by following
// the parent-child relationships between blocks.
//
// For each block, the method retrieves the complete block data including header information,
// transaction count, size, and other metadata. This comprehensive retrieval is particularly
// useful for blockchain synchronization processes, chain analysis tools, and validation
// operations that require examining sequences of blocks.
//
// Parameters:
//   - ctx: Context for the database operation, allowing for cancellation and timeouts
//   - blockHashFrom: The hash of the starting block from which to retrieve the sequence
//   - numberOfHeaders: The maximum number of blocks to retrieve in the sequence
//
// Returns:
//   - []*model.Block: An array of complete block objects in the sequence
//   - error: Any error encountered during retrieval, specifically:
//   - BlockNotFoundError if the starting block does not exist
//   - StorageError for database errors or data processing failures
func (s *SQL) GetBlocks(ctx context.Context, blockHashFrom *chainhash.Hash, numberOfHeaders uint32) ([]*model.Block, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "sql:GetBlocks")
	defer deferFn()

	// Try to get from response cache
	// Use a derived cache key to avoid conflicts with other cached data
	cacheID := chainhash.HashH([]byte(fmt.Sprintf("GetBlocks-%s-%d", blockHashFrom.String(), numberOfHeaders)))

	cached := s.responseCache.Get(cacheID)
	if cached != nil && cached.Value() != nil {
		if cacheData, ok := cached.Value().([]*model.Block); ok {
			return cacheData, nil
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	blocks := make([]*model.Block, 0, numberOfHeaders)

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
					WHERE hash = $1
					UNION ALL
					SELECT bb.id, bb.parent_id, bb.height
					FROM blocks bb
					JOIN ChainBlocks cb ON bb.id = cb.parent_id
					WHERE bb.id != cb.id
				)
				SELECT id FROM ChainBlocks
				LIMIT $2
			)
		)
		ORDER BY height DESC
	`

	rows, err := s.db.QueryContext(ctx, q, blockHashFrom[:], numberOfHeaders)
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

	// Cache the blocks result
	s.responseCache.Set(cacheID, blocks, s.cacheTTL)

	return blocks, nil
}
