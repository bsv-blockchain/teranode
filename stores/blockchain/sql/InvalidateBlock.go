// Package sql implements the blockchain.Store interface using SQL database backends.
// It provides concrete SQL-based implementations for all blockchain operations
// defined in the interface, with support for different SQL engines.
//
// This file implements the InvalidateBlock method, which is critical for handling
// blockchain reorganizations (reorgs). In Bitcoin's consensus model, when a competing
// chain with greater cumulative proof-of-work is discovered, the current chain must be
// invalidated in favor of the stronger chain. This process, known as a chain reorganization,
// is fundamental to Bitcoin's eventual consistency model and Nakamoto consensus.
//
// The implementation uses a recursive Common Table Expression (CTE) in SQL to efficiently
// invalidate an entire chain of blocks in a single database operation. This approach is
// particularly important in Teranode's high-throughput architecture, where chain
// reorganizations must be handled quickly and atomically to maintain system integrity.
// When a block is invalidated, all descendant blocks that build on it must also be
// invalidated to maintain blockchain integrity. The method also ensures that the in-memory
// cache is properly reset to reflect these changes, maintaining consistency between the
// database state and cached data.
package sql

import (
	"context"
	"database/sql"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/libsv/go-bt/v2/chainhash"
)

// InvalidateBlock marks a block and all its descendants as invalid in the blockchain.
// This implements the blockchain.Store.InvalidateBlock interface method.
//
// This method is a cornerstone of Bitcoin's consensus mechanism, handling blockchain
// reorganizations (reorgs) when a competing chain with higher cumulative proof-of-work
// is discovered. In Teranode's high-throughput architecture, efficient handling of
// chain reorganizations is critical for maintaining system integrity and consensus
// across the network.
//
// The implementation follows these key steps:
// 1. Resets the blocks cache to ensure fresh block existence checks
// 2. Verifies the target block exists in the database
// 3. Uses a recursive Common Table Expression (CTE) in SQL to efficiently identify and
//    invalidate the entire subtree of blocks that descend from the target block
// 4. Updates the 'invalid' flag for all affected blocks in a single atomic database operation
// 5. Resets both the blocks cache and response cache to ensure consistency between
//    cached data and the new database state
//
// This approach ensures that once a block is invalidated, all blocks that build on top
// of it are also invalidated, maintaining the integrity of the blockchain's consensus
// rules. The use of recursive SQL provides significant performance benefits over
// iterative approaches, particularly for deep reorganizations affecting many blocks.
//
// Parameters:
//   - ctx: Context for the database operation, allowing for cancellation and timeouts
//   - blockHash: The unique hash identifier of the block to invalidate
//
// Returns:
//   - error: Any error encountered during the invalidation process, specifically:
//     - BlockNotFoundError if the specified block doesn't exist in the database
//     - StorageError for database errors, transaction failures, or if no rows were affected
//     - ProcessingError for internal processing failures
func (s *SQL) InvalidateBlock(ctx context.Context, blockHash *chainhash.Hash) error {
	s.logger.Infof("InvalidateBlock %s", blockHash.String())

	s.blocksCache.RebuildBlockchain(nil, nil) // reset cache so that GetBlockExists goes to the DB

	exists, err := s.GetBlockExists(ctx, blockHash)
	if err != nil {
		return errors.NewStorageError("error checking block exists", err)
	}

	if !exists {
		return errors.NewStorageError("block %s does not exist", blockHash.String(), errors.ErrNotFound)
	}

	// recursively update all children blocks to invalid in 1 query
	q := `
		WITH RECURSIVE children AS (
			SELECT id, hash, previous_hash
			FROM blocks
			WHERE hash = $1
			UNION
			SELECT b.id, b.hash, b.previous_hash
			FROM blocks b	
			INNER JOIN children c ON c.hash = b.previous_hash	
		)
		UPDATE blocks
		SET invalid = true
		WHERE id IN (SELECT id FROM children)
	`

	var res sql.Result

	if res, err = s.db.ExecContext(ctx, q, blockHash.CloneBytes()); err != nil {
		return errors.NewStorageError("error updating block to invalid", err)
	}

	// check if the block was updated
	if rows, _ := res.RowsAffected(); rows <= 0 {
		return errors.NewStorageError("block %s was not updated to invalid", blockHash.String())
	}

	if err = s.ResetBlocksCache(ctx); err != nil {
		return errors.NewStorageError("error clearing caches", err)
	}

	s.ResetResponseCache()

	return nil
}
