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
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
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
//  1. Resets the blocks cache to ensure fresh block existence checks
//  2. Verifies the target block exists in the database
//  3. Uses a recursive Common Table Expression (CTE) in SQL to efficiently identify and
//     invalidate the entire subtree of blocks that descend from the target block
//  4. Updates the 'invalid' flag for all affected blocks in a single atomic database operation
//  5. Resets both the blocks cache and response cache to ensure consistency between
//     cached data and the new database state
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
//   - BlockNotFoundError if the specified block doesn't exist in the database
//   - StorageError for database errors, transaction failures, or if no rows were affected
//   - ProcessingError for internal processing failures
func (s *SQL) InvalidateBlock(ctx context.Context, blockHash *chainhash.Hash) (invalidatedHashes []chainhash.Hash, err error) {
	s.logger.Debugf("InvalidateBlock %s", blockHash.String())

	exists, err := s.GetBlockExists(ctx, blockHash)
	if err != nil {
		return nil, errors.NewStorageError("error checking block exists", err)
	}

	if !exists {
		// Block doesn't exist - this is not an error, just log it and return success
		// This makes InvalidateBlock idempotent
		s.logger.Warnf("InvalidateBlock: block %s does not exist, nothing to invalidate", blockHash.String())

		return []chainhash.Hash{}, nil
	}

	// Serialize against StoreBlock's slow path and RevalidateBlock so the
	// UPDATE and the follow-up reconciliation observe a stable view of the
	// chain. The reconciliation itself derives the actual best inside its
	// transaction, so we do not need to snapshot a pre-best on this side.
	s.slowPathMu.Lock()
	defer s.slowPathMu.Unlock()

	// recursively update all children blocks to invalid in 1 query.
	// Also set mined_set = false (invalid blocks cannot be mined; this triggers
	// block-validation to reset mining state) and on_main_chain = false so the
	// invalidated subtree is removed from the canonical chain in the same
	// transaction — no follow-up wide-window UPDATE needed.
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
		SET invalid = true, mined_set = false, on_main_chain = false
		WHERE id IN (SELECT id FROM children)
		RETURNING hash
	`

	var (
		rows      *sql.Rows
		hashBytes []byte
		hash      *chainhash.Hash
	)

	// Guard the entire window: from when invalid/on_main_chain flags change
	// until applyOnMainChainSwitch corrects the new winning branch. Balanced
	// by the defer so all exit paths (including the early error below) decrement.
	s.mainChainRebuilding.Add(1)
	defer s.mainChainRebuilding.Add(-1)

	if rows, err = s.db.QueryContext(ctx, q, blockHash.CloneBytes()); err != nil {
		return nil, errors.NewStorageError("error querying blocks to invalidate", err)
	}

	defer func() {
		err = errors.Join(err, rows.Close())

		// Invalidate caches FIRST so that getBestBlockID does not return a
		// stale best block that included the now-invalid block.
		s.blockTimestampCache.Clear()
		s.ResetResponseCache()
		if s.useInMemoryChainCheck {
			s.resetChainWalkCache()
		}

		// InvalidateBlock can move the chain tip arbitrarily deep — an
		// operator may invalidate a block far below the current tip, leaving
		// the new winning branch's fork point well below the bounded walk
		// in reconcileOnMainChain. Use the full rebuild here for correctness;
		// invalidations are rare admin operations and can afford the wider
		// lock window. migrationFullRebuildTimeout matches the startup
		// migration's bound, generous enough for multi-million-block chains.
		rebuildCtx, rebuildCancel := context.WithTimeout(context.Background(), migrationFullRebuildTimeout)
		defer rebuildCancel()

		if rebuildErr := s.rebuildOnMainChainFlag(rebuildCtx, true); rebuildErr != nil {
			s.logger.Errorf("InvalidateBlock: rebuildOnMainChainFlag: %v", rebuildErr)
		}

		if s.useInMemoryChainCheck {
			if rebuildErr := s.triggerRebuildOffChainSet(rebuildCtx); rebuildErr != nil {
				s.logger.Errorf("InvalidateBlock: %v", rebuildErr)
			} else {
				s.lastSuccessfulRebuild.Store(time.Now().Unix())
			}
		}
	}()

	for rows.Next() {
		if err = rows.Scan(&hashBytes); err != nil {
			return nil, errors.NewStorageError("error scanning invalidated block hash", err)
		}

		if hash, err = chainhash.NewHash(hashBytes); err != nil {
			return nil, errors.NewStorageError("error creating hash from bytes", err)
		}

		invalidatedHashes = append(invalidatedHashes, *hash)
	}

	if len(invalidatedHashes) == 0 {
		return nil, errors.NewStorageError("no blocks were invalidated", errors.ErrProcessing)
	}

	return invalidatedHashes, nil
}
