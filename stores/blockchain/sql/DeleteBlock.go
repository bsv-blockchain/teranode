// Package sql implements the blockchain.Store interface using SQL database backends.
//
// This file implements DeleteBlock, which physically removes a single block row
// from the blocks table. It is used by repair tooling such as cmd/rewindblockchain
// to rewind the chain when the UTXO store has drifted out of sync with on-disk
// subtree files.
package sql

import (
	"context"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util/tracing"
)

// DeleteBlock physically removes a single block row from the blocks table and
// invalidates the caches that depend on block rows. Idempotent: no error if
// the row did not exist.
//
// The blocks table has a self-referencing foreign key (parent_id REFERENCES
// blocks(id)) with no ON DELETE CASCADE, so callers must order deletions so
// that no surviving row still points at a deleted block. Rewind tooling
// achieves this by iterating in strict descending height order.
//
// Cache invalidation mirrors InvalidateBlock's defer block: after the row is
// deleted we clear the block-timestamp cache, reset the response cache that
// serves GetBestBlockHeader/GetBlockHeaders, reset the chain-walk cache if
// enabled, and trigger an async rebuild of the off-chain block-id set. The
// per-call cost is negligible (the caches use a generation counter), so
// callers deleting many blocks can do so without coordinating finalization
// — GetBestBlockHeader and friends see the new tip immediately.
func (s *SQL) DeleteBlock(ctx context.Context, blockHash *chainhash.Hash) error {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "sql:DeleteBlock")
	defer deferFn()

	if blockHash == nil {
		return errors.NewInvalidArgumentError("block hash cannot be nil")
	}

	s.logger.Debugf("DeleteBlock %s", blockHash.String())

	res, err := s.db.ExecContext(ctx, `DELETE FROM blocks WHERE hash = $1`, blockHash.CloneBytes())
	if err != nil {
		return errors.NewStorageError("error deleting block", err)
	}

	if n, affErr := res.RowsAffected(); affErr == nil && n == 0 {
		s.logger.Warnf("DeleteBlock: block %s did not exist", blockHash.String())
	}

	// Invalidate caches so subsequent reads (GetBestBlockHeader, etc.) reflect
	// the deletion. Matches InvalidateBlock's defer sequence exactly.
	s.blockTimestampCache.Clear()
	s.ResetResponseCache()
	if s.useInMemoryChainCheck {
		s.resetChainWalkCache()
		rebuildCtx, rebuildCancel := context.WithTimeout(ctx, rebuildOffChainSetTimeout)
		defer rebuildCancel()
		if rebuildErr := s.triggerRebuildOffChainSet(rebuildCtx); rebuildErr != nil {
			s.logger.Errorf("DeleteBlock: %v", rebuildErr)
		} else {
			s.lastSuccessfulRebuild.Store(time.Now().Unix())
		}
	}

	return nil
}
