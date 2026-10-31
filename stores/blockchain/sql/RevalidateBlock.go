package sql

import (
	"context"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

func (s *SQL) RevalidateBlock(ctx context.Context, blockHash *chainhash.Hash) error {
	s.logger.Infof("RevalidateBlock %s", blockHash.String())

	exists, err := s.GetBlockExists(ctx, blockHash)
	if err != nil {
		return errors.NewStorageError("error checking block exists", err)
	}

	if !exists {
		return errors.NewStorageError("block %s does not exist", blockHash.String())
	}

	// Serialize against StoreBlock's slow path and InvalidateBlock so the
	// UPDATE and the follow-up reconciliation observe a stable view of the
	// chain. See the matching note in InvalidateBlock.
	s.slowPathMu.Lock()
	defer s.slowPathMu.Unlock()

	// Hold the rebuild guard from the UPDATE through the reconciliation so
	// concurrent readers fall back to the authoritative CTE path during the
	// inconsistent window. Mirrors InvalidateBlock's pattern.
	s.mainChainRebuilding.Add(1)
	defer s.mainChainRebuilding.Add(-1)

	// Update the block to valid (not invalid) and clear the mined_set flag.
	q := `
		UPDATE blocks
		SET invalid = false, mined_set = false
		WHERE hash = $1
	`
	if _, err = s.db.ExecContext(ctx, q, blockHash.CloneBytes()); err != nil {
		return errors.NewStorageError("error updating block to valid", err)
	}

	// The UPDATE committed, so this call moves chain membership; see the matching note in
	// InvalidateBlock. Deferred so it lands after the rebuild below and before slowPathMu
	// drops. A failed UPDATE returned above and is not noted.
	defer s.noteOnMainChainWrite(noRepairFloor)

	// RevalidateBlock, like InvalidateBlock, can move the tip across an
	// arbitrarily deep fork point. Use the full rebuild for correctness —
	// see the matching note in InvalidateBlock.
	rebuildCtx, rebuildCancel := context.WithTimeout(context.Background(), migrationFullRebuildTimeout)
	defer rebuildCancel()

	// Invalidate caches FIRST so that the rebuild sees the freshly
	// revalidated state rather than the pre-revalidation cached value.
	s.blockTimestampCache.Clear()
	s.ResetResponseCache()
	if s.useInMemoryChainCheck {
		s.resetChainWalkCache()
	}

	// The UPDATE above has committed, so a failed rebuild leaves the column describing the
	// chain before the revalidation. Record it so the background repair retries the full
	// walk until it lands; a rebuild that lands here satisfies any earlier owed repair.
	if rebuildErr := s.rebuildOnMainChainFlag(rebuildCtx, true); rebuildErr != nil {
		s.logger.Errorf("RevalidateBlock: rebuildOnMainChainFlag: %v", rebuildErr)
		s.markOnMainChainDirty(true, noRepairFloor, "revalidate_block", rebuildErr)
	} else {
		s.onMainChainRepaired(true)
	}

	if s.useInMemoryChainCheck {
		if rebuildErr := s.triggerRebuildOffChainSetAfterWrite(rebuildCtx); rebuildErr != nil {
			s.logger.Errorf("RevalidateBlock: %v", rebuildErr)
		} else {
			s.lastSuccessfulRebuild.Store(time.Now().Unix())
		}
	}

	return nil
}
