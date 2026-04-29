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
	// pre-best capture, the UPDATE, and applyOnMainChainSwitch all see a
	// stable view of the chain tip. See the matching note in InvalidateBlock
	// for the failure mode this prevents.
	s.slowPathMu.Lock()
	defer s.slowPathMu.Unlock()

	// Capture the pre-revalidation best block ID. If revalidation makes this
	// block (or one of its now-valid descendants) the new best, we apply a
	// diff via applyOnMainChainSwitch instead of a wide-window rebuild.
	preBestID, _, preBestErr := s.getBestBlockID(ctx)
	if preBestErr != nil {
		s.logger.Warnf("RevalidateBlock: failed to get pre-revalidation best block: %v", preBestErr)
	}

	// Hold the rebuild guard from the UPDATE through the diff so concurrent
	// readers fall back to the authoritative CTE path during the inconsistent
	// window. Mirrors InvalidateBlock's pattern.
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

	rebuildCtx, rebuildCancel := context.WithTimeout(context.Background(), rebuildOffChainSetTimeout)
	defer rebuildCancel()

	// Invalidate caches FIRST so that getBestBlockID sees the freshly
	// revalidated state rather than the pre-revalidation cached value.
	s.blockTimestampCache.Clear()
	s.ResetResponseCache()
	if s.useInMemoryChainCheck {
		s.resetChainWalkCache()
	}

	if preBestErr != nil {
		// Couldn't capture the old tip; fall back to the bounded full rebuild.
		if rebuildErr := s.rebuildOnMainChainFlag(rebuildCtx, false); rebuildErr != nil {
			s.logger.Errorf("RevalidateBlock: rebuildOnMainChainFlag: %v", rebuildErr)
		}
	} else if newBestID, _, newBestErr := s.getBestBlockID(rebuildCtx); newBestErr != nil {
		s.logger.Errorf("RevalidateBlock: post-revalidation getBestBlockID: %v", newBestErr)
	} else if newBestID != preBestID {
		// Revalidation moved the tip — flip the divergent suffixes.
		if switchErr := s.applyOnMainChainSwitch(rebuildCtx, preBestID, newBestID); switchErr != nil {
			s.logger.Errorf("RevalidateBlock: applyOnMainChainSwitch: %v", switchErr)
		}
	}

	if s.useInMemoryChainCheck {
		if rebuildErr := s.triggerRebuildOffChainSet(rebuildCtx); rebuildErr != nil {
			s.logger.Errorf("RevalidateBlock: %v", rebuildErr)
		} else {
			s.lastSuccessfulRebuild.Store(time.Now().Unix())
		}
	}

	return nil
}
