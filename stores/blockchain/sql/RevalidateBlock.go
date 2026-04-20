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

	// Hold the rebuild guard from the UPDATE through the rebuild so concurrent
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

	// Invalidate caches FIRST so that rebuildOnMainChainFlag's getBestBlockID call
	// sees the freshly revalidated state rather than the pre-revalidation cached value.
	s.blockTimestampCache.Clear()
	s.ResetResponseCache()
	if s.useInMemoryChainCheck {
		s.resetChainWalkCache()
	}

	// Rebuild on_main_chain flags to reflect the potentially new canonical chain.
	if rebuildErr := s.rebuildOnMainChainFlag(rebuildCtx, false); rebuildErr != nil {
		s.logger.Errorf("RevalidateBlock: rebuildOnMainChainFlag: %v", rebuildErr)
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
