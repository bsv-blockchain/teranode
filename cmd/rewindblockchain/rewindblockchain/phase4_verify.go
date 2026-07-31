package rewindblockchain

import (
	"context"

	"github.com/bsv-blockchain/teranode/errors"
)

// phase4Verify runs cheap post-rewind consistency checks. It also asserts
// state["BlockPersisterHeight"] never increased across the run.
//
// Deeper checks (BlockID membership sweep, orphan subtree scan) are left out
// for now — they'd require iterator primitives we don't have on the UTXO
// store, or a directory walk on the blob store. Best addressed as a
// follow-up.
func (e *env) phase4Verify(ctx context.Context, pf *preflightResult) error {
	header, meta, err := e.blockchainStore.GetBestBlockHeader(ctx)
	if err != nil {
		return errors.NewStorageError("verify: GetBestBlockHeader", err)
	}

	if meta.Height != pf.target {
		return errors.NewProcessingError("verify: best block height %d != target %d", meta.Height, pf.target)
	}
	if header.Hash().String() != pf.targetHash.String() {
		return errors.NewProcessingError("verify: best block hash %s != target hash %s", header.Hash(), pf.targetHash)
	}

	if err = e.verifyBlockPersisterHeight(ctx, pf); err != nil {
		return err
	}

	e.logger.Infof("verify: best block at height %d matches target", pf.target)
	return nil
}

// verifyBlockPersisterHeight asserts the run did not raise
// state["BlockPersisterHeight"] — including the case where it had no readable
// value going in, since this tool never creates the key.
//
// With --force-not-idle the node may still be running, and block persister
// raises this key itself (services/blockpersister/Server.go:417). Failing there
// would condemn a run that actually succeeded, after every mutation is already
// applied, so a concurrent raise is downgraded to a warning naming the cause.
func (e *env) verifyBlockPersisterHeight(ctx context.Context, pf *preflightResult) error {
	current, known, err := e.readBlockPersisterHeight(ctx)
	if err != nil {
		return err
	}

	if !pf.persisterHeightKnown {
		if !known {
			e.logger.Infof("verify: state[%s] was unreadable before the run and still is", blockPersisterHeightKey)
			return nil
		}

		if e.opts.ForceNotIdle {
			e.logger.Warnf("verify: state[%s] was unreadable before the run and is now %d; --force-not-idle was given, so a running block persister can have written it — re-check with the node stopped", blockPersisterHeightKey, current)
			return nil
		}

		return errors.NewProcessingError("verify: state[%s] was unreadable before the run and is now %d", blockPersisterHeightKey, current)
	}

	if !known {
		e.logger.Warnf("verify: state[%s] was %d before the run and is no longer readable", blockPersisterHeightKey, pf.persisterHeight)
		return nil
	}

	if current > pf.persisterHeight {
		if e.opts.ForceNotIdle {
			e.logger.Warnf("verify: state[%s] increased from %d to %d; --force-not-idle was given, so a running block persister can have raised it concurrently — re-check with the node stopped", blockPersisterHeightKey, pf.persisterHeight, current)
			return nil
		}

		return errors.NewProcessingError("verify: state[%s] increased from %d to %d", blockPersisterHeightKey, pf.persisterHeight, current)
	}

	e.logger.Infof("verify: state[%s]=%d did not increase (was %d)", blockPersisterHeightKey, current, pf.persisterHeight)
	return nil
}
