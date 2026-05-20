package rewindblockchain

import (
	"context"

	"github.com/bsv-blockchain/teranode/errors"
)

// phase4Verify runs cheap post-rewind consistency checks.
//
// Deeper checks (BlockID membership sweep, orphan subtree scan) are left out
// for now — they'd require iterator primitives we don't have on the UTXO
// store, or a directory walk on the blob store. Best addressed as a
// follow-up.
func (e *env) phase4Verify(ctx context.Context, pf *preflightResult) error {
	header, meta, err := e.blockchainStore.GetBestBlockHeader(ctx)
	if err != nil {
		return errors.NewStorageError("verify: GetBestBlockHeader: %w", err)
	}

	if meta.Height != pf.target {
		return errors.NewProcessingError("verify: best block height %d != target %d", meta.Height, pf.target)
	}
	if header.Hash().String() != pf.targetHash.String() {
		return errors.NewProcessingError("verify: best block hash %s != target hash %s", header.Hash(), pf.targetHash)
	}

	e.logger.Infof("verify: best block at height %d matches target", pf.target)
	return nil
}
