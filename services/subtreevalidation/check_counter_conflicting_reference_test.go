package subtreevalidation

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"golang.org/x/sync/errgroup"
)

// checkCounterConflictingOnCurrentChain is the per-transaction check that
// checkCounterConflictingOnCurrentChainBatch replaced everywhere, including in
// blessMissingTransaction. Production no longer calls it; it is kept as the
// reference the equivalence test compares the batch against (perTxVerdict).
//
// It returns ErrTxInvalid when a member of txHash's counter-conflicting set is
// mined in a block of blockIds, and any failure to derive or read the set, a
// frozen output in it or in txHash's own cone included, before that mined check.
func (u *Server) checkCounterConflictingOnCurrentChain(ctx context.Context, txHash chainhash.Hash, blockIds map[uint32]bool) error {
	// the tx is conflicting, check whether the counter-conflicting transactions have already been mined on our chain
	// first get the parent transactions and check if they were spent
	counterConflictingTxHashes, err := utxo.GetCounterConflictingTxHashes(ctx, u.utxoStore, txHash, u.settings.UtxoStore.ConflictingChildrenMaxNodes)
	if err != nil {
		return errors.NewProcessingError("[checkCounterConflictingOnCurrentChain][%s] failed to get counter conflicting tx hashes", txHash.String(), err)
	}

	if len(counterConflictingTxHashes) > conflictingConeWarnThreshold {
		u.logger.Warnf("[checkCounterConflictingOnCurrentChain][%s] counter-conflicting set is %d transactions", txHash.String(), len(counterConflictingTxHashes))
	}

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(128)

	counterConflictingTxMetas := make([]*meta.Data, len(counterConflictingTxHashes))

	for idx, counterConflictingTxHash := range counterConflictingTxHashes {
		g.Go(func() error {
			// if a transaction is frozen, the counter-transaction will be the same as the coinbase placeholder
			if counterConflictingTxHash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
				return errors.NewProcessingError("[checkCounterConflictingOnCurrentChain][%s] counter conflicting tx is frozen", txHash.String())
			}

			counterConflictingTxMeta := &meta.Data{}
			if err := u.utxoStore.GetMeta(gCtx, &counterConflictingTxHash, counterConflictingTxMeta); err != nil {
				return errors.NewProcessingError("[checkCounterConflictingOnCurrentChain][%s] failed to get counter conflicting tx meta", txHash.String(), err)
			}

			counterConflictingTxMetas[idx] = counterConflictingTxMeta

			return nil
		})
	}

	if err = g.Wait(); err != nil {
		return errors.NewProcessingError("[checkCounterConflictingOnCurrentChain][%s] failed to get counter conflicting tx meta", txHash.String(), err)
	}

	// check whether any child transaction of txHash itself is frozen. This is the
	// only walk the counter-conflicting set does not already cover: every
	// counter-spender's descendant cone was walked (and frozen-checked) inside
	// GetCounterConflictingTxHashes above, and any frozen sentinel there already
	// failed the call. Re-walking each set member re-derived subsets of the same
	// cones — O(N^2) store reads that wedged the mainnet fleet at 960,827 (issue
	// 1391) — so only the single winner-cone walk remains.
	childTransactionHashes, err := utxo.GetConflictingChildren(ctx, u.utxoStore, txHash, u.settings.UtxoStore.ConflictingChildrenMaxNodes)
	if err != nil {
		return errors.NewProcessingError("[checkCounterConflictingOnCurrentChain][%s] failed to get child transactions", txHash.String(), err)
	}

	for _, childTransactionHash := range childTransactionHashes {
		if childTransactionHash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
			return errors.NewProcessingError("[checkCounterConflictingOnCurrentChain][%s] child transaction is frozen", txHash.String())
		}
	}

	// check whether the counter-conflicting transactions have already been mined on our chain
	for _, counterConflictingTxMeta := range counterConflictingTxMetas {
		for _, blockID := range counterConflictingTxMeta.BlockIDs {
			if blockIds[blockID] {
				return errors.NewTxInvalidError("[checkCounterConflictingOnCurrentChain][%s] transaction is already mined on our chain", txHash.String())
			}
		}
	}

	return nil
}

// blessConflictCheck is the conflict check blessMissingTransaction runs on a
// conflicting tx: the batch, asked of that one tx, failing on a missing record.
func (u *Server) blessConflictCheck(ctx context.Context, txHash chainhash.Hash, blockIds map[uint32]bool) error {
	_, err := u.checkCounterConflictingOnCurrentChainBatch(ctx, []chainhash.Hash{txHash}, blockIds, failOnMissingRecords)

	return err
}
