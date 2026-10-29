package subtreeprocessor

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
)

// alreadyResolvedConflictingNodes returns the conflicting nodes of a block that
// the store already shows resolved for it, and the losers of those nodes.
//
// A conflicting node names a transaction that was Conflicting when the block's
// subtree was validated. The subtree meta is history and never changes, but the
// store does: ProcessConflicting clears the flag at its step 4 and leaves the node
// as the recorded spender of every one of its inputs. A node in exactly that state
// has already been resolved for this block, by an application of the block that
// did not reach block assembly's checkpoint: a kill mid catch-up, or an in-process
// failure on a later block whose rollback rewound the in-memory state while the
// store kept the resolution. ProcessConflicting refuses such a node ("tx is not
// conflicting"), and that refusal held mainnet at 971803 for four days from
// 2026-10-19, because every retry met the same state.
//
// Re-running the resolution is not an option: GetCounterConflicting finds the
// losers through the parents' recorded spender, which is now the winner itself,
// so a re-run would walk and demote the winner's own descendants. When every node
// is resolved the caller skips ProcessConflicting; otherwise it passes the
// resolved ones with utxostore.WithResolvedWinners, so their loser walk is
// skipped while they stay winners for the rest of the resolution. Their losers
// are recovered read-only by utxostore.ResolvedForwardLosers, so the in-memory
// subtrees still evict them.
//
// Only the exact post-resolution state is skipped. A node that is not flagged but
// is not the recorded spender of one of its inputs is not a state this code can
// explain, so it is not returned and ProcessConflicting's guard refuses it.
func (stp *SubtreeProcessor) alreadyResolvedConflictingNodes(ctx context.Context, block *model.Block,
	nodes []chainhash.Hash) (resolved []chainhash.Hash, losers []chainhash.Hash, err error) {
	for _, h := range nodes {
		applied, err := utxostore.ForwardResolutionApplied(ctx, stp.utxoStore, h)
		if err != nil {
			return nil, nil, errors.NewProcessingError("[moveForwardBlock][%s] error checking conflicting node %s", block.String(), h.String(), err)
		}

		if applied {
			resolved = append(resolved, h)
		}
	}

	if len(resolved) == 0 {
		return nil, nil, nil
	}

	nodeSet := make(map[chainhash.Hash]struct{}, len(nodes))
	for _, h := range nodes {
		nodeSet[h] = struct{}{}
	}

	losers, err = utxostore.ResolvedForwardLosers(ctx, stp.utxoStore, resolved, nodeSet)
	if err != nil {
		return nil, nil, errors.NewProcessingError("[moveForwardBlock][%s] error recovering the losers of %d already-resolved conflicting transactions", block.String(), len(resolved), err)
	}

	stp.logger.Warnf("[moveForwardBlock][%s] %d of %d conflicting transactions are already resolved in the store, skipping their re-resolution and surfacing %d losers", block.String(), len(resolved), len(nodes), len(losers))

	return resolved, losers, nil
}

// mergeResolvedLosers adds the losers of already-resolved conflicting nodes to
// the losing map and conflicting set that block movement hands on, creating
// either when the ProcessConflicting call that normally fills them was skipped.
func mergeResolvedLosers(losingTxHashesMap txmap.TxMap, conflictingSet map[chainhash.Hash]struct{},
	losers []chainhash.Hash) (txmap.TxMap, map[chainhash.Hash]struct{}) {
	if len(losers) == 0 {
		return losingTxHashesMap, conflictingSet
	}

	if losingTxHashesMap == nil {
		losingTxHashesMap = txmap.NewSplitSwissMap(len(losers))
	}

	if conflictingSet == nil {
		conflictingSet = make(map[chainhash.Hash]struct{}, len(losers))
	}

	for _, h := range losers {
		// a duplicate is reported as an error and is harmless here
		_ = losingTxHashesMap.Put(h, 1)
		conflictingSet[h] = struct{}{}
	}

	return losingTxHashesMap, conflictingSet
}
