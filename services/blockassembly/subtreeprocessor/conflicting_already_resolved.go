package subtreeprocessor

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
)

// alreadyResolvedConflictingNodes splits a block's conflicting nodes into the
// ones still awaiting resolution and the losers of the ones the store already
// shows resolved for this block.
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
// so a re-run would walk and demote the winner's own descendants. The resolved
// nodes are therefore taken out of the ProcessConflicting call, and their losers
// are recovered read-only, from the parents' conflicting children and each
// loser's cascade, so the in-memory subtrees still evict them.
//
// Only the exact post-resolution state is skipped. A node that is not flagged but
// is not the recorded spender of one of its inputs is not a state this code can
// explain, so it stays pending and ProcessConflicting's guard refuses it.
func (stp *SubtreeProcessor) alreadyResolvedConflictingNodes(ctx context.Context, block *model.Block,
	nodes []chainhash.Hash) (pending []chainhash.Hash, losers []chainhash.Hash, err error) {
	nodeSet := make(map[chainhash.Hash]struct{}, len(nodes))
	for _, h := range nodes {
		nodeSet[h] = struct{}{}
	}

	parents := make(map[chainhash.Hash]*meta.Data)
	immediate := make(map[chainhash.Hash]struct{})
	resolved := 0

	for _, h := range nodes {
		h := h

		applied, err := utxostore.ForwardResolutionApplied(ctx, stp.utxoStore, h)
		if err != nil {
			return nil, nil, errors.NewProcessingError("[moveForwardBlock][%s] error checking conflicting node %s", block.String(), h.String(), err)
		}

		if !applied {
			pending = append(pending, h)
			continue
		}

		resolved++

		txMeta, err := stp.utxoStore.Get(ctx, &h, fields.TxInpoints)
		if err != nil {
			return nil, nil, errors.NewProcessingError("[moveForwardBlock][%s] error reading conflicting node %s", block.String(), h.String(), err)
		}

		if txMeta == nil {
			continue
		}

		// The immediate losers are the parents' conflicting children other than
		// this block's own nodes. A store may keep a cleared winner in that list.
		for _, parentHash := range txMeta.TxInpoints.GetParentTxHashes() {
			parentMeta, seen := parents[parentHash]
			if !seen {
				parentHash := parentHash

				parentMeta, err = stp.utxoStore.Get(ctx, &parentHash, fields.ConflictingChildren)
				if err != nil {
					return nil, nil, errors.NewProcessingError("[moveForwardBlock][%s] error reading parent %s of conflicting node %s", block.String(), parentHash.String(), h.String(), err)
				}

				parents[parentHash] = parentMeta
			}

			if parentMeta == nil {
				continue
			}

			for _, child := range parentMeta.ConflictingChildren {
				if _, isNode := nodeSet[child]; isNode {
					continue
				}

				immediate[child] = struct{}{}
			}
		}
	}

	if resolved == 0 {
		return nodes, nil, nil
	}

	loserSet := make(map[chainhash.Hash]struct{}, len(immediate))

	// Each loser's cascade: the original resolution flagged every descendant of a
	// loser, and the walk from a loser only reaches its own descendants.
	for loser := range immediate {
		loserSet[loser] = struct{}{}

		children, walkErr := stp.utxoStore.GetConflictingChildren(ctx, loser)
		if walkErr != nil {
			return nil, nil, errors.NewProcessingError("[moveForwardBlock][%s] error walking the descendants of loser %s", block.String(), loser.String(), walkErr)
		}

		for _, child := range children {
			if _, isNode := nodeSet[child]; isNode {
				continue
			}

			loserSet[child] = struct{}{}
		}
	}

	losers = make([]chainhash.Hash, 0, len(loserSet))
	for h := range loserSet {
		losers = append(losers, h)
	}

	stp.logger.Warnf("[moveForwardBlock][%s] %d of %d conflicting transactions are already resolved in the store, skipping their re-resolution and surfacing %d losers", block.String(), resolved, len(nodes), len(losers))

	return pending, losers, nil
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
