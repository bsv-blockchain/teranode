package subtreeprocessor

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
)

// reorgDrainHook, when set, sees each queue drain of a fork reorg and whether it ran on every core.
// Nil in production; a test sets it to learn which drains took the parallel path, and to queue
// transactions at the moment a drain starts.
var reorgDrainHook func(parallel bool)

// reorgParallelDrain is the switch of the parallel drain of a fork reorg, a var so a test can run the
// same reorg sequentially and compare.
var reorgParallelDrain = true

// dequeueDuringReorg is dequeueDuringBlockMovement for the drains of a fork reorg (issue 2269, PR 9).
// Where no conflict is in play it takes the drain a block gets after it is applied
// (drainQueueAfterBlock): the same batches, the same transactions skipped, the same added in the same
// order, with the filter, the tx map insert and the subtree build on every core. Conflicting hashes make
// that impossible: they skip a queued transaction when an earlier queued one was, an order the parallel
// steps do not keep. Losing hashes alone would not, since the parallel filter applies them too; a step
// that has any stays sequential because that is what "no conflict" means to the reorg, and the drain
// after a block, which runs them in parallel, is not the one under test here. Outside a fork reorg it is
// dequeueDuringBlockMovement.
//
// One difference, on a failure: a subtree the parallel build could not complete fails the reorg, where
// the sequential drain logged the failed node and went on.
func (stp *SubtreeProcessor) dequeueDuringReorg(ctx context.Context, transactionMap *SortedHashSet, losingTxHashesMap txmap.TxMap,
	conflictingHashes map[chainhash.Hash]struct{}, skipNotification bool) error {
	if !stp.parallelReorgDrain {
		return stp.dequeueDuringBlockMovement(transactionMap, losingTxHashesMap, conflictingHashes, skipNotification)
	}

	// the parallel build continues the open subtree and fails without one; placeNode creates it. A full
	// open subtree still takes the parallel path and fails the reorg: placeNode would drop every node
	// into it with only a log line.
	hasOpen := stp.currentSubtree.Load() != nil

	parallel := reorgParallelDrain && hasOpen && len(conflictingHashes) == 0 && (losingTxHashesMap == nil || losingTxHashesMap.Length() == 0)

	if reorgDrainHook != nil {
		reorgDrainHook(parallel)
	}

	if !parallel {
		return stp.dequeueDuringBlockMovement(transactionMap, losingTxHashesMap, conflictingHashes, skipNotification)
	}

	return stp.drainQueueAfterBlock(ctx, &deferredBlockDrain{
		drainQueue:       true,
		transactionMap:   transactionMap,
		skipNotification: skipNotification,
	})
}
