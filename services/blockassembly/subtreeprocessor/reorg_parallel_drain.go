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
// order, with the filter, the tx map insert and the subtree build on every core. A conflict is in play
// when the step has conflicting hashes, which skip a queued transaction when an earlier queued one was,
// an order the parallel steps do not keep, or losing hashes, which the step's own filter has to apply to
// the queue one transaction at a time. Outside a fork reorg, and for such a step, it is
// dequeueDuringBlockMovement.
func (stp *SubtreeProcessor) dequeueDuringReorg(ctx context.Context, transactionMap *SortedHashSet, losingTxHashesMap txmap.TxMap,
	conflictingHashes map[chainhash.Hash]struct{}, skipNotification bool) error {
	if !stp.parallelReorgDrain {
		return stp.dequeueDuringBlockMovement(transactionMap, losingTxHashesMap, conflictingHashes, skipNotification)
	}

	parallel := reorgParallelDrain && len(conflictingHashes) == 0 && (losingTxHashesMap == nil || losingTxHashesMap.Length() == 0)

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
