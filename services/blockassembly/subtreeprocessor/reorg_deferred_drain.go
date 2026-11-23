package subtreeprocessor

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
)

// The fast path's final drain (issue 2269, design step 9) runs after the reorg has answered, through the
// deferred drain of a single block (deferredBlockDrain), not before the awaited stores and the marks: at
// 0.3 to 1.3M tx/s it took 6 to 27 s of the critical path. The template is published after the marks, and
// the queue that built up meanwhile is drained against the union of the winning blocks afterwards.
//
// The mark-false set is read before the drain, so what the drain adds is in none of the marks.
//
// A failure of the drain cannot undo the reorg. It is logged and requests a reset, as the single-block
// drain does, which reloads the unmined transactions from the store.

// reorgFilterSet is the union of the winning blocks of a fast-path reorg, kept past its drain.
type reorgFilterSet struct {
	winners *SortedHashSet
	tip     *model.BlockHeader // the tip the reorg moved to
}

func (f *reorgFilterSet) exists(hash chainhash.Hash) bool { return f.winners.Exists(hash) }

// announceDeferredReorgDrain is, for the drain a reorg left, what handleMoveForwardRequest does for a
// block's before it answers: it says the processor is about to be busy and copies the open subtree, so a
// mining candidate asked for meanwhile is served that copy.
func (stp *SubtreeProcessor) announceDeferredReorgDrain() {
	if stp.pendingReorgDrain == nil {
		return
	}

	var snapshot *afterBlockSnapshot
	if data := stp.incompleteSubtreeSnapshot(); data != nil {
		snapshot = &afterBlockSnapshot{data: data, parentTxMap: stp.currentTxMap}
	}

	stp.afterBlockMiningData.Store(snapshot)
	stp.drainingAfterBlock.Store(true)
}

// runDeferredReorgDrain drains the queue against the union of the winning blocks, if the reorg that just
// answered left a drain to run. Until the queue is empty again, or the tip moves, the union filters every
// batch the processor dequeues: a transaction a winning block mined can miss the drain (the double-spend
// window holds its batch back, or the item bound ends the drain first) and reach an ordinary dequeue later.
//
// It does not clear the retired tx map half, as runDeferredBlockDrain does: the fast path never swapped maps.
func (stp *SubtreeProcessor) runDeferredReorgDrain(ctx context.Context) {
	drain := stp.pendingReorgDrain
	if drain == nil {
		return
	}

	stp.pendingReorgDrain = nil

	defer func() {
		stp.drainingAfterBlock.Store(false)
		stp.afterBlockMiningData.Store(nil)
	}()

	// the state it found is put back, for a caller that is not the dispatcher
	defer stp.setCurrentRunningState(stp.GetCurrentRunningState())

	stp.setCurrentRunningState(StateDequeue)

	if drainStartHook != nil {
		drainStartHook()
	}

	defer stp.startPhases("reorgDrain")()

	// armed before the drain, so that nothing dequeued after a failure of it goes in unfiltered
	stp.reorgFilter.Store(&reorgFilterSet{winners: drain.transactionMap, tip: stp.currentBlockHeader.Load()})

	stp.phases.count("fast_drain_queue", int(stp.QueueLength()))

	if err := stp.runHandlerWithRecover("deferredReorgDrain", func() error {
		return stp.drainQueueAfterBlock(ctx, drain)
	}); err != nil {
		stp.logger.Errorf("[SubtreeProcessor][reorgInPlace] error draining the queue after the reorg, requesting a block assembly reset: %v", err)
		stp.drainResetRequested.Store(true)
	} else if stp.queue.length() == 0 {
		stp.reorgFilter.Store(nil)
	}

	// addNode does not count what it adds
	stp.setTxCountFromSubtrees()

	stp.flushDiskTxMapWriters()
	stp.drainAndLogDiskTxMapErr("reorgBlocks_inplace_drain")

	stp.phases.done("fast_drain")
}

// expireReorgFilter releases the union of a fast-path reorg once nothing it could still classify is
// queued, or the tip has moved. The dequeue loop calls it before each pass.
func (stp *SubtreeProcessor) expireReorgFilter() {
	f := stp.reorgFilter.Load()
	if f == nil {
		return
	}

	if stp.queue.length() == 0 || !sameTip(stp.currentBlockHeader.Load(), f.tip) {
		stp.reorgFilter.CompareAndSwap(f, nil)
	}
}

func sameTip(a, b *model.BlockHeader) bool {
	return a == b || (a != nil && b != nil && a.Hash().IsEqual(b.Hash()))
}
