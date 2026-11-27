package subtreeprocessor

import "context"

// The fast path's final drain (issue 2269, design step 9) runs after the reorg has answered, through the
// deferred drain of a single block (deferredBlockDrain), not before the awaited stores and the marks: at
// 0.3 to 1.3M tx/s it took 6 to 27 s of the critical path. The template is published after the marks, and
// the queue that built up meanwhile is drained afterwards, against the winners' set that the reorg kept
// (keepMinedSet), as it is for every block.
//
// The mark-false set is read before the drain, so what the drain adds is in none of the marks.
//
// A failure of the drain cannot undo the reorg. It is logged and requests a reset, as the single-block
// drain does, which reloads the unmined transactions from the store.

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

// runDeferredReorgDrain drains the queue against the winning blocks' transactions, if the reorg that just
// answered left a drain to run. A transaction a winning block mined can miss the drain (the double-spend
// window holds its batch back, or the item bound ends the drain first) and reach an ordinary dequeue
// later: the set the reorg kept filters it there, until its retention has passed.
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

	// a drain that fails records the phase it failed in, as a failed reorg does
	var drainErr error

	defer stp.startPhases("reorgDrain", &drainErr)()

	stp.phases.count("fast_drain_queue", int(stp.QueueLength()))

	if drainErr = stp.runHandlerWithRecover("deferredReorgDrain", func() error {
		return stp.drainQueueAfterBlock(ctx, drain)
	}); drainErr != nil {
		stp.logger.Errorf("[SubtreeProcessor][reorgInPlace] error draining the queue after the reorg, requesting a block assembly reset: %v", drainErr)
		stp.drainResetRequested.Store(true)
	}

	// addNode does not count what it adds
	stp.setTxCountFromSubtrees()

	stp.flushDiskTxMapWriters()
	stp.drainAndLogDiskTxMapErr("reorgBlocks_inplace_drain")

	stp.phases.done("fast_drain")
}
