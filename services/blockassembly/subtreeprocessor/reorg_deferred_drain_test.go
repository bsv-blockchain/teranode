package subtreeprocessor

import (
	"context"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/stretchr/testify/require"
)

// Issue 2269, plan PR 7b: the fast path's final drain runs after the reorg has answered, through the
// deferred-drain mechanism of a single block (deferredBlockDrain). The reorg publishes its template after
// the marks; the queue that built up meanwhile is drained against the union of the winning blocks
// afterwards, and that union stays to filter later batches while the queue may still hold a transaction
// the winners mined.

// settableClock is a clock a test moves while the processor reads it.
type settableClock struct{ nanos atomic.Int64 }

func (c *settableClock) set(t time.Time) { c.nanos.Store(t.UnixNano()) }
func (c *settableClock) Now() time.Time  { return time.Unix(0, c.nanos.Load()) }

// queueScenarioTxs puts the scenario's transactions in the processor's queue as one batch.
func queueScenarioTxs(t testing.TB, sc *reorgScenario, ids []scenarioID) {
	t.Helper()

	nodes := make([]subtreepkg.Node, len(ids))
	inpoints := make([]*subtreepkg.TxInpoints, len(ids))

	for i, id := range ids {
		tx := sc.tx(t, id)
		nodes[i], inpoints[i] = tx.node, &tx.inpoints
	}

	sc.stp.AddBatch(nodes, inpoints)
}

// queueIntake queues n transactions no block holds, as startIntake does, and returns their ids.
func queueIntake(t testing.TB, sc *reorgScenario, from, n int) []scenarioID {
	t.Helper()

	ids := make([]scenarioID, n)
	nodes := make([]subtreepkg.Node, n)
	inpoints := make([]*subtreepkg.TxInpoints, n)

	for i := range n {
		ids[i] = kindIntake | scenarioID(from+i)
		p := voutZeroInpoints(sc.confirmedParent(ids[i]))
		nodes[i] = subtreepkg.Node{Hash: sc.hash(ids[i]), Fee: 200, SizeInBytes: 250}
		inpoints[i] = &p
	}

	sc.stp.AddBatch(nodes, inpoints)

	return ids
}

// firstBlockWinnerOnly returns n winner-only transactions of the first forward block.
func firstBlockWinnerOnly(t testing.TB, sc *reorgScenario, n int) []scenarioID {
	t.Helper()

	var ids []scenarioID

	for _, id := range sc.winnerIDs[:len(sc.winnerIDs)/2] {
		if id.kind() == kindWinnerOnly && len(ids) < n {
			ids = append(ids, id)
		}
	}

	require.Len(t, ids, n)

	return ids
}

// onFirstFastBeat runs fn once, at the first progress beat of the fast path, which is after the reorg's
// own head drain: what arrives then is what the final drain exists for.
func onFirstFastBeat(sc *reorgScenario, fn func()) {
	fired := false

	sc.stp.SetProgressHook(func() {
		if !fired && sc.stp.reorgPendingD1 != nil {
			fired = true

			fn()
		}
	})
}

// indexOfLine is the position of the first logged line that starts with prefix, or -1.
func (l *phaseLogger) indexOfLine(prefix string) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	return slices.IndexFunc(l.lines, func(line string) bool { return len(line) >= len(prefix) && line[:len(prefix)] == prefix })
}

func newDeferredDrainScenario(t *testing.T) (*reorgScenario, *phaseLogger) {
	t.Helper()

	sc := newReorgScenario(t, smallScenarioConfig())
	sc.stp.settings.BlockAssembly.ReorgFastPath = true

	logger := &phaseLogger{}
	sc.stp.logger = logger

	return sc, logger
}

// The reorg answers after the marks with the queue untouched, and the drain runs after it.
func TestReorgScenario_FastPathPublishesTheTemplateBeforeTheDrain(t *testing.T) {
	sc, logger := newDeferredDrainScenario(t)

	var arrivals, winners []scenarioID

	onFirstFastBeat(sc, func() {
		arrivals = queueIntake(t, sc, 0, 30)
		winners = firstBlockWinnerOnly(t, sc, 5)
		queueScenarioTxs(t, sc, winners)
	})

	require.NoError(t, sc.runWithoutDrain(t))
	require.Len(t, arrivals, 30)

	require.EqualValues(t, 35, sc.stp.QueueLength(), "the queue is not drained before the reorg answers")
	require.Equal(t, sc.expectedOrderAfter(), txHashes(assemblyNodes(sc.stp)), "the template holds the reorg's result only")
	require.NotContains(t, logger.phases("reorgBlocks"), "fast_drain")
	require.Empty(t, logger.phases("reorgDrain"), "the drain has not started")
	require.NotNil(t, sc.stp.pendingReorgDrain)

	sc.stp.runDeferredReorgDrain(context.Background())

	require.Zero(t, sc.stp.QueueLength())
	require.Nil(t, sc.stp.pendingReorgDrain)

	want := slices.Concat(sc.expectedOrderAfter(), sc.hashes(arrivals))
	require.Equal(t, want, txHashes(assemblyNodes(sc.stp)), "the drain adds what no winner mined and drops what a winner mined")

	for _, id := range winners {
		require.False(t, sc.stp.currentTxMap.Exists(sc.hash(id)), "a transaction of the first block is dropped by the union")
	}

	requireAssemblyValid(t, sc.stp, sc.onChainAfter)

	marks, drain := logger.indexOfLine("[reorgBlocks] phase marks took"), logger.indexOfLine("[reorgDrain] phase fast_drain took")
	require.NotEqual(t, -1, marks)
	require.Greater(t, drain, marks, "the drain is logged after the marks")
	require.Greater(t, drain, logger.indexOfLine("[reorgBlocks] phases total="), "and after the reorg's own summary")
	require.Nil(t, sc.stp.reorgFilter.Load(), "a drain that took everything leaves no filter behind")
}

// The set of transactions to mark off the longest chain is read before the drain, and what the drain
// adds changes neither marks nor the set.
func TestReorgScenario_FastPathMarksAreSettledBeforeTheDrain(t *testing.T) {
	sc, _ := newDeferredDrainScenario(t)

	onFirstFastBeat(sc, func() {
		queueIntake(t, sc, 0, 30)
		// a transaction of the lost block that arrives again: the map holds it, so the drain drops it
		queueScenarioTxs(t, sc, []scenarioID{sc.lostIDs[0]})
	})

	require.NoError(t, sc.runWithoutDrain(t))
	require.EqualValues(t, 31, sc.stp.QueueLength(), "the drain has not run when the marks are done")

	falseBefore, trueBefore := sortedHashes(sc.utxo.marked(false)), sortedHashes(sc.utxo.marked(true))
	require.Equal(t, sortedHashes(sc.expectedMarkFalse()), falseBefore)

	sc.stp.runDeferredReorgDrain(context.Background())

	require.Equal(t, falseBefore, sortedHashes(sc.utxo.marked(false)), "the drain marks nothing")
	require.Equal(t, trueBefore, sortedHashes(sc.utxo.marked(true)), "the drain marks nothing")
}

// A failure of the drain after the reorg answered is not a failed reorg: the committed state stays, a
// reset is requested, and the reset releases the filter.
func TestReorgScenario_FastPathDrainFailureAfterTheCommitRequestsAReset(t *testing.T) {
	sc, _ := newDeferredDrainScenario(t)

	onFirstFastBeat(sc, func() { queueIntake(t, sc, 0, 30) })

	require.NoError(t, sc.runWithoutDrain(t))

	committed := txHashes(assemblyNodes(sc.stp))
	committedTip := *sc.stp.GetCurrentBlockHeader().Hash()
	require.Equal(t, sc.expectedOrderAfter(), committed)

	// a cancelled context (shutdown) makes the parallel build of the drain fail
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	sc.stp.runDeferredReorgDrain(ctx)

	require.True(t, sc.stp.TakeDrainResetRequested(), "a failed drain requests a reset")
	require.False(t, sc.stp.failedForkReorgNeedsReset.Load(), "the reorg itself succeeded")
	require.Equal(t, committedTip, *sc.stp.GetCurrentBlockHeader().Hash())
	require.Equal(t, committed, txHashes(assemblyNodes(sc.stp))[:len(committed)], "what the reorg committed is untouched")
	require.Equal(t, sortedHashes(sc.expectedMarkFalse()), sortedHashes(sc.utxo.marked(false)))
	require.NotNil(t, sc.stp.reorgFilter.Load(), "until the reset, a batch dequeued meanwhile is still filtered")

	require.NoError(t, sc.stp.reset(sc.forward[len(sc.forward)-1].Header, nil, nil, false, func() error { return nil }))
	require.Nil(t, sc.stp.reorgFilter.Load(), "the reset discards the queue, and with it the need for the filter")
}

// A reorg that fails leaves no drain and no filter behind.
func TestReorgScenario_FastPathFailedReorgLeavesNoDeferredDrain(t *testing.T) {
	sc, r, _ := newAwaitedStoreScenario(t, 5*time.Second, failAwaited)
	_ = r

	onFirstFastBeat(sc, func() { queueIntake(t, sc, 0, 10) })

	var rec fastPathRun

	require.Error(t, recordingFastPath(&rec)(t, sc))
	require.True(t, rec.applied, "declined: %v", rec.reasons)

	require.Nil(t, sc.stp.pendingReorgDrain)
	require.Nil(t, sc.stp.reorgFilter.Load())
}

// Design section 5 item 10: a transaction a winner mined can miss the drain, here because the
// double-spend window holds its batch back, and reach an ordinary dequeue later. It is classified against
// the union, and the union is released once the queue is empty.
func TestReorgScenario_FastPathFiltersALateBatchAgainstTheRetainedUnion(t *testing.T) {
	sc, _ := newDeferredDrainScenario(t)
	sc.stp.settings.BlockAssembly.DoubleSpendWindow = time.Minute

	base := time.Now()
	clk := &settableClock{}
	clk.set(base)
	sc.stp.clock, sc.stp.queue.clock = clk, clk

	mined := firstBlockWinnerOnly(t, sc, 3)
	queueScenarioTxs(t, sc, mined)
	fresh := queueIntake(t, sc, 0, 3)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	sc.stp.Start(ctx)

	require.NoError(t, sc.stp.Reorg([]*model.Block{sc.lost}, sc.forward))

	require.Eventually(t, func() bool { return !sc.stp.DrainingAfterBlock() }, 5*time.Second, time.Millisecond)
	require.EqualValues(t, 6, sc.stp.QueueLength(), "the window holds both batches past the drain")
	require.NotNil(t, sc.stp.reorgFilter.Load(), "the union stays while the queue holds anything")

	clk.set(base.Add(2 * time.Minute))

	require.Eventually(t, func() bool { return sc.stp.QueueLength() == 0 }, 5*time.Second, time.Millisecond)

	hashes := sc.stp.GetTransactionHashes(ctx)

	for _, id := range mined {
		require.NotContains(t, hashes, sc.hash(id), "a transaction a winner mined never reaches a template")
		require.False(t, sc.stp.GetCurrentTxMap().Exists(sc.hash(id)))
	}

	for _, id := range fresh {
		require.Contains(t, hashes, sc.hash(id))
	}

	require.Eventually(t, func() bool { return sc.stp.reorgFilter.Load() == nil }, 5*time.Second, time.Millisecond,
		"the union is released once the queue is empty")
}

// Through the dispatcher: the reorg answers while the drain is still held, block assembly is told the
// processor is busy with it, a mining candidate asked for meanwhile is answered at once, and the drain then
// runs to the end.
func TestReorgScenario_FastPathDispatcherAnswersBeforeTheDrainRuns(t *testing.T) {
	sc, _ := newDeferredDrainScenario(t)

	held, release := make(chan struct{}), make(chan struct{})

	drainStartHook = func() {
		close(held)
		<-release
	}

	t.Cleanup(func() { drainStartHook = nil })

	var arrivals []scenarioID

	onFirstFastBeat(sc, func() { arrivals = queueIntake(t, sc, 0, 30) })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	sc.stp.Start(ctx)

	reorgDone := make(chan error, 1)
	go func() { reorgDone <- sc.stp.Reorg([]*model.Block{sc.lost}, sc.forward) }()

	select {
	case <-held:
	case <-time.After(10 * time.Second):
		t.Fatal("the drain never started")
	}

	select {
	case err := <-reorgDone:
		require.NoError(t, err, "the reorg answers while the drain is held")
	case <-time.After(5 * time.Second):
		t.Fatal("the reorg was held back by its drain")
	}

	require.True(t, sc.stp.DrainingAfterBlock(), "block assembly is told the drain is running")
	require.EqualValues(t, 30, sc.stp.QueueLength(), "the queue is still full")

	start := time.Now()
	_ = sc.stp.GetIncompleteSubtreeMiningData(ctx)
	require.Less(t, time.Since(start), time.Second, "a mining candidate asked for meanwhile is not held behind the drain")

	close(release)

	require.Eventually(t, func() bool { return !sc.stp.DrainingAfterBlock() && sc.stp.QueueLength() == 0 }, 5*time.Second, time.Millisecond)

	hashes := sc.stp.GetTransactionHashes(ctx)

	for _, id := range arrivals {
		require.Contains(t, hashes, sc.hash(id))
	}
}

// The union is kept for the tip it was built for: once a block moves the tip on, it is released even
// though the queue still holds a batch.
func TestReorgScenario_FastPathReleasesTheUnionWhenTheTipMoves(t *testing.T) {
	sc, _ := newDeferredDrainScenario(t)
	sc.stp.settings.BlockAssembly.DoubleSpendWindow = time.Hour

	queueIntake(t, sc, 0, 3) // held back by the window, so the queue is never empty

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	sc.stp.Start(ctx)

	require.NoError(t, sc.stp.Reorg([]*model.Block{sc.lost}, sc.forward))
	require.Eventually(t, func() bool { return !sc.stp.DrainingAfterBlock() }, 5*time.Second, time.Millisecond)
	require.EqualValues(t, 3, sc.stp.QueueLength())
	require.NotNil(t, sc.stp.reorgFilter.Load(), "the union is kept while the queue holds a batch")

	// an empty block on top of the last winner: it moves the tip and mines nothing
	tip := sc.forward[len(sc.forward)-1]
	header := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  tip.Header.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      tip.Header.Timestamp + 1,
		Nonce:          777,
	}
	next := &model.Block{Height: tip.Height + 1, Header: header, CoinbaseTx: scenarioCoinbase(t, tip.Height+1, 777, 0), Subtrees: []*chainhash.Hash{}}

	require.NoError(t, sc.stp.MoveForwardBlock(next))
	require.Equal(t, *header.Hash(), *sc.stp.GetCurrentBlockHeader().Hash())
	require.EqualValues(t, 3, sc.stp.QueueLength(), "the window still holds the batch")

	require.Eventually(t, func() bool { return sc.stp.reorgFilter.Load() == nil }, 5*time.Second, time.Millisecond,
		"a tip that moved releases the union although the queue is not empty")
}
