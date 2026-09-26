package netsync

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// These tests cover a dispatch that commits a parked block off disk: the shape it
// must arrive in, the two steps it runs, and the three ways it can end. The drain
// that produces such a dispatch lands in a later commit; here the dispatch is
// built by hand, which is also how a reviewer can read what the drain must build.

// parkedDispatchFor builds the dispatch the drain will build, for the harness
// block at the given index, with its park entry already admitted and its blob on
// disk. It streams the block through the real intake path (pipelineBlockSink +
// handleBlockOnDiskMsg, via h.deliver) rather than Admit/WriteAdmitted/FinishWrite,
// which no longer exist — the streaming path is what actually adopts a converted
// record now (blockPark.AdoptWritten).
func (h *parkWiringHarness) parkedDispatchFor(t *testing.T, index int) (*blockDispatch, parkedBlock) {
	t.Helper()

	require.NoError(t, h.deliver(t, index))

	hash := h.blocks[index].MsgBlock().BlockHash()

	// The drain takes the entry out of the index before it dispatches, so the
	// dispatch owns it and every path out settles it.
	taken, ok := h.sm.blockPark.Take(hash)
	require.True(t, ok)

	return &blockDispatch{parked: &taken, bytes: taken.size}, taken
}

// TestParkDispatch_ADispatchedParkedBlockCommitsAndTakesTheParkedTail is the happy
// path end to end through the dispatcher: the worker reads the blob and validates
// it, and the parked tail runs the post-commit bookkeeping.
func TestParkDispatch_ADispatchedParkedBlockCommitsAndTakesTheParkedTail(t *testing.T) {
	h := newParkWiringHarness(t, true)
	bd := h.withDispatcher(t)

	// The chain holds everything, which is how every park test fakes a commit.
	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(true, nil)

	d, entry := h.parkedDispatchFor(t, 1)

	require.True(t, bd.canDispatch(d), "an unwindowed dispatch is admissible into an empty frontier")

	bd.dispatch(d)
	require.Len(t, bd.frontier, 1, "the parked dispatch is in the frontier")
	require.Equal(t, entry.hash, bd.frontier[0].hash, "the entry is keyed by the parked block's own hash, not by a queue message")

	drainCompletionsUntilEmpty(t, bd)

	require.Zero(t, h.sm.blockPark.Len(), "the entry is settled, not left in the index")

	for _, name := range parkDirEntries(t, h.parkDir) {
		require.NotContains(t, name, entry.hash.String(), "a committed block's blob is deleted by the disposition")
	}

	_, failed := h.sm.recentlyFailedBlocks.Get(entry.hash)
	require.False(t, failed, "a committed block is not marked as having failed")
}

// TestParkDispatch_AReadFailureKeepsTheBlockAndIsNotJudged is the reason the read
// error is carried in a field of its own.
//
// The two classifications default opposite ways: a read failure keeps the block,
// because the store may simply have had no permit free, and a commit failure
// judges it and blames a peer. Collapse them and a node under ordinary store load
// destroys fully downloaded blocks and evicts the peers that sent them.
func TestParkDispatch_AReadFailureKeepsTheBlockAndIsNotJudged(t *testing.T) {
	h := newParkWiringHarness(t, true)
	bd := h.withDispatcher(t)

	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(true, nil)

	d, entry := h.parkedDispatchFor(t, 1)

	// An unclassified failure from the store, which is what a driver error looks
	// like: not a missing blob, not a context error.
	h.store.failReadsWith(errors.NewProcessingError("the store is having a bad day"))

	bd.dispatch(d)
	drainCompletionsUntilEmpty(t, bd)

	require.NotNil(t, d.readErr, "the read failure is recorded apart from the completion error")
	require.Equal(t, 1, h.sm.blockPark.Len(), "an unclassified read failure keeps the block")

	_, failed := h.sm.recentlyFailedBlocks.Get(entry.hash)
	require.False(t, failed, "and does not judge it")
}

// TestParkDispatch_ACommitFailureIsJudgedByTheCommitTable is the other default:
// a fault of the block itself gives the block up.
func TestParkDispatch_ACommitFailureIsJudgedByTheCommitTable(t *testing.T) {
	h := newParkWiringHarnessInState(t, true, blockchain2.FSMStateRUNNING)
	bd := h.withDispatcher(t)

	d, entry := h.parkedDispatchFor(t, 1)

	// The block's own existence check fails with a fault of the block, which is
	// the same route the park's own recovery tests use to drive a judged commit.
	h.client.On("GetBlockExists", mock.Anything, &entry.hash).
		Return(false, errors.NewBlockInvalidError("this block is not one we can take"))
	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(true, nil)

	bd.dispatch(d)
	drainCompletionsUntilEmpty(t, bd)

	require.Nil(t, d.readErr, "the blob read succeeded; this is a commit failure")
	require.Zero(t, h.sm.blockPark.Len(), "a judged block does not stay parked")

	_, failed := h.sm.recentlyFailedBlocks.Get(entry.hash)
	require.True(t, failed, "and it is marked so its descendants are suppressed")
}

// TestParkDispatch_ADrainedBlockPassesANilParentSoTheWorkerLooksItUp is the
// invariant the whole design rests on, and the one edit that would break it
// silently.
//
// Legacy must never hand block validation a block whose parent is not committed.
// A parked dispatch enforces that in the worker rather than on a promise from the
// consumer: it passes a nil parent, so HandleBlockDirect performs its own
// GetBlockHeader on the previous hash and refuses the block if the parent is not
// there. Passing &inflightParent{height} instead would look like an optimisation,
// would skip that lookup, and would take the height on trust from a park entry
// whose height came off the header list.
//
// So this test puts the parent NOT in the chain, which is the state the guard
// exists for, and requires that the lookup happened and the block was kept rather
// than judged. Under the mutation the lookup never runs.
func TestParkDispatch_ADrainedBlockPassesANilParentSoTheWorkerLooksItUp(t *testing.T) {
	h := newParkWiringHarness(t, true)
	bd := h.withDispatcher(t)

	d, entry := h.parkedDispatchFor(t, 1)

	// The block itself is not stored, so HandleBlockDirect goes on to the parent.
	// The harness answers every header lookup with "no such block", so the parent
	// is missing, which is what makes the lookup observable.
	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)

	// A parked dispatch carries no resolved parent at all any more — the field
	// itself is gone — which is what leaves HandleBlockDirect's own lookup below
	// as the only route to an answer.

	bd.dispatch(d)
	drainCompletionsUntilEmpty(t, bd)

	h.client.AssertCalled(t, "GetBlockHeader", mock.Anything, &entry.prevBlock)

	require.Equal(t, 1, h.sm.blockPark.Len(),
		"a parent that is gone keeps the block, so the sweep can retry it when the parent lands")

	_, failed := h.sm.recentlyFailedBlocks.Get(entry.hash)
	require.False(t, failed, "and a missing parent is not a judgement on the block")
}

// TestParkDispatch_TheWrongShapeIsRefusedAndTheEntryRestored covers the guard in
// dispatch.
//
// An UNWINDOWED parked block arriving into a non-empty frontier would mean the
// server-side window already holds a legacy entry, so it would be refused
// admission there.
//
// A resolved parent used to be a second wrong shape, checked here on a
// blockDispatch.parent field that no longer exists: a parked dispatch never
// carries one at all now (the field itself, and the head that used to resolve
// one for a decoded queue message, are both deleted), so there is nothing left
// to break that way.
//
// Being windowed was a third wrong shape and is not one any more. A drained
// dispatch on the window route with a known height is now marked windowed
// deliberately, so it can run beside another block instead of waiting for the
// window to empty, which is what left the validator idle between every block
// drained from the park. TestParkedDispatchMayBeWindowed covers the accepted
// case; the emptiness rule below still applies to a dispatch that is not
// windowed, which is what the height-zero case remains.
func TestParkDispatch_TheWrongShapeIsRefusedAndTheEntryRestored(t *testing.T) {
	for _, tc := range []struct {
		name   string
		break_ func(d *blockDispatch, bd *blockDispatcher)
	}{
		{
			name: "a non-empty frontier",
			break_: func(_ *blockDispatch, bd *blockDispatcher) {
				bd.frontier = append(bd.frontier, &frontierEntry{
					hash:    chainhash.HashH([]byte("something already in flight")),
					height:  750_699,
					settled: make(chan struct{}),
				})
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newParkWiringHarness(t, true)
			bd := h.withDispatcher(t)

			h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(true, nil)

			d, entry := h.parkedDispatchFor(t, 1)
			require.Zero(t, h.sm.blockPark.Len(), "precondition: the drain has taken the entry")

			tc.break_(d, bd)

			// Sampled after the case has set the scene: the non-empty-frontier case
			// puts an entry there itself, and that entry must survive the refusal.
			before := len(bd.frontier)

			bd.dispatch(d)

			require.Len(t, bd.frontier, before, "the malformed dispatch is refused")
			require.True(t, h.sm.blockPark.Has(entry.hash),
				"and its entry is put back, or the blob is stranded with its bytes charged and no index entry")
		})
	}
}

// TestParkDispatch_ShutdownRestoresAParkedDispatchInsteadOfReplying covers the
// quit arm of the consumer loop.
func TestParkDispatch_ShutdownRestoresAParkedDispatchInsteadOfReplying(t *testing.T) {
	h := newParkWiringHarness(t, true)
	bd := h.withDispatcher(t)

	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(true, nil)

	h.sm.quit = make(chan struct{})
	h.sm.settings.BlockValidation.QuickWindowBlocks = 1
	h.sm.settings.BlockValidation.QuickValidateSkipUtxoLock = true

	release := make(chan struct{})
	bd.parkedRun = func(context.Context, *blockDispatch) error {
		<-release

		return nil
	}

	d, entry := h.parkedDispatchFor(t, 1)
	bd.dispatch(d)

	// Nothing else is queued on purpose. With no drain request and no completion
	// or parkCommit pending, the quit arm is the only ready one, so the loop
	// takes it as soon as it reaches its select, which is the arm under test.
	done := make(chan struct{})

	go func() {
		h.sm.dispatchBlocks()
		close(done)
	}()

	close(h.sm.quit)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the consumer did not return on quit")
	}

	close(release)

	require.True(t, h.sm.blockPark.Has(entry.hash),
		"a parked dispatch in flight at shutdown must have its entry restored, not replied to")
}

// drainCompletionsUntilEmpty pumps the completions channel the way the consumer
// goroutine does, until the frontier is empty.
func drainCompletionsUntilEmpty(t *testing.T, bd *blockDispatcher) {
	t.Helper()

	deadline := time.After(10 * time.Second)

	for len(bd.frontier) > 0 {
		select {
		case c := <-bd.completions:
			bd.complete(c)
		case <-deadline:
			t.Fatal("timed out waiting for the frontier to drain")
		}
	}
}

// TestParkPeek_FirstChildForDoesNotClaimTheEntry is the peek half of peek-then-claim.
//
// Claiming an entry and then asking the dispatcher whether it may be admitted
// leaves a refused block stranded: out of the index, blob on disk, still charged
// against the park's byte budget, no cursor rewind, and nothing to recover it
// until the process restarts. Under a one-deep window, which the block-size
// ladder forces in a giant-block era, the depth arm refuses every candidate while
// anything else is in flight, so a refusal is the common case rather than a
// corner.
func TestParkPeek_FirstChildForDoesNotClaimTheEntry(t *testing.T) {
	h := newParkWiringHarness(t, true)

	msgBlock := h.blocks[1].MsgBlock()
	parent := msgBlock.Header.PrevBlock
	hash := msgBlock.BlockHash()

	require.NoError(t, h.deliver(t, 1))

	bytesBefore := h.sm.blockPark.Bytes()

	peeked, ok := h.sm.blockPark.FirstChildFor(parent)
	require.True(t, ok, "the parent has a committable child")
	require.Equal(t, hash, peeked.hash)
	require.Positive(t, peeked.size, "the peek carries the size the byte arm of the admission test needs")

	require.True(t, h.sm.blockPark.Has(hash), "a peek must not remove the entry")
	require.Equal(t, 1, h.sm.blockPark.Len())
	require.Equal(t, bytesBefore, h.sm.blockPark.Bytes(), "and must not change the byte total")

	// Peeking twice is the same answer, because nothing was consumed.
	again, ok := h.sm.blockPark.FirstChildFor(parent)
	require.True(t, ok)
	require.Equal(t, peeked.hash, again.hash)

	// The claim is what removes it, and it is the existing call.
	taken, ok := h.sm.blockPark.Take(hash)
	require.True(t, ok)
	require.Equal(t, hash, taken.hash)
	require.Zero(t, h.sm.blockPark.Len())

	_, ok = h.sm.blockPark.FirstChildFor(parent)
	require.False(t, ok, "and once claimed there is nothing left to peek")
}

// TestDrain_NothingIsTakenUntilTheDispatcherSaysYes is the other half of
// peek-then-claim, at the level of the loop rather than the park.
//
// With something already in flight the admission test refuses a drained
// candidate, and at depth 1 that is the common case rather than a corner. The
// candidate must be left in the park, not claimed and stranded.
func TestDrain_NothingIsTakenUntilTheDispatcherSaysYes(t *testing.T) {
	h := newParkWiringHarness(t, true)
	bd := h.withDispatcher(t)

	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	require.NoError(t, h.deliver(t, 1))
	require.Equal(t, 1, h.sm.blockPark.Len())

	parkedBytes := h.sm.blockPark.Bytes()

	// Something else is in flight, so the frontier is not empty.
	bd.frontier = append(bd.frontier, &frontierEntry{
		hash:    chainhash.HashH([]byte("a block already in flight")),
		height:  750_699,
		settled: make(chan struct{}),
	})

	h.sm.drainAsync.Store(true)
	h.sm.scheduleDrain(h.blocks[1].MsgBlock().Header.PrevBlock, 1)

	require.False(t, h.sm.drainStep(bd), "the drain step admits nothing while the frontier is busy")

	require.Equal(t, 1, h.sm.blockPark.Len(), "and it leaves the entry in the index")
	require.Equal(t, parkedBytes, h.sm.blockPark.Bytes(), "with its bytes still accounted for")
	require.Len(t, h.sm.drainQueue, 1, "and the request still queued, to be offered again")
	require.Len(t, bd.frontier, 1, "nothing was dispatched")
}

// TestDrain_AChainOfParkedBlocksDrainsOneAfterAnother covers the claim that the
// drained chain continues: a parked block that commits schedules whatever is
// parked behind it, so a run of them empties without the sweep.
//
// Where that scheduling lives matters. It is in the parked tail, not inside
// parkedBlockCommitted, because the pre-window path calls that too and would turn
// its explicit stack walk back into recursion, one frame set per link of a chain
// that can be thousands long, each frame holding a decoded block.
func TestDrain_AChainOfParkedBlocksDrainsOneAfterAnother(t *testing.T) {
	h := newParkWiringHarness(t, true)
	h.withDispatcher(t)

	h.sm.settings.BlockValidation.QuickWindowBlocks = 1
	h.sm.settings.BlockValidation.QuickValidateSkipUtxoLock = true
	h.sm.quit = make(chan struct{})

	first := h.blocks[0].MsgBlock()
	second := h.blocks[1].MsgBlock()
	third := h.blocks[2].MsgBlock()

	secondHash := second.BlockHash()
	thirdHash := third.BlockHash()

	// Both later blocks arrive before their parents and park. The streaming
	// route never calls GetBlockExists while parking (only a real commit
	// attempt does), so nothing needs scripting for either arrival.
	require.NoError(t, h.deliver(t, 2))
	require.NoError(t, h.deliver(t, 1))
	require.Equal(t, 2, h.sm.blockPark.Len(), "a chain of two blocks is parked")

	// From here the chain holds everything, which is how a commit is faked, and
	// the first block's own parent resolves so its arrival's own parkCommit
	// posts rather than committing inline.
	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(true, nil)
	h.chainHolds(t, first.Header.PrevBlock)

	// New() builds this channel unconditionally; a struct-literal harness needs
	// it wired by hand so the first block's own commit goes through the
	// dispatcher's drain step below rather than being committed inline.
	h.sm.parkCommits = make(chan parkCommit, parkSweepRPCBudget)

	go h.sm.dispatchBlocks()

	t.Cleanup(func() { close(h.sm.quit) })

	require.NoError(t, h.deliver(t, 0))

	require.True(t, WaitUntil(func() bool { return h.sm.blockPark.Len() == 0 }, 10*time.Second),
		"both parked blocks must drain, so a committed drained block has to schedule the one behind it")

	for _, name := range parkDirEntries(t, h.parkDir) {
		require.NotContains(t, name, secondHash.String(), "the first drained block's blob is deleted")
		require.NotContains(t, name, thirdHash.String(), "and so is the second's")
	}
}

// TestDrain_TheSweepsPostIsRestoredAndThenDispatched covers the consumer's own
// handling of a sweep post, which is the arm a test calling sweepParkedBlocks
// directly never reaches.
//
// The sweep takes the entry out of the index to hand it over. The consumer puts
// it back and queues a drain rather than committing it where it stands, so the
// block goes through the one path every other drained block takes: one admission
// test, one header-front advance, one worker. Committing on the arm instead would
// put a full read-and-validate back on the consumer, up to 128 times a tick.
func TestDrain_TheSweepsPostIsRestoredAndThenDispatched(t *testing.T) {
	h := newParkWiringHarness(t, true)
	h.withDispatcher(t)

	h.sm.settings.BlockValidation.QuickWindowBlocks = 1
	h.sm.settings.BlockValidation.QuickValidateSkipUtxoLock = true
	h.sm.quit = make(chan struct{})

	child := h.blocks[1].MsgBlock().BlockHash()

	require.NoError(t, h.deliver(t, 1))
	require.Equal(t, 1, h.sm.blockPark.Len())

	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(true, nil)

	// The pool gives the manager the channel the sweep posts on.
	startParkPool(t, h, 1)
	require.NotNil(t, h.sm.parkCommits)

	// The observable that separates the two possible behaviours. A commit on the
	// consumer arm and a commit through a dispatch both empty the park and delete
	// the blob, so the park alone cannot tell them apart. Only the dispatched
	// route runs the worker's step.
	var onAWorker atomic.Int32

	inner := h.sm.dispatcher.parkedRun
	h.sm.dispatcher.parkedRun = func(ctx context.Context, d *blockDispatch) error {
		onAWorker.Add(1)

		return inner(ctx, d)
	}

	go h.sm.dispatchBlocks()

	// The sweep's own hand-off, built exactly as sweepParkedBlocks builds it: the
	// entry taken out of the index, with the parent height its lookup found.
	entry, ok := h.sm.blockPark.Take(child)
	require.True(t, ok)

	h.sm.parkCommits <- parkCommit{entry: entry, parentHeight: 1}

	require.True(t, WaitUntil(func() bool { return h.sm.blockPark.Len() == 0 }, 10*time.Second),
		"the consumer must restore the posted entry and then dispatch it through the drain step")

	for _, name := range parkDirEntries(t, h.parkDir) {
		require.NotContains(t, name, child.String(), "and the commit deletes the blob")
	}

	_, failed := h.sm.recentlyFailedBlocks.Get(child)
	require.False(t, failed, "the block was committed, not given up on")

	require.Positive(t, onAWorker.Load(),
		"the posted block must be committed by a worker through the drain step, not on the consumer; committing on the arm puts a full read and validate back where this change took it from")
}
