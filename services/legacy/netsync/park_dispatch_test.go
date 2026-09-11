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
// disk.
func (h *parkWiringHarness) parkedDispatchFor(t *testing.T, index int) (*blockDispatch, parkedBlock) {
	t.Helper()

	msgBlock := h.blocks[index].MsgBlock()

	entry := parkedBlock{
		hash:      msgBlock.BlockHash(),
		prevBlock: msgBlock.Header.PrevBlock,
		height:    int32(index + 1),
		peer:      h.peer,
	}

	stored, admitted := h.sm.blockPark.Admit(entry, msgBlock)
	require.Equal(t, admitRegistered, admitted)
	require.Equal(t, parkAccepted, h.sm.blockPark.WriteAdmitted(context.Background(), stored, msgBlock))
	h.sm.blockPark.FinishWrite(stored.hash)

	// The drain takes the entry out of the index before it dispatches, so the
	// dispatch owns it and every path out settles it.
	taken, ok := h.sm.blockPark.Take(stored.hash)
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

	require.Nil(t, d.parent, "a parked dispatch carries no resolved parent")

	bd.dispatch(d)
	drainCompletionsUntilEmpty(t, bd)

	h.client.AssertCalled(t, "GetBlockHeader", mock.Anything, &entry.prevBlock)

	require.Equal(t, 1, h.sm.blockPark.Len(),
		"a parent that is gone keeps the block, so the sweep can retry it when the parent lands")

	_, failed := h.sm.recentlyFailedBlocks.Get(entry.hash)
	require.False(t, failed, "and a missing parent is not a judgement on the block")
}

// TestParkDispatch_ADispatchedParkedBlockNeverTouchesTheBacklog pins the field
// that makes the two kinds of dispatch structurally different.
//
// A parked dispatch has no queue message. Nothing incremented the backlog for it
// and nobody waits on a reply, so reaching finishBlockMsg would decrement a
// counter that was never incremented. That counter gates
// localReadBackpressured's first arm, which suppresses the sync-peer stall check
// while the pipeline is busy; underflow it once and the arm reads as empty for
// the life of the process.
func TestParkDispatch_ADispatchedParkedBlockNeverTouchesTheBacklog(t *testing.T) {
	h := newParkWiringHarness(t, true)
	bd := h.withDispatcher(t)

	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(true, nil)

	// A live block is in the pipeline, so the counter is where a running node
	// keeps it and an underflow would be visible rather than clamped at zero.
	h.sm.blockBacklog.Add(1)

	d, _ := h.parkedDispatchFor(t, 1)

	bd.dispatch(d)
	drainCompletionsUntilEmpty(t, bd)

	require.Equal(t, int64(1), h.sm.blockBacklog.Load(),
		"a parked dispatch must not decrement the backlog; the live block still in the pipeline owns that slot")
}

// TestParkDispatch_TheWrongShapeIsRefusedAndTheEntryRestored covers the guard in
// dispatch, whose two failure modes are both silent.
//
// A resolved parent would make HandleBlockDirect take the parent's word for the
// height and skip the chain lookup, and that lookup is the thing that enforces
// "never hand block validation a block whose parent is not committed". An
// UNWINDOWED parked block arriving into a non-empty frontier would mean the
// server-side window already holds a legacy entry, so it would be refused
// admission there.
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
			name: "a resolved parent",
			break_: func(d *blockDispatch, _ *blockDispatcher) {
				d.parent = &inflightParent{height: 750_699}
			},
		},
		{
			name: "a non-empty frontier",
			break_: func(_ *blockDispatch, bd *blockDispatcher) {
				bd.frontier = append(bd.frontier, &frontierEntry{
					hash:       chainhash.HashH([]byte("something already in flight")),
					height:     750_699,
					rpcStarted: make(chan struct{}),
					settled:    make(chan struct{}),
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

	h.sm.blockBacklog.Add(1)

	// Nothing is queued on purpose. With an empty queue and no pending block the
	// quit arm is the only ready one, so the loop takes it as soon as it reaches
	// its select, which is the arm under test. Feeding a message instead would
	// have it head-processed, which is a different test.
	queue := make(chan *blockQueueMsg, 1)

	done := make(chan struct{})

	go func() {
		h.sm.dispatchBlocks(queue)
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
	require.Equal(t, int64(1), h.sm.blockBacklog.Load(),
		"and the backlog must be untouched by it")
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

	entry := parkedBlock{hash: msgBlock.BlockHash(), prevBlock: parent, height: 2, peer: h.peer}

	stored, admitted := h.sm.blockPark.Admit(entry, msgBlock)
	require.Equal(t, admitRegistered, admitted)
	require.Equal(t, parkAccepted, h.sm.blockPark.WriteAdmitted(context.Background(), stored, msgBlock))
	h.sm.blockPark.FinishWrite(stored.hash)

	bytesBefore := h.sm.blockPark.Bytes()

	peeked, ok := h.sm.blockPark.FirstChildFor(parent)
	require.True(t, ok, "the parent has a committable child")
	require.Equal(t, stored.hash, peeked.hash)
	require.Positive(t, peeked.size, "the peek carries the size the byte arm of the admission test needs")

	require.True(t, h.sm.blockPark.Has(stored.hash), "a peek must not remove the entry")
	require.Equal(t, 1, h.sm.blockPark.Len())
	require.Equal(t, bytesBefore, h.sm.blockPark.Bytes(), "and must not change the byte total")

	// Peeking twice is the same answer, because nothing was consumed.
	again, ok := h.sm.blockPark.FirstChildFor(parent)
	require.True(t, ok)
	require.Equal(t, peeked.hash, again.hash)

	// The claim is what removes it, and it is the existing call.
	taken, ok := h.sm.blockPark.Take(stored.hash)
	require.True(t, ok)
	require.Equal(t, stored.hash, taken.hash)
	require.Zero(t, h.sm.blockPark.Len())

	_, ok = h.sm.blockPark.FirstChildFor(parent)
	require.False(t, ok, "and once claimed there is nothing left to peek")
}

// TestParkPeek_AWritingChildIsSkippedAndItsRefusedDrainRemembered pins the rule
// both takers now share.
//
// A block whose bytes are not on disk yet cannot be committed, its edge to its
// parent must stay, and the refused drain has to be recorded, because the drain
// is driven by a commit that has already happened and will not come round again
// on its own. Whoever finishes the write asks for it instead.
func TestParkPeek_AWritingChildIsSkippedAndItsRefusedDrainRemembered(t *testing.T) {
	h := newParkWiringHarness(t, true)

	msgBlock := h.blocks[1].MsgBlock()
	parent := msgBlock.Header.PrevBlock

	entry := parkedBlock{hash: msgBlock.BlockHash(), prevBlock: parent, height: 2, peer: h.peer}

	// Admit registers the entry with its write still owed, which is the state a
	// block is in while a park worker holds it.
	stored, admitted := h.sm.blockPark.Admit(entry, msgBlock)
	require.Equal(t, admitRegistered, admitted)

	_, ok := h.sm.blockPark.FirstChildFor(parent)
	require.False(t, ok, "a block whose bytes are not on disk yet is not committable")

	require.True(t, h.sm.blockPark.Has(stored.hash), "and it keeps its entry")

	h.sm.blockPark.mu.Lock()
	require.Len(t, h.sm.blockPark.children[parent], 1, "and its edge to its parent")
	require.True(t, h.sm.blockPark.entries[stored.hash].parentDrained,
		"and the refused drain is remembered, or the block waits for the sweep instead")
	h.sm.blockPark.mu.Unlock()

	// Once the write lands it is committable, and FinishWrite reports the drain
	// that was refused so the worker can ask for it.
	require.Equal(t, parkAccepted, h.sm.blockPark.WriteAdmitted(context.Background(), stored, msgBlock))
	require.True(t, h.sm.blockPark.FinishWrite(stored.hash), "the refused drain is handed back")

	peeked, ok := h.sm.blockPark.FirstChildFor(parent)
	require.True(t, ok, "and now the block can be committed")
	require.Equal(t, stored.hash, peeked.hash)
}

// TestDrain_TheConsumerKeepsTakingTheQueueWhileADrainedBlockIsValidated is the
// fix, and the measurement it came from.
//
// On mainnet at height 750,700 the validator was idle 60% of the time: 188 of
// 214 commits came off the park drain, and the drain ran on the block-queue
// consumer, so while it ran nothing left the queue, delivered blocks kept their
// prefetch budget, peer read loops blocked acquiring more, and the one block the
// chain was waiting for could not finish downloading. Validation and network
// never overlapped.
//
// So the claim is narrow and mechanical: with a drained block in flight, a
// message put on the block queue is taken off it and head-processed. Under the
// old code that cannot happen, because the consumer is inside HandleBlockDirect.
func TestDrain_TheConsumerKeepsTakingTheQueueWhileADrainedBlockIsValidated(t *testing.T) {
	h := newParkWiringHarness(t, true)
	bd := h.withDispatcher(t)

	// Depth 1, which is what the block-size ladder forces in the giant-block era
	// the measurement came from. The fix must hold at the least generous setting.
	h.sm.settings.BlockValidation.QuickWindowBlocks = 1
	h.sm.settings.BlockValidation.QuickValidateSkipUtxoLock = true
	h.sm.quit = make(chan struct{})

	parent := h.blocks[0].MsgBlock()
	child := h.blocks[1].MsgBlock()
	third := h.blocks[2].MsgBlock()

	// The child is parked behind its parent, through the ordinary arrival path.
	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	require.NoError(t, h.deliver(t, 1))
	require.Equal(t, 1, h.sm.blockPark.Len())

	// The drained block's validation is held open, which is what a multi-gigabyte
	// block does for minutes on a real node.
	validating := make(chan struct{})
	release := make(chan struct{})

	bd.parkedRun = func(context.Context, *blockDispatch) error {
		close(validating)
		<-release

		return nil
	}

	// The live blocks' own work is stubbed, because this test is about the
	// consumer's availability rather than about validation.
	bd.run = func(context.Context, *blockDispatch, *inflightParent) error { return nil }

	// The parent's own parent is in the chain, so the parent's head resolves and
	// it is dispatched rather than parked.
	h.chainHolds(t, parent.Header.PrevBlock)

	queue := make(chan *blockQueueMsg, 4)

	go h.sm.dispatchBlocks(queue)

	t.Cleanup(func() {
		close(h.sm.quit)

		select {
		case <-release:
		default:
			close(release)
		}
	})

	// The parent arrives and commits, and its tail is what discovers the parked
	// child. Driven through the queue rather than by calling scheduleDrain here,
	// because the drain queue is owned by the consumer goroutine alone.
	h.sm.blockDownloads.Add(h.peer, parent.BlockHash())
	h.sm.blockBacklog.Add(1)

	parentReply := make(chan error, 1)

	queue <- &blockQueueMsg{
		block:       parent,
		blockHash:   parent.BlockHash(),
		blockHeight: 1,
		peer:        h.peer,
		reply:       parentReply,
	}

	select {
	case err := <-parentReply:
		require.NoError(t, err, "the parent commits")
	case <-time.After(10 * time.Second):
		t.Fatal("the parent was never processed")
	}

	select {
	case <-validating:
	case <-time.After(10 * time.Second):
		t.Fatal("the parked block was never dispatched to a worker")
	}

	require.Len(t, bd.frontier, 1, "the drained block is in flight")

	// Here is the whole test. With the drain on the consumer this message would
	// sit in the queue until the validation above finished.
	h.sm.blockDownloads.Add(h.peer, third.BlockHash())
	h.sm.blockBacklog.Add(1)

	queue <- &blockQueueMsg{
		block:       third,
		blockHash:   third.BlockHash(),
		blockHeight: 3,
		peer:        h.peer,
		reply:       make(chan error, 1),
	}

	require.True(t, WaitUntil(func() bool { return len(queue) == 0 }, 10*time.Second),
		"the consumer must keep taking the block queue while a drained block validates; that is the whole point of the change")

	// And it was really head-processed, not merely dequeued. The head discharges
	// the delivering peer's obligation for the block, so the download ledger
	// forgetting this peer owes it is the observable that the head ran, asked
	// under the ledger's own lock rather than off a mock being used concurrently.
	require.True(t, WaitUntil(func() bool { return !h.sm.blockDownloads.HasOwner(h.peer, third.BlockHash()) }, 10*time.Second),
		"the message taken off the queue was head-processed, not just dequeued")

	// It is still in flight, which is the case worth having: this block's parent
	// is the drained block on the worker, so it chained onto it rather than
	// parking behind a parent nothing had committed yet.
	require.Len(t, bd.frontier, 1, "the drained block is still the one in flight at depth 1")

	close(release)
	_ = child
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
		hash:       chainhash.HashH([]byte("a block already in flight")),
		height:     750_699,
		rpcStarted: make(chan struct{}),
		settled:    make(chan struct{}),
	})

	h.sm.drainAsync.Store(true)
	h.sm.scheduleDrain(h.blocks[1].MsgBlock().Header.PrevBlock, 1)

	require.False(t, h.sm.drainStep(bd), "the drain step admits nothing while the frontier is busy")

	require.Equal(t, 1, h.sm.blockPark.Len(), "and it leaves the entry in the index")
	require.Equal(t, parkedBytes, h.sm.blockPark.Bytes(), "with its bytes still accounted for")
	require.Len(t, h.sm.drainQueue, 1, "and the request still queued, to be offered again")
	require.Len(t, bd.frontier, 1, "nothing was dispatched")
}

// TestDrain_TheFrontIsAdvancedAtDispatchNotAtCommit is the header-list argument,
// and it is the trap that killed the naive versions of this change.
//
// advanceHeaderListFor only pops the front when the arriving hash equals it. With
// the drain on the consumer the whole drain finished before the next message was
// head-processed, so the front was always caught up. Free the consumer and it is
// not: a live successor examined while the front still sits on an in-flight
// drained block matches nothing, never removes its own node, never learns it is
// the checkpoint block, and once the drained block's tail finally pops the front
// the walk wedges on a block that is already in the chain. That is a
// headers-first stall of minutes.
//
// So the front is popped where the live path pops it, at the start of ownership.
func TestDrain_TheFrontIsAdvancedAtDispatchNotAtCommit(t *testing.T) {
	h := newParkWiringHarness(t, true)
	bd := h.withDispatcher(t)

	child := h.blocks[1].MsgBlock().BlockHash()

	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	require.NoError(t, h.deliver(t, 1))
	require.Equal(t, 1, h.sm.blockPark.Len())

	// The delivery above already took the child's header out, wherever it sat:
	// advanceHeaderListFor matches by hash, not by position. Before that fix it
	// came out only if the child happened to be the front, and a child that was
	// not left its header behind for good.
	h.sm.headerMu.Lock()
	_, stillThere := h.sm.headerIndex[child]
	h.sm.headerMu.Unlock()
	require.False(t, stillThere, "precondition: the parked block's header is already out of the list")

	// The parent's own arrival moves the front on.
	_, _ = h.sm.advanceHeaderListFor(h.blocks[0].MsgBlock().BlockHash())

	// The validation is held open, so any assertion below is made while the
	// drained block is still in flight.
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})

	bd.parkedRun = func(context.Context, *blockDispatch) error {
		<-release

		return nil
	}

	h.sm.drainAsync.Store(true)
	h.sm.scheduleDrain(h.blocks[0].MsgBlock().BlockHash(), 1)

	require.True(t, h.sm.drainStep(bd), "the parked block is dispatched")

	h.sm.headerMu.Lock()
	front := h.sm.headerList.Front().Value.(*headerNode)
	_, stillIndexed := h.sm.headerIndex[child]
	h.sm.headerMu.Unlock()

	require.False(t, stillIndexed,
		"the drained block's node must be out of the list before it commits, or the next arriving block matches nothing")
	require.Equal(t, h.blocks[2].MsgBlock().BlockHash().String(), front.hash.String(),
		"and the front must already be on the block after it, while the drained block is still validating")

	require.NotNil(t, bd.frontier[0].d.parked.removedFront,
		"the node it removed travels with the entry, or a block given up on cannot be rewound into the walk")
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

	// Both later blocks arrive before their parents and park. One "not stored"
	// answer each, which is how every park test in this package scripts it.
	h.client.On("GetBlockExists", mock.Anything, &thirdHash).Return(false, nil).Once()
	h.client.On("GetBlockExists", mock.Anything, &secondHash).Return(false, nil).Once()

	require.NoError(t, h.deliver(t, 2))
	require.NoError(t, h.deliver(t, 1))
	require.Equal(t, 2, h.sm.blockPark.Len(), "a chain of two blocks is parked")

	// From here the chain holds everything, which is how a commit is faked, and
	// the first block's own parent resolves so its head dispatches it.
	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(true, nil)
	h.chainHolds(t, first.Header.PrevBlock)

	queue := make(chan *blockQueueMsg, 4)

	go h.sm.dispatchBlocks(queue)

	t.Cleanup(func() { close(h.sm.quit) })

	h.sm.blockDownloads.Add(h.peer, first.BlockHash())
	h.sm.blockBacklog.Add(1)

	reply := make(chan error, 1)

	queue <- &blockQueueMsg{
		block:       first,
		blockHash:   first.BlockHash(),
		blockHeight: 1,
		peer:        h.peer,
		reply:       reply,
	}

	select {
	case err := <-reply:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("the first block was never processed")
	}

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

	h.client.On("GetBlockExists", mock.Anything, &child).Return(false, nil).Once()

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

	queue := make(chan *blockQueueMsg, 1)

	go h.sm.dispatchBlocks(queue)

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

// TestDrain_NextAdmissionGivesBothSourcesATurn pins the priority decision, whose
// obvious implementations are both wrong.
//
// Both sources are ready at once routinely: the drain is open whenever the
// frontier is empty and a parent has parked children, and a live block is
// admissible in exactly that state. A select would choose uniformly and starve
// whichever source keeps losing. Always preferring the drain makes a live block
// wait behind a parked chain that can be thousands long, and that live block is
// the one the whole chain is waiting for. Always preferring the live path starves
// the drain for as long as blocks keep arriving, which is the regime this exists
// for.
func TestDrain_NextAdmissionGivesBothSourcesATurn(t *testing.T) {
	for _, tc := range []struct {
		name           string
		lastWasDrained bool
		canLive        bool
		drainOpen      bool
		want           admissionChoice
	}{
		{name: "nothing ready", want: admitNothing},
		{name: "only a live block", canLive: true, want: admitLive},
		{name: "only drain work", drainOpen: true, want: admitDrained},
		{
			name:      "both ready and the live path went last, so the drain takes it",
			canLive:   true,
			drainOpen: true,
			want:      admitDrained,
		},
		{
			name:           "both ready and the drain went last, so the live path takes it",
			lastWasDrained: true,
			canLive:        true,
			drainOpen:      true,
			want:           admitLive,
		},
		{
			name:           "the drain went last but only the drain is ready, so it goes again",
			lastWasDrained: true,
			drainOpen:      true,
			want:           admitDrained,
		},
		{
			name:           "the drain went last and only a live block is ready",
			lastWasDrained: true,
			canLive:        true,
			want:           admitLive,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, nextAdmission(tc.lastWasDrained, tc.canLive, tc.drainOpen))
		})
	}

	// Alternation, stated as the property rather than as a row: with both sources
	// ready for ever, each takes every other turn, so neither can starve.
	last := false
	drained, live := 0, 0

	for i := 0; i < 10; i++ {
		switch nextAdmission(last, true, true) {
		case admitDrained:
			drained++
			last = true
		case admitLive:
			live++
			last = false
		case admitNothing:
			t.Fatal("something is always admissible when both sources are ready")
		}
	}

	require.Equal(t, 5, drained, "the drain gets half the turns")
	require.Equal(t, 5, live, "and the live path gets the other half")
}

// TestParkJob_AFullPoolDoesNotBlockTheConsumer is the last consumer-blocking
// site, and it matters more after the drain moved off that goroutine than it did
// before.
//
// The head hands an admitted block to a park worker. With both workers mid-write
// that hand-off used to wait, and a park write of a mainnet giant block is
// minutes: the stateless merkle check alone has been measured above three
// minutes. Waiting there stops completions, sweep posts and drain steps being
// serviced for all of it, and with the drain gone the consumer reaches that
// hand-off far more often, because most arrivals in this regime park.
//
// So the job is handed to the consumer's own select and offered to a worker
// without blocking. The backpressure is unchanged: while a job is held the queue
// arm is disabled, the same rule the pending dispatch follows.
func TestParkJob_AFullPoolDoesNotBlockTheConsumer(t *testing.T) {
	h := newParkWiringHarness(t, true)
	h.withDispatcher(t)

	h.sm.settings.BlockValidation.QuickWindowBlocks = 1
	h.sm.settings.BlockValidation.QuickValidateSkipUtxoLock = true
	h.sm.quit = make(chan struct{})

	// Nothing is stored, so every arrival is an orphan and parks.
	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)

	// One worker, held inside its blob write, so the pool is full for the rest of
	// the test and the hand-off has nowhere to go.
	gate := &gatedWriteStore{
		Store:   h.sm.blockPark.store,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	h.sm.blockPark.store = gate

	startParkPool(t, h, 1)

	queue := make(chan *blockQueueMsg, 4)

	go h.sm.dispatchBlocks(queue)

	t.Cleanup(func() {
		select {
		case <-gate.release:
		default:
			close(gate.release)
		}
	})

	// The first block fills the only worker.
	first := h.blocks[1].MsgBlock()
	h.sm.blockDownloads.Add(h.peer, first.BlockHash())
	h.sm.blockBacklog.Add(1)

	queue <- &blockQueueMsg{
		block:       first,
		blockHash:   first.BlockHash(),
		blockHeight: 2,
		peer:        h.peer,
		reply:       make(chan error, 1),
	}

	select {
	case <-gate.started:
	case <-time.After(10 * time.Second):
		t.Fatal("the first block never reached the blob write")
	}

	// A second block now needs the park too, and there is no worker for it. Its
	// job is held by the consumer.
	second := h.blocks[2].MsgBlock()
	h.sm.blockDownloads.Add(h.peer, second.BlockHash())
	h.sm.blockBacklog.Add(1)

	queue <- &blockQueueMsg{
		block:       second,
		blockHash:   second.BlockHash(),
		blockHeight: 3,
		peer:        h.peer,
		reply:       make(chan error, 1),
	}

	require.True(t, WaitUntil(func() bool { return !h.sm.blockDownloads.HasOwner(h.peer, second.BlockHash()) }, 10*time.Second),
		"the second block must be head-processed even though no park worker is free")

	// And the consumer is still answering, which is the claim. A sweep post is
	// the cheapest thing to ask it for, and only that goroutine handles it.
	require.True(t, WaitUntil(func() bool {
		select {
		case h.sm.parkCommits <- parkCommit{}:
			return true
		default:
			return false
		}
	}, 5*time.Second), "the sweep can still post")

	require.True(t, WaitUntil(func() bool { return len(h.sm.parkCommits) == 0 }, 10*time.Second),
		"and the consumer is still servicing its select while a park job waits for a worker")

	// The backpressure the slot preserves: while a job is held, nothing else is
	// head-processed, which is the same rule the pending dispatch follows. A third
	// block put on the queue must stay there.
	third := h.blocks[0].MsgBlock()
	h.sm.blockDownloads.Add(h.peer, third.BlockHash())
	h.sm.blockBacklog.Add(1)

	queue <- &blockQueueMsg{
		block:       third,
		blockHash:   third.BlockHash(),
		blockHeight: 1,
		peer:        h.peer,
		reply:       make(chan error, 1),
	}

	time.Sleep(200 * time.Millisecond)

	require.Equal(t, 1, len(queue),
		"a held park job must keep the queue arm shut, or the park's backpressure is gone")
	require.True(t, h.sm.blockDownloads.HasOwner(h.peer, third.BlockHash()),
		"and the third block must not have been head-processed")

	// Now let the write finish. The held job has to be offered again, or the block
	// it belongs to is never written at all.
	close(gate.release)

	require.True(t, WaitUntil(func() bool { return h.sm.blockPark.Len() == 3 }, 15*time.Second),
		"the held job must be offered to a worker once one is free, and the third block head-processed after it")

	names := parkDirEntries(t, h.parkDir)
	require.Contains(t, names, first.BlockHash().String()+".msgBlock")
	require.Contains(t, names, second.BlockHash().String()+".msgBlock",
		"the held job's block reaches the disk, or holding it lost the block")
}

// TestFrontierRace_ABlockTheParkAlreadyHoldsIsNotRaced is one line of production
// code and it can only save work.
//
// The race asks a second peer for the block the whole chain is queued behind. A
// parked block is already downloaded, checked and on disk, waiting for its
// parent, so racing it spends a multi-gigabyte transfer on a block this node is
// holding. The frontier really can sit on one: the frontier is published from the
// header list, and a parked block whose own arrival never matched the front is
// still in that list.
func TestFrontierRace_ABlockTheParkAlreadyHoldsIsNotRaced(t *testing.T) {
	h := newParkWiringHarness(t, true)

	child := h.blocks[1].MsgBlock().BlockHash()

	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)

	require.NoError(t, h.deliver(t, 1))
	require.True(t, h.sm.blockPark.Has(child), "precondition: the block is parked")

	// A second peer, so there would be somebody to race to.
	other, _, _ := connectRacePeer(t, 73, 1000)
	registerRacePeer(h.sm, other)

	// The frontier sits on the parked block, outstanding long enough to be worth
	// racing on every other count.
	h.sm.frontierMu.Lock()
	h.sm.frontierHash = child
	h.sm.frontierHeight = 2
	h.sm.frontierSince = time.Now().Add(-time.Hour)
	h.sm.frontierRacers = nil
	h.sm.frontierMu.Unlock()

	hash, _, target, ok := h.sm.frontierRaceTarget(time.Now())

	require.False(t, ok, "a block the park already holds must not be raced")
	require.Equal(t, chainhash.Hash{}, hash)
	require.Nil(t, target)
}
