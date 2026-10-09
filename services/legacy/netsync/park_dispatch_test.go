package netsync

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
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

	return &blockDispatch{parked: &taken}, taken
}

// TestParkDispatch_ADispatchedParkedBlockCommitsAndTakesTheParkedTail is the happy
// path end to end through the dispatcher: the worker reads the blob and validates
// it, and the parked tail runs the post-commit bookkeeping.
func TestParkDispatch_ADispatchedParkedBlockCommitsAndTakesTheParkedTail(t *testing.T) {
	h := newParkWiringHarness(t, true)
	bd := h.withDispatcher(t)

	d, entry := h.parkedDispatchFor(t, 1)

	// The parent lands after the child has parked and been taken for dispatch.
	// Order matters on a real chain: with the parent already stored, the
	// child's delivery would have committed it inline instead of parking it.
	h.chainHolds(t, h.blocks[0].MsgBlock().BlockHash())

	require.True(t, bd.canDispatch(d), "an unwindowed dispatch is admissible into an empty frontier")

	bd.dispatch(d)
	require.Len(t, bd.frontier, 1, "the parked dispatch is in the frontier")
	require.Equal(t, entry.hash, bd.frontier[0].hash, "the entry is keyed by the parked block's own hash, not by a queue message")

	drainCompletionsUntilEmpty(t, bd)

	// The commit is real: the block is in the chain and block validation saw
	// it once. Everything below is the bookkeeping that follows from that.
	h.requireCommitted(t, entry.hash)

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

	// The parent is in the chain, so the dispatched child gets past the parent
	// lookup to the commit, where block validation's verdict is a fault of the
	// block itself.
	h.chainHolds(t, h.blocks[0].MsgBlock().BlockHash())
	h.validation.failOnce(entry.hash, errors.NewBlockInvalidError("this block is not one we can take"))

	bd.dispatch(d)
	drainCompletionsUntilEmpty(t, bd)

	require.Nil(t, d.readErr, "the blob read succeeded; this is a commit failure")
	require.Equal(t, 1, h.validation.callsFor(entry.hash), "the block reached block validation, which is what judged it")
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
// consumer: HandleConvertedBlock takes no parent parameter at all, so it performs
// its own GetBlockHeader on the previous hash and refuses the block if the parent
// is not there. Giving it a resolved parent to skip that lookup would look like
// an optimisation, and would take the height on trust from a park entry whose
// height came off the header list.
//
// So this test leaves the parent out of the real chain, which is the state the
// guard exists for, and requires that the lookup happened (the ParentGone row
// stamped the entry) and the block was kept rather than judged or handed to
// block validation. Under the mutation the lookup never runs.
func TestParkDispatch_ADrainedBlockPassesANilParentSoTheWorkerLooksItUp(t *testing.T) {
	h := newParkWiringHarness(t, true)
	bd := h.withDispatcher(t)

	d, entry := h.parkedDispatchFor(t, 1)

	// The block itself is not stored, so HandleConvertedBlock goes on to the
	// parent, and the parent (blocks[0]) is genuinely absent from the real
	// chain. The lookup is observable by its consequence: the ParentGone row
	// stamps the entry it puts back.

	// A parked dispatch carries no resolved parent at all any more (the field
	// itself is gone), which is what leaves HandleConvertedBlock's own lookup
	// below as the only route to an answer.

	bd.dispatch(d)
	drainCompletionsUntilEmpty(t, bd)

	require.Equal(t, 1, h.sm.blockPark.Len(),
		"a parent that is gone keeps the block, so the sweep can retry it when the parent lands")
	require.False(t, h.parkedEntry(t, entry.hash).parentMissingAt.IsZero(),
		"the worker looked the parent up and found it gone: the ParentGone row stamped the entry")
	require.Zero(t, h.validation.callCount(), "a block whose parent is missing never reaches block validation")

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
// Blocks run one at a time, so a parked dispatch that arrives while another
// block is in flight is the wrong shape: it is refused and its entry restored.
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

	h.sm.quit = make(chan struct{})
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

	h.sm.settings.BlockValidation.QuickValidateSkipUtxoLock = true
	h.sm.quit = make(chan struct{})

	first := h.blocks[0].MsgBlock()
	second := h.blocks[1].MsgBlock()
	third := h.blocks[2].MsgBlock()

	secondHash := second.BlockHash()
	thirdHash := third.BlockHash()

	// Both later blocks arrive before their parents and park: neither has its
	// parent in the chain yet.
	require.NoError(t, h.deliver(t, 2))
	require.NoError(t, h.deliver(t, 1))
	require.Equal(t, 2, h.sm.blockPark.Len(), "a chain of two blocks is parked")

	// The first block's own parent is genesis, which the real chain holds, so
	// its arrival is committable at once. New() builds this channel
	// unconditionally; a struct-literal harness needs it wired by hand so that
	// commit goes through the dispatcher's drain step below rather than being
	// committed inline.
	h.sm.parkCommits = make(chan parkCommit, parkSweepRPCBudget)

	go h.sm.dispatchBlocks()

	t.Cleanup(func() { close(h.sm.quit) })

	require.NoError(t, h.deliver(t, 0))

	require.True(t, WaitUntil(func() bool { return h.sm.blockPark.Len() == 0 }, 10*time.Second),
		"both parked blocks must drain, so a committed drained block has to schedule the one behind it")

	// All three are in the real chain, each committed once, in order.
	h.requireCommitted(t, first.BlockHash())
	h.requireCommitted(t, secondHash)
	h.requireCommitted(t, thirdHash)

	_, meta, err := h.chain.GetBestBlockHeader(h.sm.ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(3), meta.Height, "the chain tip is the last drained block")

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

	h.sm.settings.BlockValidation.QuickValidateSkipUtxoLock = true
	h.sm.quit = make(chan struct{})

	child := h.blocks[1].MsgBlock().BlockHash()

	require.NoError(t, h.deliver(t, 1))
	require.Equal(t, 1, h.sm.blockPark.Len())

	// The parent lands after the child has parked: the sweep's post below is
	// built as though the sweep had just found it in the chain.
	h.chainHolds(t, h.blocks[0].MsgBlock().BlockHash())

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

	h.requireCommitted(t, child)

	for _, name := range parkDirEntries(t, h.parkDir) {
		require.NotContains(t, name, child.String(), "and the commit deletes the blob")
	}

	_, failed := h.sm.recentlyFailedBlocks.Get(child)
	require.False(t, failed, "the block was committed, not given up on")

	require.Positive(t, onAWorker.Load(),
		"the posted block must be committed by a worker through the drain step, not on the consumer; committing on the arm puts a full read and validate back where this change took it from")
}

// TestParkDispatch_ARecordWhoseDataFileIsGoneIsDroppedBeforeValidation is the
// dispatcher's half of the commit-path completeness check. The dispatcher is the
// production commit path, so it must apply the same check and the same row as
// the serial drain, before the worker hands the record to block validation.
func TestParkDispatch_ARecordWhoseDataFileIsGoneIsDroppedBeforeValidation(t *testing.T) {
	h := newParkWiringHarnessInState(t, true, blockchain2.FSMStateRUNNING, withTransactions(1))
	bd := h.withDispatcher(t)

	d, entry := h.parkedDispatchFor(t, 1)

	// The parent is in the chain, so nothing short of the completeness check
	// stands between the dispatch and block validation.
	h.chainHolds(t, h.blocks[0].MsgBlock().BlockHash())

	record, err := h.sm.blockPark.ReadConverted(h.sm.ctx, entry.hash)
	require.NoError(t, err)
	require.NotEmpty(t, record.Subtrees, "sanity: the record must name a subtree, or there is no file to lose")
	require.NoError(t, h.store.Del(h.sm.ctx, record.Subtrees[0][:], fileformat.FileTypeSubtreeData))

	bd.dispatch(d)
	drainCompletionsUntilEmpty(t, bd)

	require.Nil(t, d.readErr, "the record read back; this is not a read failure")
	require.True(t, d.incomplete, "the worker found the record incomplete")
	require.Nil(t, d.statErr, "the stat ran and said absent; it did not fail")
	require.Zero(t, h.validation.callsFor(entry.hash), "a record whose files are gone must never reach block validation")
	require.Zero(t, h.sm.blockPark.Len(), "the record is dropped, not kept")

	for _, name := range parkDirEntries(t, h.parkDir) {
		require.NotContains(t, name, entry.hash.String(), "the dropped record must not leave its blob behind")
	}

	require.False(t, h.rec.wasRejected(entry.hash), "the files going missing on this node is not the peer's fault")

	_, failed := h.sm.recentlyFailedBlocks.Get(entry.hash)
	require.False(t, failed, "a block nobody judged must not be written off")
}
