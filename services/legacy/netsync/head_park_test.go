package netsync

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// These tests carry the design of the head's three-way question: a block whose
// parent is in flight is dispatched, a block whose parent is neither stored nor
// in flight is parked, and a parked block reaches the park with its bytes. The
// third is the one a plain merge of the park onto the dispatcher could not
// satisfy at all, because the dispatcher releases the decoded block the moment
// the worker returns and the park decision used to live after that.
//
// The harness is the park's own (newParkWiringHarness): a real park over a file
// store, three mined regtest blocks, a headers-first list with one node per
// block, and a chain that holds nothing until a test says otherwise.

// headFor runs one block through handleBlockMsgHead the way the consumer does,
// with the block owed by the harness peer, and returns what the head decided.
func (h *parkWiringHarness) headFor(t *testing.T, index int, reply chan error) (*blockDispatch, bool, error, *blockQueueMsg) {
	t.Helper()

	msgBlock := h.blocks[index].MsgBlock()
	hash := msgBlock.BlockHash()

	h.sm.blockDownloads.Add(h.peer, hash)

	bmsg := &blockQueueMsg{
		block:       msgBlock,
		blockHash:   hash,
		blockHeight: int32(index + 1),
		peer:        h.peer,
		reply:       reply,
	}

	d, finished, err := h.sm.handleBlockMsgHead(bmsg)

	return d, finished, err, bmsg
}

// withDispatcher gives the harness manager a dispatcher, which a struct-literal
// manager otherwise lacks, so the head has a frontier to ask.
func (h *parkWiringHarness) withDispatcher(t *testing.T) *blockDispatcher {
	t.Helper()

	h.sm.dispatcher = newBlockDispatcher(h.sm)

	return h.sm.dispatcher
}

// TestHead_AParentInFlightMeansDispatchNotPark is the first claim. The parent
// is in the dispatcher's frontier and nowhere else: not in the chain, so a chain
// lookup would call it missing. The head must ask the dispatcher first and hand
// the block on rather than park it.
func TestHead_AParentInFlightMeansDispatchNotPark(t *testing.T) {
	h := newParkWiringHarness(t, true)
	bd := h.withDispatcher(t)

	parent := h.blocks[0].MsgBlock().BlockHash()
	child := h.blocks[1].MsgBlock().BlockHash()

	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)

	// The parent is in flight: its worker has not returned.
	bd.frontier = append(bd.frontier, &frontierEntry{
		hash:       parent,
		height:     1,
		rpcStarted: make(chan struct{}),
		settled:    make(chan struct{}),
	})

	d, finished, err, _ := h.headFor(t, 1, nil)
	require.NoError(t, err)
	require.False(t, finished, "a block whose parent is in flight is handed to the dispatcher")
	require.NotNil(t, d)
	require.Equal(t, uint32(2), d.height, "the height comes off the in-flight parent")
	require.NotNil(t, d.msgBlock, "the dispatch carries the decoded block to the worker")

	require.False(t, h.sm.blockPark.Has(child), "a block whose parent is in flight is never parked")
	require.Zero(t, h.sm.blockPark.Len())

	h.client.AssertNotCalled(t, "GetBlockHeader", mock.Anything, &parent)
	h.client.AssertNotCalled(t, "GetBlockLocator", mock.Anything, mock.Anything, mock.Anything)
}

// TestHead_AParentNeitherStoredNorInFlightMeansPark is the second claim. The
// frontier is empty and the chain has never heard of the parent, so the block is
// parked in the head, before any dispatch, and the reply travels with the park
// job rather than being answered by the caller.
func TestHead_AParentNeitherStoredNorInFlightMeansPark(t *testing.T) {
	h := newParkWiringHarness(t, true)
	h.withDispatcher(t)

	child := h.blocks[1].MsgBlock().BlockHash()

	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)

	reply := make(chan error, 1)

	d, finished, err, bmsg := h.headFor(t, 1, reply)
	require.NoError(t, err)
	require.True(t, finished, "a parked block is finished in the head; nothing is dispatched")
	require.Nil(t, d)

	require.True(t, h.sm.blockPark.Has(child), "the block is in the park")
	require.Equal(t, 1, h.sm.blockPark.Len())

	require.Nil(t, bmsg.reply, "the reply is transferred to the park job, so the consumer does not answer for it")

	select {
	case got := <-reply:
		require.NoError(t, got, "the park job answers the caller once the block is on disk")
	case <-time.After(5 * time.Second):
		t.Fatal("the park job never answered the caller")
	}

	h.client.AssertCalled(t, "GetBlockLocator", mock.Anything, mock.Anything, mock.Anything)
}

// TestHead_AParkedBlockReachesTheParkWithItsBytes is the third claim, and the
// one the old merge could not meet: the bytes the park writes are the block's
// own, readable back and hashing to it.
func TestHead_AParkedBlockReachesTheParkWithItsBytes(t *testing.T) {
	h := newParkWiringHarness(t, true)
	h.withDispatcher(t)

	original := h.blocks[1].MsgBlock()
	child := original.BlockHash()

	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)

	_, finished, err, _ := h.headFor(t, 1, nil)
	require.NoError(t, err)
	require.True(t, finished)

	require.Contains(t, parkDirEntries(t, h.parkDir), child.String()+".msgBlock", "the blob is on disk")

	read, err := h.sm.blockPark.Read(context.Background(), child)
	require.NoError(t, err, "the parked bytes read back")
	require.Equal(t, child, read.BlockHash(), "and they are this block's bytes")
	require.Len(t, read.Transactions, len(original.Transactions), "all of them")
}

// TestHead_AStoredParentMeansDispatchWithItsHeight pins the third answer: a
// parent in the chain resolves to a dispatch carrying the parent's height and no
// frontier entry, so the worker skips the lookup the head has just made.
func TestHead_AStoredParentMeansDispatchWithItsHeight(t *testing.T) {
	h := newParkWiringHarness(t, true)
	h.withDispatcher(t)

	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	h.chainHolds(t, h.blocks[0].MsgBlock().Header.PrevBlock)

	d, finished, err, _ := h.headFor(t, 0, nil)
	require.NoError(t, err)
	require.False(t, finished)
	require.NotNil(t, d)
	require.NotNil(t, d.parent, "a stored parent is handed to the worker with its height")
	require.Nil(t, d.parent.entry, "and no frontier entry, so the hand-shake has nothing to wait on")
	require.Equal(t, uint32(2), d.height, "chainHolds answers height 1 for the parent")
	require.Zero(t, h.sm.blockPark.Len())
}

// TestTail_ACommittedBlockDrainsWhatWasParkedBehindIt covers the dispatcher's
// default tail: it runs the drain after a block that committed, so a block
// parked earlier is committed off disk the moment its parent's tail runs.
func TestTail_ACommittedBlockDrainsWhatWasParkedBehindIt(t *testing.T) {
	h := newParkWiringHarness(t, true)
	bd := h.withDispatcher(t)

	parent := h.blocks[0].MsgBlock().BlockHash()
	child := h.blocks[1].MsgBlock().BlockHash()

	// The child arrives first and parks: the head's existence check consumes
	// the one "not stored" answer, the way every park test scripts it.
	h.client.On("GetBlockExists", mock.Anything, &child).Return(false, nil).Once()
	h.client.On("GetBlockExists", mock.Anything, &parent).Return(false, nil)

	_, finished, err, _ := h.headFor(t, 1, nil)
	require.NoError(t, err)
	require.True(t, finished)
	require.Equal(t, 1, h.sm.blockPark.Len())

	// The parent goes through the head and is dispatched; its worker "commits"
	// it, which the tail learns from a nil error. From here the chain holds the
	// child, so the drain's own HandleBlockDirect finds it stored and counts it
	// committed, the same route the park's own tests use.
	h.chainHolds(t, h.blocks[0].MsgBlock().Header.PrevBlock)

	d, finished, err, bmsg := h.headFor(t, 0, nil)
	require.NoError(t, err)
	require.False(t, finished)

	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(true, nil)

	require.NoError(t, bd.tail(d, nil))

	require.True(t, bmsg.committed, "a nil error from the worker is a commit")
	require.Zero(t, h.sm.blockPark.Len(), "the tail drained the block parked behind the one that committed")
}

// TestTail_AnAbortedSuccessorGetsItsHeaderBack covers the one place this rework
// departs from the design note. An aborted successor — a block whose in-flight
// parent failed — earns no backoff of its own; the predecessor's own backoff
// throttles the whole run. It still carries the same recentlyFailedBlocks mark
// every tail failure leaves, aborted or not, because that mark is what lets a
// grandchild's own cascade check find it later. So it is not immediately
// re-askable either, not from a throttle of its own, but from the mark it
// shares with any other failed block.
func TestTail_AnAbortedSuccessorGetsItsHeaderBack(t *testing.T) {
	h := newParkWiringHarness(t, true)
	bd := h.withDispatcher(t)

	// dropBlockFromWalk is a no-op without a backoff map, so the parent's own
	// rewind needs one; the assertion on the successor is that it records nothing.
	h.sm.blockFailureBackoff = expiringmap.New[chainhash.Hash, *blockFailureState](time.Minute)
	t.Cleanup(h.sm.blockFailureBackoff.Stop)

	parent := h.blocks[0].MsgBlock().BlockHash()
	child := h.blocks[1].MsgBlock().BlockHash()

	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)
	h.chainHolds(t, h.blocks[0].MsgBlock().Header.PrevBlock)

	// Both blocks go through the head in order.
	release := make(chan struct{})

	bd.run = func(_ context.Context, d *blockDispatch, _ *inflightParent) error {
		<-release

		if d.msg.blockHash == parent {
			return errors.NewStorageError("the store is busy")
		}

		return nil
	}

	dParent, finished, err, _ := h.headFor(t, 0, nil)
	require.NoError(t, err)
	require.False(t, finished)

	bd.dispatch(dParent)

	dChild, finished, err, _ := h.headFor(t, 1, nil)
	require.NoError(t, err)
	require.False(t, finished, "the child's parent is in flight, so the child is dispatched, not parked")

	bd.dispatch(dChild)

	// The parent fails; the child is aborted behind it. Both tails run in order.
	close(release)

	deadline := time.After(5 * time.Second)

	for len(bd.frontier) > 0 {
		select {
		case c := <-bd.completions:
			bd.complete(c)
		case <-deadline:
			t.Fatal("timed out waiting for both tails")
		}
	}

	require.True(t, dChild.aborted, "the child was aborted behind its failed parent")

	_, parentBackoff := h.sm.blockFailureBackoff.Get(parent)
	require.True(t, parentBackoff, "the block that failed is throttled")

	_, childBackoff := h.sm.blockFailureBackoff.Get(child)
	require.False(t, childBackoff, "an aborted successor never ran a failing attempt and earns no backoff")

	// handleBlockMsgTail's recentlyFailedBlocks.Set runs for both, unconditional
	// on d.aborted: the child's own header could reach a grandchild one day,
	// and that grandchild's cascade check needs the child's mark to be there
	// regardless of whether the child ran a failing attempt of its own. So
	// unownedBlocks, which now honours that same map, holds the child back
	// too, not because it is throttled the way the parent is, but because it
	// carries the same cascade mark every tail failure leaves. The bound is
	// the map's own: whichever of the two clears first, a later success or the
	// ten-minute expiry, is re-askable again from that point.
	_, parentFailed := h.sm.recentlyFailedBlocks.Get(parent)
	require.True(t, parentFailed)

	_, childFailed := h.sm.recentlyFailedBlocks.Get(child)
	require.True(t, childFailed, "the child carries the cascade mark so a grandchild could be short-circuited by it, even though it earned no backoff of its own")

	before := h.rec.getDataCount()

	h.sm.fetchHeaderBlocks()

	// Unwaited is fine for the parent: its own backoff entry filters it
	// independently, so nothing is ever queued for it to race against. The
	// child has no second filter of its own, only the cascade mark, so an
	// instant check proves nothing against a real request that was queued
	// and sent but has not yet crossed the pipe when the assertion runs -
	// it must wait, the same way the sibling check in
	// block_park_front_block_test.go does.
	require.False(t, h.rec.askedForSince(before, parent),
		"the failed parent must not be asked for again yet, still inside its backoff and its own recentlyFailedBlocks mark")
	require.False(t, WaitUntil(func() bool { return h.rec.askedForSince(before, child) }, time.Second),
		"the aborted successor must not be asked for again yet either, while its own recentlyFailedBlocks mark stands")
}

// TestSweep_PostsCommitsToTheConsumerInsteadOfCommitting pins the sweep's one
// prohibition. With a channel to the consumer it must post the parked block it
// wants committed and commit nothing itself; a commit from the sweep goroutine
// would race the dispatcher for admission into block validation's window.
func TestSweep_PostsCommitsToTheConsumerInsteadOfCommitting(t *testing.T) {
	h := newParkWiringHarness(t, true)

	child := h.blocks[1].MsgBlock().BlockHash()

	h.client.On("GetBlockExists", mock.Anything, &child).Return(false, nil).Once()

	require.NoError(t, h.deliver(t, 1))
	require.Equal(t, 1, h.sm.blockPark.Len())

	// A pool gives the manager the channel the sweep posts to.
	startParkPool(t, h, 1)
	require.NotNil(t, h.sm.parkCommits)

	h.chainHolds(t, h.blocks[1].MsgBlock().Header.PrevBlock)

	// The commit's own existence check would see this, and the drain would count
	// the block committed. It must not have been asked yet.
	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(true, nil)

	h.sm.sweepParkedBlocks(time.Now().Add(parkStuckThreshold + time.Second))

	require.Len(t, h.sm.parkCommits, 1, "the sweep posts the commit to the consumer")
	require.Zero(t, h.sm.blockPark.Len(), "and has taken the entry out of the index for the consumer to commit")
	require.Contains(t, parkDirEntries(t, h.parkDir), child.String()+".msgBlock", "without touching the blob")

	_, failed := h.sm.recentlyFailedBlocks.Get(child)
	require.False(t, failed, "and without judging the block")

	commit := <-h.sm.parkCommits
	require.Equal(t, child, commit.entry.hash)
	require.Positive(t, commit.parentHeight,
		"the sweep passes the parent height its own lookup fetched, so the drained block's frontier entry is not height-blind")

	// The consumer's side, which is the same one every drained block takes: put
	// the entry back, then let the drain step claim it through one admission test.
	h.sm.commitParkedBlockAndDrain(commit.entry)

	for _, name := range parkDirEntries(t, h.parkDir) {
		require.NotContains(t, name, child.String(), "the consumer's commit is what deletes the blob")
	}
}

// TestSweep_WithoutAConsumerChannelCommitsInline is the other half: a manager
// with no pool has no channel, and the sweep then commits where it stands, which
// is what every sweep test that calls it directly relies on.
func TestSweep_WithoutAConsumerChannelCommitsInline(t *testing.T) {
	h := newParkWiringHarness(t, true)
	require.Nil(t, h.sm.parkCommits)

	child := h.blocks[1].MsgBlock().BlockHash()

	h.client.On("GetBlockExists", mock.Anything, &child).Return(false, nil).Once()

	require.NoError(t, h.deliver(t, 1))
	require.Equal(t, 1, h.sm.blockPark.Len())

	h.chainHolds(t, h.blocks[1].MsgBlock().Header.PrevBlock)
	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(true, nil)

	h.sm.sweepParkedBlocks(time.Now().Add(parkStuckThreshold + time.Second))

	require.Zero(t, h.sm.blockPark.Len())

	for _, name := range parkDirEntries(t, h.parkDir) {
		require.NotContains(t, name, child.String(), "the inline commit deleted the blob")
	}
}

// TestBlockHandler_TheSweepGoroutinePostsAndTheConsumerCommits is the
// production wiring end to end: the block handler starts the sweep on its own
// goroutine, the pool gives it a channel, the sweep posts, and the consumer —
// whichever one the settings select — commits.
func TestBlockHandler_TheSweepGoroutinePostsAndTheConsumerCommits(t *testing.T) {
	h := newParkWiringHarness(t, true)

	child := h.blocks[1].MsgBlock().BlockHash()

	h.client.On("GetBlockExists", mock.Anything, &child).Return(false, nil).Once()

	require.NoError(t, h.deliver(t, 1))
	require.Equal(t, 1, h.sm.blockPark.Len())

	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(true, nil)
	h.chainHolds(t, h.blocks[1].MsgBlock().Header.PrevBlock)

	h.sm.blockPark.mu.Lock()
	for _, entry := range h.sm.blockPark.entries {
		entry.parkedAt = time.Now().Add(-parkStuckThreshold - time.Second)
	}
	h.sm.blockPark.mu.Unlock()

	previous := parkSweepInterval
	parkSweepInterval = 10 * time.Millisecond

	h.sm.quit = make(chan struct{})
	h.sm.handlerDone = make(chan struct{})
	h.sm.msgChan = make(chan interface{}, 1)
	h.sm.startParkWorkers(1)

	go h.sm.blockHandler()

	t.Cleanup(func() {
		close(h.sm.quit)
		<-h.sm.handlerDone
		h.sm.parkWorkers.Wait()

		parkSweepInterval = previous
	})

	require.True(t, WaitUntil(func() bool { return h.sm.blockPark.Len() == 0 }, 5*time.Second),
		"the sweep goroutine must post the commit and the consumer must carry it out")

	_, failed := h.sm.recentlyFailedBlocks.Get(child)
	require.False(t, failed, "the block was committed, not given up on")
}
