package netsync

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
)

// The download budget is what stops the node holding unbounded decoded blocks.
// It is acquired AFTER a block has been read off the wire, in OnBlock, and until
// this change it was released only when the block's reply came back, which is
// after validation. So one block held budget for its whole validation, and every
// peer that had finished reading a block sat blocked in the acquire, unable to
// read its next message.
//
// Measured on mainnet at height 752,100 with the budget at 256 MiB: one or two
// blocks in flight and five or six peer read loops blocked, with the link
// delivering 27 MB/s. Freeing the block-queue consumer cannot help with that,
// because the queue it can now service is empty by construction: nothing else
// was allowed to download.
//
// The fix is to release the BYTES once the block's memory has been charged to
// whichever budget owns it next, and to keep the DEDUP HASH until the reply.
// After a dispatch that is the window's own byte charge; after a park admission
// it is the park's byte budget. Both are real bounds, so nothing becomes
// unbounded; what changes is that the download budget stops double-counting
// memory another budget is already accounting for.

// prefetchHarness is a manager with a real download budget and nothing else it
// does not need.
func prefetchHarness(t *testing.T, budget int64) *SyncManager {
	t.Helper()

	sm := &SyncManager{
		logger:                   ulogger.TestLogger{},
		blockPrefetchBudgetBytes: budget,
		blockPrefetchBudget:      semaphore.NewWeighted(budget),
		inFlightBlocks:           make(map[chainhash.Hash]*inFlightBlock),
	}

	return sm
}

// TestPrefetch_TheBytesAreGivenBackAtHandOffAndTheHashAtTheReply is the split
// this change rests on.
//
// The two halves of the admission gate had one lifetime, deliberately, so that a
// copy of a hash could not be re-admitted until the block had left the pipeline.
// The dedup half still works that way. The byte half does not need to: once the
// block is charged to the window or to the park, holding the download budget as
// well counts the same memory twice and it is the second count that shuts the
// peers out.
func TestPrefetch_TheBytesAreGivenBackAtHandOffAndTheHashAtTheReply(t *testing.T) {
	// Four times the per-block floor, so the numbers below are the shape a real
	// node has: one block can take the whole budget, and a second block still
	// needs at least the floor.
	const budget = 4 * minInFlightBlockWeight

	sm := prefetchHarness(t, budget)

	first := chainhash.HashH([]byte("the block being validated"))
	second := chainhash.HashH([]byte("a block another peer has ready"))

	// A block that takes the whole budget, which is what a block at or over the
	// budget does: AcquireBlockPrefetch clamps it and admits it alone.
	weight, err := sm.AcquireBlockPrefetch(context.Background(), nil, first, budget)
	require.NoError(t, err)
	require.Equal(t, int64(budget), weight)

	// Another peer has finished reading its own block and wants in. There is no
	// room, which is the state five or six read loops were measured in.
	require.False(t, sm.blockPrefetchBudget.TryAcquire(minInFlightBlockWeight),
		"precondition: the budget is fully committed to the first block")

	// The first block is handed over to whichever budget owns it next.
	sm.ReleaseBlockPrefetchBytes(first, weight)

	require.True(t, sm.blockPrefetchBudget.TryAcquire(minInFlightBlockWeight),
		"once the block is charged elsewhere its download bytes must come back, or peers stay shut out for the whole validation")
	sm.blockPrefetchBudget.Release(minInFlightBlockWeight)

	// The dedup half is untouched: a second copy of the same hash is still
	// refused while the block is in the pipeline.
	_, err = sm.AcquireBlockPrefetch(context.Background(), nil, first, minInFlightBlockWeight)
	require.ErrorIs(t, err, ErrDuplicateBlockInFlight,
		"the hash must be held until the block leaves the pipeline, or a duplicate is admitted and validated twice")

	// A different hash is admitted, which is the whole point.
	_, err = sm.AcquireBlockPrefetch(context.Background(), nil, second, minInFlightBlockWeight)
	require.NoError(t, err)

	// Exactly once. Releasing the same weight twice panics the semaphore, and the
	// panic would land on a peer's read loop, so the second call must be a no-op.
	// The peer makes both calls on the hand-off path: one at the hand-off and one
	// from its deferred cleanup.
	require.NotPanics(t, func() { sm.ReleaseBlockPrefetchBytes(first, weight) },
		"a second release of the same block's bytes must be a no-op, not a panic")

	require.False(t, sm.blockPrefetchBudget.TryAcquire(budget),
		"and it must not have handed the budget back twice either")

	sm.ReleaseBlockPrefetchHash(first)

	_, err = sm.AcquireBlockPrefetch(context.Background(), nil, first, minInFlightBlockWeight)
	require.NoError(t, err, "once the block has left the pipeline its hash is free again")
}

// TestPrefetch_ADispatchedBlockGivesItsDownloadBytesBack is the wiring, driven
// through the consumer loop rather than by calling the release directly.
//
// A dispatched block's memory is charged to the window's byte budget in
// bd.dispatch, so that is the moment the download bytes are owed back.
func TestPrefetch_ADispatchedBlockGivesItsDownloadBytesBack(t *testing.T) {
	h := newParkWiringHarness(t, true)
	bd := h.withDispatcher(t)

	h.sm.settings.BlockValidation.QuickWindowBlocks = 1
	h.sm.settings.BlockValidation.QuickValidateSkipUtxoLock = true
	h.sm.quit = make(chan struct{})

	block := h.blocks[0].MsgBlock()

	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(true, nil)
	h.chainHolds(t, block.Header.PrevBlock)

	// The validation is held open, so every assertion below is made while the
	// block is still in flight and its memory still live.
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})

	bd.run = func(context.Context, *blockDispatch, *inflightParent) error {
		<-release

		return nil
	}

	handedOff := make(chan struct{})

	queue := make(chan *blockQueueMsg, 1)

	go h.sm.dispatchBlocks(queue)

	t.Cleanup(func() { close(h.sm.quit) })

	h.sm.blockDownloads.Add(h.peer, block.BlockHash())
	h.sm.blockBacklog.Add(1)

	reply := make(chan error, 1)

	queue <- &blockQueueMsg{
		block:       block,
		blockHash:   block.BlockHash(),
		blockHeight: 1,
		peer:        h.peer,
		reply:       reply,
		handedOff:   handedOff,
	}

	select {
	case <-handedOff:
	case <-time.After(10 * time.Second):
		t.Fatal("a dispatched block must signal its hand-off, or its download bytes are held for the whole validation")
	}

	require.Len(t, bd.frontier, 1, "and it must still be in flight when it does, or the signal is worth nothing")

	select {
	case <-reply:
		t.Fatal("the hand-off is not the reply; the dedup hash is held until the reply")
	default:
	}

	close(release)

	select {
	case err := <-reply:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("the block never finished")
	}
}

// TestPrefetch_AParkedBlockGivesItsDownloadBytesBack is the same for the other
// route, and it is the common one: in the measured regime most arrivals park.
//
// A parked block's memory is charged to the park's own byte budget by Admit, so
// that is when the download bytes are owed back. The park worker still holds the
// decoded block while it writes, and the park's budget is what accounts for it.
func TestPrefetch_AParkedBlockGivesItsDownloadBytesBack(t *testing.T) {
	h := newParkWiringHarness(t, true)
	h.withDispatcher(t)

	h.sm.settings.BlockValidation.QuickWindowBlocks = 1
	h.sm.settings.BlockValidation.QuickValidateSkipUtxoLock = true
	h.sm.quit = make(chan struct{})

	block := h.blocks[1].MsgBlock()

	// Nothing is stored, so this block is an orphan and parks.
	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(false, nil)

	handedOff := make(chan struct{})

	queue := make(chan *blockQueueMsg, 1)

	go h.sm.dispatchBlocks(queue)

	t.Cleanup(func() { close(h.sm.quit) })

	h.sm.blockDownloads.Add(h.peer, block.BlockHash())
	h.sm.blockBacklog.Add(1)

	queue <- &blockQueueMsg{
		block:       block,
		blockHash:   block.BlockHash(),
		blockHeight: 2,
		peer:        h.peer,
		reply:       make(chan error, 1),
		handedOff:   handedOff,
	}

	select {
	case <-handedOff:
	case <-time.After(10 * time.Second):
		t.Fatal("a parked block must signal its hand-off once the park has charged it")
	}

	require.True(t, WaitUntil(func() bool { return h.sm.blockPark.Len() == 1 }, 10*time.Second),
		"and the park is what accounts for it from then on")
	require.Positive(t, h.sm.blockPark.Bytes(), "with its bytes charged there")
}
