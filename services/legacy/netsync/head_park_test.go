package netsync

import (
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// withDispatcher gives the harness manager a dispatcher, which a struct-literal
// manager otherwise lacks, so a dispatch has a frontier to join.
func (h *parkWiringHarness) withDispatcher(t *testing.T) *blockDispatcher {
	t.Helper()

	h.sm.dispatcher = newBlockDispatcher(h.sm)

	return h.sm.dispatcher
}

// startParkPool gives the harness manager the channel a running consumer would
// have — New() builds sm.parkCommits directly, unconditionally, but a
// struct-literal harness manager needs it wired by hand — so the sweep has
// somewhere to post its commit instead of running it inline. There is no more
// worker pool to start: parking itself now runs inline on the goroutine that
// streamed the block to disk (see pipelineBlockSink / handleBlockOnDiskMsg).
func startParkPool(t *testing.T, h *parkWiringHarness, size int) {
	t.Helper()

	h.sm.quit = make(chan struct{})
	h.sm.parkCommits = make(chan parkCommit, size)

	t.Cleanup(func() { close(h.sm.quit) })
}

// TestSweep_PostsCommitsToTheConsumerInsteadOfCommitting pins the sweep's one
// prohibition. With a channel to the consumer it must post the parked block it
// wants committed and commit nothing itself; a commit from the sweep goroutine
// would race the dispatcher for admission into block validation's window.
func TestSweep_PostsCommitsToTheConsumerInsteadOfCommitting(t *testing.T) {
	h := newParkWiringHarness(t, true)

	child := h.blocks[1].MsgBlock().BlockHash()

	require.NoError(t, h.deliver(t, 1))
	require.Equal(t, 1, h.sm.blockPark.Len())

	// A channel gives the manager somewhere the sweep can post to.
	startParkPool(t, h, 1)
	require.NotNil(t, h.sm.parkCommits)

	h.chainHolds(t, h.blocks[1].MsgBlock().Header.PrevBlock)

	// The commit's own existence check would see this, and the drain would count
	// the block committed. It must not have been asked yet.
	h.client.On("GetBlockExists", mock.Anything, mock.Anything).Return(true, nil)

	h.sm.sweepParkedBlocks(time.Now().Add(parkStuckThreshold + time.Second))

	require.Len(t, h.sm.parkCommits, 1, "the sweep posts the commit to the consumer")
	require.Zero(t, h.sm.blockPark.Len(), "and has taken the entry out of the index for the consumer to commit")
	require.Contains(t, parkDirEntries(t, h.parkDir), child.String()+".block", "without touching the blob")

	_, failed := h.sm.recentlyFailedBlocks.Get(child)
	require.False(t, failed, "and without judging the block")

	commit := <-h.sm.parkCommits
	require.Equal(t, child, commit.entry.hash)
	require.Positive(t, commit.parentHeight,
		"the sweep passes the parent height its own lookup fetched, so the drained block's frontier entry is not height-blind")

	// The consumer's side, which is the same one every drained block takes: put
	// the entry back, then let the drain claim it — the same composition
	// submitParkCommit's own no-consumer arm uses (block_park_drain.go).
	h.sm.blockPark.Restore(commit.entry)
	h.sm.drainParkedDescendants(commit.entry.prevBlock)

	for _, name := range parkDirEntries(t, h.parkDir) {
		require.NotContains(t, name, child.String(), "the consumer's commit is what deletes the blob")
	}
}

// TestSweep_WithoutAConsumerChannelCommitsInline is the other half: a manager
// with no channel has no way to post, and the sweep then commits where it
// stands, which is what every sweep test that calls it directly relies on.
func TestSweep_WithoutAConsumerChannelCommitsInline(t *testing.T) {
	h := newParkWiringHarness(t, true)
	require.Nil(t, h.sm.parkCommits)

	child := h.blocks[1].MsgBlock().BlockHash()

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
// goroutine and builds a dispatcher and a parkCommits channel of its own kind
// (New() would; this harness's struct-literal manager needs the channel wired
// by hand), the sweep posts, and the consumer — dispatchBlocks, running the
// windowed route by default — commits.
func TestBlockHandler_TheSweepGoroutinePostsAndTheConsumerCommits(t *testing.T) {
	h := newParkWiringHarness(t, true)

	child := h.blocks[1].MsgBlock().BlockHash()

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
	h.sm.parkCommits = make(chan parkCommit, parkSweepRPCBudget)

	go h.sm.blockHandler()

	t.Cleanup(func() {
		close(h.sm.quit)
		<-h.sm.handlerDone

		if h.sm.consumerDone != nil {
			<-h.sm.consumerDone
		}

		parkSweepInterval = previous
	})

	require.True(t, WaitUntil(func() bool { return h.sm.blockPark.Len() == 0 }, 5*time.Second),
		"the sweep goroutine must post the commit and the consumer must carry it out")

	_, failed := h.sm.recentlyFailedBlocks.Get(child)
	require.False(t, failed, "the block was committed, not given up on")
}
