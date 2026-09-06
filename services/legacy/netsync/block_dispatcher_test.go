package netsync

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// testDispatcher builds a dispatcher with an injected tail so no peer, store or
// gRPC client is needed. Each test also replaces bd.run, so nothing here ever
// reaches HandleBlockDirect.
func testDispatcher(t *testing.T, depth int) (*blockDispatcher, *tailRecorder) {
	t.Helper()

	s := test.CreateBaseTestSettings(t)
	s.BlockValidation.QuickWindowBlocks = depth
	s.BlockValidation.QuickValidateSkipUtxoLock = true
	s.BlockValidation.MaxBlocksBehindBlockAssembly = 20

	sm := &SyncManager{logger: ulogger.TestLogger{}, settings: s, ctx: context.Background()}
	bd := newBlockDispatcher(sm)
	rec := &tailRecorder{}
	bd.tail = rec.tail

	return bd, rec
}

// tailRecorder stands in for handleBlockMsgTail and records the order the tails
// ran in, with the error each one was handed.
type tailRecorder struct {
	mu      sync.Mutex
	heights []uint32
	errs    []error
}

func (r *tailRecorder) tail(d *blockDispatch, err error) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.heights = append(r.heights, d.height)
	r.errs = append(r.errs, err)

	return err
}

func (r *tailRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.heights)
}

func dispatchAt(height uint32, bytes int64) *blockDispatch {
	h := chainhash.HashH([]byte{byte(height)})

	return &blockDispatch{
		msg:      &blockQueueMsg{blockHash: h, reply: make(chan error, 1)},
		height:   height,
		windowed: true,
		bytes:    bytes,
	}
}

// drainCompletions pumps the completions channel the way the consumer goroutine
// does, until want tails have run. want == 0 means "pump briefly and expect
// nothing", which is how a test proves a tail is still blocked behind an
// unsettled frontier head.
func (bd *blockDispatcher) drainCompletions(t *testing.T, rec *tailRecorder, want int) {
	t.Helper()

	if want == 0 {
		quiet := time.After(50 * time.Millisecond)

		for {
			select {
			case c := <-bd.completions:
				bd.complete(c)
			case <-quiet:
				return
			}
		}
	}

	deadline := time.After(2 * time.Second)

	for rec.count() < want {
		select {
		case c := <-bd.completions:
			bd.complete(c)
		case <-deadline:
			t.Fatalf("timed out waiting for %d tails, got %d", want, rec.count())
		}
	}
}

func TestDispatcher_TailsRunInDispatchOrderWhenWorkersFinishOutOfOrder(t *testing.T) {
	bd, rec := testDispatcher(t, 3)

	release := map[uint32]chan struct{}{1: make(chan struct{}), 2: make(chan struct{}), 3: make(chan struct{})}
	bd.run = func(_ context.Context, d *blockDispatch, _ *inflightParent) error { <-release[d.height]; return nil }

	for h := uint32(1); h <= 3; h++ {
		d := dispatchAt(h, 1000)
		require.True(t, bd.canDispatch(d))
		bd.dispatch(d)
	}

	close(release[3])
	close(release[2])
	bd.drainCompletions(t, rec, 0)
	require.Equal(t, 0, rec.count(), "no tail before the head completed")

	close(release[1])
	bd.drainCompletions(t, rec, 3)

	rec.mu.Lock()
	defer rec.mu.Unlock()
	require.Equal(t, []uint32{1, 2, 3}, rec.heights)
}

func TestDispatcher_HeadFailureAbortsSuccessorsWithServiceErrorsAndNoBackoff(t *testing.T) {
	bd, rec := testDispatcher(t, 3)

	release := map[uint32]chan struct{}{1: make(chan struct{}), 2: make(chan struct{}), 3: make(chan struct{})}
	bd.run = func(_ context.Context, d *blockDispatch, _ *inflightParent) error {
		<-release[d.height]

		if d.height == 1 {
			return errors.NewProcessingError("block 1 broke")
		}

		return nil
	}

	dispatches := make([]*blockDispatch, 0, 3)

	for h := uint32(1); h <= 3; h++ {
		d := dispatchAt(h, 1000)
		dispatches = append(dispatches, d)
		bd.dispatch(d)
	}

	close(release[1])
	close(release[2])
	close(release[3])
	bd.drainCompletions(t, rec, 3)

	rec.mu.Lock()
	defer rec.mu.Unlock()
	require.Equal(t, []uint32{1, 2, 3}, rec.heights)
	require.False(t, errors.IsTransientLocalError(rec.errs[0]), "the head keeps its own error class")
	require.True(t, errors.IsTransientLocalError(rec.errs[1]))
	require.True(t, errors.IsTransientLocalError(rec.errs[2]))
	require.False(t, dispatches[0].aborted, "the head is at fault, so its tail still records a failure backoff")
	require.True(t, dispatches[1].aborted, "an aborted successor records no failure backoff")
	require.True(t, dispatches[2].aborted)
	require.True(t, bd.frontierEmpty())
}

func TestDispatcher_CapacityAndBudgetGateAdmission(t *testing.T) {
	bd, rec := testDispatcher(t, 2)
	// Two 1000-byte blocks fit (each charged four times its wire size); a third is
	// held out by the depth, not the budget.
	bd.budget = 12_000

	block := make(chan struct{})
	bd.run = func(context.Context, *blockDispatch, *inflightParent) error { <-block; return nil }

	require.True(t, bd.canDispatch(dispatchAt(1, 1000)))
	bd.dispatch(dispatchAt(1, 1000))
	require.True(t, bd.canDispatch(dispatchAt(2, 1000)))
	bd.dispatch(dispatchAt(2, 1000))
	require.False(t, bd.canDispatch(dispatchAt(3, 1000)), "depth 2 reached")

	close(block)
	bd.drainCompletions(t, rec, 2)
	require.True(t, bd.frontierEmpty())

	// An over-budget block is admitted only into an empty frontier, and once it is
	// in flight nothing joins it.
	over := dispatchAt(4, 20_000)
	require.True(t, bd.canDispatch(over), "over budget but the window is empty")
	bd.dispatch(over)
	require.False(t, bd.canDispatch(dispatchAt(5, 1000)), "the budget is already overdrawn")
}

func TestDispatcher_NonWindowBlockWaitsForAnEmptyFrontier(t *testing.T) {
	bd, rec := testDispatcher(t, 3)
	block := make(chan struct{})
	bd.run = func(context.Context, *blockDispatch, *inflightParent) error { <-block; return nil }

	bd.dispatch(dispatchAt(1, 1000))

	serial := dispatchAt(2, 1000)
	serial.windowed = false
	require.False(t, bd.canDispatch(serial))

	close(block)
	bd.drainCompletions(t, rec, 1)
	require.True(t, bd.canDispatch(serial))
}

func TestDispatcher_CheckpointBlockIsABarrier(t *testing.T) {
	bd, rec := testDispatcher(t, 3)
	block := make(chan struct{})
	bd.run = func(context.Context, *blockDispatch, *inflightParent) error { <-block; return nil }

	cp := dispatchAt(1, 1000)
	cp.isCheckpoint = true
	bd.dispatch(cp)
	require.False(t, bd.canDispatch(dispatchAt(2, 1000)), "nothing dispatches while a checkpoint block is in flight")

	close(block)
	bd.drainCompletions(t, rec, 1)
	require.True(t, bd.canDispatch(dispatchAt(2, 1000)))
}

func TestDispatcher_ContextErrorFromAWorkerIsSubstitutedWithAServiceError(t *testing.T) {
	bd, rec := testDispatcher(t, 2)
	bd.run = func(context.Context, *blockDispatch, *inflightParent) error { return context.Canceled }

	bd.dispatch(dispatchAt(1, 1000))
	bd.drainCompletions(t, rec, 1)

	rec.mu.Lock()
	defer rec.mu.Unlock()
	require.Error(t, rec.errs[0], "a cancelled block is never reported as accepted")
	require.True(t, errors.IsTransientLocalError(rec.errs[0]))
	require.False(t, errors.IsContextError(rec.errs[0]), "the tail's context branch must not swallow it as an accepted block")
}

// TestDispatcher_ChildOfAFailedInFlightParentIsAbortedAtAdmission covers the block
// that was head-processed while its parent was in flight but only reached the front
// of the queue after that parent failed: it must never run its worker, and its tail
// must see the same service error an in-frontier successor gets.
func TestDispatcher_ChildOfAFailedInFlightParentIsAbortedAtAdmission(t *testing.T) {
	bd, rec := testDispatcher(t, 2)

	var ran bool

	bd.run = func(_ context.Context, d *blockDispatch, _ *inflightParent) error {
		if d.height == 1 {
			return errors.NewProcessingError("block 1 broke")
		}

		ran = true

		return nil
	}

	parent := dispatchAt(1, 1000)
	bd.dispatch(parent)
	parentEntry := bd.frontier[0]
	bd.drainCompletions(t, rec, 1)

	child := dispatchAt(2, 1000)
	child.parent = &inflightParent{height: 1, entry: parentEntry}

	bd.dispatch(child)
	bd.drainCompletions(t, rec, 2)

	require.False(t, ran, "the child of a failed parent never starts its own work")
	require.True(t, child.aborted)
	require.True(t, errors.IsTransientLocalError(rec.errs[1]))
}

// TestDispatchBlocks_RepliesOncePerBlockAndDrainsTheBacklog drives the consumer
// goroutine itself with three chained blocks: every message must get exactly one
// reply, the backlog must return to zero, and shutdown must reply to whatever is
// still queued rather than leaving an awaitBlockResult goroutine parked.
func TestDispatchBlocks_RepliesOncePerBlockAndDrainsTheBacklog(t *testing.T) {
	catchingBlocks := blockchain2.FSMStateCATCHINGBLOCKS
	bestHeader := &model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}

	blockchainClient := &blockchain2.Mock{}
	blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&catchingBlocks, nil)
	blockchainClient.On("GetBestBlockHeader", mock.Anything).Return(bestHeader, &model.BlockHeaderMeta{Height: 100}, nil)
	blockchainClient.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).Return([]*chainhash.Hash{bestHeader.Hash()}, nil)

	tSettings := test.CreateBaseTestSettings(t)
	p := peer.NewInboundPeer(ulogger.TestLogger{}, tSettings, &peer.Config{})

	state := &peerSyncState{
		requestedTxns:   expiringmap.New[chainhash.Hash, struct{}](10 * time.Second),
		requestedBlocks: expiringmap.New[chainhash.Hash, struct{}](time.Minute),
	}
	t.Cleanup(func() { state.requestedTxns.Stop(); state.requestedBlocks.Stop() })

	sm := &SyncManager{
		ctx:              context.Background(),
		logger:           ulogger.TestLogger{},
		settings:         tSettings,
		chainParams:      &chaincfg.MainNetParams,
		blockchainClient: blockchainClient,
		peerStates:       txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		requestedBlocks:  expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		rejectedTxns:     txmap.NewSyncedMap[chainhash.Hash, struct{}](100),
		blockSizeTracker: newBlockSizeTracker(10),
		quit:             make(chan struct{}),
	}
	t.Cleanup(func() { sm.requestedBlocks.Stop() })
	sm.peerStates.Set(p, state)

	blocks := make([]*wire.MsgBlock, 0, 4)
	prev := chainhash.Hash{0x01}

	for i := 0; i < 4; i++ {
		b := wire.NewMsgBlock(wire.NewBlockHeader(1, &prev, &chainhash.Hash{}, 0, uint32(i)))
		blocks = append(blocks, b)
		prev = b.Header.BlockHash()

		h := b.Header.BlockHash()
		state.requestedBlocks.Set(h, struct{}{})
		sm.requestedBlocks.Set(h, struct{}{})
	}

	sm.dispatcher = newBlockDispatcher(sm)

	var started int32

	// The work itself is stubbed: this test is about the loop's replies, ordering
	// and backlog accounting, not about block validation.
	sm.dispatcher.run = func(context.Context, *blockDispatch, *inflightParent) error {
		atomic.AddInt32(&started, 1)

		return nil
	}

	queue := make(chan *blockQueueMsg, 8)
	go sm.dispatchBlocks(queue)

	replies := make([]chan error, 0, 3)

	for i := 0; i < 3; i++ {
		reply := make(chan error, 1)
		replies = append(replies, reply)
		sm.blockBacklog.Add(1)
		queue <- &blockQueueMsg{block: blocks[i], blockHash: blocks[i].Header.BlockHash(), blockHeight: int32(101 + i), peer: p, reply: reply}
	}

	for i, reply := range replies {
		select {
		case err := <-reply:
			require.NoError(t, err, "block %d", i)
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for the reply to block %d", i)
		}

		select {
		case <-reply:
			t.Fatalf("block %d was replied to twice", i)
		default:
		}
	}

	require.Equal(t, int32(3), atomic.LoadInt32(&started))
	require.Equal(t, int64(0), sm.blockBacklog.Load(), "every completion decrements the backlog exactly once")

	// A block still queued at shutdown is replied to, not dropped.
	shutdownReply := make(chan error, 1)
	sm.blockBacklog.Add(1)
	queue <- &blockQueueMsg{block: blocks[3], blockHash: blocks[3].Header.BlockHash(), blockHeight: 104, peer: p, reply: shutdownReply}
	close(sm.quit)

	select {
	case err := <-shutdownReply:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the shutdown reply")
	}

	require.Eventually(t, func() bool { return sm.blockBacklog.Load() == 0 }, 5*time.Second, 10*time.Millisecond)
}

// TestDispatcher_ParentForAndInFlight covers the two frontier lookups the head does:
// only the frontier tail can be a block's in-flight parent, and any hash in the
// frontier counts as in flight so the cascade check does not treat a block that is
// being retried as a failed parent.
func TestDispatcher_ParentForAndInFlight(t *testing.T) {
	bd, rec := testDispatcher(t, 3)
	block := make(chan struct{})
	bd.run = func(context.Context, *blockDispatch, *inflightParent) error { <-block; return nil }

	first := dispatchAt(1, 1000)
	second := dispatchAt(2, 1000)
	bd.dispatch(first)
	bd.dispatch(second)

	require.True(t, bd.inFlight(first.msg.blockHash))
	require.True(t, bd.inFlight(second.msg.blockHash))
	require.False(t, bd.inFlight(chainhash.HashH([]byte("absent"))))

	p := bd.parentFor(&second.msg.blockHash)
	require.NotNil(t, p, "the frontier tail is the parent of the next block")
	require.Equal(t, uint32(2), p.height)

	require.Nil(t, bd.parentFor(&first.msg.blockHash), "only the tail can be a parent")

	close(block)
	bd.drainCompletions(t, rec, 2)
	require.False(t, bd.inFlight(first.msg.blockHash))
	require.Nil(t, bd.parentFor(&second.msg.blockHash))
}

// loopHarness is the minimum a SyncManager needs for dispatchBlocks to run a block
// end to end with the work itself stubbed: a registered peer, the FSM in catch-up,
// and the maps the tail writes to.
type loopHarness struct {
	sm     *SyncManager
	peer   *peer.Peer
	blocks []*wire.MsgBlock
	queue  chan *blockQueueMsg
	client *blockchain2.Mock
}

func newLoopHarness(t *testing.T, blocks int) *loopHarness {
	t.Helper()

	catchingBlocks := blockchain2.FSMStateCATCHINGBLOCKS
	bestHeader := &model.BlockHeader{HashPrevBlock: &chainhash.Hash{}, HashMerkleRoot: &chainhash.Hash{}}

	client := &blockchain2.Mock{}
	client.On("GetFSMCurrentState", mock.Anything).Return(&catchingBlocks, nil)
	client.On("GetBestBlockHeader", mock.Anything).Return(bestHeader, &model.BlockHeaderMeta{Height: 100}, nil)
	client.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).Return([]*chainhash.Hash{bestHeader.Hash()}, nil)

	tSettings := test.CreateBaseTestSettings(t)
	p := peer.NewInboundPeer(ulogger.TestLogger{}, tSettings, &peer.Config{})

	state := &peerSyncState{
		requestedTxns:   expiringmap.New[chainhash.Hash, struct{}](10 * time.Second),
		requestedBlocks: expiringmap.New[chainhash.Hash, struct{}](time.Minute),
	}
	t.Cleanup(func() { state.requestedTxns.Stop(); state.requestedBlocks.Stop() })

	sm := &SyncManager{
		ctx:                  context.Background(),
		logger:               ulogger.TestLogger{},
		settings:             tSettings,
		chainParams:          &chaincfg.MainNetParams,
		blockchainClient:     client,
		peerStates:           txmap.NewSyncedMap[*peer.Peer, *peerSyncState](),
		requestedBlocks:      expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		rejectedTxns:         txmap.NewSyncedMap[chainhash.Hash, struct{}](100),
		blockSizeTracker:     newBlockSizeTracker(10),
		blockFailureBackoff:  expiringmap.New[chainhash.Hash, *blockFailureState](time.Minute),
		recentlyFailedBlocks: expiringmap.New[chainhash.Hash, struct{}](time.Minute),
		quit:                 make(chan struct{}),
	}
	t.Cleanup(func() { sm.requestedBlocks.Stop(); sm.blockFailureBackoff.Stop(); sm.recentlyFailedBlocks.Stop() })
	sm.peerStates.Set(p, state)

	h := &loopHarness{sm: sm, peer: p, client: client, queue: make(chan *blockQueueMsg, 8)}

	prev := chainhash.Hash{0x01}

	for i := 0; i < blocks; i++ {
		b := wire.NewMsgBlock(wire.NewBlockHeader(1, &prev, &chainhash.Hash{}, 0, uint32(i)))
		h.blocks = append(h.blocks, b)
		prev = b.Header.BlockHash()

		hash := b.Header.BlockHash()
		state.requestedBlocks.Set(hash, struct{}{})
		sm.requestedBlocks.Set(hash, struct{}{})
	}

	sm.dispatcher = newBlockDispatcher(sm)

	return h
}

// enqueue puts block i on the queue with its backlog counted, the way the feeder in
// blockHandler does, and returns its reply channel.
func (h *loopHarness) enqueue(i int) chan error {
	reply := make(chan error, 1)

	h.sm.blockBacklog.Add(1)
	h.queue <- &blockQueueMsg{
		block:       h.blocks[i],
		blockHash:   h.blocks[i].Header.BlockHash(),
		blockHeight: int32(101 + i),
		peer:        h.peer,
		reply:       reply,
	}

	return reply
}

// recordTails wraps the dispatcher's tail so the test can see the order the tails ran
// in without disturbing the backlog and reply pairing the real tail does.
func (h *loopHarness) recordTails(n int) chan chainhash.Hash {
	order := make(chan chainhash.Hash, n)
	orig := h.sm.dispatcher.tail

	h.sm.dispatcher.tail = func(d *blockDispatch, err error) error {
		order <- d.msg.blockHash

		return orig(d, err)
	}

	return order
}

func requireReply(t *testing.T, reply chan error, what string) error {
	t.Helper()

	select {
	case err := <-reply:
		select {
		case <-reply:
			t.Fatalf("%s was replied to twice", what)
		default:
		}

		return err
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for the reply to %s", what)
	}

	return nil
}

// TestDispatchBlocks_PendingSlotDisablesTheQueueArm pins the shape the whole design
// rests on: with one slot, the block behind the one in flight is head-processed and
// parked, and the block behind that is not taken off the queue at all until the first
// one completes. Their tails then run in queue order.
func TestDispatchBlocks_PendingSlotDisablesTheQueueArm(t *testing.T) {
	h := newLoopHarness(t, 3)

	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})

	h.sm.dispatcher.run = func(context.Context, *blockDispatch, *inflightParent) error {
		<-release

		return nil
	}

	order := h.recordTails(3)

	go h.sm.dispatchBlocks(h.queue)

	replies := []chan error{h.enqueue(0), h.enqueue(1), h.enqueue(2)}

	// The first is in flight, the second is parked in the pending slot, and the third
	// is still on the queue because the queue arm is disabled.
	require.Eventually(t, func() bool { return len(h.queue) == 1 }, 5*time.Second, 5*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, 1, len(h.queue), "the queue arm must stay disabled while a block is pending")

	close(release)

	for i, reply := range replies {
		require.NoError(t, requireReply(t, reply, "block "+string(rune('1'+i))))
	}

	for i := 0; i < 3; i++ {
		require.Equal(t, h.blocks[i].Header.BlockHash(), <-order, "tails run in queue order")
	}

	require.Equal(t, int64(0), h.sm.blockBacklog.Load())
}

// TestDispatchBlocks_ShutdownRepliesForInFlightAndPending covers the drain: the block
// whose worker is still running and the block parked in the pending slot each get
// exactly one error reply, and the backlog returns to zero.
func TestDispatchBlocks_ShutdownRepliesForInFlightAndPending(t *testing.T) {
	h := newLoopHarness(t, 2)

	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})

	h.sm.dispatcher.run = func(context.Context, *blockDispatch, *inflightParent) error {
		<-release

		return nil
	}

	go h.sm.dispatchBlocks(h.queue)

	inFlight := h.enqueue(0)
	pending := h.enqueue(1)

	require.Eventually(t, func() bool { return len(h.queue) == 0 }, 5*time.Second, 5*time.Millisecond)
	time.Sleep(100 * time.Millisecond)

	close(h.sm.quit)

	require.Error(t, requireReply(t, inFlight, "the in-flight block"))
	require.Error(t, requireReply(t, pending, "the pending block"))
	require.Equal(t, int64(0), h.sm.blockBacklog.Load())
}

// TestDispatchBlocks_PendingChildOfAFailedParentTakesTheCascadePath is the window-off
// case: the head exempted this block from the #1333 cascade check because its parent
// was in flight, and there is no resolved in-flight parent to abort it at admission.
// It must still never be revalidated, and must get exactly the treatment the head's
// own cascade branch gives.
func TestDispatchBlocks_PendingChildOfAFailedParentTakesTheCascadePath(t *testing.T) {
	h := newLoopHarness(t, 2)
	require.False(t, h.sm.windowRouteEnabled(), "precondition: this is the window-off path")

	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})

	parentHash := h.blocks[0].Header.BlockHash()
	childHash := h.blocks[1].Header.BlockHash()

	var ran []chainhash.Hash

	var mu sync.Mutex

	h.sm.dispatcher.run = func(_ context.Context, d *blockDispatch, _ *inflightParent) error {
		mu.Lock()
		ran = append(ran, d.msg.blockHash)
		mu.Unlock()

		<-release

		return errors.NewStorageError("boom")
	}

	go h.sm.dispatchBlocks(h.queue)

	parentReply := h.enqueue(0)
	childReply := h.enqueue(1)

	require.Eventually(t, func() bool { return len(h.queue) == 0 }, 5*time.Second, 5*time.Millisecond)

	close(release)

	require.Error(t, requireReply(t, parentReply, "the parent"))
	require.NoError(t, requireReply(t, childReply, "the child"), "a short-circuited descendant replies nil")

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []chainhash.Hash{parentHash}, ran, "the child of a failed parent is never revalidated")

	_, marked := h.sm.recentlyFailedBlocks.Get(childHash)
	require.True(t, marked, "a skipped descendant must itself be marked failed")

	h.client.AssertCalled(t, "GetBlockLocator", mock.Anything, mock.Anything, mock.Anything)
	require.Equal(t, int64(0), h.sm.blockBacklog.Load())
}

// TestDispatcher_AbortedSuccessorRecordsNoBackoff runs the real tail: the head keeps
// its own failure backoff, the successor it aborted earns none.
func TestDispatcher_AbortedSuccessorRecordsNoBackoff(t *testing.T) {
	h := newLoopHarness(t, 2)
	bd := h.sm.dispatcher

	rec := &tailRecorder{}
	bd.tail = func(d *blockDispatch, err error) error {
		_ = h.sm.handleBlockMsgTail(d, err)

		return rec.tail(d, err)
	}

	bd.run = func(_ context.Context, d *blockDispatch, _ *inflightParent) error {
		if d.height == 1 {
			return errors.NewStorageError("boom")
		}

		return nil
	}

	head := dispatchAt(1, 1000)
	head.peer = h.peer
	head.msg.peer = h.peer
	head.catchingBlocks = true

	successor := dispatchAt(2, 1000)
	successor.peer = h.peer
	successor.msg.peer = h.peer
	successor.catchingBlocks = true

	bd.dispatch(head)
	bd.dispatch(successor)
	bd.drainCompletions(t, rec, 2)

	_, headBackoff := h.sm.blockFailureBackoff.Get(head.msg.blockHash)
	require.True(t, headBackoff, "the block that actually failed is throttled on its next delivery")

	_, successorBackoff := h.sm.blockFailureBackoff.Get(successor.msg.blockHash)
	require.False(t, successorBackoff, "an aborted successor never ran a failing attempt of its own")
}
