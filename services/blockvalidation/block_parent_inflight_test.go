package blockvalidation

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jarcoal/httpmock"
	"github.com/jellydator/ttlcache/v3"
	"github.com/stretchr/testify/require"
)

// TestForkManager_ReservesSlotForChainExtendingBlocks pins issue 4944 item 3: blocks that do
// not extend the tip may hold at most maxParallelForks-1 slots, so slow fork blocks can never
// occupy every worker while the tip waits.
func TestForkManager_ReservesSlotForChainExtendingBlocks(t *testing.T) {
	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.MaxParallelForks = 4
	fm := NewForkManager(ulogger.TestLogger{}, tSettings)

	for i := byte(1); i <= 3; i++ {
		require.True(t, fm.StartProcessingBlock(&chainhash.Hash{i}, PriorityNearFork))
	}

	fourthFork := &chainhash.Hash{4}
	canProcess, err := fm.CanProcessBlock(ctx, fourthFork, PriorityDeepFork)
	require.NoError(t, err)
	require.False(t, canProcess, "the last slot is reserved for a chain-extending block")
	require.False(t, fm.StartProcessingBlock(fourthFork, PriorityDeepFork))

	tip := &chainhash.Hash{5}
	canProcess, err = fm.CanProcessBlock(ctx, tip, PriorityChainExtending)
	require.NoError(t, err)
	require.True(t, canProcess)
	require.True(t, fm.StartProcessingBlock(tip, PriorityChainExtending))

	fm.FinishProcessingBlock(&chainhash.Hash{1})
	require.True(t, fm.StartProcessingBlock(fourthFork, PriorityDeepFork), "a freed fork slot is usable by a fork block again")
}

func TestForkManager_NoReservationWithSingleSlot(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.MaxParallelForks = 1
	fm := NewForkManager(ulogger.TestLogger{}, tSettings)

	require.True(t, fm.StartProcessingBlock(&chainhash.Hash{1}, PriorityDeepFork), "with one slot a fork block must still be able to run")
}

// TestBlockPriorityQueue_Get_ChildWaitsForParentInFlight pins issue 4944 item 4 at the queue: a
// block whose parent is queued or processing stays queued until the parent is done, whatever
// its own effective priority.
func TestBlockPriorityQueue_Get_ChildWaitsForParentInFlight(t *testing.T) {
	initPrometheusMetrics()
	ctx := context.Background()
	pq := NewBlockPriorityQueue(ulogger.TestLogger{})
	bp := NewMockBlockProcessor()

	parent := &chainhash.Hash{1}
	child := &chainhash.Hash{2}

	pq.Add(processBlockFound{hash: parent}, PriorityChainExtending, 10)
	pq.Add(processBlockFound{hash: child, parentHash: parent}, PriorityChainExtending, 11)

	// Parent queued but not processable: the child must not jump it.
	bp.SetCanProcess(parent, false)

	for i := 0; i < DefaultSkipCount+2; i++ {
		_, status := pq.Get(ctx, bp)
		require.Equal(t, GetAllBlocked, status, "the skip promotion must not bypass the parent gate")
	}

	// Parent taken and processing: the child still waits.
	bp.SetCanProcess(parent, true)

	taken, status := pq.Get(ctx, bp)
	require.Equal(t, GetOK, status)
	require.Equal(t, parent, taken.hash)

	_, status = pq.Get(ctx, bp)
	require.Equal(t, GetAllBlocked, status)
	require.True(t, pq.Contains(*child))

	// Parent done: the child is released.
	bp.FinishProcessingBlock(parent)

	taken, status = pq.Get(ctx, bp)
	require.Equal(t, GetOK, status)
	require.Equal(t, child, taken.hash)
}

// TestRequeueBlock_StaysQueuedDuringBackoff pins that a block waiting out its retry delay is
// still in the queue. Otherwise its children would see no parent in flight during the delay
// and go to catchup.
func TestRequeueBlock_StaysQueuedDuringBackoff(t *testing.T) {
	initPrometheusMetrics()
	server := newRetryTestServer(t, 3)
	hash := &chainhash.Hash{1}

	server.requeueBlock(processBlockFound{hash: hash, baseURL: "http://peer"}, 50*time.Millisecond)
	require.True(t, server.blockPriorityQueue.Contains(*hash))

	_, status := server.blockPriorityQueue.Get(context.Background(), NewMockBlockProcessor())
	require.Equal(t, GetAllBlocked, status, "a block in its retry delay must not be handed out")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	taken, status := server.blockPriorityQueue.WaitForBlock(ctx, NewMockBlockProcessor())
	require.Equal(t, GetOK, status, "the delay expiring must wake a waiting worker")
	require.Equal(t, hash, taken.hash)
}

// newParentInFlightServer builds a Server where blocks[0..1] are stored and blocks[3] is
// served by a peer. blocks[2] is the parent under test and is never stored.
func newParentInFlightServer(t *testing.T) (*Server, []*chainhash.Hash) {
	t.Helper()

	server, blocks := newParentInFlightServerWithBlocks(t)

	return server, []*chainhash.Hash{blocks[2].Hash(), blocks[3].Hash()}
}

// newParentInFlightServerWithBlocks is newParentInFlightServer returning the whole test chain.
func newParentInFlightServerWithBlocks(t *testing.T) (*Server, []*model.Block) {
	t.Helper()

	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	blockchainClient, err := blockchain.NewLocalClient(logger, tSettings, blockchain_store.NewMockStore(), nil, nil)
	require.NoError(t, err)

	bv := NewBlockValidation(ctx, logger, tSettings, blockchainClient, memory.New(), memory.New(), &utxo.MockUtxostore{}, &validator.MockValidator{}, nil)

	blocks := testhelpers.CreateTestBlockChain(t, 4)
	for i := 0; i < 2; i++ {
		require.NoError(t, blockchainClient.AddBlock(ctx, blocks[i], "test-peer"))
	}

	childBytes, err := blocks[3].Bytes()
	require.NoError(t, err)

	httpmock.ActivateNonDefault(util.HTTPClient())
	t.Cleanup(httpmock.DeactivateAndReset)
	httpmock.RegisterResponder("GET", fmt.Sprintf("http://peer/block/%s", blocks[3].Hash()), httpmock.NewBytesResponder(200, childBytes))

	server := &Server{
		logger:              logger,
		settings:            tSettings,
		blockchainClient:    blockchainClient,
		blockValidation:     bv,
		blockPriorityQueue:  NewBlockPriorityQueue(logger),
		blockClassifier:     NewBlockClassifier(logger, 10, blockchainClient),
		forkManager:         NewForkManager(logger, tSettings),
		catchupCh:           make(chan processBlockCatchup, 10),
		processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
		catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](),
	}

	return server, blocks
}

// TestAddBlockToPriorityQueue_ParentProcessing_QueuesChild pins issue 4944 item 4: a child
// whose parent is still being validated is queued behind it, not sent to catchup (which
// moves the FSM to CATCHINGBLOCKS and stops mining). It inherits the parent's class.
func TestAddBlockToPriorityQueue_ParentProcessing_QueuesChild(t *testing.T) {
	initPrometheusMetrics()
	server, hashes := newParentInFlightServer(t)
	parent, child := hashes[0], hashes[1]

	require.True(t, server.forkManager.StartProcessingBlock(parent, PriorityChainExtending))

	server.addBlockToPriorityQueue(context.Background(), processBlockFound{hash: child, baseURL: "http://peer", peerID: "p1"})

	require.Empty(t, server.catchupCh, "a parent still validating is not a gap to catch up")

	item, ok := server.blockPriorityQueue.hashIndex[*child]
	require.True(t, ok, "the child must be queued behind its parent")
	require.Equal(t, PriorityChainExtending, item.priority)
	require.Equal(t, parent, item.blockFound.parentHash)
}

func TestAddBlockToPriorityQueue_ParentQueued_QueuesChild(t *testing.T) {
	initPrometheusMetrics()
	server, hashes := newParentInFlightServer(t)
	parent, child := hashes[0], hashes[1]

	server.blockPriorityQueue.Add(processBlockFound{hash: parent, baseURL: "http://peer"}, PriorityNearFork, 2)

	server.addBlockToPriorityQueue(context.Background(), processBlockFound{hash: child, baseURL: "http://peer", peerID: "p1"})

	require.Empty(t, server.catchupCh)

	item, ok := server.blockPriorityQueue.hashIndex[*child]
	require.True(t, ok)
	require.Equal(t, PriorityNearFork, item.priority)
}

func TestAddBlockToPriorityQueue_ParentUnknown_GoesToCatchup(t *testing.T) {
	initPrometheusMetrics()
	server, hashes := newParentInFlightServer(t)
	child := hashes[1]

	server.addBlockToPriorityQueue(context.Background(), processBlockFound{hash: child, baseURL: "http://peer", peerID: "p1"})

	require.Eventually(t, func() bool { return len(server.catchupCh) == 1 }, 2*time.Second, 10*time.Millisecond)
	require.False(t, server.blockPriorityQueue.Contains(*child))
}

// slotReleaseRecorder records, at the moment the fork manager releases a slot, whether the
// block is back on the queue.
type slotReleaseRecorder struct {
	pq       *BlockPriorityQueue
	hash     chainhash.Hash
	inQueue  bool
	released bool
}

func (r *slotReleaseRecorder) Broadcast() {}

func (r *slotReleaseRecorder) Signal() {
	r.released = true
	r.inQueue = r.pq.Contains(r.hash)
}

// TestFinishProcessedBlock_RequeuesBeforeReleasingSlot pins the order in the worker: a block
// that will be retried is back on the queue before its slot is released. Releasing first
// wakes a worker while the block is neither queued nor processing, and a child waiting
// behind it is taken and sent to catchup.
func TestFinishProcessedBlock_RequeuesBeforeReleasingSlot(t *testing.T) {
	initPrometheusMetrics()
	server := newRetryTestServer(t, 3)
	server.forkManager = NewForkManager(ulogger.TestLogger{}, server.settings)

	parent := &chainhash.Hash{1}
	recorder := &slotReleaseRecorder{pq: server.blockPriorityQueue, hash: *parent}
	server.forkManager.SetPriorityQueue(recorder)

	require.True(t, server.forkManager.StartProcessingBlock(parent, PriorityChainExtending))

	deadlineErr := errors.NewServiceError("failed block validation BlockFound", errors.New(errors.ERR_ERROR, "context deadline exceeded"))
	server.finishProcessedBlock(0, processBlockFound{hash: parent, baseURL: "http://peer"}, deadlineErr)

	require.True(t, recorder.released)
	require.True(t, recorder.inQueue, "the block must be requeued before its slot is released")
	require.False(t, server.forkManager.IsProcessingBlock(parent))
}

// TestBlockPriorityQueue_Get_ChildWaitsForParentBeingFetched pins that a parent still being
// fetched counts as in flight: its child must not be handed out before the parent is queued.
func TestBlockPriorityQueue_Get_ChildWaitsForParentBeingFetched(t *testing.T) {
	initPrometheusMetrics()
	ctx := context.Background()
	pq := NewBlockPriorityQueue(ulogger.TestLogger{})
	bp := NewMockBlockProcessor()

	parent := &chainhash.Hash{1}
	child := &chainhash.Hash{2}

	pq.MarkFetching(parent)
	pq.Add(processBlockFound{hash: child, parentHash: parent}, PriorityChainExtending, 11)

	_, status := pq.Get(ctx, bp)
	require.Equal(t, GetAllBlocked, status, "the child must wait while its parent is being fetched")

	// The parent's fetch failed: nothing to wait for any more.
	pq.UnmarkFetching(parent)

	taken, status := pq.Get(ctx, bp)
	require.Equal(t, GetOK, status)
	require.Equal(t, child, taken.hash)
}

// TestAddBlockToPriorityQueue_ParentFetching_QueuesChild pins that a child announced while its
// parent is still being fetched by another intake worker is queued, not sent to catchup.
func TestAddBlockToPriorityQueue_ParentFetching_QueuesChild(t *testing.T) {
	initPrometheusMetrics()
	server, hashes := newParentInFlightServer(t)
	parent, child := hashes[0], hashes[1]

	server.blockPriorityQueue.MarkFetching(parent)

	server.addBlockToPriorityQueue(context.Background(), processBlockFound{hash: child, baseURL: "http://peer", peerID: "p1"})

	require.Empty(t, server.catchupCh, "a parent still being fetched is not a gap to catch up")
	require.True(t, server.blockPriorityQueue.Contains(*child))
}

// TestProcessBlockFoundChannel_MarksBlockFetching pins that an announced block counts as in
// flight for the whole fetch, and stops counting once the fetch is over (here: it failed).
func TestProcessBlockFoundChannel_MarksBlockFetching(t *testing.T) {
	initPrometheusMetrics()
	server, hashes := newParentInFlightServer(t)
	child := hashes[1]

	var pendingDuringFetch bool

	httpmock.RegisterResponder("GET", fmt.Sprintf("http://peer/block/%s", child), func(*http.Request) (*http.Response, error) {
		pendingDuringFetch = server.blockPriorityQueue.IsPending(*child)
		return httpmock.NewStringResponse(http.StatusInternalServerError, "unavailable"), nil
	})

	_ = server.processBlockFoundChannel(context.Background(), processBlockFound{hash: child, baseURL: "http://peer", peerID: "p1"})

	require.True(t, pendingDuringFetch, "the block must count as in flight while it is fetched")
	require.False(t, server.blockPriorityQueue.IsPending(*child), "the fetching mark must be cleared once the fetch is over")
}

// TestFinishProcessedBlock_PromotesWaitingChildToParentClass pins that a child queued while its
// parent was still being fetched, and so classified without it, takes the parent's class when
// the parent finishes. Otherwise the next tip block keeps a fork class and loses the reserved
// slot to fork blocks.
func TestFinishProcessedBlock_PromotesWaitingChildToParentClass(t *testing.T) {
	initPrometheusMetrics()
	server := newRetryTestServer(t, 3)
	server.settings.BlockValidation.MaxParallelForks = 4
	server.forkManager = NewForkManager(ulogger.TestLogger{}, server.settings)

	parent := &chainhash.Hash{1}
	child := &chainhash.Hash{2}

	server.blockPriorityQueue.Add(processBlockFound{hash: child, parentHash: parent}, PriorityDeepFork, 11)
	require.True(t, server.forkManager.StartProcessingBlock(parent, PriorityChainExtending))

	for i := byte(10); i < 13; i++ {
		require.True(t, server.forkManager.StartProcessingBlock(&chainhash.Hash{i}, PriorityNearFork))
	}

	server.finishProcessedBlock(0, processBlockFound{hash: parent, priority: PriorityChainExtending}, nil)

	priority, ok := server.blockPriorityQueue.PriorityOf(child)
	require.True(t, ok)
	require.Equal(t, PriorityChainExtending, priority)

	taken, status := server.blockPriorityQueue.Get(context.Background(), server.forkManager)
	require.Equal(t, GetOK, status, "the promoted child must get the slot reserved for the tip")
	require.Equal(t, child, taken.hash)
}

// TestBlockPriorityQueue_MarkFetching_FirstFetcherOnly pins that only the first fetcher of a
// block holds the mark. A duplicate announcement still fetching after the first one queued the
// block must not hold children behind a parent that is already queued or stored.
func TestBlockPriorityQueue_MarkFetching_FirstFetcherOnly(t *testing.T) {
	pq := NewBlockPriorityQueue(ulogger.TestLogger{})
	hash := &chainhash.Hash{1}

	require.True(t, pq.MarkFetching(hash))
	require.False(t, pq.MarkFetching(hash), "a second fetcher must not take the mark")

	pq.UnmarkFetching(hash)
	require.False(t, pq.IsPending(*hash))
}

// TestProcessBlockFoundChannel_DuplicateFetchKeepsFirstMark pins that a worker fetching a block
// another worker is already fetching does not clear that worker's mark when it finishes.
func TestProcessBlockFoundChannel_DuplicateFetchKeepsFirstMark(t *testing.T) {
	initPrometheusMetrics()
	server, hashes := newParentInFlightServer(t)
	child := hashes[1]

	httpmock.RegisterResponder("GET", fmt.Sprintf("http://peer/block/%s", child), httpmock.NewStringResponder(http.StatusInternalServerError, "unavailable"))

	require.True(t, server.blockPriorityQueue.MarkFetching(child))

	_ = server.processBlockFoundChannel(context.Background(), processBlockFound{hash: child, baseURL: "http://peer", peerID: "p1"})

	require.True(t, server.blockPriorityQueue.IsPending(*child), "the first fetcher's mark must survive a duplicate fetch")
}

// TestProcessBlockFoundChannel_DuplicateOfInFlightBlockNotMarked pins that a duplicate
// announcement of a block already queued or being validated does not mark it as fetching.
// Otherwise a child would keep waiting on the duplicate's fetch after the block is stored.
func TestProcessBlockFoundChannel_DuplicateOfInFlightBlockNotMarked(t *testing.T) {
	initPrometheusMetrics()
	server, hashes := newParentInFlightServer(t)
	child := hashes[1]

	require.True(t, server.forkManager.StartProcessingBlock(child, PriorityChainExtending))

	var pendingDuringFetch bool

	httpmock.RegisterResponder("GET", fmt.Sprintf("http://peer/block/%s", child), func(*http.Request) (*http.Response, error) {
		pendingDuringFetch = server.blockPriorityQueue.IsPending(*child)
		return httpmock.NewStringResponse(http.StatusInternalServerError, "unavailable"), nil
	})

	_ = server.processBlockFoundChannel(context.Background(), processBlockFound{hash: child, baseURL: "http://peer", peerID: "p1"})

	require.False(t, pendingDuringFetch, "a block already being validated must not also be marked as fetching")
}

// TestIsWaiting_ParentVisibleAcrossRequeueHandoff pins that a child never sees its parent as gone
// while the parent moves between the queue and a processing slot. A retried parent goes back on
// the queue and then releases its slot; a reader checking the queue and then the slot without
// holding the slot state could miss it in between and release the child to catchup.
func TestIsWaiting_ParentVisibleAcrossRequeueHandoff(t *testing.T) {
	initPrometheusMetrics()
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.MaxParallelForks = 4
	fm := NewForkManager(ulogger.TestLogger{}, tSettings)
	pq := NewBlockPriorityQueue(ulogger.TestLogger{})

	parent := &chainhash.Hash{1}
	child := &PrioritizedBlock{blockFound: processBlockFound{hash: &chainhash.Hash{2}, parentHash: parent}}

	pq.Add(processBlockFound{hash: parent}, PriorityChainExtending, 10)

	stop := make(chan struct{})
	done := make(chan struct{})

	go func() {
		defer close(done)

		for {
			select {
			case <-stop:
				return
			default:
			}

			taken, ok := pq.claimAndTake(fm, parent, PriorityChainExtending)
			if !ok {
				continue
			}

			// The worker's retry path: back on the queue, then release the slot.
			pq.RequeueAfter(taken, PriorityChainExtending, 10, 0)
			fm.FinishProcessingBlock(parent)
		}
	}()

	missed := 0
	for i := 0; i < 200000; i++ {
		if !pq.isWaiting(fm, child) {
			missed++
		}
	}

	close(stop)
	<-done

	require.Zero(t, missed, "the child saw its parent as neither queued nor processing")
}

// TestForkManager_IsBlockInFlight_CountsBlockValidatingOutsideQueue pins the scale-3 incident of
// 2026-10-26: a parent validated by catchup, gRPC or legacy sync never takes a worker slot, so
// the queue and slot state alone report it as gone and its child goes to catchup.
func TestForkManager_IsBlockInFlight_CountsBlockValidatingOutsideQueue(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	fm := NewForkManager(ulogger.TestLogger{}, tSettings)
	parent := &chainhash.Hash{1}
	notPending := func(chainhash.Hash) bool { return false }

	require.False(t, fm.IsBlockInFlight(parent, notPending))

	fm.SetValidatingCheck(func(hash *chainhash.Hash) bool { return *hash == *parent })

	require.True(t, fm.IsBlockInFlight(parent, notPending), "a block being validated outside the queue is in flight")
	require.False(t, fm.IsBlockInFlight(&chainhash.Hash{2}, notPending))
}

// TestAddBlockToPriorityQueue_ParentValidatingOutsideQueue_QueuesChild: a child whose parent is
// being validated by another path is queued behind it, not sent to catchup.
func TestAddBlockToPriorityQueue_ParentValidatingOutsideQueue_QueuesChild(t *testing.T) {
	initPrometheusMetrics()
	server, hashes := newParentInFlightServer(t)
	parent, child := hashes[0], hashes[1]

	server.wireValidationInFlight()
	server.blockValidation.blocksCurrentlyValidating.Set(*parent, &validationResult{done: make(chan struct{})})

	server.addBlockToPriorityQueue(context.Background(), processBlockFound{hash: child, baseURL: "http://peer", peerID: "p1"})

	require.Empty(t, server.catchupCh, "a parent still validating is not a gap to catch up")
	require.True(t, server.blockPriorityQueue.Contains(*child), "the child must be queued behind its parent")
}

// TestRunOncePerBlock_WakesQueueAfterRelease: a child held behind a parent validated outside the
// queue is only released by a queue wake-up. The hook must run once the block no longer counts as
// validating, or the woken worker still sees the parent in flight and parks again.
func TestRunOncePerBlock_WakesQueueAfterRelease(t *testing.T) {
	server, hashes := newParentInFlightServer(t)
	bv := server.blockValidation
	parent := hashes[0]

	type hookCall struct{ stillValidating bool }
	calls := make(chan hookCall, 1)
	bv.SetValidationDoneHooks(nil, func() { calls <- hookCall{stillValidating: bv.IsValidating(parent)} })

	require.NoError(t, bv.runOncePerBlock(parent, nil, func(*ValidateBlockOptions) error {
		require.True(t, bv.IsValidating(parent))
		return nil
	}))

	select {
	case call := <-calls:
		require.False(t, call.stillValidating, "the hook must run once the block no longer counts as validating")
	case <-time.After(2 * time.Second):
		t.Fatal("the validation-done hook never ran")
	}
}

// TestWireValidationInFlight_ReleasesHeldChild: end to end through the queue. The child waits
// while its parent validates outside the queue and is handed out once the validation is done,
// without any other queue event.
func TestWireValidationInFlight_ReleasesHeldChild(t *testing.T) {
	initPrometheusMetrics()
	server, hashes := newParentInFlightServer(t)
	parent, child := hashes[0], hashes[1]
	server.wireValidationInFlight()

	release := make(chan struct{})
	validated := make(chan error, 1)

	go func() {
		validated <- server.blockValidation.runOncePerBlock(parent, nil, func(*ValidateBlockOptions) error {
			<-release
			return nil
		})
	}()

	require.Eventually(t, func() bool { return server.blockValidation.IsValidating(parent) }, 2*time.Second, time.Millisecond)

	server.blockPriorityQueue.Add(processBlockFound{hash: child, parentHash: parent}, PriorityChainExtending, 11)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	taken := make(chan processBlockFound, 1)
	go func() {
		block, status := server.blockPriorityQueue.WaitForBlock(ctx, server.forkManager)
		if status == GetOK {
			taken <- block
		}
	}()

	select {
	case <-taken:
		t.Fatal("the child was handed out while its parent was still validating")
	case <-time.After(200 * time.Millisecond):
	}

	close(release)
	require.NoError(t, <-validated)

	select {
	case block := <-taken:
		require.Equal(t, *child, *block.hash)
	case <-ctx.Done():
		t.Fatal("the child was never released after its parent finished validating")
	}
}

// TestValidationDone_PromotesChildOfNewTip pins that a child held behind a parent validated
// outside the queue takes the tip class once that parent is stored as the best block. It was
// classified while the parent was unknown, so it got a fork class, and only a worker-validated
// parent promotes its children; without this the next tip block cannot use the reserved slot.
func TestValidationDone_PromotesChildOfNewTip(t *testing.T) {
	initPrometheusMetrics()
	server, blocks := newParentInFlightServerWithBlocks(t)
	parent, child := blocks[2], blocks[3].Hash()

	server.blockPriorityQueue.Add(processBlockFound{hash: child, parentHash: parent.Hash()}, PriorityDeepFork, 3)
	require.NoError(t, server.blockchainClient.AddBlock(context.Background(), parent, "test-peer"))

	server.settleValidatedBlock(server.blockchainClient, parent.Hash(), nil)

	priority, ok := server.blockPriorityQueue.PriorityOf(child)
	require.True(t, ok)
	require.Equal(t, PriorityChainExtending, priority)
}

// TestValidationDone_FailedParentDoesNotPromote pins that a parent whose validation failed
// passes no class to its children.
func TestValidationDone_FailedParentDoesNotPromote(t *testing.T) {
	initPrometheusMetrics()
	server, blocks := newParentInFlightServerWithBlocks(t)
	parent, child := blocks[2], blocks[3].Hash()

	server.blockPriorityQueue.Add(processBlockFound{hash: child, parentHash: parent.Hash()}, PriorityDeepFork, 3)

	server.settleValidatedBlock(server.blockchainClient, parent.Hash(), errors.NewBlockInvalidError("invalid"))

	priority, ok := server.blockPriorityQueue.PriorityOf(child)
	require.True(t, ok)
	require.Equal(t, PriorityDeepFork, priority)
}

// TestRunOncePerBlock_NotValidatingOnceDone pins that a block stops counting as validating as soon
// as its result is published, not after the grace window. The window only serves duplicate
// arrivals; holding children through it delayed every worker-validated parent's child by it.
func TestRunOncePerBlock_NotValidatingOnceDone(t *testing.T) {
	server, hashes := newParentInFlightServer(t)
	bv := server.blockValidation
	parent := hashes[0]

	type wake struct {
		at         time.Time
		validating bool
	}

	wakes := make(chan wake, 1)
	bv.SetValidationDoneHooks(nil, func() { wakes <- wake{at: time.Now(), validating: bv.IsValidating(parent)} })

	start := time.Now()
	require.NoError(t, bv.runOncePerBlock(parent, nil, func(*ValidateBlockOptions) error { return nil }))

	select {
	case w := <-wakes:
		require.False(t, w.validating, "a finished validation must not hold children")
		require.Less(t, w.at.Sub(start), validationResultGrace, "the queue must be woken when the result is published, not after the grace window")
	case <-time.After(2 * time.Second):
		t.Fatal("the validation-done hook never ran")
	}
}

// TestRunOncePerBlock_HoldsChildrenUntilSettled pins that a finished parent keeps counting as
// validating until the settle hook has given its children their class. Otherwise an unrelated
// wake-up in between starts a child with the fork class it got while the parent was unknown.
func TestRunOncePerBlock_HoldsChildrenUntilSettled(t *testing.T) {
	server, hashes := newParentInFlightServer(t)
	bv := server.blockValidation
	parent := hashes[0]

	settling := make(chan struct{})
	release := make(chan struct{})
	woken := make(chan struct{})

	bv.SetValidationDoneHooks(func(*chainhash.Hash, error) {
		close(settling)
		<-release
	}, func() { close(woken) })

	require.NoError(t, bv.runOncePerBlock(parent, nil, func(*ValidateBlockOptions) error { return nil }))

	<-settling
	require.True(t, bv.IsValidating(parent), "the parent must hold its children while they are being classified")

	select {
	case <-woken:
		t.Fatal("the queue was woken before the children were classified")
	default:
	}

	close(release)

	select {
	case <-woken:
		require.False(t, bv.IsValidating(parent))
	case <-time.After(2 * time.Second):
		t.Fatal("the queue was never woken")
	}
}

// TestRunOncePerBlock_SettlePanicReleasesChildren pins that a panic in the settle hook neither
// takes the process down nor leaves children held: the parent stops counting as validating and
// the queue is still woken.
func TestRunOncePerBlock_SettlePanicReleasesChildren(t *testing.T) {
	server, hashes := newParentInFlightServer(t)
	bv := server.blockValidation
	parent := hashes[0]

	woken := make(chan struct{})
	bv.SetValidationDoneHooks(func(*chainhash.Hash, error) { panic("settle failed") }, func() { close(woken) })

	require.NoError(t, bv.runOncePerBlock(parent, nil, func(*ValidateBlockOptions) error { return nil }))

	select {
	case <-woken:
		require.False(t, bv.IsValidating(parent))
	case <-time.After(2 * time.Second):
		t.Fatal("the queue was never woken after the settle hook panicked")
	}
}

// TestRunOncePerBlock_SlowSettleOutlastsGrace pins that a settle hook slower than the grace
// window still holds the children: the entry must not be dropped before the hook is done.
func TestRunOncePerBlock_SlowSettleOutlastsGrace(t *testing.T) {
	server, hashes := newParentInFlightServer(t)
	bv := server.blockValidation
	parent := hashes[0]

	release := make(chan struct{})
	bv.SetValidationDoneHooks(func(*chainhash.Hash, error) { <-release }, nil)

	require.NoError(t, bv.runOncePerBlock(parent, nil, func(*ValidateBlockOptions) error { return nil }))

	time.Sleep(3 * validationResultGrace)
	require.True(t, bv.IsValidating(parent), "the parent must hold its children until the settle hook is done")

	close(release)
	require.Eventually(t, func() bool { return !bv.IsValidating(parent) }, 2*time.Second, time.Millisecond)
}

// fetchMarked reports whether hash holds a fetching mark, ignoring whether it is queued.
func fetchMarked(pq *BlockPriorityQueue, hash *chainhash.Hash) bool {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	_, ok := pq.fetching[*hash]

	return ok
}

// TestClaimFetchMark_DuplicateOfInFlightBlockReleased pins that a fetch of a block that is already
// queued, holding a slot or validated outside the queue gives up its mark straight away. Once that
// block is stored, its children must not wait on the duplicate's fetch.
func TestClaimFetchMark_DuplicateOfInFlightBlockReleased(t *testing.T) {
	initPrometheusMetrics()

	tests := []struct {
		name  string
		setup func(server *Server, hash *chainhash.Hash)
	}{
		{"queued", func(server *Server, hash *chainhash.Hash) {
			server.blockPriorityQueue.Add(processBlockFound{hash: hash}, PriorityChainExtending, 2)
		}},
		{"processing", func(server *Server, hash *chainhash.Hash) {
			require.True(t, server.forkManager.StartProcessingBlock(hash, PriorityChainExtending))
		}},
		{"validating outside the queue", func(server *Server, hash *chainhash.Hash) {
			server.wireValidationInFlight()
			server.blockValidation.blocksCurrentlyValidating.Set(*hash, &validationResult{done: make(chan struct{}), settled: make(chan struct{})})
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server, hashes := newParentInFlightServer(t)
			parent := hashes[0]

			tt.setup(server, parent)

			release := server.claimFetchMark(parent)
			defer release()

			require.False(t, fetchMarked(server.blockPriorityQueue, parent), "a duplicate fetch must not hold a mark")
		})
	}
}

// TestClaimFetchMark_FirstFetcherQueuesDuringDuplicateCheck pins oskarszoon's interleaving: a
// duplicate sees the block not in flight, then the first fetcher marks, queues and releases it.
// Checking before marking let the duplicate take a mark after that and keep it, so once the block
// was stored its children waited on the duplicate's fetch. No mark exists at the start, so the
// duplicate's in-flight check reaches the validating check, where the first fetcher runs. With the
// mark taken first, the duplicate holds it by then and the first fetcher queues without one.
func TestClaimFetchMark_FirstFetcherQueuesDuringDuplicateCheck(t *testing.T) {
	initPrometheusMetrics()
	server, hashes := newParentInFlightServer(t)
	parent := hashes[0]
	pq := server.blockPriorityQueue

	var (
		firstFetcherRan       bool
		duplicateMarkedFirst  bool
		firstFetcherHeldAMark bool
	)

	server.forkManager.SetValidatingCheck(func(hash *chainhash.Hash) bool {
		if !hash.IsEqual(parent) || firstFetcherRan {
			return false
		}

		firstFetcherRan = true
		duplicateMarkedFirst = fetchMarked(pq, parent)

		firstFetcherHeldAMark = pq.MarkFetching(parent)
		pq.Add(processBlockFound{hash: parent}, PriorityChainExtending, 2)

		if firstFetcherHeldAMark {
			pq.UnmarkFetching(parent)
		}

		return false
	})

	duplicate := server.claimFetchMark(parent)
	defer duplicate()

	require.True(t, firstFetcherRan, "the first fetcher must run inside the duplicate's in-flight check")

	// The duplicate holds a mark either way; it must be one taken before the first fetcher queued.
	require.True(t, fetchMarked(pq, parent))
	require.True(t, duplicateMarkedFirst, "the duplicate kept a mark taken after the first fetcher queued the block and let go of its own")
	require.False(t, firstFetcherHeldAMark)
}

// TestClaimFetchMark_FreeBlockHeldUntilReleased pins the normal case and that releasing twice is
// harmless.
func TestClaimFetchMark_FreeBlockHeldUntilReleased(t *testing.T) {
	initPrometheusMetrics()
	server, hashes := newParentInFlightServer(t)
	parent := hashes[0]

	release := server.claimFetchMark(parent)
	require.True(t, fetchMarked(server.blockPriorityQueue, parent))

	release()
	release()
	require.False(t, fetchMarked(server.blockPriorityQueue, parent))
}

// TestSettleValidatedBlock_ClearsFetchingMark pins that a successful validation on any path drops
// the block's fetching mark, so a duplicate still fetching it no longer holds its children.
func TestSettleValidatedBlock_ClearsFetchingMark(t *testing.T) {
	initPrometheusMetrics()
	server, blocks := newParentInFlightServerWithBlocks(t)
	parent, child := blocks[2], blocks[3].Hash()

	require.True(t, server.blockPriorityQueue.MarkFetching(parent.Hash()))
	server.blockPriorityQueue.Add(processBlockFound{hash: child, parentHash: parent.Hash()}, PriorityChainExtending, 3)
	require.NoError(t, server.blockchainClient.AddBlock(context.Background(), parent, "test-peer"))

	server.settleValidatedBlock(server.blockchainClient, parent.Hash(), nil)

	require.False(t, fetchMarked(server.blockPriorityQueue, parent.Hash()))

	taken, status := server.blockPriorityQueue.Get(context.Background(), server.forkManager)
	require.Equal(t, GetOK, status, "the child must no longer wait on the duplicate fetch")
	require.Equal(t, child, taken.hash)
}

// TestWireValidationInFlight_SuccessClearsDuplicateFetchMark: end to end through the installed
// validation-done hook. A duplicate fetch still holds the parent's mark when the parent validates
// outside the queue; the child must be handed out once the validation is done. The hook is the
// only production path that drops that mark.
func TestWireValidationInFlight_SuccessClearsDuplicateFetchMark(t *testing.T) {
	initPrometheusMetrics()
	server, blocks := newParentInFlightServerWithBlocks(t)
	parent, child := blocks[2], blocks[3].Hash()
	server.wireValidationInFlight()

	require.True(t, server.blockPriorityQueue.MarkFetching(parent.Hash()), "the duplicate fetch holds the mark")
	server.blockPriorityQueue.Add(processBlockFound{hash: child, parentHash: parent.Hash()}, PriorityChainExtending, 3)

	require.NoError(t, server.blockValidation.runOncePerBlock(parent.Hash(), nil, func(*ValidateBlockOptions) error {
		return server.blockchainClient.AddBlock(context.Background(), parent, "test-peer")
	}))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	taken, status := server.blockPriorityQueue.WaitForBlock(ctx, server.forkManager)
	require.Equal(t, GetOK, status, "the child must not wait on the duplicate fetch once its parent validated")
	require.Equal(t, child, taken.hash)
	require.False(t, fetchMarked(server.blockPriorityQueue, parent.Hash()))
}

// TestSettleValidatedBlock_FailedValidationKeepsFetchingMark pins that a failed validation leaves
// another fetch's mark alone: that fetch may bring a good copy.
func TestSettleValidatedBlock_FailedValidationKeepsFetchingMark(t *testing.T) {
	initPrometheusMetrics()
	server, hashes := newParentInFlightServer(t)
	parent := hashes[0]

	require.True(t, server.blockPriorityQueue.MarkFetching(parent))

	server.settleValidatedBlock(server.blockchainClient, parent, errors.NewBlockInvalidError("invalid"))

	require.True(t, fetchMarked(server.blockPriorityQueue, parent))
}
