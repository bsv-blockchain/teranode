package blockvalidation

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
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

	return server, []*chainhash.Hash{blocks[2].Hash(), blocks[3].Hash()}
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
