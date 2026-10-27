package subtreeprocessor

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

func heldQueue(maxItems, maxHeld int64, held *atomic.Int64) *LockFreeQueue {
	q := NewLockFreeQueueWithLimit(maxItems)
	q.setHeldLimit(maxHeld, held.Load)

	return q
}

func TestEnqueueBatchIfRoom_RefusesWhenHeldPlusQueuedCrossesLimit(t *testing.T) {
	var held atomic.Int64

	held.Store(90)

	q := heldQueue(0, 100, &held)

	require.True(t, q.enqueueBatchIfRoom(mkNodes(10), mkInpoints(10)), "a batch landing exactly on the limit is admitted")
	require.Equal(t, int64(10), q.length())

	require.False(t, q.enqueueBatchIfRoom(mkNodes(1), mkInpoints(1)), "one more crosses the limit")
	require.Equal(t, int64(10), q.length(), "a refused batch leaves no reservation behind")

	batch, found := q.dequeueBatch(0)
	require.True(t, found)
	require.Len(t, batch.nodes, 10)

	_, found = q.dequeueBatch(0)
	require.False(t, found, "the refused batch was never published")
}

func TestEnqueueBatchIfRoom_AdmitsAgainOnceHeldCountFalls(t *testing.T) {
	var held atomic.Int64

	held.Store(100)

	q := heldQueue(0, 100, &held)

	require.False(t, q.enqueueBatchIfRoom(mkNodes(1), mkInpoints(1)), "full from transactions already in subtrees")

	held.Store(0)

	require.True(t, q.enqueueBatchIfRoom(mkNodes(1), mkInpoints(1)), "a block cleared the subtrees")
}

func TestEnqueueBatchIfRoom_HeldLimitDisabledIgnoresHeldCount(t *testing.T) {
	var held atomic.Int64

	held.Store(1 << 40)

	q := heldQueue(0, 0, &held)

	require.True(t, q.enqueueBatchIfRoom(mkNodes(5), mkInpoints(5)))
}

func TestEnqueueBatchIfRoom_BothBoundsApply(t *testing.T) {
	var held atomic.Int64

	q := heldQueue(10, 100, &held)

	require.True(t, q.enqueueBatchIfRoom(mkNodes(10), mkInpoints(10)))
	require.False(t, q.enqueueBatchIfRoom(mkNodes(1), mkInpoints(1)), "the queue cap applies although held room remains")

	_, found := q.dequeueBatch(0)
	require.True(t, found)

	q.settle(10) // the dispatcher counted the batch into held

	held.Store(95)

	require.False(t, q.enqueueBatchIfRoom(mkNodes(6), mkInpoints(6)), "the held limit applies although the queue is empty")
	require.True(t, q.enqueueBatchIfRoom(mkNodes(5), mkInpoints(5)))
}

func TestEnqueueBatchIfRoom_OversizeBatchAdmittedAloneUnlessAlreadyFull(t *testing.T) {
	var held atomic.Int64

	q := heldQueue(0, 10, &held)

	require.True(t, q.enqueueBatchIfRoom(mkNodes(50), mkInpoints(50)), "a batch bigger than the whole limit is admitted alone onto an empty queue")

	require.False(t, q.enqueueBatchIfRoom(mkNodes(50), mkInpoints(50)), "never a second one while the first is queued")

	q2 := heldQueue(0, 10, &held)

	held.Store(10)

	require.False(t, q2.enqueueBatchIfRoom(mkNodes(50), mkInpoints(50)), "an already full node refuses even an oversize batch")
}

func TestEnqueueBatchIfRoom_ConcurrentProducersNeverExceedHeldLimit(t *testing.T) {
	var held atomic.Int64

	q := heldQueue(0, 1000, &held)

	var admitted atomic.Int64

	done := make(chan struct{})

	for g := 0; g < 8; g++ {
		go func() {
			defer func() { done <- struct{}{} }()

			for i := 0; i < 500; i++ {
				if q.enqueueBatchIfRoom(mkNodes(1), mkInpoints(1)) {
					admitted.Add(1)
				}
			}
		}()
	}

	for g := 0; g < 8; g++ {
		<-done
	}

	require.Equal(t, int64(1000), admitted.Load(), "exactly the limit is admitted, no more, no fewer")
	require.Equal(t, int64(1000), q.length())
}

func Test_normalizeMaxTransactionsInMemory(t *testing.T) {
	logger := ulogger.TestLogger{}
	sendBatchSize := 100
	floor := int64(maxBatchesPerIteration)*int64(sendBatchSize) + 1

	require.Equal(t, int64(0), normalizeMaxTransactionsInMemory(logger, 0, sendBatchSize), "0 disables the limit")
	require.Equal(t, floor, normalizeMaxTransactionsInMemory(logger, 1, sendBatchSize), "a limit below one drain pass plus the coinbase placeholder is clamped up")
	require.Equal(t, floor, normalizeMaxTransactionsInMemory(logger, uint64(floor), sendBatchSize))
	require.Equal(t, floor+1, normalizeMaxTransactionsInMemory(logger, uint64(floor+1), sendBatchSize))
	require.Equal(t, int64(1<<62), normalizeMaxTransactionsInMemory(logger, 1<<62, sendBatchSize))
	require.Equal(t, int64(math.MaxInt64), normalizeMaxTransactionsInMemory(logger, ^uint64(0), sendBatchSize), "a value beyond int64 saturates rather than wrapping negative")
}

func newLimitedSubtreeProcessor(t *testing.T, ctx context.Context, limit uint64) *SubtreeProcessor {
	t.Helper()

	return newLimitedSubtreeProcessorWithBatchSize(t, ctx, limit, -1)
}

// newLimitedSubtreeProcessorWithBatchSize builds the processor with the given send batch size, or
// the test default when sendBatchSize is negative.
func newLimitedSubtreeProcessorWithBatchSize(t *testing.T, ctx context.Context, limit uint64, sendBatchSize int) *SubtreeProcessor {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockAssembly.InitialMerkleItemsPerSubtree = 4
	tSettings.BlockAssembly.MaxTransactionsInMemory = limit

	if sendBatchSize >= 0 {
		tSettings.BlockAssembly.SendBatchSize = sendBatchSize
	}

	newSubtreeChan := make(chan NewSubtreeRequest, 16)
	go func() {
		for req := range newSubtreeChan {
			if req.ErrChan != nil {
				req.ErrChan <- nil
			}
		}
	}()
	t.Cleanup(func() { close(newSubtreeChan) })

	stp, err := NewSubtreeProcessor(ctx, ulogger.TestLogger{}, tSettings, blob_memory.New(), new(blockchain.Mock), new(utxo.MockUtxostore), newSubtreeChan)
	require.NoError(t, err)

	return stp
}

func TestSubtreeProcessor_LimitDisabledByDefault(t *testing.T) {
	stp := newLimitedSubtreeProcessor(t, context.Background(), 0)

	require.Equal(t, int64(0), stp.MaxTransactionsInMemory())

	stp.txCount.Store(1 << 40)

	require.True(t, stp.AddBatchIfRoom(mkNodes(10), mkInpoints(10)))
}

func TestSubtreeProcessor_LimitCountsTransactionsAlreadyInSubtrees(t *testing.T) {
	stp := newLimitedSubtreeProcessor(t, context.Background(), 1)
	limit := normalizeMaxTransactionsInMemory(ulogger.TestLogger{}, 1, stp.settings.BlockAssembly.SendBatchSize)

	require.Equal(t, limit, stp.MaxTransactionsInMemory(), "the processor enforces the normalized limit, not the raw setting")

	stp.txCount.Store(uint64(limit) - 5) // nolint:gosec

	require.False(t, stp.AddBatchIfRoom(mkNodes(6), mkInpoints(6)), "subtree-held transactions count against the limit")
	require.Equal(t, int64(0), stp.QueueLength(), "a refusal leaves nothing queued")

	require.True(t, stp.AddBatchIfRoom(mkNodes(5), mkInpoints(5)))
	require.False(t, stp.AddBatchIfRoom(mkNodes(1), mkInpoints(1)), "queued transactions count too")

	stp.txCount.Store(0)
	stp.queue.queueLength.Store(0)

	require.True(t, stp.AddBatchIfRoom(mkNodes(1), mkInpoints(1)), "room returns once the holding drops, as after a block")
}

func distinctNodes(n int, seed string) ([]subtree.Node, []*subtree.TxInpoints) {
	nodes := make([]subtree.Node, n)

	for i := range nodes {
		nodes[i] = subtree.Node{Hash: chainhash.HashH([]byte(seed + string(rune('a'+i%26)) + string(rune('A'+i/26)))), Fee: 1, SizeInBytes: 100}
	}

	return nodes, mkInpoints(n)
}

func TestEnqueueBatchInBlockIfRoom_IgnoresHeldLimitButNotQueueBound(t *testing.T) {
	var held atomic.Int64

	held.Store(100)

	q := heldQueue(0, 100, &held)

	require.False(t, q.enqueueBatchIfRoom(mkNodes(1), mkInpoints(1)), "mempool ingest is refused at the limit")
	require.True(t, q.enqueueBatchInBlockIfRoom(mkNodes(5), mkInpoints(5)), "a block transaction is admitted past the limit")
	require.Equal(t, int64(5), q.length())
	require.Equal(t, int64(105), q.heldCount(), "an admitted block transaction counts toward the total")
	require.False(t, q.enqueueBatchIfRoom(mkNodes(1), mkInpoints(1)), "so mempool ingest stays refused")

	bounded := heldQueue(10, 100, &held)

	require.True(t, bounded.enqueueBatchInBlockIfRoom(mkNodes(10), mkInpoints(10)))
	require.False(t, bounded.enqueueBatchInBlockIfRoom(mkNodes(1), mkInpoints(1)), "the ingest queue bound still applies to a block transaction")
	require.Equal(t, int64(10), bounded.length(), "a refused batch leaves no reservation behind")
}

func TestHeldTotal_CountsDequeuedItemsUntilSettled(t *testing.T) {
	var held atomic.Int64

	held.Store(90)

	q := heldQueue(0, 100, &held)

	require.True(t, q.enqueueBatchIfRoom(mkNodes(10), mkInpoints(10)))
	require.Equal(t, int64(100), q.heldCount())

	batch, found := q.dequeueBatch(0)
	require.True(t, found)
	require.Len(t, batch.nodes, 10)

	require.Equal(t, int64(0), q.length(), "the queue is empty")
	require.Equal(t, int64(100), q.heldCount(), "dequeued items stay counted until the subtree count includes them, so the total does not dip")
	require.False(t, q.enqueueBatchIfRoom(mkNodes(1), mkInpoints(1)), "a producer landing in that window is refused")
	require.True(t, q.heldWouldExceed(1))

	held.Add(10) // the dispatcher adds the batch to the subtree count
	q.settle(10)

	require.Equal(t, int64(100), q.heldCount(), "settling after counting leaves the total unchanged")
	require.Equal(t, int64(0), q.unsettled.Load())
	require.False(t, q.enqueueBatchIfRoom(mkNodes(1), mkInpoints(1)), "still full")

	held.Store(0)

	require.True(t, q.enqueueBatchIfRoom(mkNodes(1), mkInpoints(1)), "room returns once a block clears the subtrees")
}

func TestHeldWouldExceed_FalseWithoutLimit(t *testing.T) {
	var held atomic.Int64

	held.Store(1 << 40)

	q := heldQueue(0, 0, &held)

	require.False(t, q.heldWouldExceed(1<<20))
}

func Test_normalizeMaxTransactionsInMemory_FloorIsAtLeastTwo(t *testing.T) {
	logger := ulogger.TestLogger{}

	require.Equal(t, int64(2), normalizeMaxTransactionsInMemory(logger, 1, 0), "a send batch size of 0 must not leave a floor of 1, which the coinbase placeholder already fills")
	require.Equal(t, int64(2), normalizeMaxTransactionsInMemory(logger, 2, 0))
	require.Equal(t, int64(3), normalizeMaxTransactionsInMemory(logger, 3, 0))
}

func TestSubtreeProcessor_ZeroSendBatchSizeStillAdmitsABatch(t *testing.T) {
	stp := newLimitedSubtreeProcessorWithBatchSize(t, context.Background(), 1, 0)

	require.Equal(t, int64(2), stp.MaxTransactionsInMemory())

	nodes, inpoints := distinctNodes(1, "zero-batch")

	require.True(t, stp.AddBatchIfRoom(nodes, inpoints), "the placeholder plus one transaction fits the floor, so ingest is not wedged")
}

func TestSubtreeProcessor_HeldAccessors(t *testing.T) {
	stp := newLimitedSubtreeProcessor(t, context.Background(), 0)

	require.False(t, stp.HeldLimitExceededBy(1<<30), "no limit, nothing exceeds it")

	stp.txCount.Store(7)

	nodes, inpoints := distinctNodes(3, "held-accessors")
	require.True(t, stp.AddBatchIfRoom(nodes, inpoints))
	require.Equal(t, int64(10), stp.HeldCount(), "subtree-held plus queued")
}

func TestSetTxCountFromSubtrees_NeverShowsAPartialTotal(t *testing.T) {
	stp := newTestProcessorNoStart(t)

	expected := stp.txCount.Load()
	stp.setTxCountFromSubtrees()
	expected = stp.txCount.Load()
	require.NotZero(t, expected, "the first subtree carries the coinbase placeholder")

	var (
		stop sync.WaitGroup
		done atomic.Bool
		low  atomic.Uint64
	)

	low.Store(math.MaxUint64)

	stop.Add(1)

	go func() {
		defer stop.Done()

		for !done.Load() {
			if v := stp.txCount.Load(); v < low.Load() {
				low.Store(v)
			}
		}
	}()

	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		stp.setTxCountFromSubtrees()
	}

	done.Store(true)
	stop.Wait()

	require.Equal(t, expected, low.Load(), "a producer reading the count for the in-memory limit must never see a total below the real one")
}

func TestSubtreeProcessor_DequeueDuringBlockMovementSettlesWhatItTook(t *testing.T) {
	stp := newTestProcessorNoStart(t)

	enqueueAt := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	stp.queue.clock = fixedClock{t: enqueueAt}
	stp.clock = fixedClock{t: enqueueAt.Add(time.Millisecond)}

	nodes, inpoints := distinctNodes(5, "drain-settle")
	stp.queue.enqueueBatch(nodes, inpoints)

	require.NoError(t, stp.dequeueDuringBlockMovement(nil, nil, nil, true))

	require.Equal(t, int64(0), stp.queue.length())
	require.Equal(t, int64(0), stp.queue.unsettled.Load(), "a drain that took items must release them, or the limit refuses for good")
}

func TestSubtreeProcessor_DispatcherSettlesWhatItDequeued(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stp := newLimitedSubtreeProcessor(t, ctx, 0)
	stp.Start(ctx)

	before := stp.txCount.Load()

	nodes, inpoints := distinctNodes(40, "dispatch-settle")
	require.True(t, stp.AddBatchIfRoom(nodes, inpoints))

	require.Eventually(t, func() bool {
		return stp.queue.length() == 0 && stp.txCount.Load() >= before+40
	}, 5*time.Second, 5*time.Millisecond, "the dispatcher places the batch in a subtree")

	require.Eventually(t, func() bool { return stp.queue.unsettled.Load() == 0 }, 5*time.Second, 5*time.Millisecond,
		"the dispatcher must release what it dequeued once the subtree count includes it")

	require.Equal(t, int64(stp.txCount.Load()), stp.HeldCount(), "with nothing queued or unsettled the held total is the subtree count") // nolint:gosec
}
