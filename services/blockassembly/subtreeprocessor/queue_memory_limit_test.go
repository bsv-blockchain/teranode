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

func TestEnqueueBatchBounded_RefusesWhenHeldPlusQueuedCrossesLimit(t *testing.T) {
	var held atomic.Int64

	held.Store(90)

	q := heldQueue(0, 100, &held)

	require.True(t, q.enqueueBatchBounded(mkNodes(10), mkInpoints(10), false) == Admitted, "a batch landing exactly on the limit is admitted")
	require.Equal(t, int64(10), q.length())

	require.False(t, q.enqueueBatchBounded(mkNodes(1), mkInpoints(1), false) == Admitted, "one more crosses the limit")
	require.Equal(t, int64(10), q.length(), "a refused batch leaves no reservation behind")

	batch, found := q.dequeueBatch(0)
	require.True(t, found)
	require.Len(t, batch.nodes, 10)

	_, found = q.dequeueBatch(0)
	require.False(t, found, "the refused batch was never published")
}

func TestEnqueueBatchBounded_AdmitsAgainOnceHeldCountFalls(t *testing.T) {
	var held atomic.Int64

	held.Store(100)

	q := heldQueue(0, 100, &held)

	require.False(t, q.enqueueBatchBounded(mkNodes(1), mkInpoints(1), false) == Admitted, "full from transactions already in subtrees")

	held.Store(0)

	require.True(t, q.enqueueBatchBounded(mkNodes(1), mkInpoints(1), false) == Admitted, "a block cleared the subtrees")
}

func TestEnqueueBatchBounded_HeldLimitDisabledIgnoresHeldCount(t *testing.T) {
	var held atomic.Int64

	held.Store(1 << 40)

	q := heldQueue(0, 0, &held)

	require.True(t, q.enqueueBatchBounded(mkNodes(5), mkInpoints(5), false) == Admitted)
}

func TestEnqueueBatchBounded_BothBoundsApply(t *testing.T) {
	var held atomic.Int64

	q := heldQueue(10, 100, &held)

	require.True(t, q.enqueueBatchBounded(mkNodes(10), mkInpoints(10), false) == Admitted)
	require.False(t, q.enqueueBatchBounded(mkNodes(1), mkInpoints(1), false) == Admitted, "the queue cap applies although held room remains")

	_, found := q.dequeueBatch(0)
	require.True(t, found)

	q.settle(10) // the dispatcher counted the batch into held

	held.Store(95)

	require.False(t, q.enqueueBatchBounded(mkNodes(6), mkInpoints(6), false) == Admitted, "the held limit applies although the queue is empty")
	require.True(t, q.enqueueBatchBounded(mkNodes(5), mkInpoints(5), false) == Admitted)
}

func TestEnqueueBatchBounded_OversizeBatchAdmittedAloneUnlessAlreadyFull(t *testing.T) {
	var held atomic.Int64

	q := heldQueue(0, 10, &held)

	require.True(t, q.enqueueBatchBounded(mkNodes(50), mkInpoints(50), false) == Admitted, "a batch bigger than the whole limit is admitted alone onto an empty queue")

	require.False(t, q.enqueueBatchBounded(mkNodes(50), mkInpoints(50), false) == Admitted, "never a second one while the first is queued")

	q2 := heldQueue(0, 10, &held)

	held.Store(10)

	require.False(t, q2.enqueueBatchBounded(mkNodes(50), mkInpoints(50), false) == Admitted, "an already full node refuses even an oversize batch")
}

func TestEnqueueBatchBounded_ConcurrentProducersNeverExceedHeldLimit(t *testing.T) {
	var held atomic.Int64

	q := heldQueue(0, 1000, &held)

	var admitted atomic.Int64

	done := make(chan struct{})

	for g := 0; g < 8; g++ {
		go func() {
			defer func() { done <- struct{}{} }()

			for i := 0; i < 500; i++ {
				if q.enqueueBatchBounded(mkNodes(1), mkInpoints(1), false) == Admitted {
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

	require.True(t, stp.AddBatchBounded(mkNodes(10), mkInpoints(10), false) == Admitted)
}

func TestSubtreeProcessor_LimitCountsTransactionsAlreadyInSubtrees(t *testing.T) {
	stp := newLimitedSubtreeProcessor(t, context.Background(), 1)
	limit := normalizeMaxTransactionsInMemory(ulogger.TestLogger{}, 1, stp.settings.BlockAssembly.SendBatchSize)

	require.Equal(t, limit, stp.MaxTransactionsInMemory(), "the processor enforces the normalized limit, not the raw setting")

	stp.txCount.Store(uint64(limit) - 5) // nolint:gosec

	require.False(t, stp.AddBatchBounded(mkNodes(6), mkInpoints(6), false) == Admitted, "subtree-held transactions count against the limit")
	require.Equal(t, int64(0), stp.QueueLength(), "a refusal leaves nothing queued")

	require.True(t, stp.AddBatchBounded(mkNodes(5), mkInpoints(5), false) == Admitted)
	require.False(t, stp.AddBatchBounded(mkNodes(1), mkInpoints(1), false) == Admitted, "queued transactions count too")

	stp.txCount.Store(0)
	stp.queue.queueLength.Store(0)

	require.True(t, stp.AddBatchBounded(mkNodes(1), mkInpoints(1), false) == Admitted, "room returns once the holding drops, as after a block")
}

func distinctNodes(n int, seed string) ([]subtree.Node, []*subtree.TxInpoints) {
	nodes := make([]subtree.Node, n)

	for i := range nodes {
		nodes[i] = subtree.Node{Hash: chainhash.HashH([]byte(seed + string(rune('a'+i%26)) + string(rune('A'+i/26)))), Fee: 1, SizeInBytes: 100}
	}

	return nodes, mkInpoints(n)
}

func TestEnqueueBatchBounded_InBlock_IgnoresHeldLimitButNotQueueBound(t *testing.T) {
	var held atomic.Int64

	held.Store(100)

	q := heldQueue(0, 100, &held)

	require.False(t, q.enqueueBatchBounded(mkNodes(1), mkInpoints(1), false) == Admitted, "mempool ingest is refused at the limit")
	require.True(t, q.enqueueBatchBounded(mkNodes(5), mkInpoints(5), true) == Admitted, "a block transaction is admitted past the limit")
	require.Equal(t, int64(5), q.length())
	require.Equal(t, int64(105), q.heldCount(), "an admitted block transaction counts toward the total")
	require.False(t, q.enqueueBatchBounded(mkNodes(1), mkInpoints(1), false) == Admitted, "so mempool ingest stays refused")

	bounded := heldQueue(10, 100, &held)

	require.True(t, bounded.enqueueBatchBounded(mkNodes(10), mkInpoints(10), true) == Admitted)
	require.False(t, bounded.enqueueBatchBounded(mkNodes(1), mkInpoints(1), true) == Admitted, "the ingest queue bound still applies to a block transaction")
	require.Equal(t, int64(10), bounded.length(), "a refused batch leaves no reservation behind")
}

func TestEnqueueBatchBounded_UnsettledExcessIsTheQueuesRefusal(t *testing.T) {
	var held atomic.Int64

	held.Store(10)

	q := heldQueue(0, 100, &held)

	require.Equal(t, Admitted, q.enqueueBatchBounded(mkNodes(90), mkInpoints(90), false))

	batch, found := q.dequeueBatch(0)
	require.True(t, found)
	require.Len(t, batch.nodes, 90)

	require.Equal(t, RefusedQueue, q.enqueueBatchBounded(mkNodes(10), mkInpoints(10), false),
		"a drain chunk not yet settled must not make a producer's refusal final while a block has freed room")

	held.Store(100)

	require.Equal(t, RefusedHeld, q.enqueueBatchBounded(mkNodes(1), mkInpoints(1), false),
		"placed transactions alone over the limit are the held bound")

	q.settle(90)

	require.Equal(t, RefusedHeld, q.enqueueBatchBounded(mkNodes(1), mkInpoints(1), false))
}

func TestHeldTotal_CountsDequeuedItemsUntilSettled(t *testing.T) {
	var held atomic.Int64

	held.Store(90)

	q := heldQueue(0, 100, &held)

	require.True(t, q.enqueueBatchBounded(mkNodes(10), mkInpoints(10), false) == Admitted)
	require.Equal(t, int64(100), q.heldCount())

	batch, found := q.dequeueBatch(0)
	require.True(t, found)
	require.Len(t, batch.nodes, 10)

	require.Equal(t, int64(0), q.length(), "the queue is empty")
	require.Equal(t, int64(100), q.heldCount(), "dequeued items stay counted until the subtree count includes them, so the total does not dip")
	require.False(t, q.enqueueBatchBounded(mkNodes(1), mkInpoints(1), false) == Admitted, "a producer landing in that window is refused")
	require.Equal(t, RefusedQueue, q.enqueueBatchBounded(mkNodes(1), mkInpoints(1), false),
		"the excess is unsettled items a drain pass is about to place, not what a block must free")

	held.Add(10) // the dispatcher adds the batch to the subtree count
	q.settle(10)

	require.Equal(t, int64(100), q.heldCount(), "settling after counting leaves the total unchanged")
	require.Equal(t, int64(0), q.unsettled.Load())
	require.False(t, q.enqueueBatchBounded(mkNodes(1), mkInpoints(1), false) == Admitted, "still full")

	held.Store(0)

	require.True(t, q.enqueueBatchBounded(mkNodes(1), mkInpoints(1), false) == Admitted, "room returns once a block clears the subtrees")
}

func TestEnqueueBatchBounded_NeverRefusesByHeldWithoutLimit(t *testing.T) {
	var held atomic.Int64

	held.Store(1 << 40)

	q := heldQueue(0, 0, &held)

	require.Equal(t, Admitted, q.enqueueBatchBounded(mkNodes(1<<10), mkInpoints(1<<10), false))
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

	require.True(t, stp.AddBatchBounded(nodes, inpoints, false) == Admitted, "the placeholder plus one transaction fits the floor, so ingest is not wedged")
}

func TestSubtreeProcessor_HeldAccessors(t *testing.T) {
	stp := newLimitedSubtreeProcessor(t, context.Background(), 0)

	stp.txCount.Store(7)

	nodes, inpoints := distinctNodes(3, "held-accessors")
	require.True(t, stp.AddBatchBounded(nodes, inpoints, false) == Admitted)
	require.Equal(t, int64(10), stp.HeldCount(), "subtree-held plus queued")
}

func TestSetTxCountFromSubtrees_NeverShowsAPartialTotal(t *testing.T) {
	stp := newTestProcessorNoStart(t)

	stp.setTxCountFromSubtrees()
	expected := stp.txCount.Load()
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
	require.True(t, stp.AddBatchBounded(nodes, inpoints, false) == Admitted)

	require.Eventually(t, func() bool {
		return stp.queue.length() == 0 && stp.txCount.Load() >= before+40
	}, 5*time.Second, 5*time.Millisecond, "the dispatcher places the batch in a subtree")

	require.Eventually(t, func() bool { return stp.queue.unsettled.Load() == 0 }, 5*time.Second, 5*time.Millisecond,
		"the dispatcher must release what it dequeued once the subtree count includes it")

	require.Equal(t, int64(stp.txCount.Load()), stp.HeldCount(), "with nothing queued or unsettled the held total is the subtree count") // nolint:gosec
}

func enqueueDistinctBatches(t *testing.T, stp *SubtreeProcessor, batches, perBatch int) {
	t.Helper()

	enqueueAt := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	stp.queue.clock = fixedClock{t: enqueueAt}
	stp.clock = fixedClock{t: enqueueAt.Add(time.Millisecond)}

	for i := 0; i < batches; i++ {
		nodes, inpoints := distinctNodes(perBatch, "drain-chunk-"+string(rune('a'+i)))
		stp.queue.enqueueBatch(nodes, inpoints)
	}
}

func TestSubtreeProcessor_DrainKeepsTheHeldTotalAtEveryChunkBoundary(t *testing.T) {
	stp := newTestProcessorNoStart(t)
	stp.queue.maxHeld = 1 << 20

	const (
		batches  = 5
		perBatch = 4
	)

	enqueueDistinctBatches(t, stp, batches, perBatch)

	startHeld := stp.HeldCount()
	chunks := 0

	err := stp.forEachDrainChunk(perBatch, func(chunk []*TxBatch) (int, error) {
		require.GreaterOrEqual(t, stp.HeldCount(), startHeld,
			"the transactions an earlier chunk placed must still count, or producers are admitted past the limit")

		placed := 0

		for _, batch := range chunk {
			for i, node := range batch.nodes {
				require.NoError(t, stp.addNode(node, batch.txInpoints[i], true))

				placed++
			}
		}

		chunks++

		return placed, nil
	})

	require.NoError(t, err)
	require.Equal(t, batches, chunks, "precondition: the drain crossed chunk boundaries")
	require.Equal(t, int64(0), stp.queue.length())
	require.Equal(t, int64(0), stp.queue.unsettled.Load())
	require.Equal(t, int64(len(collectSubtreeHashes(stp))), int64(stp.txCount.Load()), "the count includes everything the drain placed") // nolint:gosec
	require.GreaterOrEqual(t, stp.HeldCount(), startHeld)
}

func TestSubtreeProcessor_DrainSettlesWhenTheChunkCallbackPanics(t *testing.T) {
	stp := newTestProcessorNoStart(t)

	enqueueDistinctBatches(t, stp, 3, 4)

	require.Panics(t, func() {
		_ = stp.forEachDrainChunk(1, func(_ []*TxBatch) (int, error) {
			panic("chunk callback failed")
		})
	})

	require.Equal(t, int64(0), stp.queue.unsettled.Load(), "a panic recovered above the drain must not leave the chunk counted as unsettled for good")
}

func TestSubtreeProcessor_DequeueDuringBlockMovementCountsWhatItPlaced(t *testing.T) {
	stp := newTestProcessorNoStart(t)
	stp.queue.maxHeld = 1 << 20

	enqueueDistinctBatches(t, stp, 3, 5)

	require.NoError(t, stp.dequeueDuringBlockMovement(nil, nil, nil, true))

	require.Equal(t, int64(len(collectSubtreeHashes(stp))), int64(stp.txCount.Load()), "the drain counts what it placed before it settles") // nolint:gosec
	require.GreaterOrEqual(t, stp.txCount.Load(), uint64(15))
	require.Equal(t, int64(stp.txCount.Load()), stp.HeldCount()) // nolint:gosec
}

func TestEnqueueBatchBounded_ClassifiesEachRefusal(t *testing.T) {
	t.Run("queue bound", func(t *testing.T) {
		var held atomic.Int64

		q := heldQueue(10, 0, &held)

		require.Equal(t, Admitted, q.enqueueBatchBounded(mkNodes(10), mkInpoints(10), false))
		require.Equal(t, RefusedQueue, q.enqueueBatchBounded(mkNodes(1), mkInpoints(1), false))
		require.Equal(t, int64(10), q.length(), "a refusal leaves no reservation behind")
	})

	t.Run("held bound", func(t *testing.T) {
		var held atomic.Int64

		held.Store(95)

		q := heldQueue(0, 100, &held)

		require.Equal(t, RefusedHeld, q.enqueueBatchBounded(mkNodes(6), mkInpoints(6), false))
		require.Equal(t, int64(0), q.length())
		require.Equal(t, Admitted, q.enqueueBatchBounded(mkNodes(5), mkInpoints(5), false))
		require.Equal(t, RefusedQueue, q.enqueueBatchBounded(mkNodes(1), mkInpoints(1), false),
			"the excess is queued items, which the dispatcher may still place, so it is the queue's refusal for now")

		batch, found := q.dequeueBatch(0)
		require.True(t, found)
		require.Len(t, batch.nodes, 5)
		held.Store(100)
		q.settle(5)

		require.Equal(t, RefusedHeld, q.enqueueBatchBounded(mkNodes(1), mkInpoints(1), false),
			"once the queued items are placed in subtrees the refusal is the held bound")
	})

	t.Run("held bound wins when both bounds refuse", func(t *testing.T) {
		var held atomic.Int64

		held.Store(100)

		q := heldQueue(5, 100, &held)

		require.Equal(t, RefusedHeld, q.enqueueBatchBounded(mkNodes(6), mkInpoints(6), false), "an over-the-limit node is the cause that outlasts the queue")
	})

	t.Run("queued items never make a refusal the held bound", func(t *testing.T) {
		var held atomic.Int64

		held.Store(980)

		q := heldQueue(0, 1000, &held)

		q.queueLength.Add(50)

		require.Equal(t, RefusedQueue, q.enqueueBatchBounded(mkNodes(10), mkInpoints(10), false),
			"another producer's refused reservation must not make this batch a held refusal, which the validator never retries")

		q.queueLength.Add(-50)

		require.Equal(t, Admitted, q.enqueueBatchBounded(mkNodes(10), mkInpoints(10), false),
			"alone the batch holds 990 of 1000")
		require.Equal(t, RefusedHeld, q.enqueueBatchBounded(mkNodes(50), mkInpoints(50), false),
			"a batch that breaks the limit on top of what is placed is still a held refusal")
	})

	t.Run("a refusal inflated by other producers is the queue bound", func(t *testing.T) {
		var held atomic.Int64

		held.Store(890)

		q := heldQueue(100, 1000, &held)

		require.Equal(t, Admitted, q.enqueueBatchBounded(mkNodes(85), mkInpoints(85), false))

		q.queueLength.Add(20)

		require.Equal(t, RefusedQueue, q.enqueueBatchBounded(mkNodes(10), mkInpoints(10), false),
			"reservations that are about to roll back must not turn a queue refusal into a held one")

		q.queueLength.Add(-20)

		require.Equal(t, Admitted, q.enqueueBatchBounded(mkNodes(10), mkInpoints(10), false),
			"with only its own batch the producer fits both bounds")
	})

	t.Run("a block batch is refused by the queue bound only", func(t *testing.T) {
		var held atomic.Int64

		held.Store(100)

		q := heldQueue(5, 100, &held)

		require.Equal(t, Admitted, q.enqueueBatchBounded(mkNodes(5), mkInpoints(5), true), "past the in-memory limit")
		require.Equal(t, RefusedQueue, q.enqueueBatchBounded(mkNodes(1), mkInpoints(1), true))
	})

	t.Run("oversize for the queue waits for an empty queue", func(t *testing.T) {
		var held atomic.Int64

		q := heldQueue(10, 0, &held)

		require.Equal(t, Admitted, q.enqueueBatchBounded(mkNodes(2), mkInpoints(2), false))
		require.Equal(t, RefusedOversize, q.enqueueBatchBounded(mkNodes(11), mkInpoints(11), false))

		batch, found := q.dequeueBatch(0)
		require.True(t, found)
		require.Len(t, batch.nodes, 2)
		q.settle(2)

		require.Equal(t, Admitted, q.enqueueBatchBounded(mkNodes(11), mkInpoints(11), false), "admitted once the queue is empty")
	})

	t.Run("oversize for the in-memory limit waits for an empty queue", func(t *testing.T) {
		var held atomic.Int64

		held.Store(1)

		q := heldQueue(0, 10, &held)

		require.Equal(t, Admitted, q.enqueueBatchBounded(mkNodes(2), mkInpoints(2), false))
		require.Equal(t, RefusedOversize, q.enqueueBatchBounded(mkNodes(11), mkInpoints(11), false),
			"the batch can never fit, so it waits for the empty queue its admission relies on")

		batch, found := q.dequeueBatch(0)
		require.True(t, found)
		require.Len(t, batch.nodes, 2)
		q.settle(2)

		require.Equal(t, Admitted, q.enqueueBatchBounded(mkNodes(11), mkInpoints(11), false), "admitted once the queue is empty")
	})

	t.Run("an in-block batch larger than the in-memory limit is never refused by it", func(t *testing.T) {
		var held atomic.Int64

		held.Store(10)

		q := heldQueue(20, 10, &held)

		require.Equal(t, Admitted, q.enqueueBatchBounded(mkNodes(15), mkInpoints(15), true))
		require.Equal(t, RefusedQueue, q.enqueueBatchBounded(mkNodes(11), mkInpoints(11), true),
			"the queue bound refuses it, and the in-memory limit it is exempt from must not be blamed")

	})

	t.Run("oversize at the in-memory limit is refused by the limit", func(t *testing.T) {
		var held atomic.Int64

		held.Store(10)

		q := heldQueue(0, 10, &held)

		require.Equal(t, RefusedHeld, q.enqueueBatchBounded(mkNodes(11), mkInpoints(11), false),
			"a node already at the limit clears only when a block connects, so there is nothing to wait for")
	})
}

func TestSubtreeProcessor_UnminedLoadReadsFullUntilItCompletes(t *testing.T) {
	stp := newLimitedSubtreeProcessor(t, context.Background(), 1)
	limit := stp.MaxTransactionsInMemory()

	require.Equal(t, Admitted, stp.AddBatchBounded(mkNodes(2), mkInpoints(2), false), "ingest before the load is admitted against the real count")

	stp.SetUnminedLoading(true)

	nodes, inpoints := distinctNodes(10, "unmined-load")

	for i := range nodes {
		require.NoError(t, stp.AddDirectly(&nodes[i], inpoints[i], true))
		require.GreaterOrEqual(t, stp.HeldCount(), limit, "the load raises the subtree count one transaction at a time, which must not read as room")
	}

	require.Equal(t, RefusedHeld, stp.AddBatchBounded(mkNodes(1), mkInpoints(1), false), "mempool ingest is refused as held while the load runs")
	require.Equal(t, Admitted, stp.AddBatchBounded(mkNodes(1), mkInpoints(1), true), "a block transaction is exempt")

	stp.SetUnminedLoading(false)

	require.Less(t, stp.HeldCount(), limit, "the held count returns to the real total once the load completes")
	require.Equal(t, Admitted, stp.AddBatchBounded(mkNodes(1), mkInpoints(1), false), "and ingest is admitted again")
}

func TestSubtreeProcessor_UnminedLoadingNests(t *testing.T) {
	stp := newLimitedSubtreeProcessor(t, context.Background(), 1)
	limit := stp.MaxTransactionsInMemory()

	stp.SetUnminedLoading(true)
	stp.SetUnminedLoading(true)
	stp.SetUnminedLoading(false)

	require.GreaterOrEqual(t, stp.HeldCount(), limit, "a reload inside a reset must not end the reset's own hold")

	stp.SetUnminedLoading(false)

	require.Less(t, stp.HeldCount(), limit)
}

func TestSubtreeProcessor_DrainAddsWhatEachChunkReportsWithoutARecount(t *testing.T) {
	stp := newTestProcessorNoStart(t)

	require.Equal(t, int64(0), stp.MaxTransactionsInMemory(), "precondition: no in-memory limit")

	enqueueDistinctBatches(t, stp, 3, 4)

	before := stp.txCount.Load()
	chunks := 0

	err := stp.forEachDrainChunk(4, func(chunk []*TxBatch) (int, error) {
		for _, batch := range chunk {
			for i, node := range batch.nodes {
				require.NoError(t, stp.addNode(node, batch.txInpoints[i], true))
			}
		}

		chunks++

		return 1, nil
	})

	require.NoError(t, err)
	require.Equal(t, 3, chunks, "precondition: the drain crossed chunk boundaries")
	require.Equal(t, before+3, stp.txCount.Load(), "each chunk adds the number its callback reported, not a walk of every chained subtree")
	require.Equal(t, int64(0), stp.queue.unsettled.Load(), "the chunks are still settled")
}

func TestSubtreeProcessor_DrainDoesNotCountADuplicateAsPlaced(t *testing.T) {
	stp := newTestProcessorNoStart(t)

	nodes, inpoints := distinctNodes(3, "drain-duplicate")

	require.NoError(t, stp.addNode(nodes[0], inpoints[0], true), "the first copy is already placed, as the unmined load leaves it")

	before := stp.txCount.Load()

	enqueueAt := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	stp.queue.clock = fixedClock{t: enqueueAt}
	stp.clock = fixedClock{t: enqueueAt.Add(time.Millisecond)}
	stp.queue.enqueueBatch(nodes[:2], inpoints[:2])

	stp.DrainQueue(map[chainhash.Hash]struct{}{chainhash.HashH([]byte("unrelated-drop")): {}})

	require.Equal(t, int64(0), stp.queue.length(), "precondition: the drain took the batch")
	require.Equal(t, before+1, stp.txCount.Load(), "only the transaction that was not already placed is added")
}

func TestSubtreeProcessor_DequeueDuringBlockMovementCountsANodeWhoseSubtreeFailedToComplete(t *testing.T) {
	stp := newTestProcessorNoStart(t)

	stp.newSubtreeChan = make(chan NewSubtreeRequest)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	stp.processorCtx.Store(&cancelled)

	perSubtree := int(stp.currentItemsPerFile.Load())

	enqueueDistinctBatches(t, stp, 1, perSubtree-1)

	require.NoError(t, stp.dequeueDuringBlockMovement(nil, nil, nil, true), "a failed subtree completion is logged, not returned")

	require.Len(t, stp.chainedSubtrees, 1, "precondition: the last node completed the subtree, and sending it failed")
	require.Equal(t, int64(len(collectSubtreeHashes(stp))), int64(stp.txCount.Load()), "the node that completed the subtree is in it, so it is counted") // nolint:gosec
	require.Equal(t, int64(stp.txCount.Load()), stp.HeldCount())                                                                                         // nolint:gosec
}

func TestSubtreeProcessor_SequentialDrainCountsOnlyWhatItPlaced(t *testing.T) {
	stp := newTestProcessorNoStart(t)

	nodes, inpoints := distinctNodes(5, "drain-sequential")

	require.NoError(t, stp.addNode(nodes[0], inpoints[0], true))
	require.NoError(t, stp.addNode(nodes[1], inpoints[1], true))

	_, wasSet := stp.currentTxMap.SetIfNotExists(nodes[2].Hash, inpoints[2])
	require.True(t, wasSet, "the unmined load leaves a transaction in the map before any subtree holds it")

	chunkInpoints := []*subtree.TxInpoints{inpoints[0], inpoints[1], nil, inpoints[3], inpoints[4]}

	enqueueAt := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	stp.queue.clock = fixedClock{t: enqueueAt}
	stp.clock = fixedClock{t: enqueueAt.Add(time.Millisecond)}
	stp.queue.enqueueBatch(nodes, chunkInpoints)

	before := stp.txCount.Load()

	err := stp.forEachDrainChunk(drainChunkItems, func(batches []*TxBatch) (int, error) {
		return stp.addDrainedBatches(context.Background(), batches, &deferredBlockDrain{})
	})

	require.NoError(t, err)
	require.Equal(t, int64(0), stp.queue.length(), "precondition: the drain took the batch")
	require.Equal(t, before+3, stp.txCount.Load(), "the nil-inpoints transaction and the two new ones are placed, the two already in subtrees are not")
	require.Equal(t, int64(0), stp.queue.unsettled.Load())
}

func TestSubtreeProcessor_UnminedLoadingEndWithoutStartLeavesTheCountAtZero(t *testing.T) {
	stp := newLimitedSubtreeProcessor(t, context.Background(), 1)

	stp.SetUnminedLoading(false)

	require.False(t, stp.UnminedLoading(), "an unmatched end must not go negative and cancel a later start")

	stp.SetUnminedLoading(true)

	require.True(t, stp.UnminedLoading())
}
