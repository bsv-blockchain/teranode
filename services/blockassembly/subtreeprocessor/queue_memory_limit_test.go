package subtreeprocessor

import (
	"context"
	"math"
	"sync/atomic"
	"testing"

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

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockAssembly.InitialMerkleItemsPerSubtree = 4
	tSettings.BlockAssembly.MaxTransactionsInMemory = limit

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
