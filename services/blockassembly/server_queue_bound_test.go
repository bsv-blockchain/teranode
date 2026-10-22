package blockassembly

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/testutil"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// batchReq builds a valid row-format AddTxBatchRequest with n distinct txids.
func batchReq(n int) *blockassembly_api.AddTxBatchRequest {
	reqs := make([]*blockassembly_api.AddTxRequest, n)
	for i := range reqs {
		txid := make([]byte, 32)
		txid[0] = byte(i)
		txid[1] = byte(i >> 8)
		reqs[i] = &blockassembly_api.AddTxRequest{Txid: txid, Fee: 1, Size: 100}
	}

	return &blockassembly_api.AddTxBatchRequest{TxRequests: reqs}
}

// columnarReq builds a valid columnar AddTxBatchColumnarRequest with n txids and
// no parents (zeroed offset tables), which passes the handler's structural
// validation.
func columnarReq(n int) *blockassembly_api.AddTxBatchColumnarRequest {
	txids := make([]byte, n*32)
	for i := 0; i < n; i++ {
		txids[i*32] = byte(i)
		txids[i*32+1] = byte(i >> 8)
	}

	return &blockassembly_api.AddTxBatchColumnarRequest{
		TxidsPacked:          txids,
		Fees:                 make([]uint64, n),
		Sizes:                make([]uint64, n),
		ParentTxHashesPacked: []byte{},
		ParentTxOffsets:      make([]uint32, n+1),
		VoutIdxsPacked:       []uint32{},
		VoutIdxsTxOffsets:    make([]uint32, n+1),
	}
}

// baWithProcessor builds a BlockAssembly server around a supplied subtree
// processor without calling Init, so the ingest handlers can be exercised in
// isolation with a controllable queue and no background metrics goroutine.
func baWithProcessor(t *testing.T, mutate func(s *settings.Settings), stp subtreeprocessor.Interface) *BlockAssembly {
	t.Helper()

	s := test.CreateBaseTestSettings(t)
	mutate(s)

	ba := New(ulogger.TestLogger{}, s, nil, nil, nil, nil)
	ba.blockAssembler = &BlockAssembler{subtreeProcessor: stp}

	return ba
}

// Test 8 — both batch handlers shed with codes.ResourceExhausted when the queue
// is over the limit and the wait timeout is 0.
func TestAddTxBatch_ShedsWhenQueueFull(t *testing.T) {
	stp := &subtreeprocessor.MockSubtreeProcessor{}
	stp.On("AddBatchIfRoom", mock.Anything, mock.Anything).Return(false)
	stp.On("QueueLength").Return(int64(150)).Maybe()
	stp.On("QueueMaxItems").Return(int64(100)).Maybe()

	ba := baWithProcessor(t, func(s *settings.Settings) {
		s.BlockAssembly.MaxQueueItems = 100
		s.BlockAssembly.QueueFullWaitTimeout = 0
		s.BlockAssembly.StoreTxInpointsForSubtreeMeta = false
	}, stp)

	_, err := ba.AddTxBatch(context.Background(), batchReq(1))
	require.Error(t, err)
	require.Equal(t, codes.ResourceExhausted, status.Code(err), "row batch handler must shed with ResourceExhausted")

	_, err = ba.AddTxBatchColumnar(context.Background(), columnarReq(1))
	require.Error(t, err)
	require.Equal(t, codes.ResourceExhausted, status.Code(err), "columnar batch handler must shed with ResourceExhausted")
}

// Test 9 — BlockAssembly.Disabled short-circuits before the capacity guard: the
// handlers return Ok, never touch the queue, and never count a shed.
func TestAddTxBatch_DisabledShortCircuits(t *testing.T) {
	stp := &subtreeprocessor.MockSubtreeProcessor{}
	// Intentionally no AddBatchIfRoom expectation: a call would fail the test.

	ba := baWithProcessor(t, func(s *settings.Settings) {
		s.BlockAssembly.Disabled = true
		s.BlockAssembly.MaxQueueItems = 1
		s.BlockAssembly.QueueFullWaitTimeout = 0
		s.BlockAssembly.StoreTxInpointsForSubtreeMeta = false
	}, stp)

	shedBefore := promtestutil.ToFloat64(prometheusBlockAssemblyQueueShed)

	resp, err := ba.AddTxBatch(context.Background(), batchReq(5))
	require.NoError(t, err)
	require.True(t, resp.Ok)

	respCol, err := ba.AddTxBatchColumnar(context.Background(), columnarReq(5))
	require.NoError(t, err)
	require.True(t, respCol.Ok)

	require.Equal(t, shedBefore, promtestutil.ToFloat64(prometheusBlockAssemblyQueueShed),
		"a disabled block assembler must not count a shed")
	stp.AssertNotCalled(t, "AddBatchIfRoom", mock.Anything, mock.Anything)
}

// Test 10a — the bounded wait absorbs a transient full queue: the fast path
// refuses once, then a poll within the window succeeds and the handler returns
// Ok without wedging.
func TestAddTxBatch_WaitThenSucceeds(t *testing.T) {
	stp := &subtreeprocessor.MockSubtreeProcessor{}
	stp.On("AddBatchIfRoom", mock.Anything, mock.Anything).Return(false).Once()
	stp.On("AddBatchIfRoom", mock.Anything, mock.Anything).Return(true)

	ba := baWithProcessor(t, func(s *settings.Settings) {
		s.BlockAssembly.MaxQueueItems = 100
		s.BlockAssembly.QueueFullWaitTimeout = 500 * time.Millisecond
		s.BlockAssembly.StoreTxInpointsForSubtreeMeta = false
	}, stp)

	resp, err := ba.AddTxBatch(context.Background(), batchReq(1))
	require.NoError(t, err)
	require.True(t, resp.Ok, "room freed within the wait window must yield Ok")
}

// Test 10b — the handler returns on ctx.Done rather than waiting out a long
// window, and a caller cancellation is NOT reported as a queue shed: the shed
// counter does not move and the error is the context-cancelled class, not the
// resource-exhausted class whose locked-shed recovery semantics do not apply.
func TestAddTxBatch_ReturnsOnContextDone(t *testing.T) {
	stp := &subtreeprocessor.MockSubtreeProcessor{}
	stp.On("AddBatchIfRoom", mock.Anything, mock.Anything).Return(false)
	stp.On("QueueLength").Return(int64(150)).Maybe()

	ba := baWithProcessor(t, func(s *settings.Settings) {
		s.BlockAssembly.MaxQueueItems = 100
		s.BlockAssembly.QueueFullWaitTimeout = 10 * time.Second
		s.BlockAssembly.StoreTxInpointsForSubtreeMeta = false
	}, stp)

	shedBefore := promtestutil.ToFloat64(prometheusBlockAssemblyQueueShed)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	_, err := ba.AddTxBatch(ctx, batchReq(1))
	elapsed := time.Since(start)

	require.Error(t, err)
	require.Less(t, elapsed, 5*time.Second, "handler must return on ctx.Done, not wait out the 10s window")
	require.NotEqual(t, codes.ResourceExhausted, status.Code(err), "cancellation must not surface as a shed")
	require.ErrorIs(t, errors.UnwrapGRPC(err), errors.ErrContextCanceled, "cancellation is the context-cancelled class")
	require.Equal(t, shedBefore, promtestutil.ToFloat64(prometheusBlockAssemblyQueueShed), "cancellation must not increment the shed counter")
}

// Test 10c — with MaxQueueItems = 0 the bound is disabled and neither handler
// ever refuses, exercised through a real (undrained) subtree processor.
func TestAddTxBatch_UnboundedNeverRefuses(t *testing.T) {
	s := newBoundedRealServer(t, 0, 100*time.Millisecond)

	for i := 0; i < 10; i++ {
		resp, err := s.AddTxBatch(context.Background(), batchReq(50))
		require.NoError(t, err)
		require.True(t, resp.Ok)
	}

	resp, err := s.AddTxBatchColumnar(context.Background(), columnarReq(50))
	require.NoError(t, err)
	require.True(t, resp.Ok)
}

// newBoundedRealServer builds a fully-initialized BlockAssembly server with the
// queue bound set, backed by in-memory stores. The subtree processor is not
// started, so the queue does not drain on its own.
func newBoundedRealServer(t *testing.T, maxItems int64, wait time.Duration, sendBatchSize ...int) *BlockAssembly {
	t.Helper()

	common := testutil.NewCommonTestSetup(t)
	common.Settings.BlockAssembly.MaxQueueItems = maxItems
	common.Settings.BlockAssembly.QueueFullWaitTimeout = wait
	common.Settings.BlockAssembly.StoreTxInpointsForSubtreeMeta = false

	// The queue clamps a cap below one full drain pass (maxBatchesPerIteration x
	// sendBatchSize) upward, so a test wanting a small enforced cap has to shrink the
	// batch size too.
	if len(sendBatchSize) == 1 {
		common.Settings.BlockAssembly.SendBatchSize = sendBatchSize[0]
	}

	subtreeStore := testutil.NewMemoryBlobStore()

	ctx, cancel := context.WithCancel(common.Ctx)

	blockchainClient := testutil.NewMemorySQLiteBlockchainClient(common.Logger, common.Settings, t)
	utxoStore := testutil.NewSQLiteMemoryUTXOStore(ctx, common.Logger, common.Settings, t)
	_ = utxoStore.SetBlockHeight(123)

	s := New(common.Logger, common.Settings, nil, utxoStore, subtreeStore, blockchainClient)
	s.SetSkipWaitForPendingBlocks(true)
	require.NoError(t, s.Init(ctx))

	t.Cleanup(func() {
		cancel()
		_ = s.Stop(context.Background())

		if s.blockAssembler != nil {
			s.blockAssembler.Wait()
		}
	})

	return s
}

// A shed must not be counted as an add. Every ingest handler used to increment
// teranode_blockassembly_add_tx before attempting the enqueue, so a transaction
// the queue refused was counted as added and the shed rate could not be
// reconciled against the add rate — the one reconciliation an operator needs to
// size the cap.
//
// Exercised through a real, undrained queue, so the shed is the queue's own
// decision rather than a mock's. All three subtests run with BlockAssembly.Disabled
// at its default false; the disabled short-circuit is covered separately.
func TestIngest_ShedIsNotCountedAsAdded(t *testing.T) {
	initPrometheusMetrics()

	// Enforced cap of 64 items: 64 batches per drain pass x a sendBatchSize of 1.
	s := newBoundedRealServer(t, 64, 0, 1)
	require.Equal(t, int64(64), s.blockAssembler.QueueMaxItems(), "precondition: the enforced cap is what was asked for")

	// Fill it exactly, so every call below is refused for room.
	resp, err := s.AddTxBatch(context.Background(), batchReq(64))
	require.NoError(t, err)
	require.True(t, resp.Ok)

	requireShedNotCounted := func(t *testing.T, call func() error) {
		t.Helper()

		addsBefore := promtestutil.ToFloat64(prometheusBlockAssemblyAddTxCounter)
		shedBefore := promtestutil.ToFloat64(prometheusBlockAssemblyQueueShed)

		err := call()
		require.Error(t, err)
		require.Equal(t, codes.ResourceExhausted, status.Code(err), "a full queue sheds with a retryable status")

		require.Equal(t, shedBefore+1, promtestutil.ToFloat64(prometheusBlockAssemblyQueueShed),
			"the shed is counted")
		require.Equal(t, addsBefore, promtestutil.ToFloat64(prometheusBlockAssemblyAddTxCounter),
			"and must NOT also be counted as an add")
	}

	t.Run("AddTx", func(t *testing.T) {
		requireShedNotCounted(t, func() error {
			txid := make([]byte, 32)
			txid[0] = 0xAA

			_, err := s.AddTx(context.Background(), &blockassembly_api.AddTxRequest{Txid: txid, Fee: 1, Size: 100})

			return err
		})
	})

	t.Run("AddTxBatch", func(t *testing.T) {
		requireShedNotCounted(t, func() error {
			_, err := s.AddTxBatch(context.Background(), batchReq(5))

			return err
		})
	})

	t.Run("AddTxBatchColumnar", func(t *testing.T) {
		requireShedNotCounted(t, func() error {
			_, err := s.AddTxBatchColumnar(context.Background(), columnarReq(5))

			return err
		})
	})
}

// newHeldLimitedRealServer builds a fully-initialized BlockAssembly server with the in-memory
// transaction limit set, backed by in-memory stores. The subtree processor is not started, so
// nothing drains. A sendBatchSize of 1 puts the clamp floor at 65, which is the limit used here.
func newHeldLimitedRealServer(t *testing.T, limit uint64, wait time.Duration) *BlockAssembly {
	t.Helper()

	common := testutil.NewCommonTestSetup(t)
	common.Settings.BlockAssembly.MaxTransactionsInMemory = limit
	common.Settings.BlockAssembly.QueueFullWaitTimeout = wait
	common.Settings.BlockAssembly.StoreTxInpointsForSubtreeMeta = false
	common.Settings.BlockAssembly.SendBatchSize = 1

	subtreeStore := testutil.NewMemoryBlobStore()

	ctx, cancel := context.WithCancel(common.Ctx)

	blockchainClient := testutil.NewMemorySQLiteBlockchainClient(common.Logger, common.Settings, t)
	utxoStore := testutil.NewSQLiteMemoryUTXOStore(ctx, common.Logger, common.Settings, t)
	_ = utxoStore.SetBlockHeight(123)

	s := New(common.Logger, common.Settings, nil, utxoStore, subtreeStore, blockchainClient)
	s.SetSkipWaitForPendingBlocks(true)
	require.NoError(t, s.Init(ctx))

	t.Cleanup(func() {
		cancel()
		_ = s.Stop(context.Background())

		if s.blockAssembler != nil {
			s.blockAssembler.Wait()
		}
	})

	return s
}

func TestIngest_HeldLimitRefusesAtOnceAndMarksTheShed(t *testing.T) {
	initPrometheusMetrics()

	s := newHeldLimitedRealServer(t, 65, 5*time.Second)
	require.Equal(t, int64(65), s.blockAssembler.MaxTransactionsInMemory(), "precondition: the enforced limit is what was asked for")

	// The placeholder already holds one, so 64 fills the limit exactly.
	_, err := s.AddTxBatch(context.Background(), batchReq(64))
	require.NoError(t, err)
	require.Equal(t, int64(65), s.blockAssembler.HeldCount())

	heldShedBefore := promtestutil.ToFloat64(prometheusBlockAssemblyHeldShed)
	queueShedBefore := promtestutil.ToFloat64(prometheusBlockAssemblyQueueShed)

	start := time.Now()
	_, err = s.AddTxBatch(context.Background(), batchReq(1))

	require.Error(t, err)
	require.Less(t, time.Since(start), time.Second, "a refusal by the in-memory limit must not poll for the 5s queue wait")
	require.Equal(t, codes.ResourceExhausted, status.Code(err), "same class as a queue-full shed, so every consumer handles it")

	unwrapped := errors.UnwrapGRPC(err)
	require.True(t, errors.Is(unwrapped, errors.ErrThresholdExceeded))
	require.True(t, errors.Is(unwrapped, errors.ErrServiceUnavailable), "the marker the validator reads to skip its retry")

	require.Equal(t, heldShedBefore+1, promtestutil.ToFloat64(prometheusBlockAssemblyHeldShed))
	require.Equal(t, queueShedBefore+1, promtestutil.ToFloat64(prometheusBlockAssemblyQueueShed), "a held shed is also a shed")

	_, err = s.AddTx(context.Background(), batchReq(1).TxRequests[0])
	require.Error(t, err, "the unary handler refuses at the limit too")

	_, err = s.AddTxBatchColumnar(context.Background(), columnarReq(1))
	require.Error(t, err, "so does the columnar handler")
}

func TestIngest_HeldLimitDoesNotRefuseBlockTransactions(t *testing.T) {
	initPrometheusMetrics()

	s := newHeldLimitedRealServer(t, 65, 0)

	_, err := s.AddTxBatch(context.Background(), batchReq(64))
	require.NoError(t, err)

	_, err = s.AddTxBatch(context.Background(), batchReq(1))
	require.Error(t, err, "precondition: mempool ingest is refused at the limit")

	rowBatch := batchReq(10)
	rowBatch.InBlock = true

	_, err = s.AddTxBatch(context.Background(), rowBatch)
	require.NoError(t, err, "a row batch of block transactions is admitted past the limit")

	columnar := columnarReq(10)
	columnar.InBlock = true

	_, err = s.AddTxBatchColumnar(context.Background(), columnar)
	require.NoError(t, err, "so is a columnar batch")

	single := batchReq(1).TxRequests[0]
	single.InBlock = true

	_, err = s.AddTx(context.Background(), single)
	require.NoError(t, err, "and a unary block transaction")

	require.Equal(t, int64(65+10+10+1), s.blockAssembler.HeldCount(), "block transactions count toward the total")

	_, err = s.AddTxBatch(context.Background(), batchReq(1))
	require.Error(t, err, "so mempool ingest stays refused")

	stats, err := s.GetBlockAssemblyQueueStats(context.Background(), &blockassembly_api.EmptyMessage{})
	require.NoError(t, err)
	require.Equal(t, int64(65+10+10+1), stats.HeldCount)
	require.Equal(t, int64(65), stats.MaxHeld)
}

func TestIngest_QueueBoundShedIsNotMarkedAsTheHeldLimit(t *testing.T) {
	initPrometheusMetrics()

	s := newBoundedRealServer(t, 64, 0, 1)

	_, err := s.AddTxBatch(context.Background(), batchReq(64))
	require.NoError(t, err)

	heldShedBefore := promtestutil.ToFloat64(prometheusBlockAssemblyHeldShed)

	_, err = s.AddTxBatch(context.Background(), batchReq(1))
	require.Error(t, err)

	unwrapped := errors.UnwrapGRPC(err)
	require.True(t, errors.Is(unwrapped, errors.ErrThresholdExceeded))
	require.False(t, errors.Is(unwrapped, errors.ErrServiceUnavailable), "a queue-full shed clears in milliseconds and keeps its retry")
	require.Equal(t, heldShedBefore, promtestutil.ToFloat64(prometheusBlockAssemblyHeldShed))
}

func TestIngest_BlockBatchStillHitsTheQueueBound(t *testing.T) {
	initPrometheusMetrics()

	s := newBoundedRealServer(t, 64, 0, 1)

	_, err := s.AddTxBatch(context.Background(), batchReq(64))
	require.NoError(t, err)

	inBlock := batchReq(1)
	inBlock.InBlock = true

	_, err = s.AddTxBatch(context.Background(), inBlock)
	require.Error(t, err, "the exemption is from the in-memory limit only; the ingest queue bound still protects the queue")
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
}
