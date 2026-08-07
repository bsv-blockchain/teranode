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
func newBoundedRealServer(t *testing.T, maxItems int64, wait time.Duration) *BlockAssembly {
	t.Helper()

	common := testutil.NewCommonTestSetup(t)
	common.Settings.BlockAssembly.MaxQueueItems = maxItems
	common.Settings.BlockAssembly.QueueFullWaitTimeout = wait
	common.Settings.BlockAssembly.StoreTxInpointsForSubtreeMeta = false

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
