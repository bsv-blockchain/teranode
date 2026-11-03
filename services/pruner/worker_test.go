package pruner

import (
	"context"
	"database/sql"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	blockassembly_api "github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob/storetypes"
	blockchainsql "github.com/bsv-blockchain/teranode/stores/blockchain/sql"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	prunerstore "github.com/bsv-blockchain/teranode/stores/utxo/pruner"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// runProcessor runs the pruner processor until ctx is cancelled, and makes the test wait for
// it to return, so a cycle still running when the test ends cannot move the process-global
// metrics the next test reads.
func runProcessor(t *testing.T, ctx context.Context, s *Server) {
	t.Helper()

	done := make(chan struct{})

	go func() {
		s.prunerProcessor(ctx)
		close(done)
	}()

	t.Cleanup(func() { <-done })
}

// runBlobWorker runs the blob deletion worker until the server's context is cancelled, and
// makes the test wait for it to return.
func runBlobWorker(t *testing.T, s *Server) {
	t.Helper()

	done := make(chan struct{})

	go func() {
		s.blobDeletionWorker()
		close(done)
	}()

	t.Cleanup(func() { <-done })
}

// getCounterValue reads the current value of a prometheus counter with the given label.
// Fails the test if the metric cannot be read.
func getCounterValue(t *testing.T, cv *prometheus.CounterVec, label string) float64 {
	t.Helper()
	m := &dto.Metric{}
	counter, err := cv.GetMetricWithLabelValues(label)
	require.NoError(t, err, "failed to get metric with label %q", label)
	require.NoError(t, counter.Write(m), "failed to write metric")
	require.NotNil(t, m.Counter, "metric counter is nil")
	return m.Counter.GetValue()
}

// TestMinBlockHeightSkipsPruning verifies that prunerProcessor skips all pruning
// operations when blockHeight <= MinBlockHeight and increments the prunerSkipped
// metric with reason "below_min_height".
func TestMinBlockHeightSkipsPruning(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := ulogger.New("test")

	server := &Server{
		ctx:         ctx,
		logger:      logger,
		pruneNotify: make(chan pruneSignal, 2),
		blobNotify:  make(chan pruneSignal, 1),
		settings: &settings.Settings{
			Pruner: settings.PrunerSettings{
				MinBlockHeight: 100,
			},
		},
	}

	// Capture skip metric before test
	skipsBefore := getCounterValue(t, prunerSkipped, "below_min_height")

	// Start the processor in a goroutine
	runProcessor(t, ctx, server)

	// Send a signal below the minimum height - should be skipped
	server.pruneNotify <- pruneSignal{blockHeight: 50, blockHash: chainhash.Hash{}}

	// Send a signal at exactly the minimum height - should also be skipped (<=)
	server.pruneNotify <- pruneSignal{blockHeight: 100, blockHash: chainhash.Hash{}}

	// Wait for the counter to reflect both skips (deterministic sync on the metric itself)
	require.Eventually(t, func() bool {
		return getCounterValue(t, prunerSkipped, "below_min_height")-skipsBefore >= 2
	}, time.Second, 10*time.Millisecond)

	// Verify no phase processing occurred by checking lastProcessedHeight is still 0
	// (if pruning had run, it would have been updated)
	require.Equal(t, uint32(0), server.lastProcessedHeight.Load(),
		"lastProcessedHeight should remain 0 when all signals are below MinBlockHeight")

	// Verify blob deletion worker was NOT notified
	select {
	case <-server.blobNotify:
		t.Fatal("blob deletion worker should not have been notified for skipped heights")
	default:
		// Expected: no blob notification
	}

	// Verify prunerSkipped metric incremented exactly twice
	skipsAfter := getCounterValue(t, prunerSkipped, "below_min_height")
	require.Equal(t, float64(2), skipsAfter-skipsBefore,
		"prunerSkipped{reason=below_min_height} should have incremented by 2")
}

// TestMinBlockHeightZeroAllowsPruning verifies that with MinBlockHeight=0 (default),
// pruning proceeds normally without the height check blocking.
func TestMinBlockHeightZeroAllowsPruning(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := ulogger.New("test")

	server := &Server{
		ctx:         ctx,
		logger:      logger,
		pruneNotify: make(chan pruneSignal, 1),
		blobNotify:  make(chan pruneSignal, 1),
		settings: &settings.Settings{
			Pruner: settings.PrunerSettings{
				MinBlockHeight: 0, // Default - no minimum
			},
		},
	}

	// Start the processor in a goroutine
	runProcessor(t, ctx, server)

	// Send a signal at height 1 - should proceed past the min height check
	// and the block assembly safety check (which passes when blockAssemblyClient is nil).
	server.pruneNotify <- pruneSignal{blockHeight: 1, blockHash: chainhash.Hash{}}

	// With MinBlockHeight=0 and no block assembly client (nil), pruning should proceed.
	// The blobNotify channel should have received a signal (block assembly check passes when client is nil).
	select {
	case sig := <-server.blobNotify:
		require.Equal(t, uint32(1), sig.blockHeight, "blob worker should be notified at height 1")
	case <-time.After(time.Second):
		t.Fatal("blob deletion worker should have been notified when MinBlockHeight is 0")
	}
}

// TestMinBlockHeightAboveThresholdProceeds verifies that pruning proceeds normally
// when blockHeight exceeds MinBlockHeight.
func TestMinBlockHeightAboveThresholdProceeds(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := ulogger.New("test")

	server := &Server{
		ctx:         ctx,
		logger:      logger,
		pruneNotify: make(chan pruneSignal, 1),
		blobNotify:  make(chan pruneSignal, 1),
		settings: &settings.Settings{
			Pruner: settings.PrunerSettings{
				MinBlockHeight: 100,
			},
		},
	}

	// Start the processor in a goroutine
	runProcessor(t, ctx, server)

	// Send a signal above the minimum height - should proceed
	server.pruneNotify <- pruneSignal{blockHeight: 101, blockHash: chainhash.Hash{}}

	// With no block assembly client (nil), the safety check passes and blob worker is notified
	select {
	case sig := <-server.blobNotify:
		require.Equal(t, uint32(101), sig.blockHeight, "blob worker should be notified at height 101")
	case <-time.After(time.Second):
		t.Fatal("blob deletion worker should have been notified when blockHeight > MinBlockHeight")
	}
}

// TestWaitForBlockMinedStatusProceeds verifies that when blockAssemblyClient is non-nil
// and GetBlockIsMined returns true, pruning proceeds past the mined_set wait.
func TestWaitForBlockMinedStatusProceeds(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := ulogger.New("test")

	blockchainMock := &blockchain.Mock{}
	// No overtaken-tip repair hold.
	blockchainMock.On("GetState", mock.Anything, model.OvertakenTipRepairHoldKey).Return([]byte(nil), sql.ErrNoRows).Maybe()
	blockAssemblyMock := &blockassembly.Mock{}

	server := &Server{
		ctx:                 ctx,
		logger:              logger,
		pruneNotify:         make(chan pruneSignal, 1),
		blobNotify:          make(chan pruneSignal, 1),
		blockchainClient:    blockchainMock,
		blockAssemblyClient: blockAssemblyMock,
		settings: &settings.Settings{
			Pruner: settings.PrunerSettings{
				MinBlockHeight:           0,
				BlockAssemblyWaitTimeout: 5 * time.Second,
			},
		},
	}

	blockHash := chainhash.Hash{0x01}

	// GetBlockIsMined returns true immediately
	blockchainMock.On("GetBlockIsMined", mock.Anything, &blockHash).Return(true, nil)

	// Block assembly safety check: return running state at correct height
	blockAssemblyMock.On("GetBlockAssemblyState", mock.Anything).Return(
		&blockassembly_api.StateMessage{
			BlockAssemblyState: "running",
			CurrentHeight:      200,
		}, nil,
	)

	runProcessor(t, ctx, server)

	server.pruneNotify <- pruneSignal{blockHeight: 200, blockHash: blockHash}

	// The pruner should proceed through both the mined_set wait and the BA safety check,
	// then notify the blob deletion worker.
	select {
	case sig := <-server.blobNotify:
		require.Equal(t, uint32(200), sig.blockHeight, "blob worker should be notified at height 200")
	case <-time.After(3 * time.Second):
		t.Fatal("pruning should have proceeded when GetBlockIsMined returns true")
	}
}

// TestWaitForBlockMinedStatusTimeout verifies that when blockAssemblyClient is non-nil
// and GetBlockIsMined consistently returns false, the pruner skips the block after timeout.
func TestWaitForBlockMinedStatusTimeout(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := ulogger.New("test")

	blockchainMock := &blockchain.Mock{}
	// No overtaken-tip repair hold.
	blockchainMock.On("GetState", mock.Anything, model.OvertakenTipRepairHoldKey).Return([]byte(nil), sql.ErrNoRows).Maybe()
	blockAssemblyMock := &blockassembly.Mock{}

	server := &Server{
		ctx:                 ctx,
		logger:              logger,
		pruneNotify:         make(chan pruneSignal, 1),
		blobNotify:          make(chan pruneSignal, 1),
		blockchainClient:    blockchainMock,
		blockAssemblyClient: blockAssemblyMock,
		settings: &settings.Settings{
			Pruner: settings.PrunerSettings{
				MinBlockHeight:           0,
				BlockAssemblyWaitTimeout: 1 * time.Second, // Short timeout for test
			},
		},
	}

	blockHash := chainhash.Hash{0x02}

	// GetBlockIsMined always returns false - block never gets mined_set=true
	blockchainMock.On("GetBlockIsMined", mock.Anything, &blockHash).Return(false, nil)

	runProcessor(t, ctx, server)

	server.pruneNotify <- pruneSignal{blockHeight: 300, blockHash: blockHash}

	// The pruner should time out waiting for mined_set and skip, so blob worker
	// should NOT be notified.
	select {
	case <-server.blobNotify:
		t.Fatal("blob deletion worker should NOT be notified when mined_set wait times out")
	case <-time.After(3 * time.Second):
		// Expected: no blob notification after timeout
	}

	// Verify lastProcessedHeight was NOT updated (pruning was skipped)
	require.Equal(t, uint32(0), server.lastProcessedHeight.Load(),
		"lastProcessedHeight should remain 0 when mined_set wait times out")
}

// TestWaitForBlockMinedStatusSkippedWhenNoBAClient verifies that without a
// blockAssemblyClient, the mined_set wait is skipped and pruning proceeds.
func TestWaitForBlockMinedStatusSkippedWhenNoBAClient(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := ulogger.New("test")

	server := &Server{
		ctx:                 ctx,
		logger:              logger,
		pruneNotify:         make(chan pruneSignal, 1),
		blobNotify:          make(chan pruneSignal, 1),
		blockchainClient:    nil,
		blockAssemblyClient: nil, // No BA client - mined_set wait should be skipped
		settings: &settings.Settings{
			Pruner: settings.PrunerSettings{
				MinBlockHeight: 0,
			},
		},
	}

	runProcessor(t, ctx, server)

	server.pruneNotify <- pruneSignal{blockHeight: 400, blockHash: chainhash.Hash{0x03}}

	// Without blockAssemblyClient, both the mined_set wait and the BA safety check
	// are skipped, so blob worker should be notified.
	select {
	case sig := <-server.blobNotify:
		require.Equal(t, uint32(400), sig.blockHeight, "blob worker should be notified at height 400")
	case <-time.After(time.Second):
		t.Fatal("pruning should proceed without blockAssemblyClient")
	}
}

// TestSkipDuringCatchupSkipsWhenCatchingUp verifies that when
// pruner_skipDuringCatchup=true and the FSM reports CATCHINGBLOCKS, the
// pruner records prunerSkipped("catchup_mode"), never notifies the blob
// deletion worker, and never advances lastProcessedHeight.
// IDLE is covered because an operator STOP can park a node while its catchup
// batch is still marking transactions mined.
func TestSkipDuringCatchupSkipsWhenCatchingUp(t *testing.T) {
	for _, state := range []blockchain.FSMStateType{blockchain.FSMStateCATCHINGBLOCKS, blockchain.FSMStateIDLE} {
		t.Run(state.String(), func(t *testing.T) { testSkipDuringCatchupSkipsInState(t, state) })
	}
}

func testSkipDuringCatchupSkipsInState(t *testing.T, state blockchain.FSMStateType) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := ulogger.New("test")

	blockchainMock := &blockchain.Mock{}
	// No overtaken-tip repair hold.
	blockchainMock.On("GetState", mock.Anything, model.OvertakenTipRepairHoldKey).Return([]byte(nil), sql.ErrNoRows).Maybe()
	blockchainMock.On("GetFSMCurrentState", mock.Anything).Return(&state, nil)

	server := &Server{
		ctx:              ctx,
		logger:           logger,
		pruneNotify:      make(chan pruneSignal, 1),
		blobNotify:       make(chan pruneSignal, 1),
		blockchainClient: blockchainMock,
		settings: &settings.Settings{
			Pruner: settings.PrunerSettings{
				SkipDuringCatchup: true,
			},
		},
	}

	skipsBefore := getCounterValue(t, prunerSkipped, "catchup_mode")

	runProcessor(t, ctx, server)

	server.pruneNotify <- pruneSignal{blockHeight: 500, blockHash: chainhash.Hash{0x04}}

	require.Eventually(t, func() bool {
		return getCounterValue(t, prunerSkipped, "catchup_mode")-skipsBefore >= 1
	}, time.Second, 10*time.Millisecond)

	select {
	case <-server.blobNotify:
		t.Fatal("blob deletion worker should not have been notified while catching up")
	default:
		// Expected: no blob notification
	}

	require.Equal(t, uint32(0), server.lastProcessedHeight.Load(),
		"lastProcessedHeight should remain 0 when the block is skipped for catchup")

	blockchainMock.AssertExpectations(t)
}

// TestSkipDuringCatchupProceedsWhenRunning verifies that when
// pruner_skipDuringCatchup=true but the FSM reports RUNNING, pruning
// proceeds normally and the blob deletion worker is notified.
func TestSkipDuringCatchupProceedsWhenRunning(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := ulogger.New("test")

	blockchainMock := &blockchain.Mock{}
	// No overtaken-tip repair hold.
	blockchainMock.On("GetState", mock.Anything, model.OvertakenTipRepairHoldKey).Return([]byte(nil), sql.ErrNoRows).Maybe()
	running := blockchain.FSMStateRUNNING
	blockchainMock.On("GetFSMCurrentState", mock.Anything).Return(&running, nil)

	server := &Server{
		ctx:              ctx,
		logger:           logger,
		pruneNotify:      make(chan pruneSignal, 1),
		blobNotify:       make(chan pruneSignal, 1),
		blockchainClient: blockchainMock,
		settings: &settings.Settings{
			Pruner: settings.PrunerSettings{
				SkipDuringCatchup: true,
			},
		},
	}

	runProcessor(t, ctx, server)

	server.pruneNotify <- pruneSignal{blockHeight: 501, blockHash: chainhash.Hash{0x05}}

	select {
	case sig := <-server.blobNotify:
		require.Equal(t, uint32(501), sig.blockHeight, "blob worker should be notified when FSM is RUNNING")
	case <-time.After(time.Second):
		t.Fatal("pruning should have proceeded when FSM state is RUNNING")
	}

	blockchainMock.AssertExpectations(t)
}

// TestSkipDuringCatchupWithNilBlockchainClientDoesNotPanic verifies the
// missing nil guard: pruner_skipDuringCatchup=true with blockchainClient==nil
// must not panic on the s.blockchainClient.GetFSMCurrentState call, mirroring
// the existing blockAssemblyClient!=nil guard used for the mined_set wait.
// Existing test harnesses construct &Server{} with blockchainClient nil, so
// without this guard, flipping the flag on in such a harness panics.
func TestSkipDuringCatchupWithNilBlockchainClientDoesNotPanic(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server := &Server{
		ctx:              ctx,
		logger:           ulogger.New("test"),
		pruneNotify:      make(chan pruneSignal, 1),
		blobNotify:       make(chan pruneSignal, 1),
		blockchainClient: nil, // no client wired, mirrors real test harness construction
		settings: &settings.Settings{
			Pruner: settings.PrunerSettings{
				SkipDuringCatchup: true,
			},
		},
	}

	// A missing nil guard panics inside the prunerProcessor goroutine, which
	// crashes the test binary (not recoverable via require.NotPanics since
	// the panic occurs on a different goroutine); a passing run without a
	// crash, receiving the blob notification below, proves the guard exists.
	runProcessor(t, ctx, server)

	server.pruneNotify <- pruneSignal{blockHeight: 502, blockHash: chainhash.Hash{0x06}}

	select {
	case sig := <-server.blobNotify:
		require.Equal(t, uint32(502), sig.blockHeight, "blob worker should be notified when the FSM state cannot be determined")
	case <-time.After(time.Second):
		t.Fatal("pruning should have proceeded without touching a nil blockchainClient")
	}
}

// TestStart_FSMContextCancellation verifies graceful shutdown handling when
// the context is cancelled during the FSM wait. The error must be returned
// (not swallowed) and must be a context error so the service manager can
// distinguish it from a real failure.
func TestStart_FSMContextCancellation(t *testing.T) {
	ctx := context.Background()

	mockBlockchainClient := &blockchain.Mock{}
	// No overtaken-tip repair hold.
	mockBlockchainClient.On("GetState", mock.Anything, model.OvertakenTipRepairHoldKey).Return([]byte(nil), sql.ErrNoRows).Maybe()
	mockBlockchainClient.On("WaitUntilFSMTransitionFromIdleState", mock.Anything).Return(context.Canceled)

	server := &Server{
		ctx:              ctx,
		logger:           ulogger.New("test"),
		settings:         &settings.Settings{},
		blockchainClient: mockBlockchainClient,
	}

	readyCh := make(chan struct{})
	err := server.Start(ctx, readyCh)

	require.Error(t, err)
	require.True(t, errors.IsContextError(err), "expected context error, got %v", err)
	mockBlockchainClient.AssertExpectations(t)
}

// TestProcessExpiredPreservationsPhaseRuns verifies that Phase 1b calls
// ProcessExpiredPreservations with the current block height when the feature is enabled
// (pruner_skipProcessExpiredPreservations=false, the default). Phase 1 is skipped so the test
// does not need to mock the unmined-tx iterator, and prunerService is nil so Phase 2 is skipped.
func TestProcessExpiredPreservationsPhaseRuns(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	called := make(chan uint32, 1)
	mockStore := &utxo.MockUtxostore{}
	mockStore.On("ProcessExpiredPreservations", mock.Anything, uint32(101)).
		Run(func(args mock.Arguments) { called <- args.Get(1).(uint32) }).
		Return(nil)

	server := &Server{
		ctx:         ctx,
		logger:      ulogger.New("test"),
		pruneNotify: make(chan pruneSignal, 1),
		blobNotify:  make(chan pruneSignal, 1),
		utxoStore:   mockStore,
		settings: &settings.Settings{
			Pruner: settings.PrunerSettings{
				SkipPreserveParents:             true,  // skip Phase 1 (no iterator to mock)
				SkipProcessExpiredPreservations: false, // Phase 1b enabled
			},
		},
	}

	runProcessor(t, ctx, server)
	server.pruneNotify <- pruneSignal{blockHeight: 101, blockHash: chainhash.Hash{}}

	select {
	case h := <-called:
		require.Equal(t, uint32(101), h, "Phase 1b must call ProcessExpiredPreservations with the block height")
	case <-time.After(2 * time.Second):
		t.Fatal("Phase 1b did not call ProcessExpiredPreservations")
	}
}

// TestProcessExpiredPreservationsPhaseSkipped verifies the kill-switch: when
// pruner_skipProcessExpiredPreservations=true, Phase 1b is skipped and ProcessExpiredPreservations
// is never called, while the rest of the cycle still proceeds (blob worker is notified).
func TestProcessExpiredPreservationsPhaseSkipped(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	called := make(chan struct{}, 1)
	mockStore := &utxo.MockUtxostore{}
	mockStore.On("ProcessExpiredPreservations", mock.Anything, mock.Anything).
		Run(func(mock.Arguments) { called <- struct{}{} }).
		Return(nil)

	server := &Server{
		ctx:         ctx,
		logger:      ulogger.New("test"),
		pruneNotify: make(chan pruneSignal, 1),
		blobNotify:  make(chan pruneSignal, 1),
		utxoStore:   mockStore,
		settings: &settings.Settings{
			Pruner: settings.PrunerSettings{
				SkipPreserveParents:             true,
				SkipProcessExpiredPreservations: true, // Phase 1b disabled (kill-switch)
			},
		},
	}

	runProcessor(t, ctx, server)
	server.pruneNotify <- pruneSignal{blockHeight: 101, blockHash: chainhash.Hash{}}

	// blobNotify is signalled before the phases run; once seen, Phase 1b would have executed if enabled.
	select {
	case <-server.blobNotify:
	case <-time.After(2 * time.Second):
		t.Fatal("blob deletion worker should have been notified")
	}

	select {
	case <-called:
		t.Fatal("Phase 1b ran despite pruner_skipProcessExpiredPreservations=true")
	case <-time.After(200 * time.Millisecond):
		// expected: ProcessExpiredPreservations was not called
	}
}

// failingIterator is a typed nil used only as the (ignored) iterator return value
// of a mocked GetPrunableUnminedTxIterator call that fails.
type failingIterator struct{ utxo.UnminedTxIterator }

// recordingPruner records Prune calls so tests can assert whether Phase 2 ran.
type recordingPruner struct {
	prunerstore.Service
	calls chan uint32
}

func (r *recordingPruner) Prune(_ context.Context, height uint32, _ string) (int64, error) {
	r.calls <- height
	return 0, nil
}

// TestPhase1FailureSkipsPhase2 verifies that when Phase 1 (parent preservation) fails, Phase 2
// (DAH deletion) does not run in that cycle, the error is counted, the cycle is counted as
// skipped, and lastProcessedHeight is not advanced so the height is retried on the next trigger.
func TestPhase1FailureSkipsPhase2(t *testing.T) {
	initPrometheusMetrics()
	prunerPreserveParentsFailing.Set(0) // process-global gauge: reset so state does not leak between tests or -count runs

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockStore := &utxo.MockUtxostore{}
	mockStore.On("GetPrunableUnminedTxIterator", mock.Anything).
		Return((*failingIterator)(nil), errors.NewStorageError("boom"))
	mockStore.On("ProcessExpiredPreservations", mock.Anything, mock.Anything).Return(nil)

	pruneSvc := &recordingPruner{calls: make(chan uint32, 2)}

	server := &Server{
		ctx:           ctx,
		logger:        ulogger.New("test"),
		pruneNotify:   make(chan pruneSignal, 1),
		blobNotify:    make(chan pruneSignal, 1),
		utxoStore:     mockStore,
		prunerService: pruneSvc,
		settings: &settings.Settings{
			UtxoStore: settings.UtxoStoreSettings{
				UnminedTxRetention:       10,
				ParentPreservationBlocks: 100,
			},
		},
	}

	errsBefore := getCounterValue(t, prunerErrors, "preserve_parents")
	skippedBefore := getCounterValue(t, prunerSkipped, "preserve_parents_failed")

	runProcessor(t, ctx, server)
	server.pruneNotify <- pruneSignal{blockHeight: 101, blockHash: chainhash.Hash{}}

	require.Eventually(t, func() bool {
		return getCounterValue(t, prunerSkipped, "preserve_parents_failed") == skippedBefore+1
	}, 2*time.Second, 10*time.Millisecond, "cycle should be counted as skipped when Phase 1 fails")

	require.Equal(t, errsBefore+1, getCounterValue(t, prunerErrors, "preserve_parents"))
	require.Empty(t, pruneSvc.calls, "Phase 2 must not run when Phase 1 failed")
	require.Equal(t, uint32(0), server.lastProcessedHeight.Load(),
		"lastProcessedHeight must not advance so the height is retried")
	require.Equal(t, float64(1), testutil.ToFloat64(prunerPreserveParentsFailing), "one consecutive Phase 1 failure")
	mockStore.AssertCalled(t, "ProcessExpiredPreservations", mock.Anything, uint32(101))
}

// emptyIterator is an unmined tx iterator with nothing in it.
type emptyIterator struct{ utxo.UnminedTxIterator }

func (emptyIterator) Next(context.Context) ([]*utxo.UnminedTransaction, error) { return nil, nil }
func (emptyIterator) Err() error                                               { return nil }
func (emptyIterator) Close() error                                             { return nil }

func newPhase1TestServer(ctx context.Context, store *utxo.MockUtxostore, pruneSvc *recordingPruner, skipPreserve bool) *Server {
	return &Server{
		ctx:           ctx,
		logger:        ulogger.New("test"),
		pruneNotify:   make(chan pruneSignal, 1),
		blobNotify:    make(chan pruneSignal, 1),
		utxoStore:     store,
		prunerService: pruneSvc,
		settings: &settings.Settings{
			UtxoStore: settings.UtxoStoreSettings{
				UnminedTxRetention:       10,
				ParentPreservationBlocks: 100,
			},
			Pruner: settings.PrunerSettings{SkipPreserveParents: skipPreserve},
		},
	}
}

// A cycle skipped because Phase 1 failed does not wedge pruning: the next block
// runs Phase 1 again and, once it succeeds, Phase 2 and the height advance.
func TestPhase1FailureThenSuccessPrunes(t *testing.T) {
	initPrometheusMetrics()
	prunerPreserveParentsFailing.Set(0) // process-global gauge: reset so state does not leak between tests or -count runs

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockStore := &utxo.MockUtxostore{}
	mockStore.On("GetPrunableUnminedTxIterator", mock.Anything).
		Return((*failingIterator)(nil), errors.NewStorageError("boom")).Once()
	mockStore.On("GetPrunableUnminedTxIterator", mock.Anything).Return(emptyIterator{}, nil)
	mockStore.On("ProcessExpiredPreservations", mock.Anything, mock.Anything).Return(nil)

	pruneSvc := &recordingPruner{calls: make(chan uint32, 2)}
	server := newPhase1TestServer(ctx, mockStore, pruneSvc, false)

	runProcessor(t, ctx, server)

	server.pruneNotify <- pruneSignal{blockHeight: 101, blockHash: chainhash.Hash{}}
	require.Eventually(t, func() bool { return testutil.ToFloat64(prunerPreserveParentsFailing) == 1 },
		2*time.Second, 10*time.Millisecond)

	server.pruneNotify <- pruneSignal{blockHeight: 102, blockHash: chainhash.Hash{}}

	select {
	case h := <-pruneSvc.calls:
		require.Equal(t, uint32(102), h)
	case <-time.After(2 * time.Second):
		t.Fatal("Phase 2 did not run after Phase 1 succeeded again")
	}

	require.Eventually(t, func() bool { return server.lastProcessedHeight.Load() == 102 },
		2*time.Second, 10*time.Millisecond)
	require.Equal(t, float64(0), testutil.ToFloat64(prunerPreserveParentsFailing), "a success resets the failure streak")
}

// A Phase 1 error caused by shutdown is not a Phase 1 failure: the streak gauge and
// the preserve_parents error counter must not move, and the processor must return.
func TestPhase1ErrorOnContextCancelIsNotAFailure(t *testing.T) {
	initPrometheusMetrics()
	prunerPreserveParentsFailing.Set(0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockStore := &utxo.MockUtxostore{}
	mockStore.On("GetPrunableUnminedTxIterator", mock.Anything).
		Run(func(mock.Arguments) { cancel() }).
		Return((*failingIterator)(nil), errors.NewStorageError("boom"))
	// Stubbed so that if the shutdown guard regresses, the flow reaches Phase 1b
	// and the assertions below fail cleanly instead of the mock panicking.
	mockStore.On("ProcessExpiredPreservations", mock.Anything, mock.Anything).Return(nil)

	pruneSvc := &recordingPruner{calls: make(chan uint32, 2)}
	server := newPhase1TestServer(ctx, mockStore, pruneSvc, false)

	errsBefore := getCounterValue(t, prunerErrors, "preserve_parents")
	skippedBefore := getCounterValue(t, prunerSkipped, "preserve_parents_failed")

	done := make(chan struct{})
	go func() {
		server.prunerProcessor(ctx)
		close(done)
	}()
	server.pruneNotify <- pruneSignal{blockHeight: 101, blockHash: chainhash.Hash{}}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("processor did not return after the context was cancelled")
	}

	require.Equal(t, float64(0), testutil.ToFloat64(prunerPreserveParentsFailing), "shutdown must not count as a Phase 1 failure")
	require.Equal(t, errsBefore, getCounterValue(t, prunerErrors, "preserve_parents"))
	require.Equal(t, skippedBefore, getCounterValue(t, prunerSkipped, "preserve_parents_failed"))
	require.Empty(t, pruneSvc.calls, "Phase 2 must not run")
}

// With pruner_skipPreserveParents set, Phase 1 is skipped on purpose, not failed:
// Phase 2 still runs.
func TestSkipPreserveParentsStillPrunes(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockStore := &utxo.MockUtxostore{}
	mockStore.On("ProcessExpiredPreservations", mock.Anything, mock.Anything).Return(nil)

	pruneSvc := &recordingPruner{calls: make(chan uint32, 2)}
	server := newPhase1TestServer(ctx, mockStore, pruneSvc, true)

	runProcessor(t, ctx, server)
	server.pruneNotify <- pruneSignal{blockHeight: 101, blockHash: chainhash.Hash{}}

	select {
	case h := <-pruneSvc.calls:
		require.Equal(t, uint32(101), h)
	case <-time.After(2 * time.Second):
		t.Fatal("Phase 2 did not run with pruner_skipPreserveParents set")
	}

	mockStore.AssertNotCalled(t, "GetPrunableUnminedTxIterator", mock.Anything)
}

// repairHoldClient answers GetState for the overtaken-tip repair hold with whatever hold
// holds, or with next after the first read when next is set; it records blob deletion batch
// requests and makes no other blockchain call.
type repairHoldClient struct {
	blockchain.ClientI
	hold     atomic.Pointer[repairHoldAnswer]
	next     atomic.Pointer[repairHoldAnswer]
	reads    atomic.Int32
	acquired atomic.Int32
	// acquiredAt is the safe height of the last blob deletion batch request.
	acquiredAt atomic.Uint32
	// side holds the hashes of blocks off the best chain; every other block is on it.
	side sync.Map
	// chainChecks counts best chain membership checks, which fail while chainCheckFails is set.
	chainChecks     atomic.Int32
	chainCheckFails atomic.Bool
}

// Block ids repairHoldClient gives a block on the best chain and a block off it.
const (
	bestChainBlockID uint32 = 1
	sideBlockID      uint32 = 2
)

func (c *repairHoldClient) GetBlockHeaderIDs(_ context.Context, blockHash *chainhash.Hash, _ uint64) ([]uint32, error) {
	if _, ok := c.side.Load(*blockHash); ok {
		return []uint32{sideBlockID}, nil
	}

	return []uint32{bestChainBlockID}, nil
}

func (c *repairHoldClient) CheckBlockIsInCurrentChain(_ context.Context, blockIDs []uint32) (bool, error) {
	c.chainChecks.Add(1)

	if c.chainCheckFails.Load() {
		return false, errors.NewStorageError("chain check failed")
	}

	return len(blockIDs) == 1 && blockIDs[0] == bestChainBlockID, nil
}

func (c *repairHoldClient) AcquireBlobDeletionBatch(_ context.Context, safeHeight uint32, _ int, _ int, _ []storetypes.BlobStoreType) (string, []*blockchain_api.ScheduledDeletion, error) {
	c.acquiredAt.Store(safeHeight)
	c.acquired.Add(1)

	return "", nil, nil
}

type repairHoldAnswer struct {
	data []byte
	err  error
}

func (c *repairHoldClient) GetState(_ context.Context, key string) ([]byte, error) {
	if key != model.OvertakenTipRepairHoldKey {
		return nil, errors.NewProcessingError("unexpected state key %s", key)
	}

	a := c.hold.Load()
	if c.reads.Add(1) > 1 && c.next.Load() != nil {
		a = c.next.Load()
	}

	return a.data, a.err
}

func newRepairHoldTestServer(ctx context.Context, client *repairHoldClient, pruneSvc *recordingPruner) *Server {
	mockStore := &utxo.MockUtxostore{}
	mockStore.On("GetPrunableUnminedTxIterator", mock.Anything).Return(emptyIterator{}, nil)
	mockStore.On("ProcessExpiredPreservations", mock.Anything, mock.Anything).Return(nil)

	server := newPhase1TestServer(ctx, mockStore, pruneSvc, false)
	server.blockchainClient = client

	return server
}

// While block validation holds pruning for an overtaken block whose repair has not
// finished, the pruner deletes nothing in phase 2 and the height is not advanced (the blob
// worker checks the hold itself, see TestRepairHoldStopsBlobDeletion). Once the hold is
// released, the next block prunes as usual.
func TestRepairHoldSkipsDeletion(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	held := model.EncodeOvertakenTipRepairHold([]model.HeldBlock{{Hash: chainhash.HashH([]byte("b")), ID: 7, Height: 90}})

	client := &repairHoldClient{}
	client.hold.Store(&repairHoldAnswer{data: held})

	pruneSvc := &recordingPruner{calls: make(chan uint32, 2)}
	server := newRepairHoldTestServer(ctx, client, pruneSvc)

	skippedBefore := getCounterValue(t, prunerSkipped, "overtaken_tip_repair_hold")

	runProcessor(t, ctx, server)
	server.pruneNotify <- pruneSignal{blockHeight: 101, blockHash: chainhash.Hash{}}

	require.Eventually(t, func() bool {
		return getCounterValue(t, prunerSkipped, "overtaken_tip_repair_hold") == skippedBefore+1
	}, 2*time.Second, 10*time.Millisecond, "the cycle is counted as held")

	require.Empty(t, pruneSvc.calls, "no DAH deletion while held")
	require.Equal(t, uint32(0), server.lastProcessedHeight.Load(), "the height is retried")
	require.Equal(t, float64(1), testutil.ToFloat64(prunerOvertakenTipRepairHeldBlocks))

	client.hold.Store(&repairHoldAnswer{data: model.EncodeOvertakenTipRepairHold(nil)})
	server.pruneNotify <- pruneSignal{blockHeight: 102, blockHash: chainhash.Hash{}}

	select {
	case h := <-pruneSvc.calls:
		require.Equal(t, uint32(102), h)
	case <-time.After(2 * time.Second):
		t.Fatal("pruning did not resume once the hold was released")
	}

	require.Equal(t, float64(0), testutil.ToFloat64(prunerOvertakenTipRepairHeldBlocks))
}

// A node whose block validation has never held pruning has no hold record: that is not a
// hold.
func TestRepairHoldMissingIsNotHeld(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := &repairHoldClient{}
	client.hold.Store(&repairHoldAnswer{err: errors.NewNotFoundError("no state for key %s", model.OvertakenTipRepairHoldKey, sql.ErrNoRows)})

	pruneSvc := &recordingPruner{calls: make(chan uint32, 2)}
	server := newRepairHoldTestServer(ctx, client, pruneSvc)

	runProcessor(t, ctx, server)
	server.pruneNotify <- pruneSignal{blockHeight: 101, blockHash: chainhash.Hash{}}

	select {
	case h := <-pruneSvc.calls:
		require.Equal(t, uint32(101), h)
	case <-time.After(2 * time.Second):
		t.Fatal("a missing hold record held pruning")
	}
}

// A hold that cannot be read may be holding: the cycle deletes nothing and is retried.
func TestRepairHoldReadErrorHolds(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := &repairHoldClient{}
	client.hold.Store(&repairHoldAnswer{err: errors.NewServiceError("blockchain unavailable")})

	pruneSvc := &recordingPruner{calls: make(chan uint32, 2)}
	server := newRepairHoldTestServer(ctx, client, pruneSvc)

	errsBefore := getCounterValue(t, prunerErrors, "overtaken_tip_repair_hold")

	runProcessor(t, ctx, server)
	server.pruneNotify <- pruneSignal{blockHeight: 101, blockHash: chainhash.Hash{}}

	require.Eventually(t, func() bool {
		return getCounterValue(t, prunerErrors, "overtaken_tip_repair_hold") == errsBefore+1
	}, 2*time.Second, 10*time.Millisecond)

	require.Empty(t, pruneSvc.calls, "no DAH deletion when the hold cannot be read")
	require.Equal(t, uint32(0), server.lastProcessedHeight.Load())
}

// A hold written while a cycle's phase 1 runs must still stop that cycle's phase 2: the hold
// is read again just before deleting.
func TestRepairHoldReadAgainBeforePhase2(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := &repairHoldClient{}
	client.hold.Store(&repairHoldAnswer{data: model.EncodeOvertakenTipRepairHold(nil)})
	client.next.Store(&repairHoldAnswer{data: model.EncodeOvertakenTipRepairHold([]model.HeldBlock{{Hash: chainhash.HashH([]byte("b")), ID: 7, Height: 90}})})

	pruneSvc := &recordingPruner{calls: make(chan uint32, 2)}
	server := newRepairHoldTestServer(ctx, client, pruneSvc)

	skippedBefore := getCounterValue(t, prunerSkipped, "overtaken_tip_repair_hold")

	runProcessor(t, ctx, server)
	server.pruneNotify <- pruneSignal{blockHeight: 101, blockHash: chainhash.Hash{}}

	require.Eventually(t, func() bool {
		return getCounterValue(t, prunerSkipped, "overtaken_tip_repair_hold") == skippedBefore+1
	}, 2*time.Second, 10*time.Millisecond, "the cycle is held by the second read")

	require.Empty(t, pruneSvc.calls, "no DAH deletion")
	require.Equal(t, uint32(0), server.lastProcessedHeight.Load())
}

// The blob deletion worker checks the hold itself: a signal queued before the hold was
// written, or a pass already under way, must not delete an overtaken block's subtrees while
// its repair still needs them.
func TestRepairHoldStopsBlobDeletion(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := &repairHoldClient{}
	client.hold.Store(&repairHoldAnswer{data: model.EncodeOvertakenTipRepairHold([]model.HeldBlock{{Hash: chainhash.HashH([]byte("b")), ID: 7, Height: 90}})})

	server := newRepairHoldTestServer(ctx, client, &recordingPruner{calls: make(chan uint32, 1)})
	server.settings.Pruner.BlobDeletionBatchSize = 10

	server.processBlobDeletionsAtHeight(101, chainhash.Hash{})
	require.Zero(t, client.acquired.Load(), "no deletion batch is taken while held")

	client.hold.Store(&repairHoldAnswer{data: model.EncodeOvertakenTipRepairHold(nil)})

	server.processBlobDeletionsAtHeight(102, chainhash.Hash{})
	require.NotZero(t, client.acquired.Load(), "deletion resumes once the hold is released")
}

// newBlobRepairHoldServer returns a server whose blob passes check block assembly, which is
// running at ba's height, and read the hold from client.
func newBlobRepairHoldServer(ctx context.Context, client *repairHoldClient, ba *baHeightClient) *Server {
	server := newRepairHoldTestServer(ctx, client, &recordingPruner{calls: make(chan uint32, 1)})
	server.blockAssemblyClient = ba
	server.settings.Pruner.BlockTrigger = settings.PrunerBlockTriggerOnBlockPersisted
	server.settings.Pruner.BlockAssemblyWaitTimeout = 50 * time.Millisecond
	server.settings.Pruner.BlobDeletionBatchSize = 10
	server.repairHoldRecheckInterval = 20 * time.Millisecond

	return server
}

// A blob deletion pass skipped for the hold is tried again once the hold is released, without
// another block: the pruner may already have finished that height (a hold written after its
// last read) or be waiting out the phase 1 gap cooldown, and then nothing else wakes the blob
// worker on an idle chain. After a reorg to a lower tip, the retry runs at the lower height,
// which block assembly accepts, and it stops once a pass runs.
func TestRepairHoldReleaseResumesIdleBlobDeletion(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := &repairHoldClient{}
	client.hold.Store(&repairHoldAnswer{data: model.EncodeOvertakenTipRepairHold(testHeldBlock)})

	ba := &baHeightClient{}
	server := newBlobRepairHoldServer(ctx, client, ba)

	runBlobWorker(t, server)

	for _, h := range []uint32{102, 101} {
		ba.height.Store(h)

		reads := client.reads.Load()
		server.blobNotify <- pruneSignal{blockHeight: h}
		require.Eventually(t, func() bool { return client.reads.Load() > reads }, 2*time.Second, 5*time.Millisecond)
	}

	require.Never(t, func() bool { return client.acquired.Load() > 0 }, 100*time.Millisecond, 10*time.Millisecond, "precondition: held")

	client.hold.Store(&repairHoldAnswer{data: model.EncodeOvertakenTipRepairHold(nil)})

	require.Eventually(t, func() bool { return client.acquired.Load() == 1 }, 2*time.Second, 5*time.Millisecond, "the held pass runs once the hold is released")
	require.Equal(t, uint32(101), client.acquiredAt.Load(), "at the height of the latest signal")
	require.Never(t, func() bool { return client.acquired.Load() > 1 }, 100*time.Millisecond, 10*time.Millisecond, "the retry stops once a pass ran")
}

// A pass held at one height, followed after the release by a signal for a lower winning tip
// (a shorter chain with more work), deletes at the lower height: the pending retry for the
// height the chain left does not replace the newer signal.
func TestRepairHoldBlobRetryFollowsLowerTipAfterRelease(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := &repairHoldClient{}
	client.hold.Store(&repairHoldAnswer{data: model.EncodeOvertakenTipRepairHold(testHeldBlock)})

	ba := &baHeightClient{}
	ba.height.Store(102)

	server := newBlobRepairHoldServer(ctx, client, ba)
	server.repairHoldRecheckInterval = time.Hour

	runBlobWorker(t, server)

	server.blobNotify <- pruneSignal{blockHeight: 102}
	require.Eventually(t, func() bool { return client.reads.Load() > 0 }, 2*time.Second, 5*time.Millisecond, "the pass at 102 is held")

	client.hold.Store(&repairHoldAnswer{data: model.EncodeOvertakenTipRepairHold(nil)})
	ba.height.Store(101)
	server.blobNotify <- pruneSignal{blockHeight: 101}

	require.Eventually(t, func() bool { return client.acquired.Load() > 0 }, 2*time.Second, 5*time.Millisecond, "the pass at the lower tip runs")
	require.Equal(t, uint32(101), client.acquiredAt.Load())
}

// A retry signal from a timer that fired just before its retry was replaced or dropped is
// itself dropped, and the pending retry still runs once the hold is released.
func TestRepairHoldStaleBlobRetryIsDropped(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := &repairHoldClient{}
	client.hold.Store(&repairHoldAnswer{data: model.EncodeOvertakenTipRepairHold(testHeldBlock)})

	ba := &baHeightClient{}
	ba.height.Store(102)

	server := newBlobRepairHoldServer(ctx, client, ba)
	server.repairHoldRecheckInterval = time.Second

	runBlobWorker(t, server)

	// With no retry pending, a retry signal is stale too.
	server.blobNotify <- pruneSignal{blockHeight: 102, retry: true}
	require.Never(t, func() bool { return client.reads.Load() > 0 }, 100*time.Millisecond, 10*time.Millisecond, "the stale retry runs no pass")

	server.blobNotify <- pruneSignal{blockHeight: 102}
	require.Eventually(t, func() bool { return client.reads.Load() > 0 }, 2*time.Second, 5*time.Millisecond, "the pass at 102 is held")

	// 101's timer fired just before 102's pass replaced it, and its signal arrives now.
	reads := client.reads.Load()
	server.blobNotify <- pruneSignal{blockHeight: 101, retry: true}
	require.Never(t, func() bool { return client.reads.Load() > reads }, 100*time.Millisecond, 10*time.Millisecond, "the stale retry runs no pass")

	client.hold.Store(&repairHoldAnswer{data: model.EncodeOvertakenTipRepairHold(nil)})

	require.Eventually(t, func() bool { return client.acquired.Load() > 0 }, 2*time.Second, 5*time.Millisecond, "the pending retry runs")
	require.Equal(t, uint32(102), client.acquiredAt.Load())
}

// A held cycle that enters the phase 1 gap cooldown still gets its blob deletion once the
// hold is released, without another block, and phase 2 still waits for the cooldown to end.
func TestRepairHoldReleaseInGapCooldownResumesBlobDeletion(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server, pruneSvc := newPhase1GapTestServer(t, ctx)
	client := &blobCountingClient{ClientI: server.blockchainClient}
	server.blockchainClient = client
	server.settings.Pruner.BlobDeletionBatchSize = 10
	server.settings.Pruner.BlobDeletionSafetyWindow = 0
	server.repairHoldRecheckInterval = 20 * time.Millisecond

	setPreservationRecord(t, server, parentPreservationRecord{lastSuccess: 1100, phase2From: 1200})
	require.NoError(t, client.SetState(ctx, model.OvertakenTipRepairHoldKey, model.EncodeOvertakenTipRepairHold(testHeldBlock)))

	runProcessor(t, ctx, server)
	runBlobWorker(t, server)

	requirePhase2Skipped(t, server, pruneSvc, 1150)
	require.Never(t, func() bool { return client.acquired.Load() > 0 }, 100*time.Millisecond, 10*time.Millisecond, "precondition: held")

	require.NoError(t, client.SetState(ctx, model.OvertakenTipRepairHoldKey, model.EncodeOvertakenTipRepairHold(nil)))

	require.Eventually(t, func() bool { return client.acquired.Load() > 0 }, 2*time.Second, 5*time.Millisecond, "blob deletion runs once the hold is released")
	require.Equal(t, uint32(1150), client.acquiredAt.Load())
	require.Never(t, func() bool { return len(pruneSvc.calls) > 0 }, 100*time.Millisecond, 10*time.Millisecond, "phase 2 waits out the cooldown")
}

// A held pass's retry survives a later pass that stops before reading the hold, because block
// assembly has not caught up: once both are ready, the held blobs are deleted without another
// block.
func TestRepairHoldBlobRetrySurvivesNotReadyPass(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := &repairHoldClient{}
	client.hold.Store(&repairHoldAnswer{data: model.EncodeOvertakenTipRepairHold(testHeldBlock)})

	ba := &baHeightClient{}
	ba.height.Store(102)

	server := newRepairHoldTestServer(ctx, client, &recordingPruner{calls: make(chan uint32, 1)})
	server.blockAssemblyClient = ba
	server.settings.Pruner.BlockTrigger = settings.PrunerBlockTriggerOnBlockPersisted
	server.settings.Pruner.BlockAssemblyWaitTimeout = 50 * time.Millisecond
	server.settings.Pruner.BlobDeletionBatchSize = 10
	server.repairHoldRecheckInterval = 20 * time.Millisecond

	runBlobWorker(t, server)

	server.blobNotify <- pruneSignal{blockHeight: 102}
	require.Eventually(t, func() bool { return client.reads.Load() > 0 }, 2*time.Second, 5*time.Millisecond, "the pass at 102 is held")

	// Block assembly is still at 102, so the pass at 103 stops before it reads the hold.
	calls := ba.calls.Load()
	server.blobNotify <- pruneSignal{blockHeight: 103}
	require.Eventually(t, func() bool { return ba.calls.Load() > calls }, 2*time.Second, 5*time.Millisecond)

	client.hold.Store(&repairHoldAnswer{data: model.EncodeOvertakenTipRepairHold(nil)})
	ba.height.Store(103)

	require.Eventually(t, func() bool { return client.acquired.Load() > 0 }, 2*time.Second, 5*time.Millisecond, "the held blobs are deleted once both are ready")
	require.Equal(t, uint32(103), client.acquiredAt.Load())
}

// baHeightClient reports block assembly running at height.
type baHeightClient struct {
	blockassembly.ClientI
	height atomic.Uint32
	calls  atomic.Int32
}

func (c *baHeightClient) GetBlockAssemblyState(context.Context) (*blockassembly_api.StateMessage, error) {
	c.calls.Add(1)

	return &blockassembly_api.StateMessage{BlockAssemblyState: "running", CurrentHeight: c.height.Load()}, nil
}

// blobCountingClient records blob deletion batch requests and passes every other call on.
type blobCountingClient struct {
	blockchain.ClientI
	acquired   atomic.Int32
	acquiredAt atomic.Uint32
}

func (c *blobCountingClient) AcquireBlobDeletionBatch(_ context.Context, safeHeight uint32, _ int, _ int, _ []storetypes.BlobStoreType) (string, []*blockchain_api.ScheduledDeletion, error) {
	c.acquiredAt.Store(safeHeight)
	c.acquired.Add(1)

	return "", nil, nil
}

// blockingPruner blocks Prune until its context is done or release is closed, and reports
// whether the context was cancelled.
type blockingPruner struct {
	prunerstore.Service
	started   chan struct{}
	release   chan struct{}
	cancelled atomic.Bool
	calls     chan uint32
}

func (p *blockingPruner) Prune(ctx context.Context, height uint32, _ string) (int64, error) {
	p.calls <- height
	close(p.started)

	select {
	case <-ctx.Done():
		p.cancelled.Store(true)
		return 0, ctx.Err()
	case <-p.release:
		return 0, nil
	}
}

// A hold written while phase 2 is already deleting stops the deletion: the pruner watches the
// hold while Prune runs and cancels it, and the height is retried.
func TestRepairHoldCancelsRunningPrune(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := &repairHoldClient{}
	client.hold.Store(&repairHoldAnswer{data: model.EncodeOvertakenTipRepairHold(nil)})

	pruneSvc := &blockingPruner{started: make(chan struct{}), release: make(chan struct{}), calls: make(chan uint32, 2)}
	defer close(pruneSvc.release)

	mockStore := &utxo.MockUtxostore{}
	mockStore.On("GetPrunableUnminedTxIterator", mock.Anything).Return(emptyIterator{}, nil)
	mockStore.On("ProcessExpiredPreservations", mock.Anything, mock.Anything).Return(nil)

	server := newPhase1TestServer(ctx, mockStore, nil, false)
	server.prunerService = pruneSvc
	server.blockchainClient = client
	server.repairHoldPollInterval = 10 * time.Millisecond
	server.repairHoldRecheckInterval = time.Hour

	runProcessor(t, ctx, server)
	server.pruneNotify <- pruneSignal{blockHeight: 101, blockHash: chainhash.Hash{}}

	select {
	case <-pruneSvc.started:
	case <-time.After(2 * time.Second):
		t.Fatal("phase 2 did not start")
	}

	client.hold.Store(&repairHoldAnswer{data: model.EncodeOvertakenTipRepairHold([]model.HeldBlock{{Hash: chainhash.HashH([]byte("b")), ID: 7, Height: 90}})})

	require.Eventually(t, pruneSvc.cancelled.Load, 2*time.Second, 10*time.Millisecond, "the running deletion is cancelled")
	require.Never(t, func() bool { return server.lastProcessedHeight.Load() != 0 }, 200*time.Millisecond, 20*time.Millisecond, "the height is retried")
}

// On an idle chain, a held height is pruned once the hold is released, without waiting for
// another block.
func TestRepairHoldReleaseResumesIdlePruning(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := &repairHoldClient{}
	client.hold.Store(&repairHoldAnswer{data: model.EncodeOvertakenTipRepairHold([]model.HeldBlock{{Hash: chainhash.HashH([]byte("b")), ID: 7, Height: 90}})})

	pruneSvc := &recordingPruner{calls: make(chan uint32, 2)}
	server := newRepairHoldTestServer(ctx, client, pruneSvc)
	server.repairHoldRecheckInterval = 20 * time.Millisecond

	runProcessor(t, ctx, server)
	server.pruneNotify <- pruneSignal{blockHeight: 101, blockHash: chainhash.Hash{}}

	require.Never(t, func() bool { return len(pruneSvc.calls) > 0 }, 100*time.Millisecond, 10*time.Millisecond, "precondition: held")

	client.hold.Store(&repairHoldAnswer{data: model.EncodeOvertakenTipRepairHold(nil)})

	select {
	case h := <-pruneSvc.calls:
		require.Equal(t, uint32(101), h)
	case <-time.After(2 * time.Second):
		t.Fatal("the held height was not pruned once the hold was released")
	}
}

var testHeldBlock = []model.HeldBlock{{Hash: chainhash.HashH([]byte("b")), ID: 7, Height: 90}}

// A retry timer for a lower held height can fire after a higher height was held. Its signal
// is dropped without a cycle and does not replace the pending retry, so the release prunes the
// higher height without waiting for the next block.
func TestRepairHoldStaleRetryKeepsPendingRetry(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())

	client := &repairHoldClient{}
	client.hold.Store(&repairHoldAnswer{data: model.EncodeOvertakenTipRepairHold(testHeldBlock)})

	pruneSvc := &recordingPruner{calls: make(chan uint32, 4)}
	server := newRepairHoldTestServer(ctx, client, pruneSvc)
	server.repairHoldRecheckInterval = time.Hour

	done := make(chan struct{})
	go func() {
		server.prunerProcessor(ctx)
		close(done)
	}()

	server.pruneNotify <- pruneSignal{blockHeight: 101}
	require.Eventually(t, func() bool { return client.reads.Load() == 1 }, 2*time.Second, 5*time.Millisecond)
	server.pruneNotify <- pruneSignal{blockHeight: 102}
	require.Eventually(t, func() bool { return client.reads.Load() == 2 }, 2*time.Second, 5*time.Millisecond)

	// 101's timer fired just before 102's cycle replaced it, and its signal arrives now.
	server.pruneNotify <- pruneSignal{blockHeight: 101, retry: true}
	require.Never(t, func() bool { return client.reads.Load() > 2 }, 100*time.Millisecond, 10*time.Millisecond, "the stale retry runs no cycle")

	cancel()
	<-done

	require.NotNil(t, server.repairHoldRetry, "a retry is pending")

	// Release the hold and fire the pending retry now, on an idle chain.
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	client.hold.Store(&repairHoldAnswer{data: model.EncodeOvertakenTipRepairHold(nil)})
	server.repairHoldRetry.Reset(0)

	runProcessor(t, ctx2, server)

	select {
	case h := <-pruneSvc.calls:
		require.Equal(t, uint32(102), h, "the pending retry's height is the one retried")
	case <-time.After(2 * time.Second):
		t.Fatal("the held height was not retried")
	}
}

// After a reorg to a lower tip (a shorter chain with more work) while pruning is held, the
// retry takes the lower height of the newer signal: the higher height the chain left is never
// reached again on an idle chain, so retrying it would never prune.
func TestRepairHoldRetryFollowsLowerTip(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := &repairHoldClient{}
	client.hold.Store(&repairHoldAnswer{data: model.EncodeOvertakenTipRepairHold(testHeldBlock)})

	pruneSvc := &recordingPruner{calls: make(chan uint32, 4)}
	server := newRepairHoldTestServer(ctx, client, pruneSvc)
	server.repairHoldRecheckInterval = 20 * time.Millisecond

	runProcessor(t, ctx, server)

	server.pruneNotify <- pruneSignal{blockHeight: 102}
	require.Eventually(t, func() bool { return client.reads.Load() >= 1 }, 2*time.Second, 5*time.Millisecond)

	reads := client.reads.Load()
	server.pruneNotify <- pruneSignal{blockHeight: 101}
	require.Eventually(t, func() bool { return client.reads.Load() > reads }, 2*time.Second, 5*time.Millisecond)

	client.hold.Store(&repairHoldAnswer{data: model.EncodeOvertakenTipRepairHold(nil)})

	select {
	case h := <-pruneSvc.calls:
		require.Equal(t, uint32(101), h, "the lower tip is pruned")
	case <-time.After(2 * time.Second):
		t.Fatal("the held height was not pruned once the hold was released")
	}
}

// A held retry moves to a newer signal only when that signal's block is on the best chain. A
// reorg to a shorter chain with more work moves it to the lower tip; a side block below the tip
// leaves it at the tip, and so does a lower block whose chain cannot be checked. Each holds
// on an idle chain, with no block after the lower one, in the pruner processor and in the blob
// deletion worker.
func TestRepairHoldRetryFollowsBestChainOnly(t *testing.T) {
	tip, lower := chainhash.HashH([]byte("102")), chainhash.HashH([]byte("101"))

	for _, tc := range []struct {
		name       string
		lowerWins  bool
		checkFails bool
		want       uint32
	}{
		{"lower winning tip", true, false, 101},
		{"lower side block", false, false, 102},
		{"lower block chain check fails", false, true, 102},
	} {
		// reorg marks the block that is off the best chain once the lower block arrives.
		reorg := func(client *repairHoldClient) {
			client.chainCheckFails.Store(tc.checkFails)

			if tc.lowerWins {
				client.side.Store(tip, struct{}{})
			} else {
				client.side.Store(lower, struct{}{})
			}
		}

		t.Run(tc.name+"/processor", func(t *testing.T) {
			initPrometheusMetrics()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			client := &repairHoldClient{}
			client.hold.Store(&repairHoldAnswer{data: model.EncodeOvertakenTipRepairHold(testHeldBlock)})

			pruneSvc := &recordingPruner{calls: make(chan uint32, 4)}
			server := newRepairHoldTestServer(ctx, client, pruneSvc)
			server.repairHoldRecheckInterval = 20 * time.Millisecond

			runProcessor(t, ctx, server)

			server.pruneNotify <- pruneSignal{blockHeight: 102, blockHash: tip}
			require.Eventually(t, func() bool { return client.reads.Load() > 0 }, 2*time.Second, 5*time.Millisecond, "102 is held")

			reorg(client)
			server.pruneNotify <- pruneSignal{blockHeight: 101, blockHash: lower}
			require.Eventually(t, func() bool { return client.chainChecks.Load() > 0 }, 2*time.Second, 5*time.Millisecond, "the lower block's chain is checked")

			client.hold.Store(&repairHoldAnswer{data: model.EncodeOvertakenTipRepairHold(nil)})

			select {
			case h := <-pruneSvc.calls:
				require.Equal(t, tc.want, h)
			case <-time.After(2 * time.Second):
				t.Fatal("the held height was not pruned once the hold was released")
			}
		})

		t.Run(tc.name+"/blob worker", func(t *testing.T) {
			initPrometheusMetrics()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			client := &repairHoldClient{}
			client.hold.Store(&repairHoldAnswer{data: model.EncodeOvertakenTipRepairHold(testHeldBlock)})

			ba := &baHeightClient{}
			ba.height.Store(102)

			server := newBlobRepairHoldServer(ctx, client, ba)

			runBlobWorker(t, server)

			server.blobNotify <- pruneSignal{blockHeight: 102, blockHash: tip}
			require.Eventually(t, func() bool { return client.reads.Load() > 0 }, 2*time.Second, 5*time.Millisecond, "102 is held")

			reorg(client)
			if tc.lowerWins {
				ba.height.Store(101)
			}

			server.blobNotify <- pruneSignal{blockHeight: 101, blockHash: lower}
			require.Eventually(t, func() bool { return client.chainChecks.Load() > 0 }, 2*time.Second, 5*time.Millisecond, "the lower block's chain is checked")

			client.hold.Store(&repairHoldAnswer{data: model.EncodeOvertakenTipRepairHold(nil)})

			require.Eventually(t, func() bool { return client.acquired.Load() > 0 }, 2*time.Second, 5*time.Millisecond, "the held blobs are deleted once the hold is released")
			require.Equal(t, tc.want, client.acquiredAt.Load())
		})
	}
}

// A block signal at or below the last processed height is dropped, but it may have displaced
// the pending retry's signal from the channel after its timer fired. The pending retry is armed
// again, so the held height is still pruned on an idle chain.
func TestRepairHoldDroppedSignalRearmsRetry(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())

	client := &repairHoldClient{}
	client.hold.Store(&repairHoldAnswer{data: model.EncodeOvertakenTipRepairHold(testHeldBlock)})

	pruneSvc := &recordingPruner{calls: make(chan uint32, 4)}
	server := newRepairHoldTestServer(ctx, client, pruneSvc)
	server.repairHoldRecheckInterval = time.Hour

	done := make(chan struct{})
	go func() {
		server.prunerProcessor(ctx)
		close(done)
	}()

	server.pruneNotify <- pruneSignal{blockHeight: 102}
	require.Eventually(t, func() bool { return client.reads.Load() > 0 }, 2*time.Second, 5*time.Millisecond, "102 is held")

	cancel()
	<-done

	// The retry timer fired, and its signal was displaced by a block the processor is already past.
	require.NotNil(t, server.repairHoldRetry, "a retry is pending")
	server.repairHoldRetry.Stop()
	server.lastProcessedHeight.Store(101)
	server.repairHoldRecheckInterval = 20 * time.Millisecond
	client.hold.Store(&repairHoldAnswer{data: model.EncodeOvertakenTipRepairHold(nil)})

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	runProcessor(t, ctx2, server)
	server.pruneNotify <- pruneSignal{blockHeight: 101}

	select {
	case h := <-pruneSvc.calls:
		require.Equal(t, uint32(102), h)
	case <-time.After(2 * time.Second):
		t.Fatal("the held height was not retried after a dropped signal displaced its retry")
	}
}

// A cycle run by the held-height retry wakes the blob worker with a block signal, which the
// blob worker runs rather than dropping as a stale retry of its own.
func TestRepairHoldRetryCycleWakesBlobWorker(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := &repairHoldClient{}
	client.hold.Store(&repairHoldAnswer{data: model.EncodeOvertakenTipRepairHold(testHeldBlock)})

	server := newRepairHoldTestServer(ctx, client, &recordingPruner{calls: make(chan uint32, 4)})
	server.settings.Pruner.BlobDeletionBatchSize = 10
	server.repairHoldRecheckInterval = 20 * time.Millisecond

	runProcessor(t, ctx, server)
	server.pruneNotify <- pruneSignal{blockHeight: 101}

	// Take the held cycle's wake, so only the retry cycle can wake the blob worker.
	select {
	case <-server.blobNotify:
	case <-time.After(2 * time.Second):
		t.Fatal("the held cycle did not wake the blob worker")
	}

	client.hold.Store(&repairHoldAnswer{data: model.EncodeOvertakenTipRepairHold(nil)})
	runBlobWorker(t, server)

	require.Eventually(t, func() bool { return client.acquired.Load() > 0 }, 2*time.Second, 5*time.Millisecond, "the retry cycle's blob pass runs")
	require.Equal(t, uint32(101), client.acquiredAt.Load())
}

// A retried height whose phase 1 fails keeps its retry: once the store recovers, the height
// is pruned without another block.
func TestRepairHoldRetrySurvivesPhase1Failure(t *testing.T) {
	initPrometheusMetrics()
	prunerPreserveParentsFailing.Set(0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := &repairHoldClient{}
	client.hold.Store(&repairHoldAnswer{data: model.EncodeOvertakenTipRepairHold(testHeldBlock)})

	mockStore := &utxo.MockUtxostore{}
	mockStore.On("GetPrunableUnminedTxIterator", mock.Anything).Return(emptyIterator{}, nil).Once()
	mockStore.On("GetPrunableUnminedTxIterator", mock.Anything).Return((*failingIterator)(nil), errors.NewStorageError("boom")).Once()
	mockStore.On("GetPrunableUnminedTxIterator", mock.Anything).Return(emptyIterator{}, nil)
	mockStore.On("ProcessExpiredPreservations", mock.Anything, mock.Anything).Return(nil)

	pruneSvc := &recordingPruner{calls: make(chan uint32, 4)}
	server := newPhase1TestServer(ctx, mockStore, pruneSvc, false)
	server.blockchainClient = client
	server.repairHoldRecheckInterval = 20 * time.Millisecond

	runProcessor(t, ctx, server)
	server.pruneNotify <- pruneSignal{blockHeight: 101}

	require.Eventually(t, func() bool { return client.reads.Load() >= 1 }, 2*time.Second, 5*time.Millisecond)
	client.hold.Store(&repairHoldAnswer{data: model.EncodeOvertakenTipRepairHold(nil)})

	select {
	case h := <-pruneSvc.calls:
		require.Equal(t, uint32(101), h)
	case <-time.After(2 * time.Second):
		t.Fatal("the held height was not pruned after phase 1 recovered")
	}

	mockStore.AssertNumberOfCalls(t, "GetPrunableUnminedTxIterator", 3)
}

// A retried height that is still held skips the whole cycle: phase 1 already ran for it, so
// a long hold does not rescan unmined transactions every recheck interval.
func TestRepairHoldRetryWhileHeldSkipsPhase1(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := &repairHoldClient{}
	client.hold.Store(&repairHoldAnswer{data: model.EncodeOvertakenTipRepairHold(testHeldBlock)})

	pruneSvc := &recordingPruner{calls: make(chan uint32, 2)}
	server := newRepairHoldTestServer(ctx, client, pruneSvc)
	server.repairHoldRecheckInterval = 5 * time.Millisecond

	runProcessor(t, ctx, server)
	server.pruneNotify <- pruneSignal{blockHeight: 101}

	require.Eventually(t, func() bool { return client.reads.Load() >= 5 }, 2*time.Second, 5*time.Millisecond, "the held height is retried")

	mockStore := server.utxoStore.(*utxo.MockUtxostore)
	mockStore.AssertNumberOfCalls(t, "GetPrunableUnminedTxIterator", 1)
	mockStore.AssertNumberOfCalls(t, "ProcessExpiredPreservations", 1)
	require.Empty(t, pruneSvc.calls)
}

// On mainnet and testnet the pruner refuses to start when the gap between the UTXO retention
// and the unmined retention is below the deepest fork the node accepts (CoinbaseMaturity): a
// hold written after a cycle's last check would then not be enough to protect a due parent.
// Other networks only log it.
func TestInitRefusesInsufficientRepairHoldSlack(t *testing.T) {
	for _, tc := range []struct {
		name      string
		params    chaincfg.Params
		retention uint32
		refuse    bool
	}{
		{"mainnet", chaincfg.MainNetParams, 150, true},
		{"testnet", chaincfg.TestNetParams, 150, true},
		{"regtest", chaincfg.RegressionNetParams, 150, false},
		// Slack equal to CoinbaseMaturity leaves no room for a gap between phase 1 runs.
		{"mainnet slack equals maturity", chaincfg.MainNetParams, 200, true},
		{"mainnet slack above maturity", chaincfg.MainNetParams, 201, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			params := tc.params
			server := &Server{
				logger: ulogger.TestLogger{},
				settings: &settings.Settings{
					ChainCfgParams:             &params,
					GlobalBlockHeightRetention: tc.retention,
					UtxoStore:                  settings.UtxoStoreSettings{UnminedTxRetention: 100},
				},
			}

			err := server.Init(context.Background())
			require.Error(t, err, "no UTXO store is set, so Init fails either way")

			if tc.refuse {
				require.Contains(t, err.Error(), "repair hold slack")
			} else {
				require.NotContains(t, err.Error(), "repair hold slack")
			}
		})
	}
}

// failingSetStateClient is a blockchain client whose state writes fail until recovered is set.
type failingSetStateClient struct {
	blockchain.ClientI
	recovered atomic.Bool
}

func (c *failingSetStateClient) SetState(ctx context.Context, key string, data []byte) error {
	if !c.recovered.Load() {
		return errors.NewStorageError("state write failed")
	}

	return c.ClientI.SetState(ctx, key, data)
}

// serviceStateClient reads and writes blockchain state through the blockchain service's own
// handlers, with the protobuf round trip the gRPC client adds: a missing key arrives as a
// gRPC error and empty bytes as nil.
type serviceStateClient struct {
	blockchain.ClientI
	service *blockchain.Blockchain
}

func (c *serviceStateClient) GetState(ctx context.Context, key string) ([]byte, error) {
	resp, err := c.service.GetState(ctx, &blockchain_api.GetStateRequest{Key: key})
	if err != nil {
		return nil, errors.UnwrapGRPC(err)
	}

	wire, err := proto.Marshal(resp)
	if err != nil {
		return nil, err
	}

	var got blockchain_api.StateResponse
	if err = proto.Unmarshal(wire, &got); err != nil {
		return nil, err
	}

	return got.Data, nil
}

func (c *serviceStateClient) SetState(ctx context.Context, key string, data []byte) error {
	wire, err := proto.Marshal(&blockchain_api.SetStateRequest{Key: key, Data: data})
	if err != nil {
		return err
	}

	var req blockchain_api.SetStateRequest
	if err = proto.Unmarshal(wire, &req); err != nil {
		return err
	}

	if _, err = c.service.SetState(ctx, &req); err != nil {
		return errors.UnwrapGRPC(err)
	}

	return nil
}

// newPhase1GapTestServer builds a pruner with mainnet's CoinbaseMaturity (100), the default
// retentions (288 and 144, so phase 1 may miss up to 44 blocks) and a blockchain service on a
// sqlitememory store, which holds the parent preservation record.
func newPhase1GapTestServer(t *testing.T, ctx context.Context) (*Server, *recordingPruner) {
	t.Helper()

	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	params := chaincfg.MainNetParams
	tSettings.ChainCfgParams = &params
	tSettings.GlobalBlockHeightRetention = 288
	tSettings.UtxoStore.BlockHeightRetentionAdjustment = 0
	tSettings.UtxoStore.UnminedTxRetention = 144
	tSettings.UtxoStore.ParentPreservationBlocks = 1440
	tSettings.Pruner.MinBlockHeight = 0
	tSettings.Pruner.SkipDuringCatchup = false
	tSettings.Pruner.SkipPreserveParents = false
	tSettings.Pruner.SkipProcessExpiredPreservations = false

	chainURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	chainStore, err := blockchainsql.New(logger, chainURL, tSettings)
	require.NoError(t, err)
	t.Cleanup(func() { _ = chainStore.Close(context.Background()) })

	service, err := blockchain.New(ctx, logger, tSettings, chainStore, nil)
	require.NoError(t, err)

	client := &serviceStateClient{service: service}

	mockStore := &utxo.MockUtxostore{}
	mockStore.On("GetPrunableUnminedTxIterator", mock.Anything).Return(emptyIterator{}, nil)
	mockStore.On("ProcessExpiredPreservations", mock.Anything, mock.Anything).Return(nil)

	pruneSvc := &recordingPruner{calls: make(chan uint32, 8)}

	return &Server{
		ctx:              ctx,
		logger:           logger,
		settings:         tSettings,
		utxoStore:        mockStore,
		blockchainClient: client,
		prunerService:    pruneSvc,
		pruneNotify:      make(chan pruneSignal, 1),
		blobNotify:       make(chan pruneSignal, 1),
	}, pruneSvc
}

func setPreservationRecord(t *testing.T, s *Server, rec parentPreservationRecord) {
	t.Helper()
	require.NoError(t, s.blockchainClient.SetState(context.Background(), ParentPreservationStateKey, rec.encode()))
}

func readPreservationRecord(t *testing.T, s *Server) parentPreservationRecord {
	t.Helper()

	data, err := s.blockchainClient.GetState(context.Background(), ParentPreservationStateKey)
	require.NoError(t, err)

	rec, err := decodeParentPreservationRecord(data)
	require.NoError(t, err)

	return rec
}

// requirePhase2Skipped sends a signal and waits for its cycle to end in the phase 1 gap
// cooldown, without deleting.
func requirePhase2Skipped(t *testing.T, s *Server, pruneSvc *recordingPruner, height uint32) {
	t.Helper()

	before := getCounterValue(t, prunerSkipped, "phase1_gap_cooldown")
	s.pruneNotify <- pruneSignal{blockHeight: height}

	require.Eventually(t, func() bool { return getCounterValue(t, prunerSkipped, "phase1_gap_cooldown") == before+1 },
		2*time.Second, 5*time.Millisecond, "height %d: phase 2 waits out the cooldown", height)
	require.Empty(t, pruneSvc.calls, "height %d: nothing is deleted", height)
}

func requirePruned(t *testing.T, s *Server, pruneSvc *recordingPruner, height uint32) {
	t.Helper()

	s.pruneNotify <- pruneSignal{blockHeight: height}

	select {
	case h := <-pruneSvc.calls:
		require.Equal(t, height, h)
	case <-time.After(2 * time.Second):
		t.Fatalf("height %d was not pruned", height)
	}
}

// Phase 1 last succeeded at 1000 and then failed for 100 blocks. Meanwhile a child spent
// its parent at 1000 and stayed unmined until it was mined at 1099, so phase 1 never
// preserved the parent, whose DAH is 1288. Once phase 1 recovers, phase 2 must not delete
// until the child's block can no longer be overtaken (CoinbaseMaturity deep, height 1200):
// a reorg before that makes the child unmined again and needs the parent, and a repair hold
// written after the delete committed would be too late.
func TestPhase1GapDelaysPhase2(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server, pruneSvc := newPhase1GapTestServer(t, ctx)
	setPreservationRecord(t, server, parentPreservationRecord{lastSuccess: 1000})

	runProcessor(t, ctx, server)

	requirePhase2Skipped(t, server, pruneSvc, 1100)
	require.Equal(t, parentPreservationRecord{lastSuccess: 1100, phase2From: 1200}, readPreservationRecord(t, server))

	// Phase 1 keeps up from here, so the cooldown does not move.
	for _, h := range []uint32{1144, 1188, 1199} {
		requirePhase2Skipped(t, server, pruneSvc, h)
	}

	requirePruned(t, server, pruneSvc, 1200)
}

// A gap of up to retention - unmined retention - CoinbaseMaturity blocks (44 here) cannot
// miss a parent whose child's block is still shallow, so it does not pause phase 2.
func TestPhase1ShortGapStillPrunes(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server, pruneSvc := newPhase1GapTestServer(t, ctx)
	setPreservationRecord(t, server, parentPreservationRecord{lastSuccess: 1000})

	runProcessor(t, ctx, server)

	requirePruned(t, server, pruneSvc, 1044)
	requirePhase2Skipped(t, server, pruneSvc, 1089)
}

// The cooldown is persisted: a pruner restarted inside it keeps waiting, even though its
// first phase 1 follows the last one closely.
func TestPhase1GapCooldownSurvivesRestart(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server, pruneSvc := newPhase1GapTestServer(t, ctx)
	setPreservationRecord(t, server, parentPreservationRecord{lastSuccess: 1100, phase2From: 1200})

	runProcessor(t, ctx, server)

	for _, h := range []uint32{1101, 1145, 1189} {
		requirePhase2Skipped(t, server, pruneSvc, h)
	}

	requirePruned(t, server, pruneSvc, 1200)
}

// Without a record, phase 1 is taken to have last run at height 0: a new chain prunes, a node
// upgrading at a later height waits CoinbaseMaturity blocks once.
func TestPhase1GapWithoutRecord(t *testing.T) {
	initPrometheusMetrics()

	t.Run("new chain", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		server, pruneSvc := newPhase1GapTestServer(t, ctx)
		runProcessor(t, ctx, server)

		requirePruned(t, server, pruneSvc, 40)
	})

	t.Run("upgraded node", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		server, pruneSvc := newPhase1GapTestServer(t, ctx)
		runProcessor(t, ctx, server)

		requirePhase2Skipped(t, server, pruneSvc, 900000)
		require.Equal(t, parentPreservationRecord{lastSuccess: 900000, phase2From: 900100}, readPreservationRecord(t, server))
	})
}

// A record that cannot be written means a restart could forget a gap, so phase 2 does not
// run in that cycle.
func TestPhase1RecordWriteFailureSkipsPhase2(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server, pruneSvc := newPhase1GapTestServer(t, ctx)
	server.blockchainClient = &failingSetStateClient{ClientI: server.blockchainClient}

	before := getCounterValue(t, prunerErrors, "parent_preservation_record")

	runProcessor(t, ctx, server)
	server.pruneNotify <- pruneSignal{blockHeight: 40}

	require.Eventually(t, func() bool { return getCounterValue(t, prunerErrors, "parent_preservation_record") == before+1 },
		2*time.Second, 5*time.Millisecond)
	require.Never(t, func() bool { return len(pruneSvc.calls) > 0 }, 100*time.Millisecond, 10*time.Millisecond)
	require.Equal(t, uint32(0), server.lastProcessedHeight.Load())
}

// With utxostore_parentPreservationBlocks=0, phase 1 returns without preserving anything. That
// is no successful phase 1: it must not be recorded, or fixing the setting later would hide
// the gap in which nothing was preserved.
func TestPhase1WithoutPreservationIsNotRecorded(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server, pruneSvc := newPhase1GapTestServer(t, ctx)
	server.settings.UtxoStore.ParentPreservationBlocks = 0

	runProcessor(t, ctx, server)

	requirePruned(t, server, pruneSvc, 200)

	_, err := server.blockchainClient.GetState(context.Background(), ParentPreservationStateKey)
	require.True(t, isStateNotFound(err), "no record is written: %v", err)
}

// A retry signal that arrives after its retry was cleared, from a timer that fired just
// before, is dropped without running a cycle.
func TestStaleRetrySignalIsDropped(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := &repairHoldClient{}
	client.hold.Store(&repairHoldAnswer{data: model.EncodeOvertakenTipRepairHold(nil)})

	pruneSvc := &recordingPruner{calls: make(chan uint32, 2)}
	server := newRepairHoldTestServer(ctx, client, pruneSvc)

	runProcessor(t, ctx, server)
	server.pruneNotify <- pruneSignal{blockHeight: 101, retry: true}

	require.Never(t, func() bool { return client.reads.Load() > 0 || len(pruneSvc.calls) > 0 }, 100*time.Millisecond, 10*time.Millisecond)
	server.utxoStore.(*utxo.MockUtxostore).AssertNotCalled(t, "GetPrunableUnminedTxIterator", mock.Anything)
}

// The first held height gets its retry even when phase 1 fails in that cycle: once the hold
// is released and phase 1 recovers, it is pruned without another block.
func TestRepairHoldFirstCyclePhase1FailureIsRetried(t *testing.T) {
	initPrometheusMetrics()
	prunerPreserveParentsFailing.Set(0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := &repairHoldClient{}
	client.hold.Store(&repairHoldAnswer{data: model.EncodeOvertakenTipRepairHold(testHeldBlock)})

	mockStore := &utxo.MockUtxostore{}
	mockStore.On("GetPrunableUnminedTxIterator", mock.Anything).Return((*failingIterator)(nil), errors.NewStorageError("boom")).Once()
	mockStore.On("GetPrunableUnminedTxIterator", mock.Anything).Return(emptyIterator{}, nil)
	mockStore.On("ProcessExpiredPreservations", mock.Anything, mock.Anything).Return(nil)

	pruneSvc := &recordingPruner{calls: make(chan uint32, 4)}
	server := newPhase1TestServer(ctx, mockStore, pruneSvc, false)
	server.blockchainClient = client
	server.repairHoldRecheckInterval = 20 * time.Millisecond

	runProcessor(t, ctx, server)
	server.pruneNotify <- pruneSignal{blockHeight: 101}

	require.Eventually(t, func() bool { return testutil.ToFloat64(prunerPreserveParentsFailing) == 1 }, 2*time.Second, 5*time.Millisecond)
	client.hold.Store(&repairHoldAnswer{data: model.EncodeOvertakenTipRepairHold(nil)})

	select {
	case h := <-pruneSvc.calls:
		require.Equal(t, uint32(101), h)
	case <-time.After(2 * time.Second):
		t.Fatal("the held height was not retried after its first cycle's phase 1 failed")
	}
}

// The first held height gets its retry even when the parent preservation record cannot be
// written in that cycle.
func TestRepairHoldFirstCycleRecordFailureIsRetried(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server, pruneSvc := newPhase1GapTestServer(t, ctx)
	server.repairHoldRecheckInterval = 20 * time.Millisecond

	inner := server.blockchainClient
	require.NoError(t, inner.SetState(ctx, model.OvertakenTipRepairHoldKey, model.EncodeOvertakenTipRepairHold(testHeldBlock)))

	failing := &failingSetStateClient{ClientI: inner}
	server.blockchainClient = failing

	before := getCounterValue(t, prunerErrors, "parent_preservation_record")

	runProcessor(t, ctx, server)
	server.pruneNotify <- pruneSignal{blockHeight: 40}

	require.Eventually(t, func() bool { return getCounterValue(t, prunerErrors, "parent_preservation_record") > before },
		2*time.Second, 5*time.Millisecond)

	require.NoError(t, inner.SetState(ctx, model.OvertakenTipRepairHoldKey, model.EncodeOvertakenTipRepairHold(nil)))
	failing.recovered.Store(true)

	select {
	case h := <-pruneSvc.calls:
		require.Equal(t, uint32(40), h)
	case <-time.After(2 * time.Second):
		t.Fatal("the held height was not retried after its first cycle's record write failed")
	}
}

// A hold record that cannot be decoded may list blocks, so it holds: nothing is deleted.
func TestRepairHoldUndecodableHolds(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := &repairHoldClient{}
	client.hold.Store(&repairHoldAnswer{data: []byte{0x7f, 0x01}})

	pruneSvc := &recordingPruner{calls: make(chan uint32, 2)}
	server := newRepairHoldTestServer(ctx, client, pruneSvc)
	server.repairHoldRecheckInterval = time.Hour

	before := getCounterValue(t, prunerErrors, "overtaken_tip_repair_hold")

	runProcessor(t, ctx, server)
	server.pruneNotify <- pruneSignal{blockHeight: 101}

	require.Eventually(t, func() bool { return getCounterValue(t, prunerErrors, "overtaken_tip_repair_hold") > before },
		2*time.Second, 5*time.Millisecond)
	require.Never(t, func() bool { return len(pruneSvc.calls) > 0 }, 100*time.Millisecond, 10*time.Millisecond, "nothing is deleted")
	require.Zero(t, client.acquired.Load(), "no blob deletion")
}

// A hold read cut short by the cycle's own cancellation, such as the watcher's read when phase
// 2 finishes, is not a failed read: it is not logged or counted as one.
func TestRepairHoldCancelledReadIsNotAnError(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	client := &repairHoldClient{}
	client.hold.Store(&repairHoldAnswer{err: context.Canceled})

	server := newRepairHoldTestServer(context.Background(), client, &recordingPruner{calls: make(chan uint32, 1)})

	before := getCounterValue(t, prunerErrors, "overtaken_tip_repair_hold")

	require.True(t, server.overtakenTipRepairHeld(ctx, "x", 1), "a read that did not complete holds")
	require.Equal(t, before, getCounterValue(t, prunerErrors, "overtaken_tip_repair_hold"))
}

// Slack equal to CoinbaseMaturity leaves no gap phase 1 may miss: off mainnet and testnet the
// cooldown is then off (checkRepairHoldSlack reports it). One block more allows a gap of one.
func TestPhase1GapLimitSlackBoundary(t *testing.T) {
	initPrometheusMetrics()

	for _, tc := range []struct {
		name      string
		retention uint32
		pruned    bool
	}{
		{"slack equals maturity", 244, true},
		{"slack one above maturity", 245, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			server, pruneSvc := newPhase1GapTestServer(t, ctx)
			params := chaincfg.RegressionNetParams
			params.CoinbaseMaturity = 100
			server.settings.ChainCfgParams = &params
			server.settings.GlobalBlockHeightRetention = tc.retention

			runProcessor(t, ctx, server)

			if tc.pruned {
				requirePruned(t, server, pruneSvc, 900000)
			} else {
				requirePhase2Skipped(t, server, pruneSvc, 900000)
			}
		})
	}
}
