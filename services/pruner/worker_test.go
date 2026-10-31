package pruner

import (
	"context"
	"database/sql"
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
	"github.com/bsv-blockchain/teranode/stores/utxo"
	prunerstore "github.com/bsv-blockchain/teranode/stores/utxo/pruner"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

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
	go server.prunerProcessor(ctx)

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
	go server.prunerProcessor(ctx)

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
	go server.prunerProcessor(ctx)

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

	go server.prunerProcessor(ctx)

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

	go server.prunerProcessor(ctx)

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

	go server.prunerProcessor(ctx)

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

	go server.prunerProcessor(ctx)

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

	go server.prunerProcessor(ctx)

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
	go server.prunerProcessor(ctx)

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

	go server.prunerProcessor(ctx)
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

	go server.prunerProcessor(ctx)
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

	go server.prunerProcessor(ctx)
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

	go server.prunerProcessor(ctx)

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

	go server.prunerProcessor(ctx)
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
}

func (c *repairHoldClient) AcquireBlobDeletionBatch(context.Context, uint32, int, int, []storetypes.BlobStoreType) (string, []*blockchain_api.ScheduledDeletion, error) {
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
// finished, the pruner deletes nothing: no DAH deletion and no blob deletion, and the height
// is not advanced. Once the hold is released, the next block prunes as usual.
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

	go server.prunerProcessor(ctx)
	server.pruneNotify <- pruneSignal{blockHeight: 101, blockHash: chainhash.Hash{}}

	require.Eventually(t, func() bool {
		return getCounterValue(t, prunerSkipped, "overtaken_tip_repair_hold") == skippedBefore+1
	}, 2*time.Second, 10*time.Millisecond, "the cycle is counted as held")

	require.Empty(t, pruneSvc.calls, "no DAH deletion while held")
	require.Empty(t, server.blobNotify, "no blob deletion while held")
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

	go server.prunerProcessor(ctx)
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

	go server.prunerProcessor(ctx)
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

	go server.prunerProcessor(ctx)
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

	go server.prunerProcessor(ctx)
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

	go server.prunerProcessor(ctx)
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

// On mainnet and testnet the pruner refuses to start when the gap between the UTXO retention
// and the unmined retention is below the deepest fork the node accepts (CoinbaseMaturity): a
// hold written after a cycle's last check would then not be enough to protect a due parent.
// Other networks only log it.
func TestInitRefusesInsufficientRepairHoldSlack(t *testing.T) {
	for _, tc := range []struct {
		name   string
		params chaincfg.Params
		refuse bool
	}{
		{"mainnet", chaincfg.MainNetParams, true},
		{"testnet", chaincfg.TestNetParams, true},
		{"regtest", chaincfg.RegressionNetParams, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			params := tc.params
			server := &Server{
				logger: ulogger.TestLogger{},
				settings: &settings.Settings{
					ChainCfgParams:             &params,
					GlobalBlockHeightRetention: 150,
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
