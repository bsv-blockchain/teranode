package pruner

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	blockassembly_api "github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/prometheus/client_golang/prometheus"
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

// TestStart_FSMContextCancellation verifies graceful shutdown handling when
// the context is cancelled during the FSM wait. The error must be returned
// (not swallowed) and must be a context error so the service manager can
// distinguish it from a real failure.
func TestStart_FSMContextCancellation(t *testing.T) {
	ctx := context.Background()

	mockBlockchainClient := &blockchain.Mock{}
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
