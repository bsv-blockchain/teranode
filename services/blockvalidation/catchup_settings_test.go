package blockvalidation

import (
	"testing"

	"github.com/bsv-blockchain/teranode/services/blockvalidation/catchup"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

func TestCatchupSettingsPropagate(t *testing.T) {
	initPrometheusMetrics()

	t.Run("settings are stored in server", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockValidation.CatchupParallelFetchEnabled = false
		tSettings.BlockValidation.CatchupParallelFetchWorkers = 7
		tSettings.BlockValidation.CatchupMinThroughputKBps = 500
		tSettings.BlockValidation.CatchupCheckpointHash = "00000000000000000102d94fde9bd0807a2cc7c9"
		tSettings.BlockValidation.CatchupCheckpointHeight = 100000
		tSettings.BlockValidation.CatchupAllowQuickValidation = false
		tSettings.BlockValidation.MaxBlocksBehindBlockAssembly = 50

		server := New(logger, tSettings, nil, nil, nil, nil, nil, nil, nil, nil)

		require.False(t, server.settings.BlockValidation.CatchupParallelFetchEnabled)
		require.Equal(t, 7, server.settings.BlockValidation.CatchupParallelFetchWorkers)
		require.Equal(t, 500, server.settings.BlockValidation.CatchupMinThroughputKBps)
		require.Equal(t, "00000000000000000102d94fde9bd0807a2cc7c9", server.settings.BlockValidation.CatchupCheckpointHash)
		require.Equal(t, int32(100000), server.settings.BlockValidation.CatchupCheckpointHeight)
		require.False(t, server.settings.BlockValidation.CatchupAllowQuickValidation)
		require.Equal(t, 50, server.settings.BlockValidation.MaxBlocksBehindBlockAssembly)
	})

	t.Run("default catchup parallel fetch workers", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockValidation.CatchupParallelFetchWorkers = 0

		server := New(logger, tSettings, nil, nil, nil, nil, nil, nil, nil, nil)
		require.Equal(t, 0, server.settings.BlockValidation.CatchupParallelFetchWorkers)
	})
}

func TestCatchupPerformanceMonitorConfig(t *testing.T) {
	tests := []struct {
		name              string
		minThroughputKBps int
	}{
		{"default throughput", 0},
		{"low throughput", 10},
		{"medium throughput", 100},
		{"high throughput", 1000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := DefaultPerformanceMonitorConfig()
			if tt.minThroughputKBps > 0 {
				config.MinThroughputKBPerSec = float64(tt.minThroughputKBps)
			}

			monitor := NewCatchupPerformanceMonitor(ulogger.TestLogger{}, "test-peer", "http://test:8080", config)
			require.NotNil(t, monitor)
		})
	}
}

func TestPeerCircuitBreakersFromSettings(t *testing.T) {
	initPrometheusMetrics()

	tests := []struct {
		name             string
		failureThreshold int
		failuresToOpen   int
	}{
		{"aggressive threshold 1", 1, 1},
		{"default threshold 5", 5, 5},
		{"conservative threshold 20", 20, 20},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := ulogger.TestLogger{}
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.BlockValidation.CircuitBreakerFailureThreshold = tt.failureThreshold
			tSettings.BlockValidation.CircuitBreakerSuccessThreshold = 1
			tSettings.BlockValidation.CircuitBreakerTimeoutSeconds = 30

			server := New(logger, tSettings, nil, nil, nil, nil, nil, nil, nil, nil)

			breaker := server.peerCircuitBreakers.GetBreaker("peer-test")

			for i := 0; i < tt.failuresToOpen-1; i++ {
				breaker.CanCall()
				breaker.RecordFailure()
			}
			require.Equal(t, catchup.StateClosed, breaker.GetState())

			breaker.CanCall()
			breaker.RecordFailure()
			require.Equal(t, catchup.StateOpen, breaker.GetState())
		})
	}
}

func TestServerForkManagerSettingsPropagate(t *testing.T) {
	initPrometheusMetrics()

	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.MaxParallelForks = 16
	tSettings.BlockValidation.MaxTrackedForks = 500

	server := New(logger, tSettings, nil, nil, nil, nil, nil, nil, nil, nil)

	require.Equal(t, 16, server.forkManager.maxParallelForks)
	require.Equal(t, 500, server.forkManager.maxTrackedForks)
}
