package blockvalidation

import (
	"testing"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

func TestServerBlockFoundChBufferSize(t *testing.T) {
	initPrometheusMetrics()

	tests := []struct {
		name       string
		bufferSize int
	}{
		{"buffer size 1", 1},
		{"buffer size 100", 100},
		{"buffer size 1000", 1000},
		{"buffer size 5000", 5000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := ulogger.TestLogger{}
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.BlockValidation.BlockFoundChBufferSize = tt.bufferSize

			server := New(logger, tSettings, nil, nil, nil, nil, nil, nil, nil, nil)
			require.Equal(t, tt.bufferSize, cap(server.blockFoundCh))
		})
	}
}

func TestServerCatchupChBufferSize(t *testing.T) {
	initPrometheusMetrics()

	tests := []struct {
		name       string
		bufferSize int
	}{
		{"buffer size 1", 1},
		{"buffer size 50", 50},
		{"buffer size 100", 100},
		{"buffer size 500", 500},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := ulogger.TestLogger{}
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.BlockValidation.CatchupChBufferSize = tt.bufferSize

			server := New(logger, tSettings, nil, nil, nil, nil, nil, nil, nil, nil)
			require.Equal(t, tt.bufferSize, cap(server.catchupCh))
		})
	}
}

func TestServerNearForkThresholdDefault(t *testing.T) {
	initPrometheusMetrics()

	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.NearForkThreshold = 0

	server := New(logger, tSettings, nil, nil, nil, nil, nil, nil, nil, nil)
	expectedThreshold := uint32(tSettings.ChainCfgParams.CoinbaseMaturity / 2)
	require.Equal(t, expectedThreshold, server.blockClassifier.nearForkThreshold)
}

func TestServerNearForkThresholdOverride(t *testing.T) {
	initPrometheusMetrics()

	tests := []struct {
		name      string
		threshold int
	}{
		{"threshold 10", 10},
		{"threshold 50", 50},
		{"threshold 100", 100},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := ulogger.TestLogger{}
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.BlockValidation.NearForkThreshold = tt.threshold

			server := New(logger, tSettings, nil, nil, nil, nil, nil, nil, nil, nil)
			require.Equal(t, uint32(tt.threshold), server.blockClassifier.nearForkThreshold)
		})
	}
}

func TestServerCircuitBreakerFromSettings(t *testing.T) {
	initPrometheusMetrics()

	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.CircuitBreakerFailureThreshold = 10
	tSettings.BlockValidation.CircuitBreakerSuccessThreshold = 3
	tSettings.BlockValidation.CircuitBreakerTimeoutSeconds = 60

	server := New(logger, tSettings, nil, nil, nil, nil, nil, nil, nil, nil)

	breaker := server.peerCircuitBreakers.GetBreaker("test-peer")

	for i := 0; i < 9; i++ {
		breaker.CanCall()
		breaker.RecordFailure()
	}
	require.True(t, breaker.CanCall(), "should still allow calls below failure threshold of 10")

	breaker.RecordFailure()
	require.False(t, breaker.CanCall(), "should block calls after reaching failure threshold of 10")
}
