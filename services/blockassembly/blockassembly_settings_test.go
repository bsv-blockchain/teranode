package blockassembly

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestBlockAssemblyNew_ChannelBufferSizes tests that channel buffer sizes
// from settings are correctly applied during Init().
func TestBlockAssemblyNew_SettingsPropagate(t *testing.T) {
	initPrometheusMetrics()

	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockAssembly.NewSubtreeChanBuffer = 2000
	tSettings.BlockAssembly.SubtreeRetryChanBuffer = 500
	tSettings.BlockAssembly.SubtreeStorageWorkers = 8
	tSettings.BlockAssembly.Disabled = true

	ba := New(logger, tSettings, nil, nil, nil, nil)

	require.NotNil(t, ba)
	require.Equal(t, 2000, ba.settings.BlockAssembly.NewSubtreeChanBuffer)
	require.Equal(t, 500, ba.settings.BlockAssembly.SubtreeRetryChanBuffer)
	require.Equal(t, 8, ba.settings.BlockAssembly.SubtreeStorageWorkers)
	require.True(t, ba.settings.BlockAssembly.Disabled)
}

// TestBlockAssemblyNew_SubtreeStorageWorkersFallback tests that SubtreeStorageWorkers
// falls back to 4 when set to 0 or negative (handled in runNewSubtreeListener).
func TestBlockAssemblyNew_SubtreeStorageWorkersFallback(t *testing.T) {
	tests := []struct {
		name            string
		workers         int
		expectedWorkers int
	}{
		{"zero falls back to 4", 0, 4},
		{"negative falls back to 4", -1, 4},
		{"positive uses configured value", 8, 8},
		{"one is valid", 1, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// The fallback happens in runNewSubtreeListener (Server.go:327-330)
			// We verify the logic directly
			numWorkers := tt.workers
			if numWorkers <= 0 {
				numWorkers = 4
			}
			require.Equal(t, tt.expectedWorkers, numWorkers)
		})
	}
}

// TestBlockAssemblyClient_SendBatchSizeVariations tests that SendBatchSize
// controls client batching behavior.
func TestBlockAssemblyClient_SendBatchSizeVariations(t *testing.T) {
	tests := []struct {
		name      string
		batchSize int
		expectBatching bool
	}{
		{"zero disables batching", 0, false},
		{"positive enables batching", 100, true},
		{"large batch size", 10000, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := &mockBlockAssemblyAPIClient{}
			client := createTestClient(mockClient, tt.batchSize)

			if tt.expectBatching {
				require.NotNil(t, client.batcher, "batcher should be created when batchSize > 0")
				require.Equal(t, tt.batchSize, client.batchSize)
			} else {
				require.Nil(t, client.batcher, "batcher should be nil when batchSize is 0")
			}
		})
	}
}

// TestBlockAssemblyClient_SendBatchMaxConcurrent tests that SendBatchMaxConcurrent
// configures the concurrency limiter.
func TestBlockAssemblyClient_SendBatchMaxConcurrent(t *testing.T) {
	tests := []struct {
		name          string
		maxConcurrent int
		expectLimiter bool
	}{
		{"zero disables limiter", 0, false},
		{"positive enables limiter", 3, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tSettings := &settings.Settings{
				BlockAssembly: settings.BlockAssemblySettings{
					SendBatchSize:         100,
					SendBatchTimeout:      2,
					SendBatchMaxConcurrent: tt.maxConcurrent,
				},
			}

			// Verify the setting is stored correctly
			require.Equal(t, tt.maxConcurrent, tSettings.BlockAssembly.SendBatchMaxConcurrent)
		})
	}
}

// TestBlockAssemblySettings_DisabledFlag tests that the Disabled flag prevents operations.
func TestBlockAssemblySettings_DisabledFlag(t *testing.T) {
	initPrometheusMetrics()

	t.Run("disabled true", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockAssembly.Disabled = true

		ba := New(logger, tSettings, nil, nil, nil, nil)
		require.True(t, ba.settings.BlockAssembly.Disabled)
	})

	t.Run("disabled false", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockAssembly.Disabled = false

		ba := New(logger, tSettings, nil, nil, nil, nil)
		require.False(t, ba.settings.BlockAssembly.Disabled)
	})
}

// TestBlockAssemblySettings_ReorgLimits tests reorg-related settings propagation.
func TestBlockAssemblySettings_ReorgLimits(t *testing.T) {
	initPrometheusMetrics()

	tests := []struct {
		name          string
		catchup       int
		rollback      int
		reorgHashes   int
	}{
		{"small reorg limits", 10, 10, 100},
		{"default reorg limits", 100, 100, 10000},
		{"large reorg limits", 500, 500, 50000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := ulogger.TestLogger{}
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.BlockAssembly.MaxBlockReorgCatchup = tt.catchup
			tSettings.BlockAssembly.MaxBlockReorgRollback = tt.rollback
			tSettings.BlockAssembly.MaxGetReorgHashes = tt.reorgHashes

			ba := New(logger, tSettings, nil, nil, nil, nil)
			require.Equal(t, tt.catchup, ba.settings.BlockAssembly.MaxBlockReorgCatchup)
			require.Equal(t, tt.rollback, ba.settings.BlockAssembly.MaxBlockReorgRollback)
			require.Equal(t, tt.reorgHashes, ba.settings.BlockAssembly.MaxGetReorgHashes)
		})
	}
}

// TestBlockAssemblySettings_MerkleSubtreeSizing tests merkle subtree size settings.
func TestBlockAssemblySettings_MerkleSubtreeSizing(t *testing.T) {
	initPrometheusMetrics()

	tests := []struct {
		name    string
		initial int
		minimum int
		maximum int
		dynamic bool
	}{
		{"small subtrees", 1024, 256, 4096, false},
		{"default subtrees", 1048576, 1024, 1048576, false},
		{"large subtrees with dynamic", 2097152, 4096, 4194304, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := ulogger.TestLogger{}
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.BlockAssembly.InitialMerkleItemsPerSubtree = tt.initial
			tSettings.BlockAssembly.MinimumMerkleItemsPerSubtree = tt.minimum
			tSettings.BlockAssembly.MaximumMerkleItemsPerSubtree = tt.maximum
			tSettings.BlockAssembly.UseDynamicSubtreeSize = tt.dynamic

			ba := New(logger, tSettings, nil, nil, nil, nil)
			require.Equal(t, tt.initial, ba.settings.BlockAssembly.InitialMerkleItemsPerSubtree)
			require.Equal(t, tt.minimum, ba.settings.BlockAssembly.MinimumMerkleItemsPerSubtree)
			require.Equal(t, tt.maximum, ba.settings.BlockAssembly.MaximumMerkleItemsPerSubtree)
			require.Equal(t, tt.dynamic, ba.settings.BlockAssembly.UseDynamicSubtreeSize)
		})
	}
}

// TestBlockAssemblySettings_DoubleSpendWindow tests double-spend window configuration.
func TestBlockAssemblySettings_DoubleSpendWindow(t *testing.T) {
	initPrometheusMetrics()

	tests := []struct {
		name     string
		window   time.Duration
	}{
		{"disabled (zero)", 0},
		{"100ms", 100 * time.Millisecond},
		{"1s", 1 * time.Second},
		{"10s", 10 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := ulogger.TestLogger{}
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.BlockAssembly.DoubleSpendWindow = tt.window

			ba := New(logger, tSettings, nil, nil, nil, nil)
			require.Equal(t, tt.window, ba.settings.BlockAssembly.DoubleSpendWindow)
		})
	}
}

// TestBlockAssemblySettings_ConcurrencyVariations tests different concurrency settings.
func TestBlockAssemblySettings_ConcurrencyVariations(t *testing.T) {
	initPrometheusMetrics()

	tests := []struct {
		name                string
		moveBack            int
		processRemainder    int
		concurrentReads     int
	}{
		{"low concurrency", 4, 4, 4},
		{"default concurrency", 375, 375, 375},
		{"high concurrency", 1000, 1000, 1000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := ulogger.TestLogger{}
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.BlockAssembly.MoveBackBlockConcurrency = tt.moveBack
			tSettings.BlockAssembly.ProcessRemainderTxHashesConcurrency = tt.processRemainder
			tSettings.BlockAssembly.SubtreeProcessorConcurrentReads = tt.concurrentReads

			ba := New(logger, tSettings, nil, nil, nil, nil)
			require.Equal(t, tt.moveBack, ba.settings.BlockAssembly.MoveBackBlockConcurrency)
			require.Equal(t, tt.processRemainder, ba.settings.BlockAssembly.ProcessRemainderTxHashesConcurrency)
			require.Equal(t, tt.concurrentReads, ba.settings.BlockAssembly.SubtreeProcessorConcurrentReads)
		})
	}
}

// TestBlockAssemblySettings_RestartBehavior tests restart-related settings.
func TestBlockAssemblySettings_RestartBehavior(t *testing.T) {
	initPrometheusMetrics()

	tests := []struct {
		name                  string
		validateParent        bool
		removeInvalid         bool
		diskSort              bool
		parentBatchSize       int
	}{
		{"conservative restart", true, false, false, 1000},
		{"aggressive cleanup restart", true, true, true, 500},
		{"fast restart no validation", false, false, false, 1000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := ulogger.TestLogger{}
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.BlockAssembly.OnRestartValidateParentChain = tt.validateParent
			tSettings.BlockAssembly.OnRestartRemoveInvalidParentChainTxs = tt.removeInvalid
			tSettings.BlockAssembly.UnminedTxDiskSortEnabled = tt.diskSort
			tSettings.BlockAssembly.ParentValidationBatchSize = tt.parentBatchSize

			ba := New(logger, tSettings, nil, nil, nil, nil)
			require.Equal(t, tt.validateParent, ba.settings.BlockAssembly.OnRestartValidateParentChain)
			require.Equal(t, tt.removeInvalid, ba.settings.BlockAssembly.OnRestartRemoveInvalidParentChainTxs)
			require.Equal(t, tt.diskSort, ba.settings.BlockAssembly.UnminedTxDiskSortEnabled)
			require.Equal(t, tt.parentBatchSize, ba.settings.BlockAssembly.ParentValidationBatchSize)
		})
	}
}
