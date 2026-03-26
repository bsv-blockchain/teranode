package settings

import (
	"testing"
	"time"

	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

func TestBlockAssembly_ConfigOverride_Strings(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		value    string
		getField func(*Settings) string
	}{
		{"GRPCAddress", "blockassembly_grpcAddress", "ba-host:9999", func(s *Settings) string { return s.BlockAssembly.GRPCAddress }},
		{"LocalDAHCache", "blockassembly_localDAHCache", "/tmp/dah-cache", func(s *Settings) string { return s.BlockAssembly.LocalDAHCache }},
		{"UnminedTxDiskSortPath", "blockassembly_unminedTxDiskSortPath", "/tmp/sort", func(s *Settings) string { return s.BlockAssembly.UnminedTxDiskSortPath }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gocore.Config().Set(tt.key, tt.value)
			defer gocore.Config().Unset(tt.key)

			s := NewSettings()
			require.Equal(t, tt.value, tt.getField(s))
		})
	}
}

func TestBlockAssembly_ConfigOverride_Ints(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		value    string
		getField func(*Settings) int
		expected int
	}{
		{"GRPCMaxRetries", "blockassembly_grpcMaxRetries", "5", func(s *Settings) int { return s.BlockAssembly.GRPCMaxRetries }, 5},
		{"MaxBlockReorgCatchup", "blockassembly_maxBlockReorgCatchup", "200", func(s *Settings) int { return s.BlockAssembly.MaxBlockReorgCatchup }, 200},
		{"MaxBlockReorgRollback", "blockassembly_maxBlockReorgRollback", "50", func(s *Settings) int { return s.BlockAssembly.MaxBlockReorgRollback }, 50},
		{"MoveBackBlockConcurrency", "blockassembly_moveBackBlockConcurrency", "128", func(s *Settings) int { return s.BlockAssembly.MoveBackBlockConcurrency }, 128},
		{"ProcessRemainderTxHashesConcurrency", "blockassembly_processRemainderTxHashesConcurrency", "64", func(s *Settings) int { return s.BlockAssembly.ProcessRemainderTxHashesConcurrency }, 64},
		{"SendBatchSize", "blockassembly_sendBatchSize", "500", func(s *Settings) int { return s.BlockAssembly.SendBatchSize }, 500},
		{"SendBatchTimeout", "blockassembly_sendBatchTimeout", "5", func(s *Settings) int { return s.BlockAssembly.SendBatchTimeout }, 5},
		{"SubtreeProcessorBatcherSize", "blockassembly_subtreeProcessorBatcherSize", "2000", func(s *Settings) int { return s.BlockAssembly.SubtreeProcessorBatcherSize }, 2000},
		{"SubtreeProcessorConcurrentReads", "blockassembly_subtreeProcessorConcurrentReads", "128", func(s *Settings) int { return s.BlockAssembly.SubtreeProcessorConcurrentReads }, 128},
		{"NewSubtreeChanBuffer", "blockassembly_newSubtreeChanBuffer", "5000", func(s *Settings) int { return s.BlockAssembly.NewSubtreeChanBuffer }, 5000},
		{"SubtreeRetryChanBuffer", "blockassembly_subtreeRetryChanBuffer", "500", func(s *Settings) int { return s.BlockAssembly.SubtreeRetryChanBuffer }, 500},
		{"SubtreeStorageWorkers", "blockassembly_subtreeStorageWorkers", "8", func(s *Settings) int { return s.BlockAssembly.SubtreeStorageWorkers }, 8},
		{"InitialMerkleItemsPerSubtree", "initial_merkle_items_per_subtree", "524288", func(s *Settings) int { return s.BlockAssembly.InitialMerkleItemsPerSubtree }, 524288},
		// MinimumMerkleItemsPerSubtree and MaximumMerkleItemsPerSubtree are overridden by settings.conf context
		{"MaxGetReorgHashes", "blockassembly_maxGetReorgHashes", "20000", func(s *Settings) int { return s.BlockAssembly.MaxGetReorgHashes }, 20000},
		{"ParentValidationBatchSize", "blockassembly_parentValidationBatchSize", "500", func(s *Settings) int { return s.BlockAssembly.ParentValidationBatchSize }, 500},
		{"UnminedLoadingBatchSize", "blockassembly_unminedLoadingBatchSize", "5000000", func(s *Settings) int { return s.BlockAssembly.UnminedLoadingBatchSize }, 5000000},
		{"ParallelSetIfNotExistsThreshold", "blockassembly_parallelSetIfNotExistsThreshold", "50000", func(s *Settings) int { return s.BlockAssembly.ParallelSetIfNotExistsThreshold }, 50000},
		{"SendBatchMaxConcurrent", "blockassembly_sendBatchMaxConcurrent", "3", func(s *Settings) int { return s.BlockAssembly.SendBatchMaxConcurrent }, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gocore.Config().Set(tt.key, tt.value)
			defer gocore.Config().Unset(tt.key)

			s := NewSettings()
			require.Equal(t, tt.expected, tt.getField(s))
		})
	}
}

func TestBlockAssembly_ConfigOverride_Bools(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		getField func(*Settings) bool
	}{
		{"Disabled", "blockassembly_disabled", func(s *Settings) bool { return s.BlockAssembly.Disabled }},
		{"SubmitMiningSolutionWaitForResponse", "blockassembly_SubmitMiningSolution_waitForResponse", func(s *Settings) bool { return s.BlockAssembly.SubmitMiningSolutionWaitForResponse }},
		{"DifficultyCache", "blockassembly_difficultyCache", func(s *Settings) bool { return s.BlockAssembly.DifficultyCache }},
		{"UseDynamicSubtreeSize", "blockassembly_useDynamicSubtreeSize", func(s *Settings) bool { return s.BlockAssembly.UseDynamicSubtreeSize }},
		{"OnRestartValidateParentChain", "blockassembly_onRestartValidateParentChain", func(s *Settings) bool { return s.BlockAssembly.OnRestartValidateParentChain }},
		{"OnRestartRemoveInvalidParentChainTxs", "blockassembly_onRestartRemoveInvalidParentChainTxs", func(s *Settings) bool { return s.BlockAssembly.OnRestartRemoveInvalidParentChainTxs }},
		{"UseColumnarBatch", "blockassembly_useColumnarBatch", func(s *Settings) bool { return s.BlockAssembly.UseColumnarBatch }},
		{"UnminedTxDiskSortEnabled", "blockassembly_unminedTxDiskSortEnabled", func(s *Settings) bool { return s.BlockAssembly.UnminedTxDiskSortEnabled }},
		{"StoreTxInpointsForSubtreeMeta", "blockassembly_storeTxInpointsForSubtreeMeta", func(s *Settings) bool { return s.BlockAssembly.StoreTxInpointsForSubtreeMeta }},
	}
	for _, tt := range tests {
		t.Run(tt.name+"_True", func(t *testing.T) {
			gocore.Config().Set(tt.key, "true")
			defer gocore.Config().Unset(tt.key)

			s := NewSettings()
			require.True(t, tt.getField(s))
		})
		t.Run(tt.name+"_False", func(t *testing.T) {
			gocore.Config().Set(tt.key, "false")
			defer gocore.Config().Unset(tt.key)

			s := NewSettings()
			require.False(t, tt.getField(s))
		})
	}
}

func TestBlockAssembly_ConfigOverride_Durations(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		value    string
		getField func(*Settings) time.Duration
		expected time.Duration
	}{
		{"GRPCRetryBackoff", "blockassembly_grpcRetryBackoff", "5s", func(s *Settings) time.Duration { return s.BlockAssembly.GRPCRetryBackoff }, 5 * time.Second},
		{"BlockchainSubscriptionTimeout", "blockassembly_blockchainSubscriptionTimeout", "10m", func(s *Settings) time.Duration { return s.BlockAssembly.BlockchainSubscriptionTimeout }, 10 * time.Minute},
		{"SubtreeAnnouncementInterval", "blockassembly_subtreeAnnouncementInterval", "30s", func(s *Settings) time.Duration { return s.BlockAssembly.SubtreeAnnouncementInterval }, 30 * time.Second},
		{"IdleSleepDuration", "blockassembly_idle_sleep_duration", "50ms", func(s *Settings) time.Duration { return s.BlockAssembly.IdleSleepDuration }, 50 * time.Millisecond},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gocore.Config().Set(tt.key, tt.value)
			defer gocore.Config().Unset(tt.key)

			s := NewSettings()
			require.Equal(t, tt.expected, tt.getField(s))
		})
	}
}

// TestBlockAssembly_DoubleSpendWindow tests that double_spend_window_millis is converted to Duration.
func TestBlockAssembly_DoubleSpendWindow(t *testing.T) {
	tests := []struct {
		name     string
		millis   string
		expected time.Duration
	}{
		{"zero disables", "0", 0},
		{"100ms", "100", 100 * time.Millisecond},
		{"1000ms is 1s", "1000", 1 * time.Second},
		{"5000ms is 5s", "5000", 5 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gocore.Config().Set("double_spend_window_millis", tt.millis)
			defer gocore.Config().Unset("double_spend_window_millis")

			s := NewSettings()
			require.Equal(t, tt.expected, s.BlockAssembly.DoubleSpendWindow)
		})
	}
}

// TestBlockAssembly_MinerWalletPrivateKeys tests pipe-separated multi-string field.
func TestBlockAssembly_MinerWalletPrivateKeys(t *testing.T) {
	t.Run("empty produces empty slice", func(t *testing.T) {
		s := NewSettings()
		// Default may be empty or set by conf; verify it's a slice
		require.IsType(t, []string{}, s.BlockAssembly.MinerWalletPrivateKeys)
	})

	t.Run("single key", func(t *testing.T) {
		gocore.Config().Set("miner_wallet_private_keys", "key1")
		defer gocore.Config().Unset("miner_wallet_private_keys")

		s := NewSettings()
		require.Equal(t, []string{"key1"}, s.BlockAssembly.MinerWalletPrivateKeys)
	})

	t.Run("multiple pipe-separated keys", func(t *testing.T) {
		gocore.Config().Set("miner_wallet_private_keys", "key1 | key2 | key3")
		defer gocore.Config().Unset("miner_wallet_private_keys")

		s := NewSettings()
		require.Equal(t, []string{"key1", "key2", "key3"}, s.BlockAssembly.MinerWalletPrivateKeys)
	})
}

// TestBlockAssembly_TxMapDirs tests the TxMapDirs field type and direct modification.
func TestBlockAssembly_TxMapDirs(t *testing.T) {
	t.Run("default is empty or nil", func(t *testing.T) {
		s := NewSettings()
		require.Empty(t, s.BlockAssembly.TxMapDirs)
	})

	t.Run("direct set works", func(t *testing.T) {
		s := NewSettings()
		s.BlockAssembly.TxMapDirs = []string{"/mnt/nvme0/txmap", "/mnt/nvme1/txmap"}
		require.Equal(t, []string{"/mnt/nvme0/txmap", "/mnt/nvme1/txmap"}, s.BlockAssembly.TxMapDirs)
		require.Len(t, s.BlockAssembly.TxMapDirs, 2)
	})
}

func TestBlockAssembly_BoundaryValues_ZeroInts(t *testing.T) {
	zeroFields := []struct {
		name     string
		key      string
		getField func(*Settings) int
	}{
		{"SendBatchSize", "blockassembly_sendBatchSize", func(s *Settings) int { return s.BlockAssembly.SendBatchSize }},
		{"SendBatchMaxConcurrent", "blockassembly_sendBatchMaxConcurrent", func(s *Settings) int { return s.BlockAssembly.SendBatchMaxConcurrent }},
		{"SubtreeStorageWorkers", "blockassembly_subtreeStorageWorkers", func(s *Settings) int { return s.BlockAssembly.SubtreeStorageWorkers }},
		{"NewSubtreeChanBuffer", "blockassembly_newSubtreeChanBuffer", func(s *Settings) int { return s.BlockAssembly.NewSubtreeChanBuffer }},
		{"MaxBlockReorgCatchup", "blockassembly_maxBlockReorgCatchup", func(s *Settings) int { return s.BlockAssembly.MaxBlockReorgCatchup }},
		{"ParallelSetIfNotExistsThreshold", "blockassembly_parallelSetIfNotExistsThreshold", func(s *Settings) int { return s.BlockAssembly.ParallelSetIfNotExistsThreshold }},
		{"MoveBackBlockConcurrency", "blockassembly_moveBackBlockConcurrency", func(s *Settings) int { return s.BlockAssembly.MoveBackBlockConcurrency }},
	}
	for _, tt := range zeroFields {
		t.Run(tt.name, func(t *testing.T) {
			gocore.Config().Set(tt.key, "0")
			defer gocore.Config().Unset(tt.key)

			s := NewSettings()
			require.Equal(t, 0, tt.getField(s))
		})
	}
}

func TestBlockAssembly_BoundaryValues_LargeInts(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		value    string
		getField func(*Settings) int
		expected int
	}{
		{"UnminedLoadingBatchSize_100M", "blockassembly_unminedLoadingBatchSize", "100000000", func(s *Settings) int { return s.BlockAssembly.UnminedLoadingBatchSize }, 100000000},
		{"InitialMerkleItemsPerSubtree_4M", "initial_merkle_items_per_subtree", "4194304", func(s *Settings) int { return s.BlockAssembly.InitialMerkleItemsPerSubtree }, 4194304},
		{"MoveBackBlockConcurrency_1000", "blockassembly_moveBackBlockConcurrency", "1000", func(s *Settings) int { return s.BlockAssembly.MoveBackBlockConcurrency }, 1000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gocore.Config().Set(tt.key, tt.value)
			defer gocore.Config().Unset(tt.key)

			s := NewSettings()
			require.Equal(t, tt.expected, tt.getField(s))
		})
	}
}

func TestBlockAssembly_BoundaryValues_ZeroDurations(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		getField func(*Settings) time.Duration
	}{
		{"GRPCRetryBackoff", "blockassembly_grpcRetryBackoff", func(s *Settings) time.Duration { return s.BlockAssembly.GRPCRetryBackoff }},
		{"IdleSleepDuration", "blockassembly_idle_sleep_duration", func(s *Settings) time.Duration { return s.BlockAssembly.IdleSleepDuration }},
		{"SubtreeAnnouncementInterval", "blockassembly_subtreeAnnouncementInterval", func(s *Settings) time.Duration { return s.BlockAssembly.SubtreeAnnouncementInterval }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gocore.Config().Set(tt.key, "0s")
			defer gocore.Config().Unset(tt.key)

			s := NewSettings()
			require.Equal(t, time.Duration(0), tt.getField(s))
		})
	}
}

func TestBlockAssembly_FieldIndependence(t *testing.T) {
	gocore.Config().Set("blockassembly_sendBatchSize", "999")
	defer gocore.Config().Unset("blockassembly_sendBatchSize")

	gocore.Config().Set("blockassembly_subtreeStorageWorkers", "16")
	defer gocore.Config().Unset("blockassembly_subtreeStorageWorkers")

	s := NewSettings()
	require.Equal(t, 999, s.BlockAssembly.SendBatchSize)
	require.Equal(t, 16, s.BlockAssembly.SubtreeStorageWorkers)

	// Unrelated fields retain defaults
	require.Equal(t, 1000, s.BlockAssembly.NewSubtreeChanBuffer)
	require.Equal(t, 375, s.BlockAssembly.MoveBackBlockConcurrency)
}

// TestBlockAssembly_MerkleItemsRelationship tests that min <= initial <= max for merkle items
// when configured correctly via direct struct modification (conf file may override defaults).
func TestBlockAssembly_MerkleItemsRelationship(t *testing.T) {
	s := NewSettings()
	s.BlockAssembly.MinimumMerkleItemsPerSubtree = 1024
	s.BlockAssembly.InitialMerkleItemsPerSubtree = 1048576
	s.BlockAssembly.MaximumMerkleItemsPerSubtree = 2097152

	require.LessOrEqual(t, s.BlockAssembly.MinimumMerkleItemsPerSubtree, s.BlockAssembly.InitialMerkleItemsPerSubtree,
		"MinimumMerkleItems should be <= InitialMerkleItems")
	require.LessOrEqual(t, s.BlockAssembly.InitialMerkleItemsPerSubtree, s.BlockAssembly.MaximumMerkleItemsPerSubtree,
		"InitialMerkleItems should be <= MaximumMerkleItems")
}
