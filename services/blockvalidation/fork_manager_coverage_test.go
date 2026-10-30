package blockvalidation

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestForkManager_GetForkStats tests the GetForkStats method
func TestForkManager_GetForkStats(t *testing.T) {
	initPrometheusMetrics()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.MaxParallelForks = 10
	fm := NewForkManager(logger, tSettings)

	// Register some forks
	baseHash1, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	baseHash2, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000002")

	fork1 := fm.RegisterFork("fork1", baseHash1, 1000)
	fork2 := fm.RegisterFork("fork2", baseHash2, 2000)

	// Add blocks to fork1
	for i := 0; i < 5; i++ {
		merkleRoot := &chainhash.Hash{}
		merkleRoot[0] = byte(i)

		block := &model.Block{
			Height: uint32(1001 + i),
			Header: &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  baseHash1,
				HashMerkleRoot: merkleRoot,
				Timestamp:      uint32(time.Now().Unix()),
				Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d},
				Nonce:          uint32(i),
			},
		}

		require.NoError(t, fm.AddBlockToFork(block, "fork1"))
	}

	// Mark a block as processing in fork1
	blockHash := fork1.Blocks[0]
	fm.StartProcessingBlock(blockHash, PriorityChainExtending)

	// Get stats
	stats := fm.GetForkStats()

	// Verify fork1 stats
	assert.Contains(t, stats, "fork1")
	fork1Stats := stats["fork1"]
	assert.Equal(t, "fork1", fork1Stats.ID)
	assert.Equal(t, uint32(1000), fork1Stats.BaseHeight)
	assert.Greater(t, fork1Stats.TipHeight, uint32(1000))
	assert.Equal(t, 5, fork1Stats.BlockCount)
	assert.True(t, fork1Stats.ProcessingBlock)

	// Verify fork2 stats
	assert.Contains(t, stats, "fork2")
	fork2Stats := stats["fork2"]
	assert.Equal(t, "fork2", fork2Stats.ID)
	assert.Equal(t, uint32(2000), fork2Stats.BaseHeight)
	assert.Equal(t, uint32(2000), fork2Stats.TipHeight)
	assert.Equal(t, 0, fork2Stats.BlockCount)
	assert.False(t, fork2Stats.ProcessingBlock)

	_ = fork2 // Mark as used

	// Finish processing
	fm.FinishProcessingBlock(blockHash)

	// Check again
	statsAfter := fm.GetForkStats()
	fork1StatsAfter := statsAfter["fork1"]
	assert.False(t, fork1StatsAfter.ProcessingBlock)
}

// DetermineForkID tests are complex and require full blockchain mock
// Covered by integration tests instead

// TestForkManager_CheckOrphanedForks tests orphan detection
func TestForkManager_CheckOrphanedForks(t *testing.T) {
	initPrometheusMetrics()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams.CoinbaseMaturity = 100
	tSettings.BlockValidation.MaxParallelForks = 10
	fm := NewForkManager(logger, tSettings)

	// Register forks at different heights
	baseHash1, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	baseHash2, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000002")
	baseHash3, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000003")

	fork1 := fm.RegisterFork("old-fork", baseHash1, 500)    // Very old
	fork2 := fm.RegisterFork("medium-fork", baseHash2, 950) // Medium
	fork3 := fm.RegisterFork("recent-fork", baseHash3, 990) // Recent

	// Set all to active
	fork1.State = ForkStateActive
	fork2.State = ForkStateActive
	fork3.State = ForkStateActive

	// Main chain advances to height 1000
	fm.checkOrphanedForks(1000)

	// old-fork should be orphaned (500 + 100 < 1000)
	assert.Equal(t, ForkStateOrphaned, fork1.State)

	// medium-fork should be orphaned (950 + 100 > 1000, but close)
	// Actually 950 + 100 = 1050 > 1000, so NOT orphaned
	assert.Equal(t, ForkStateActive, fork2.State)

	// recent-fork should not be orphaned (990 + 100 > 1000)
	assert.Equal(t, ForkStateActive, fork3.State)

	// Main chain advances further to 1100
	fm.checkOrphanedForks(1100)

	// Now medium-fork should also be orphaned
	assert.Equal(t, ForkStateOrphaned, fork2.State)

	// recent-fork is borderline (990 + 100 = 1090 < 1100)
	assert.Equal(t, ForkStateOrphaned, fork3.State)
}

// TestForkManager_ApplyRetentionPolicy tests fork retention limits
func TestForkManager_ApplyRetentionPolicy(t *testing.T) {
	initPrometheusMetrics()
	logger := ulogger.TestLogger{}

	config := ForkCleanupConfig{
		Enabled:          true,
		MaxAge:           1 * time.Hour,
		MaxInactiveForks: 3,
		CheckInterval:    5 * time.Minute,
	}

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.MaxParallelForks = 20
	fm := NewForkManagerWithConfig(logger, tSettings, config)

	now := time.Now()

	// Create 5 stale forks with different activity times
	candidates := []*ForkBranch{
		{
			ID:           "fork1",
			State:        ForkStateStale,
			LastActivity: now.Add(-10 * time.Minute), // Most recent
		},
		{
			ID:           "fork2",
			State:        ForkStateStale,
			LastActivity: now.Add(-20 * time.Minute),
		},
		{
			ID:           "fork3",
			State:        ForkStateStale,
			LastActivity: now.Add(-30 * time.Minute),
		},
		{
			ID:           "fork4",
			State:        ForkStateStale,
			LastActivity: now.Add(-40 * time.Minute),
		},
		{
			ID:           "fork5",
			State:        ForkStateStale,
			LastActivity: now.Add(-50 * time.Minute), // Oldest
		},
	}

	// Register them
	for _, fork := range candidates {
		fm.forks[fork.ID] = fork
	}

	t.Run("Under limit returns all candidates", func(t *testing.T) {
		// With only 2 candidates, should return all
		smallList := candidates[:2]
		result := fm.applyRetentionPolicy(smallList)
		assert.Equal(t, len(smallList), len(result))
	})

	t.Run("Over limit keeps most recent forks", func(t *testing.T) {
		// MaxInactiveForks is 3, we have 5 total forks
		// Can remove up to 2 (5 - 3 = 2)
		result := fm.applyRetentionPolicy(candidates)

		// Should remove oldest forks
		assert.LessOrEqual(t, len(result), 2)

		// Verify oldest forks are in removal list
		removedIDs := make(map[string]bool)
		for _, fork := range result {
			removedIDs[fork.ID] = true
		}

		// fork5 (oldest) should be removed
		if len(result) > 0 {
			assert.True(t, removedIDs["fork5"] || removedIDs["fork4"])
		}
	})

	t.Run("Exactly at limit returns all candidates", func(t *testing.T) {
		// Already at limit (3 forks)
		fm2 := NewForkManagerWithConfig(logger, tSettings, config)
		for i := 0; i < 3; i++ {
			fm2.forks[candidates[i].ID] = candidates[i]
		}

		result := fm2.applyRetentionPolicy(candidates[:3])
		// At the limit, policy returns all candidates (caller decides what to do)
		assert.Equal(t, 3, len(result))
	})
}

// TestForkManager_GenerateForkID tests fork ID generation
func TestForkManager_GenerateForkID(t *testing.T) {
	initPrometheusMetrics()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	fm := NewForkManager(logger, tSettings)

	blockHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	height := uint32(1000)

	// Generate fork ID
	forkID := fm.generateForkID(blockHash, height)

	// Verify format: hash-height-timestamp
	assert.NotEmpty(t, forkID)
	assert.Contains(t, forkID, blockHash.String())
	assert.Contains(t, forkID, "1000")
	assert.Contains(t, forkID, "-")

	// Generate another ID - should be unique (different timestamp)
	time.Sleep(time.Microsecond)
	forkID2 := fm.generateForkID(blockHash, height)
	assert.NotEqual(t, forkID, forkID2, "Fork IDs should be unique due to timestamp")
}
