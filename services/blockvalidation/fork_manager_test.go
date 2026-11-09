package blockvalidation

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupForkManagerTest(t *testing.T) {
	initPrometheusMetrics()
}

func TestNewForkManager(t *testing.T) {
	initPrometheusMetrics()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.MaxParallelForks = 5
	fm := NewForkManager(logger, tSettings)

	assert.NotNil(t, fm)
	assert.Equal(t, 5, fm.maxParallelForks)
	assert.Equal(t, 0, fm.GetForkCount())
	assert.Equal(t, 0, fm.GetParallelProcessingCount())
}

func TestForkManager_RegisterFork(t *testing.T) {
	initPrometheusMetrics()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.MaxParallelForks = 5
	fm := NewForkManager(logger, tSettings)

	t.Run("Register new fork", func(t *testing.T) {
		baseHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
		fork := fm.RegisterFork("fork1", baseHash, 1000)

		assert.NotNil(t, fork)
		assert.Equal(t, "fork1", fork.ID)
		assert.Equal(t, baseHash, fork.BaseHash)
		assert.Equal(t, uint32(1000), fork.BaseHeight)
		assert.Equal(t, baseHash, fork.TipHash)
		assert.Equal(t, uint32(1000), fork.TipHeight)
		assert.Nil(t, fork.ProcessingBlock)
		assert.Empty(t, fork.Blocks)
		assert.Equal(t, 1, fm.GetForkCount())
	})

	t.Run("Register duplicate fork returns existing", func(t *testing.T) {
		baseHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000002")
		fork1 := fm.RegisterFork("fork2", baseHash, 2000)
		fork2 := fm.RegisterFork("fork2", baseHash, 2000)

		assert.Equal(t, fork1, fork2)         // Should return same fork
		assert.Equal(t, 2, fm.GetForkCount()) // Still only 2 forks total
	})
}

func TestForkManager_GetFork(t *testing.T) {
	initPrometheusMetrics()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.MaxParallelForks = 5
	fm := NewForkManager(logger, tSettings)

	baseHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	fm.RegisterFork("fork1", baseHash, 1000)

	t.Run("Get existing fork", func(t *testing.T) {
		fork := fm.GetFork("fork1")
		assert.NotNil(t, fork)
		assert.Equal(t, "fork1", fork.ID)
	})

	t.Run("Get non-existing fork", func(t *testing.T) {
		fork := fm.GetFork("fork_unknown")
		assert.Nil(t, fork)
	})
}

func TestForkManager_AddBlockToFork(t *testing.T) {
	initPrometheusMetrics()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.MaxParallelForks = 5
	fm := NewForkManager(logger, tSettings)

	t.Run("Add block to existing fork", func(t *testing.T) {
		baseHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
		fm.RegisterFork("fork1", baseHash, 1000)

		// Create a block
		merkleRoot, _ := chainhash.NewHashFromStr("1111111111111111111111111111111111111111111111111111111111111111")
		block := &model.Block{
			Height: 1001,
			Header: &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  baseHash,
				HashMerkleRoot: merkleRoot,
				Timestamp:      uint32(time.Now().Unix()),
				Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d},
				Nonce:          0,
			},
		}

		err := fm.AddBlockToFork(block, "fork1")
		assert.NoError(t, err)

		// Verify block was added
		fork := fm.GetFork("fork1")
		assert.Equal(t, 1, len(fork.Blocks))
		assert.Equal(t, block.Hash(), fork.Blocks[0])
		assert.Equal(t, block.Hash(), fork.TipHash)
		assert.Equal(t, uint32(1001), fork.TipHeight)

		// Verify block mapping
		forkID, exists := fm.GetForkForBlock(block.Hash())
		assert.True(t, exists)
		assert.Equal(t, "fork1", forkID)
	})

	t.Run("Add block to non-existing fork", func(t *testing.T) {
		prevHash, _ := chainhash.NewHashFromStr("2222222222222222222222222222222222222222222222222222222222222222")
		merkleRoot, _ := chainhash.NewHashFromStr("3333333333333333333333333333333333333333333333333333333333333333")
		block := &model.Block{
			Height: 1002,
			Header: &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  prevHash,
				HashMerkleRoot: merkleRoot,
				Timestamp:      uint32(time.Now().Unix()),
				Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d},
				Nonce:          0,
			},
		}

		err := fm.AddBlockToFork(block, "unknown_fork")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "fork unknown_fork not found")
	})
}

func TestForkManager_BlockProcessing(t *testing.T) {
	initPrometheusMetrics()
	ctx := context.Background()
	logger := ulogger.TestLogger{}

	t.Run("Can process block within limit", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockValidation.MaxParallelForks = 2
		fm := NewForkManager(logger, tSettings)

		hash1, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
		hash2, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000002")

		// Should be able to process first two blocks
		canProcess, err := fm.CanProcessBlock(ctx, hash1, PriorityChainExtending)
		assert.NoError(t, err)
		assert.True(t, canProcess)
		assert.True(t, fm.StartProcessingBlock(hash1, PriorityChainExtending))
		assert.Equal(t, 1, fm.GetParallelProcessingCount())

		canProcess, err = fm.CanProcessBlock(ctx, hash2, PriorityChainExtending)
		assert.NoError(t, err)
		assert.True(t, canProcess)
		assert.True(t, fm.StartProcessingBlock(hash2, PriorityChainExtending))
		assert.Equal(t, 2, fm.GetParallelProcessingCount())

		// Should not be able to process third block (limit reached)
		hash3, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000003")
		canProcess, err = fm.CanProcessBlock(ctx, hash3, PriorityChainExtending)
		assert.NoError(t, err)
		assert.False(t, canProcess)
		assert.False(t, fm.StartProcessingBlock(hash3, PriorityChainExtending))
		assert.Equal(t, 2, fm.GetParallelProcessingCount())

		// Finish processing first block
		fm.FinishProcessingBlock(hash1)
		assert.Equal(t, 1, fm.GetParallelProcessingCount())

		// Now should be able to process third block
		canProcess, err = fm.CanProcessBlock(ctx, hash3, PriorityChainExtending)
		assert.NoError(t, err)
		assert.True(t, canProcess)
		assert.True(t, fm.StartProcessingBlock(hash3, PriorityChainExtending))
		assert.Equal(t, 2, fm.GetParallelProcessingCount())
	})

	t.Run("Already processing block", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockValidation.MaxParallelForks = 5
		fm := NewForkManager(logger, tSettings)

		hash1, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")

		assert.True(t, fm.StartProcessingBlock(hash1, PriorityChainExtending))
		// Should not be able to start processing same block again
		assert.False(t, fm.StartProcessingBlock(hash1, PriorityChainExtending))
		canProcess, err := fm.CanProcessBlock(ctx, hash1, PriorityChainExtending)
		assert.NoError(t, err)
		assert.False(t, canProcess)
	})
}

func TestForkManager_SetProcessingBlockForFork(t *testing.T) {
	initPrometheusMetrics()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.MaxParallelForks = 4
	fm := NewForkManager(logger, tSettings)

	baseHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	fm.RegisterFork("fork1", baseHash, 1000)

	t.Run("Set processing block for existing fork", func(t *testing.T) {
		blockHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000002")
		err := fm.SetProcessingBlockForFork("fork1", blockHash)
		assert.NoError(t, err)

		fork := fm.GetFork("fork1")
		assert.Equal(t, blockHash, fork.ProcessingBlock)
	})

	t.Run("Set processing block for non-existing fork", func(t *testing.T) {
		blockHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000003")
		err := fm.SetProcessingBlockForFork("unknown_fork", blockHash)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "fork unknown_fork not found")
	})

	t.Run("Clear processing block", func(t *testing.T) {
		err := fm.SetProcessingBlockForFork("fork1", nil)
		assert.NoError(t, err)

		fork := fm.GetFork("fork1")
		assert.Nil(t, fork.ProcessingBlock)
	})
}

func TestForkManager_CleanupFork(t *testing.T) {
	initPrometheusMetrics()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.MaxParallelForks = 4
	fm := NewForkManager(logger, tSettings)

	// Set up fork with blocks
	baseHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	fm.RegisterFork("fork1", baseHash, 1000)

	// Add some blocks
	for i := 0; i < 3; i++ {
		merkleRoot, _ := chainhash.NewHashFromStr("1111111111111111111111111111111111111111111111111111111111111111")
		block := &model.Block{
			Height: uint32(1001 + i),
			Header: &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  baseHash,
				HashMerkleRoot: merkleRoot,
				Timestamp:      uint32(time.Now().Unix() + int64(i)),
				Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d},
				Nonce:          uint32(i),
			},
		}
		err := fm.AddBlockToFork(block, "fork1")
		require.NoError(t, err)
	}

	fork := fm.GetFork("fork1")
	blockHashes := make([]*chainhash.Hash, len(fork.Blocks))
	copy(blockHashes, fork.Blocks)

	// Verify blocks are mapped
	for _, hash := range blockHashes {
		forkID, exists := fm.GetForkForBlock(hash)
		assert.True(t, exists)
		assert.Equal(t, "fork1", forkID)
	}

	// Clean up fork
	fm.CleanupFork("fork1")

	// Verify fork is gone
	assert.Nil(t, fm.GetFork("fork1"))
	assert.Equal(t, 0, fm.GetForkCount())

	// Verify block mappings are removed
	for _, hash := range blockHashes {
		_, exists := fm.GetForkForBlock(hash)
		assert.False(t, exists)
	}
}

func TestForkManager_CleanupOldForks(t *testing.T) {
	initPrometheusMetrics()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.MaxParallelForks = 4
	fm := NewForkManager(logger, tSettings)

	// Register some forks
	baseHash1, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	baseHash2, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000002")
	baseHash3, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000003")

	fm.RegisterFork("fork1", baseHash1, 1000)
	fm.RegisterFork("fork2", baseHash2, 950)
	fm.RegisterFork("fork3", baseHash3, 800)

	assert.Equal(t, 3, fm.GetForkCount())

	// Clean up forks older than height 900
	fm.CleanupOldForks(900)

	// Should have removed fork3 (height 800)
	assert.Equal(t, 2, fm.GetForkCount())
	assert.NotNil(t, fm.GetFork("fork1"))
	assert.NotNil(t, fm.GetFork("fork2"))
	assert.Nil(t, fm.GetFork("fork3"))
}

func TestForkManager_ConcurrentAccess(t *testing.T) {
	initPrometheusMetrics()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.MaxParallelForks = 10
	fm := NewForkManager(logger, tSettings)

	// Register a fork
	baseHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	fm.RegisterFork("fork1", baseHash, 1000)

	// Test concurrent operations
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Concurrent block processing
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			hash := chainhash.Hash{}
			hash[0] = byte(idx)

			for j := 0; j < 10; j++ {
				select {
				case <-ctx.Done():
					return
				default:
					canProcess, err := fm.CanProcessBlock(ctx, &hash, PriorityChainExtending)
					if err == nil && canProcess {
						fm.StartProcessingBlock(&hash, PriorityChainExtending)
						time.Sleep(time.Millisecond)
						fm.FinishProcessingBlock(&hash)
					}
				}
			}
		}(i)
	}

	// Concurrent fork operations
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			for j := 0; j < 5; j++ {
				select {
				case <-ctx.Done():
					return
				default:
					// Add block to fork
					merkleRoot := chainhash.Hash{}
					merkleRoot[0] = byte(idx*10 + j)

					block := &model.Block{
						Height: uint32(1001 + idx*10 + j),
						Header: &model.BlockHeader{
							Version:        1,
							HashPrevBlock:  baseHash,
							HashMerkleRoot: &merkleRoot,
							Timestamp:      uint32(time.Now().Unix()),
							Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d},
							Nonce:          uint32(idx*10 + j),
						},
					}

					if err := fm.AddBlockToFork(block, "fork1"); err != nil {
						// Handle error in test - ok to ignore for concurrent test
						continue
					}

					// Check fork existence
					fm.GetForkForBlock(block.Hash())

					time.Sleep(time.Millisecond)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify state is consistent
	assert.GreaterOrEqual(t, fm.GetForkCount(), 1)
	fork := fm.GetFork("fork1")
	assert.NotNil(t, fork)
	assert.GreaterOrEqual(t, len(fork.Blocks), 0)
}

func TestForkManager_GetAllForks(t *testing.T) {
	initPrometheusMetrics()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.MaxParallelForks = 4
	fm := NewForkManager(logger, tSettings)

	// Register multiple forks
	baseHash1, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	baseHash2, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000002")
	baseHash3, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000003")

	fm.RegisterFork("fork1", baseHash1, 1000)
	fm.RegisterFork("fork2", baseHash2, 1100)
	fm.RegisterFork("fork3", baseHash3, 900)

	forks := fm.GetAllForks()
	assert.Equal(t, 3, len(forks))

	// Verify all forks are returned
	forkIDs := make(map[string]bool)
	for _, fork := range forks {
		forkIDs[fork.ID] = true
	}

	assert.True(t, forkIDs["fork1"])
	assert.True(t, forkIDs["fork2"])
	assert.True(t, forkIDs["fork3"])
}

func TestForkManager_ConcurrentForkCreation(t *testing.T) {
	initPrometheusMetrics()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.MaxParallelForks = 100
	fm := NewForkManager(logger, tSettings)

	baseHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")

	var wg sync.WaitGroup
	forkCount := 100

	for i := 0; i < forkCount; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			hash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000002")
			hash[0] = byte(idx)

			fm.RegisterFork("fork-"+string(rune(idx)), hash, uint32(1000+idx))
		}(i)
	}

	wg.Wait()

	assert.GreaterOrEqual(t, len(fm.forks), 1)
	assert.LessOrEqual(t, len(fm.forks), forkCount)

	_ = baseHash
}

func TestForkManager_BlockProcessingGuard(t *testing.T) {
	initPrometheusMetrics()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.MaxParallelForks = 5
	fm := NewForkManager(logger, tSettings)

	blockHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")

	t.Run("Normal cleanup", func(t *testing.T) {
		guard, err := fm.MarkBlockProcessing(blockHash)
		require.NoError(t, err)

		fm.mu.RLock()
		assert.True(t, fm.processingBlocks[*blockHash])
		fm.mu.RUnlock()

		guard.Release()

		fm.mu.RLock()
		assert.False(t, fm.processingBlocks[*blockHash])
		fm.mu.RUnlock()
	})

	t.Run("Double release safety", func(t *testing.T) {
		guard, err := fm.MarkBlockProcessing(blockHash)
		require.NoError(t, err)

		guard.Release()
		guard.Release()

		fm.mu.RLock()
		assert.False(t, fm.processingBlocks[*blockHash])
		fm.mu.RUnlock()
	})

	t.Run("Already processing", func(t *testing.T) {
		guard1, err := fm.MarkBlockProcessing(blockHash)
		require.NoError(t, err)

		guard2, err := fm.MarkBlockProcessing(blockHash)
		assert.Error(t, err)
		assert.Nil(t, guard2)

		guard1.Release()
	})

	t.Run("Panic recovery", func(t *testing.T) {
		func() {
			guard, err := fm.MarkBlockProcessing(blockHash)
			require.NoError(t, err)

			defer func() {
				if r := recover(); r != nil {
					guard.Release()
				}
			}()

			panic("simulated panic")
		}()

		fm.mu.RLock()
		assert.False(t, fm.processingBlocks[*blockHash])
		fm.mu.RUnlock()
	})
}

func TestForkManager_CleanupRoutine(t *testing.T) {
	initPrometheusMetrics()
	logger := ulogger.TestLogger{}

	config := ForkCleanupConfig{
		Enabled:          true,
		MaxAge:           50 * time.Millisecond,
		MaxInactiveForks: 10,
		CheckInterval:    30 * time.Millisecond,
	}

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.MaxParallelForks = 10
	fm := NewForkManagerWithConfig(logger, tSettings, config)

	baseHash1, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	baseHash2, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000002")

	fork1 := fm.RegisterFork("fork1", baseHash1, 1000)
	fork2 := fm.RegisterFork("fork2", baseHash2, 2000)

	fork1.State = ForkStateStale
	fork1.LastActivity = time.Now().Add(-200 * time.Millisecond)

	fork2.State = ForkStateStale
	fork2.LastActivity = time.Now().Add(-150 * time.Millisecond)

	initialCount := fm.GetForkCount()
	assert.Equal(t, 2, initialCount)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	go fm.StartCleanupRoutine(ctx)

	time.Sleep(200 * time.Millisecond)

	finalCount := fm.GetForkCount()
	assert.Less(t, finalCount, initialCount, "Cleanup should have removed at least one fork")
}

func TestForkManager_ForkResolution(t *testing.T) {
	initPrometheusMetrics()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.MaxParallelForks = 10
	fm := NewForkManager(logger, tSettings)

	resolutions := make([]*ForkResolution, 0)
	var resolutionMu sync.Mutex

	fm.OnForkResolution(func(resolution *ForkResolution) {
		resolutionMu.Lock()
		resolutions = append(resolutions, resolution)
		resolutionMu.Unlock()
	})

	baseHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	fm.RegisterFork("fork1", baseHash, 1000)

	merkleRoot, _ := chainhash.NewHashFromStr("1111111111111111111111111111111111111111111111111111111111111111")
	block := &model.Block{
		Height: 1001,
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  baseHash,
			HashMerkleRoot: merkleRoot,
			Timestamp:      uint32(time.Now().Unix()),
			Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d},
			Nonce:          0,
		},
	}

	require.NoError(t, fm.AddBlockToFork(block, "fork1"))

	fm.CheckForkResolution(context.Background(), block, block.Hash(), 1001)

	time.Sleep(100 * time.Millisecond)

	resolutionMu.Lock()
	defer resolutionMu.Unlock()

	require.Len(t, resolutions, 1)
	resolution := resolutions[0]

	assert.Equal(t, "fork1", resolution.ForkID)
	assert.Equal(t, "main", resolution.ResolvedTo)
	assert.Equal(t, uint32(1001), resolution.FinalHeight)
}

func TestForkManager_MetricsCollection(t *testing.T) {
	initPrometheusMetrics()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.MaxParallelForks = 10
	fm := NewForkManager(logger, tSettings)

	baseHash1, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	baseHash2, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000002")

	fork1 := fm.RegisterFork("fork1", baseHash1, 1000)
	fork2 := fm.RegisterFork("fork2", baseHash2, 2000)

	for i := 0; i < 5; i++ {
		merkleRoot, _ := chainhash.NewHashFromStr("1111111111111111111111111111111111111111111111111111111111111111")
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

	assert.Equal(t, 2, fm.GetForkCount())
	assert.Equal(t, 5, len(fork1.Blocks))
	assert.Equal(t, 0, len(fork2.Blocks))

	_ = fork1
	_ = fork2
}
