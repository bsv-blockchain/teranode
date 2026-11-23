package blockvalidation

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestForkManager_AtomicCounter_ConcurrentGuardReleases tests that concurrent guard releases
// maintain correct counter synchronization
func TestForkManager_AtomicCounter_ConcurrentGuardReleases(t *testing.T) {
	initPrometheusMetrics()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.MaxParallelForks = 100
	fm := NewForkManager(logger, tSettings)

	numGuards := 50
	guards := make([]*BlockProcessingGuard, numGuards)

	// Mark multiple blocks as processing
	for i := 0; i < numGuards; i++ {
		hash := &chainhash.Hash{}
		hash[0] = byte(i)

		guard, err := fm.MarkBlockProcessing(hash)
		require.NoError(t, err)
		guards[i] = guard
	}

	// Verify counter equals map size
	fm.mu.RLock()
	mapSize := len(fm.processingBlocks)
	counterValue := int(fm.processingCount.Load())
	fm.mu.RUnlock()

	assert.Equal(t, numGuards, mapSize, "Map size should equal number of guards")
	assert.Equal(t, numGuards, counterValue, "Counter should equal number of guards")

	// Release all guards concurrently
	var wg sync.WaitGroup
	for i := 0; i < numGuards; i++ {
		wg.Add(1)
		go func(guard *BlockProcessingGuard) {
			defer wg.Done()
			guard.Release()
		}(guards[i])
	}

	wg.Wait()

	// Verify counter and map are both zero
	fm.mu.RLock()
	finalMapSize := len(fm.processingBlocks)
	finalCounterValue := int(fm.processingCount.Load())
	fm.mu.RUnlock()

	assert.Equal(t, 0, finalMapSize, "Map should be empty after all releases")
	assert.Equal(t, 0, finalCounterValue, "Counter should be zero after all releases")
	assert.Equal(t, finalMapSize, finalCounterValue, "Counter must equal map size")
}

// TestForkManager_AtomicCounter_CleanupDuringProcessing tests cleanup while blocks
// are actively being processed
func TestForkManager_AtomicCounter_CleanupDuringProcessing(t *testing.T) {
	initPrometheusMetrics()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.MaxParallelForks = 50
	fm := NewForkManager(logger, tSettings)

	// Create fork with blocks
	baseHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	fm.RegisterFork("test-fork", baseHash, 1000)

	// Add blocks to fork
	numBlocks := 20
	blockHashes := make([]*chainhash.Hash, numBlocks)

	for i := 0; i < numBlocks; i++ {
		merkleRoot := &chainhash.Hash{}
		merkleRoot[0] = byte(i)

		block := &model.Block{
			Height: uint32(1001 + i),
			Header: &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  baseHash,
				HashMerkleRoot: merkleRoot,
				Timestamp:      uint32(time.Now().Unix()),
				Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d},
				Nonce:          uint32(i),
			},
		}

		require.NoError(t, fm.AddBlockToFork(block, "test-fork"))
		blockHashes[i] = block.Hash()
	}

	// Start processing some blocks
	numProcessing := 10
	for i := 0; i < numProcessing; i++ {
		fm.StartProcessingBlock(blockHashes[i], PriorityChainExtending)
	}

	// Verify initial state
	fm.mu.RLock()
	initialMapSize := len(fm.processingBlocks)
	initialCounter := int(fm.processingCount.Load())
	fm.mu.RUnlock()

	assert.Equal(t, numProcessing, initialMapSize)
	assert.Equal(t, numProcessing, initialCounter)

	// Cleanup fork while blocks are being processed
	fm.CleanupFork("test-fork")

	// Verify counter and map are synchronized after cleanup
	fm.mu.RLock()
	finalMapSize := len(fm.processingBlocks)
	finalCounter := int(fm.processingCount.Load())
	fm.mu.RUnlock()

	assert.Equal(t, 0, finalMapSize, "All blocks should be removed from map")
	assert.Equal(t, 0, finalCounter, "Counter should be zero after cleanup")
	assert.Equal(t, finalMapSize, finalCounter, "Counter must equal map size after cleanup")
}

// TestForkManager_AtomicCounter_NoCounterDrift tests that counter never drifts
// from actual map size across various operations
func TestForkManager_AtomicCounter_NoCounterDrift(t *testing.T) {
	initPrometheusMetrics()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.MaxParallelForks = 20
	fm := NewForkManager(logger, tSettings)

	// Track counter drift in background
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	driftDetected := atomic.Bool{}
	var driftWg sync.WaitGroup
	driftWg.Add(1)

	go func() {
		defer driftWg.Done()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				fm.mu.RLock()
				mapSize := len(fm.processingBlocks)
				counterValue := int(fm.processingCount.Load())
				fm.mu.RUnlock()

				if mapSize != counterValue {
					t.Logf("DRIFT DETECTED: map size=%d, counter=%d", mapSize, counterValue)
					driftDetected.Store(true)
				}
			}
		}
	}()

	// Perform mixed operations
	var wg sync.WaitGroup

	// Worker 1: Start and finish processing
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			for j := 0; j < 10; j++ {
				select {
				case <-ctx.Done():
					return
				default:
					hash := &chainhash.Hash{}
					hash[0] = byte(idx)
					hash[1] = byte(j)

					if fm.StartProcessingBlock(hash, PriorityChainExtending) {
						time.Sleep(5 * time.Millisecond)
						fm.FinishProcessingBlock(hash)
					}
				}
			}
		}(i)
	}

	// Worker 2: Mark with guard and release
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			for j := 0; j < 10; j++ {
				select {
				case <-ctx.Done():
					return
				default:
					hash := &chainhash.Hash{}
					hash[2] = byte(idx)
					hash[3] = byte(j)

					guard, err := fm.MarkBlockProcessing(hash)
					if err == nil {
						time.Sleep(3 * time.Millisecond)
						guard.Release()
					}
				}
			}
		}(i)
	}

	wg.Wait()
	cancel()
	driftWg.Wait()

	// Final verification
	fm.mu.RLock()
	finalMapSize := len(fm.processingBlocks)
	finalCounter := int(fm.processingCount.Load())
	fm.mu.RUnlock()

	assert.False(t, driftDetected.Load(), "Counter drift was detected during operations")
	assert.Equal(t, finalMapSize, finalCounter, "Final counter must equal map size")
}

// TestForkManager_AtomicCounter_RaceBetweenCleanupAndFinish tests race condition
// between CleanupFork and FinishProcessingBlock
func TestForkManager_AtomicCounter_RaceBetweenCleanupAndFinish(t *testing.T) {
	initPrometheusMetrics()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.MaxParallelForks = 30
	fm := NewForkManager(logger, tSettings)

	// Create fork
	baseHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	fm.RegisterFork("race-fork", baseHash, 1000)

	// Add blocks
	numBlocks := 15
	blockHashes := make([]*chainhash.Hash, numBlocks)

	for i := 0; i < numBlocks; i++ {
		merkleRoot := &chainhash.Hash{}
		merkleRoot[0] = byte(i)

		block := &model.Block{
			Height: uint32(1001 + i),
			Header: &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  baseHash,
				HashMerkleRoot: merkleRoot,
				Timestamp:      uint32(time.Now().Unix()),
				Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d},
				Nonce:          uint32(i),
			},
		}

		require.NoError(t, fm.AddBlockToFork(block, "race-fork"))
		blockHashes[i] = block.Hash()
	}

	// Start processing all blocks
	for i := 0; i < numBlocks; i++ {
		fm.StartProcessingBlock(blockHashes[i], PriorityChainExtending)
	}

	var wg sync.WaitGroup

	// Goroutine 1: Call FinishProcessingBlock on all blocks
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numBlocks; i++ {
			fm.FinishProcessingBlock(blockHashes[i])
			time.Sleep(time.Millisecond)
		}
	}()

	// Goroutine 2: Call CleanupFork concurrently
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(5 * time.Millisecond) // Let some finish first
		fm.CleanupFork("race-fork")
	}()

	wg.Wait()

	// Verify no counter drift occurred
	fm.mu.RLock()
	finalMapSize := len(fm.processingBlocks)
	finalCounter := int(fm.processingCount.Load())
	fm.mu.RUnlock()

	assert.GreaterOrEqual(t, finalCounter, 0, "Counter should never go negative")
	assert.Equal(t, finalMapSize, finalCounter, "Counter must equal map size after race")
}

// TestForkManager_AtomicCounter_RaceBetweenGuardAndCleanup tests race between
// guard release and fork cleanup
func TestForkManager_AtomicCounter_RaceBetweenGuardAndCleanup(t *testing.T) {
	initPrometheusMetrics()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.MaxParallelForks = 30
	fm := NewForkManager(logger, tSettings)

	// Create fork
	baseHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	fm.RegisterFork("guard-race-fork", baseHash, 1000)

	// Add blocks
	numBlocks := 10
	guards := make([]*BlockProcessingGuard, numBlocks)

	for i := 0; i < numBlocks; i++ {
		merkleRoot := &chainhash.Hash{}
		merkleRoot[0] = byte(i)

		block := &model.Block{
			Height: uint32(1001 + i),
			Header: &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  baseHash,
				HashMerkleRoot: merkleRoot,
				Timestamp:      uint32(time.Now().Unix()),
				Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d},
				Nonce:          uint32(i),
			},
		}

		require.NoError(t, fm.AddBlockToFork(block, "guard-race-fork"))

		// Mark as processing with guard
		guard, err := fm.MarkBlockProcessing(block.Hash())
		require.NoError(t, err)
		guards[i] = guard
	}

	var wg sync.WaitGroup

	// Goroutine 1: Release all guards
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numBlocks; i++ {
			guards[i].Release()
			time.Sleep(2 * time.Millisecond)
		}
	}()

	// Goroutine 2: Cleanup fork mid-way through guard releases
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(10 * time.Millisecond) // Let some guards release first
		fm.CleanupFork("guard-race-fork")
	}()

	wg.Wait()

	// Verify counter integrity
	fm.mu.RLock()
	finalMapSize := len(fm.processingBlocks)
	finalCounter := int(fm.processingCount.Load())
	fm.mu.RUnlock()

	assert.GreaterOrEqual(t, finalCounter, 0, "Counter should never go negative")
	assert.Equal(t, finalMapSize, finalCounter, "Counter must equal map size")
}

// TestForkManager_AtomicCounter_CleanupOldForksWithProcessing tests CleanupOldForks
// with actively processing blocks
func TestForkManager_AtomicCounter_CleanupOldForksWithProcessing(t *testing.T) {
	initPrometheusMetrics()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.MaxParallelForks = 50
	fm := NewForkManager(logger, tSettings)

	// Create multiple forks at different heights
	baseHash1, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	baseHash2, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000002")
	baseHash3, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000003")

	fm.RegisterFork("old-fork-1", baseHash1, 800)
	fm.RegisterFork("old-fork-2", baseHash2, 850)
	fm.RegisterFork("new-fork", baseHash3, 1000)

	// Add blocks to old forks and start processing them
	oldForkBlocks := make([]*chainhash.Hash, 0)

	for forkIdx, forkID := range []string{"old-fork-1", "old-fork-2"} {
		for i := 0; i < 5; i++ {
			merkleRoot := &chainhash.Hash{}
			merkleRoot[0] = byte(forkIdx*10 + i)

			block := &model.Block{
				Height: uint32(801 + forkIdx*50 + i),
				Header: &model.BlockHeader{
					Version:        1,
					HashPrevBlock:  baseHash1,
					HashMerkleRoot: merkleRoot,
					Timestamp:      uint32(time.Now().Unix()),
					Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d},
					Nonce:          uint32(i),
				},
			}

			require.NoError(t, fm.AddBlockToFork(block, forkID))
			blockHash := block.Hash()
			oldForkBlocks = append(oldForkBlocks, blockHash)

			// Mark half as processing
			if i%2 == 0 {
				fm.StartProcessingBlock(blockHash, PriorityChainExtending)
			}
		}
	}

	// Verify initial state
	fm.mu.RLock()
	initialMapSize := len(fm.processingBlocks)
	initialCounter := int(fm.processingCount.Load())
	fm.mu.RUnlock()

	assert.Greater(t, initialMapSize, 0, "Should have processing blocks")
	assert.Equal(t, initialMapSize, initialCounter, "Initial counter should equal map size")

	// Cleanup old forks (will remove old-fork-1 and old-fork-2)
	fm.CleanupOldForks(900)

	// Verify counter and map are synchronized
	fm.mu.RLock()
	finalMapSize := len(fm.processingBlocks)
	finalCounter := int(fm.processingCount.Load())
	remainingForks := fm.GetForkCount()
	fm.mu.RUnlock()

	assert.Equal(t, 1, remainingForks, "Only new-fork should remain")
	assert.GreaterOrEqual(t, finalCounter, 0, "Counter should never go negative")
	assert.Equal(t, finalMapSize, finalCounter, "Counter must equal map size after cleanup")
}

// TestForkManager_AtomicCounter_DoubleReleaseScenario tests double release doesn't
// cause double decrement
func TestForkManager_AtomicCounter_DoubleReleaseScenario(t *testing.T) {
	initPrometheusMetrics()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.MaxParallelForks = 10
	fm := NewForkManager(logger, tSettings)

	blockHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")

	// Mark block as processing
	guard, err := fm.MarkBlockProcessing(blockHash)
	require.NoError(t, err)

	// Verify initial state
	fm.mu.RLock()
	assert.Equal(t, 1, len(fm.processingBlocks))
	assert.Equal(t, 1, int(fm.processingCount.Load()))
	fm.mu.RUnlock()

	// Release once
	guard.Release()

	// Verify after first release
	fm.mu.RLock()
	firstMapSize := len(fm.processingBlocks)
	firstCounter := int(fm.processingCount.Load())
	fm.mu.RUnlock()

	assert.Equal(t, 0, firstMapSize)
	assert.Equal(t, 0, firstCounter)

	// Try to release again (should be safe, no double decrement)
	guard.Release()

	// Verify counter didn't go negative
	fm.mu.RLock()
	finalMapSize := len(fm.processingBlocks)
	finalCounter := int(fm.processingCount.Load())
	fm.mu.RUnlock()

	assert.Equal(t, 0, finalMapSize, "Map should still be empty")
	assert.Equal(t, 0, finalCounter, "Counter should still be zero (not negative)")
	assert.GreaterOrEqual(t, finalCounter, 0, "Counter must never go negative")
}
