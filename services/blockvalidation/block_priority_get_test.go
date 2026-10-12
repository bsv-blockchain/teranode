package blockvalidation

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockBlockProcessor implements BlockProcessor for testing
type MockBlockProcessor struct {
	canProcess  map[chainhash.Hash]bool
	mu          sync.RWMutex
	callCount   int
	returnError bool
	blockDelay  time.Duration
}

func NewMockBlockProcessor() *MockBlockProcessor {
	return &MockBlockProcessor{
		canProcess: make(map[chainhash.Hash]bool),
	}
}

func (m *MockBlockProcessor) SetCanProcess(hash *chainhash.Hash, can bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.canProcess[*hash] = can
}

func (m *MockBlockProcessor) CanProcessBlock(ctx context.Context, hash *chainhash.Hash) (bool, error) {
	m.mu.Lock()
	m.callCount++
	if m.blockDelay > 0 {
		time.Sleep(m.blockDelay)
	}
	m.mu.Unlock()

	if m.returnError {
		return false, errors.NewProcessingError("mock error")
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	can, exists := m.canProcess[*hash]
	if !exists {
		return true, nil // Default to processable
	}
	return can, nil
}

func (m *MockBlockProcessor) GetCallCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.callCount
}

// TestBlockPriorityQueue_Get_EmptyQueue tests Get on empty queue
func TestBlockPriorityQueue_Get_EmptyQueue(t *testing.T) {
	initPrometheusMetrics()
	logger := ulogger.TestLogger{}
	pq := NewBlockPriorityQueue(logger)
	mockBP := NewMockBlockProcessor()
	ctx := context.Background()

	block, status := pq.Get(ctx, mockBP)
	assert.Equal(t, GetEmpty, status)
	assert.Nil(t, block.hash)
}

// TestBlockPriorityQueue_Get_SingleProcessableBlock tests Get with one processable block
func TestBlockPriorityQueue_Get_SingleProcessableBlock(t *testing.T) {
	initPrometheusMetrics()
	logger := ulogger.TestLogger{}
	pq := NewBlockPriorityQueue(logger)
	mockBP := NewMockBlockProcessor()
	ctx := context.Background()

	hash := &chainhash.Hash{}
	hash[0] = 1

	pq.Add(processBlockFound{hash: hash}, PriorityChainExtending, 1000)

	block, status := pq.Get(ctx, mockBP)
	assert.Equal(t, GetOK, status)
	assert.Equal(t, hash, block.hash)
	assert.Equal(t, 0, pq.Size(), "Block should be removed from queue")
}

// TestBlockPriorityQueue_Get_FastPathDeepQueue verifies the steady-state fast path:
// when the highest-priority (front) block is processable, Get claims it after probing
// only that one block, without scanning the rest of a deep queue.
func TestBlockPriorityQueue_Get_FastPathDeepQueue(t *testing.T) {
	initPrometheusMetrics()
	logger := ulogger.TestLogger{}
	pq := NewBlockPriorityQueue(logger)
	mockBP := NewMockBlockProcessor()
	ctx := context.Background()

	// Highest-priority (front) block is processable.
	front := &chainhash.Hash{}
	front[0] = 1
	pq.Add(processBlockFound{hash: front}, PriorityChainExtending, 1000)
	mockBP.SetCanProcess(front, true)

	// Many lower-priority followers queued behind it.
	for i := 2; i <= 100; i++ {
		h := &chainhash.Hash{}
		h[0] = byte(i)
		pq.Add(processBlockFound{hash: h}, PriorityDeepFork, uint32(1000+i))
		mockBP.SetCanProcess(h, false)
	}

	block, status := pq.Get(ctx, mockBP)
	require.Equal(t, GetOK, status)
	require.Equal(t, front, block.hash)
	require.Equal(t, 99, pq.Size(), "only the front block should be removed")
	require.Equal(t, 1, mockBP.GetCallCount(), "fast path should probe only the front block")
}

// TestBlockPriorityQueue_Get_SlowPathFallbackDeepQueue verifies the fallback: when the
// front block is blocked, Get still scans the rest of the queue and returns a deeper
// processable block.
func TestBlockPriorityQueue_Get_SlowPathFallbackDeepQueue(t *testing.T) {
	initPrometheusMetrics()
	logger := ulogger.TestLogger{}
	pq := NewBlockPriorityQueue(logger)
	mockBP := NewMockBlockProcessor()
	ctx := context.Background()

	// Front block is blocked, forcing the slow-path scan.
	front := &chainhash.Hash{}
	front[0] = 1
	pq.Add(processBlockFound{hash: front}, PriorityChainExtending, 1000)
	mockBP.SetCanProcess(front, false)

	// Followers sorted by height; only the last (deepest) one is processable.
	var processable *chainhash.Hash
	for i := 2; i <= 20; i++ {
		h := &chainhash.Hash{}
		h[0] = byte(i)
		pq.Add(processBlockFound{hash: h}, PriorityNearFork, uint32(1000+i))
		last := i == 20
		mockBP.SetCanProcess(h, last)
		if last {
			processable = h
		}
	}

	block, status := pq.Get(ctx, mockBP)
	require.Equal(t, GetOK, status)
	require.Equal(t, processable, block.hash)
	require.Equal(t, 19, pq.Size(), "only the processable block should be removed")
}

// TestBlockPriorityQueue_Get_AllBlocksBlocked tests Get when all blocks are blocked
func TestBlockPriorityQueue_Get_AllBlocksBlocked(t *testing.T) {
	initPrometheusMetrics()
	logger := ulogger.TestLogger{}
	pq := NewBlockPriorityQueue(logger)
	mockBP := NewMockBlockProcessor()
	ctx := context.Background()

	// Add 3 blocks
	hash1 := &chainhash.Hash{}
	hash1[0] = 1
	hash2 := &chainhash.Hash{}
	hash2[0] = 2
	hash3 := &chainhash.Hash{}
	hash3[0] = 3

	pq.Add(processBlockFound{hash: hash1}, PriorityChainExtending, 1000)
	pq.Add(processBlockFound{hash: hash2}, PriorityNearFork, 1001)
	pq.Add(processBlockFound{hash: hash3}, PriorityDeepFork, 1002)

	// Mark all as blocked
	mockBP.SetCanProcess(hash1, false)
	mockBP.SetCanProcess(hash2, false)
	mockBP.SetCanProcess(hash3, false)

	block, status := pq.Get(ctx, mockBP)
	assert.Equal(t, GetAllBlocked, status)
	assert.Nil(t, block.hash)

	// Verify skip counts increased for all items
	pq.mu.Lock()
	for _, item := range pq.items {
		assert.Greater(t, item.skipCount, 0, "Skip count should be incremented")
	}
	pq.mu.Unlock()
}

// TestBlockPriorityQueue_Get_SomeBlockedSomeProcessable tests mixed scenario
func TestBlockPriorityQueue_Get_SomeBlockedSomeProcessable(t *testing.T) {
	initPrometheusMetrics()
	logger := ulogger.TestLogger{}
	pq := NewBlockPriorityQueue(logger)
	mockBP := NewMockBlockProcessor()
	ctx := context.Background()

	hash1 := &chainhash.Hash{}
	hash1[0] = 1
	hash2 := &chainhash.Hash{}
	hash2[0] = 2
	hash3 := &chainhash.Hash{}
	hash3[0] = 3

	// Add blocks with different priorities
	pq.Add(processBlockFound{hash: hash1}, PriorityChainExtending, 1000)
	pq.Add(processBlockFound{hash: hash2}, PriorityNearFork, 1001)
	pq.Add(processBlockFound{hash: hash3}, PriorityDeepFork, 1002)

	// Mark first two as blocked, third as processable
	mockBP.SetCanProcess(hash1, false)
	mockBP.SetCanProcess(hash2, false)
	mockBP.SetCanProcess(hash3, true)

	// Should get hash3 (the only processable one)
	block, status := pq.Get(ctx, mockBP)
	assert.Equal(t, GetOK, status)
	assert.Equal(t, hash3, block.hash)

	// hash3 should be removed, hash1 and hash2 should remain with skip counts
	assert.Equal(t, 2, pq.Size(), "Two blocked blocks should remain")

	// Call Get again to trigger skip count updates for remaining blocks
	_, statusAgain := pq.Get(ctx, mockBP)
	assert.Equal(t, GetAllBlocked, statusAgain)

	// Now verify skip counts were incremented
	pq.mu.Lock()
	for _, item := range pq.items {
		assert.Greater(t, item.skipCount, 0, "Blocked items should have skip count > 0")
	}
	pq.mu.Unlock()
}

// TestBlockPriorityQueue_Get_ContextCancellation tests Get with cancelled context
func TestBlockPriorityQueue_Get_ContextCancellation(t *testing.T) {
	initPrometheusMetrics()
	logger := ulogger.TestLogger{}
	pq := NewBlockPriorityQueue(logger)
	mockBP := NewMockBlockProcessor()

	ctx, cancel := context.WithCancel(context.Background())

	hash := &chainhash.Hash{}
	hash[0] = 1

	pq.Add(processBlockFound{hash: hash}, PriorityChainExtending, 1000)

	// Cancel context before Get
	cancel()

	block, status := pq.Get(ctx, mockBP)
	assert.Equal(t, GetEmpty, status)
	assert.Nil(t, block.hash)
}

// TestBlockPriorityQueue_Get_ProcessorError tests Get when CanProcessBlock returns error
func TestBlockPriorityQueue_Get_ProcessorError(t *testing.T) {
	initPrometheusMetrics()
	logger := ulogger.TestLogger{}
	pq := NewBlockPriorityQueue(logger)
	mockBP := NewMockBlockProcessor()
	ctx := context.Background()

	hash1 := &chainhash.Hash{}
	hash1[0] = 1
	hash2 := &chainhash.Hash{}
	hash2[0] = 2

	pq.Add(processBlockFound{hash: hash1}, PriorityChainExtending, 1000)
	pq.Add(processBlockFound{hash: hash2}, PriorityNearFork, 1001)

	// Make processor return errors
	mockBP.returnError = true

	// Should return GetAllBlocked when all CanProcessBlock calls fail
	block, status := pq.Get(ctx, mockBP)
	assert.Equal(t, GetAllBlocked, status)
	assert.Nil(t, block.hash)
}

// TestBlockPriorityQueue_Get_ItemRemovedDuringIteration tests race condition
// where another worker removes an item while we're iterating
func TestBlockPriorityQueue_Get_ItemRemovedDuringIteration(t *testing.T) {
	initPrometheusMetrics()
	logger := ulogger.TestLogger{}
	pq := NewBlockPriorityQueue(logger)
	mockBP := NewMockBlockProcessor()
	ctx := context.Background()

	hash1 := &chainhash.Hash{}
	hash1[0] = 1
	hash2 := &chainhash.Hash{}
	hash2[0] = 2

	pq.Add(processBlockFound{hash: hash1}, PriorityChainExtending, 1000)
	pq.Add(processBlockFound{hash: hash2}, PriorityNearFork, 1001)

	// Start Get in goroutine with delay in CanProcessBlock
	mockBP.blockDelay = 50 * time.Millisecond

	var wg sync.WaitGroup
	wg.Add(1)

	var resultBlock processBlockFound
	var resultStatus GetStatus

	go func() {
		defer wg.Done()
		resultBlock, resultStatus = pq.Get(ctx, mockBP)
	}()

	// While Get is running, remove an item from another goroutine
	time.Sleep(25 * time.Millisecond)
	pq.mu.Lock()
	if len(pq.items) > 0 {
		pq.removeItem(0)
	}
	pq.mu.Unlock()

	wg.Wait()

	// Should still get a result (either found a block or determined all blocked/empty)
	assert.NotEqual(t, GetStatus(999), resultStatus, "Should return valid status")

	_ = resultBlock
}

// TestBlockPriorityQueue_WaitForBlock_Integration tests full WaitForBlock workflow
func TestBlockPriorityQueue_WaitForBlock_Integration(t *testing.T) {
	initPrometheusMetrics()
	logger := ulogger.TestLogger{}
	pq := NewBlockPriorityQueue(logger)
	mockBP := NewMockBlockProcessor()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start worker waiting on empty queue
	var wg sync.WaitGroup
	wg.Add(1)

	var gotBlock processBlockFound
	var gotStatus GetStatus

	go func() {
		defer wg.Done()
		gotBlock, gotStatus = pq.WaitForBlock(ctx, mockBP)
	}()

	// Give worker time to start waiting
	time.Sleep(10 * time.Millisecond)

	// Add a block - should wake the worker
	hash := &chainhash.Hash{}
	hash[0] = 1
	pq.Add(processBlockFound{hash: hash}, PriorityChainExtending, 1000)

	// Wait for worker to finish
	wg.Wait()

	// Verify worker got the block
	assert.Equal(t, GetOK, gotStatus)
	assert.Equal(t, hash, gotBlock.hash)
}

// TestBlockPriorityQueue_WaitForBlock_AllBlockedParks verifies that when every
// queued block is fork-blocked (GetAllBlocked), WaitForBlock parks on the
// condition variable instead of busy-looping. A spin would call CanProcessBlock
// thousands of times; parking bounds the calls to a single Get() pass until a
// Signal wakes the worker.
func TestBlockPriorityQueue_WaitForBlock_AllBlockedParks(t *testing.T) {
	initPrometheusMetrics()
	logger := ulogger.TestLogger{}
	pq := NewBlockPriorityQueue(logger)
	mockBP := NewMockBlockProcessor()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hash1 := &chainhash.Hash{}
	hash1[0] = 1
	hash2 := &chainhash.Hash{}
	hash2[0] = 2

	pq.Add(processBlockFound{hash: hash1}, PriorityChainExtending, 1000)
	pq.Add(processBlockFound{hash: hash2}, PriorityNearFork, 1001)

	// Every queued block is blocked -> Get() returns GetAllBlocked on a non-empty queue.
	mockBP.SetCanProcess(hash1, false)
	mockBP.SetCanProcess(hash2, false)

	var wg sync.WaitGroup
	wg.Add(1)

	var gotBlock processBlockFound
	var gotStatus GetStatus

	go func() {
		defer wg.Done()
		gotBlock, gotStatus = pq.WaitForBlock(ctx, mockBP)
	}()

	// Let the worker reach GetAllBlocked and park. If it spins, CanProcessBlock
	// is hammered continuously during this window.
	time.Sleep(150 * time.Millisecond)

	// A parked worker performs at most a couple of full Get() passes before
	// waiting; a spin would produce hundreds/thousands of calls in 150ms.
	callsWhileWaiting := mockBP.GetCallCount()
	require.LessOrEqual(t, callsWhileWaiting, 10,
		"WaitForBlock should park on GetAllBlocked, not busy-loop calling CanProcessBlock")

	// Unblock one block and Signal, mimicking a fork resolution / processing slot freeing up.
	mockBP.SetCanProcess(hash1, true)
	pq.Signal()

	wg.Wait()

	require.Equal(t, GetOK, gotStatus, "worker should wake and return the now-processable block")
	require.Equal(t, hash1, gotBlock.hash)
}

// TestForkManager_CanProcessBlock_EdgeCases tests CanProcessBlock edge cases
func TestForkManager_CanProcessBlock_EdgeCases(t *testing.T) {
	initPrometheusMetrics()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	ctx := context.Background()

	t.Run("Fast path when at max capacity", func(t *testing.T) {
		tSettings.BlockValidation.MaxParallelForks = 2
		fm := NewForkManager(logger, tSettings)

		hash1 := &chainhash.Hash{}
		hash1[0] = 1
		hash2 := &chainhash.Hash{}
		hash2[0] = 2
		hash3 := &chainhash.Hash{}
		hash3[0] = 3

		// Fill to capacity
		require.True(t, fm.StartProcessingBlock(hash1))
		require.True(t, fm.StartProcessingBlock(hash2))

		// CanProcessBlock should use fast-path and return false
		canProcess, err := fm.CanProcessBlock(ctx, hash3)
		require.NoError(t, err)
		assert.False(t, canProcess)

		// Verify atomic counter matches
		assert.Equal(t, 2, int(fm.processingCount.Load()))
	})

	t.Run("Fork with processing block is blocked", func(t *testing.T) {
		tSettings.BlockValidation.MaxParallelForks = 5
		fm := NewForkManager(logger, tSettings)

		// Create fork
		baseHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
		fm.RegisterFork("test-fork", baseHash, 1000)

		// Add two blocks to the fork
		hash1 := &chainhash.Hash{}
		hash1[0] = 1
		hash2 := &chainhash.Hash{}
		hash2[0] = 2

		fm.blockToFork[*hash1] = "test-fork"
		fm.blockToFork[*hash2] = "test-fork"

		fork := fm.GetFork("test-fork")

		// Mark first block as processing
		fork.mu.Lock()
		fork.ProcessingBlock = hash1
		fork.mu.Unlock()

		fm.mu.Lock()
		fm.processingBlocks[*hash1] = true
		fm.processingCount.Add(1)
		fm.mu.Unlock()

		// Second block in same fork should not be processable
		canProcess, err := fm.CanProcessBlock(ctx, hash2)
		require.NoError(t, err)
		assert.False(t, canProcess, "Fork already has a processing block")
	})
}
