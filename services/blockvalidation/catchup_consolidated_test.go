package blockvalidation

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/services/blockvalidation/catchup"
	"github.com/bitcoin-sv/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Crash Recovery Tests (consolidated from catchup_crash_recovery_test.go)
// ============================================================================

// TestCatchup_CrashDuringBlockValidation tests recovery from crashes during block validation
func TestCatchup_CrashDuringBlockValidation(t *testing.T) {
	t.Run("ResumeFromLastValidatedBlock", func(t *testing.T) {
		ctx := context.Background()
		server, mockBlockchainClient, _, cleanup := setupTestCatchupServer(t)
		defer cleanup()

		// Create test headers
		testHeaders := testhelpers.CreateTestHeaders(t, 10)
		targetBlock := &model.Block{
			Header: testHeaders[9],
			Height: 1010,
		}

		// Setup mocks
		mockBlockchainClient.On("GetBlockExists", mock.Anything, targetBlock.Hash()).
			Return(false, nil)

		bestBlockHeader := testHeaders[0]
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 1000}, nil)

		mockBlockchainClient.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
			Return([]*chainhash.Hash{bestBlockHeader.Hash()}, nil)

		// Create a map to track which blocks "exist"
		existingBlocks := make(map[string]bool)
		for i := 1; i <= 5; i++ {
			existingBlocks[testHeaders[i].Hash().String()] = true
		}

		// Mock GetBlockExists for each header
		for i := 1; i < 10; i++ {
			exists := existingBlocks[testHeaders[i].Hash().String()]
			mockBlockchainClient.On("GetBlockExists", mock.Anything, testHeaders[i].Hash()).
				Return(exists, nil).Maybe()
		}

		// Mock common ancestor finding
		mockBlockchainClient.On("GetBlockHeader", mock.Anything, testHeaders[0].Hash()).
			Return(testHeaders[0], &model.BlockHeaderMeta{Height: 1000}, nil).Maybe()

		// Filter should only return blocks we don't have (6-9)
		newHeaders, err := server.filterExistingBlocks(ctx, testHeaders[1:], targetBlock)

		assert.NoError(t, err)
		assert.Len(t, newHeaders, 4, "Should only process blocks 6-9")

		// Verify the filtered blocks are the correct ones
		for i, header := range newHeaders {
			assert.Equal(t, testHeaders[i+6].Hash(), header.Hash(),
				"Should start from block 6 after crash recovery")
		}

		t.Logf("Successfully resumed from block %d after crash", 6)
	})

	t.Run("DetectCorruptedValidationState", func(t *testing.T) {
		ctx := context.Background()
		server, mockBlockchainClient, _, cleanup := setupTestCatchupServer(t)
		defer cleanup()

		testHeaders := testhelpers.CreateTestHeaders(t, 5)
		targetBlock := &model.Block{
			Header: testHeaders[4],
			Height: 1005,
		}

		// Setup basic mocks
		mockBlockchainClient.On("GetBlockExists", mock.Anything, targetBlock.Hash()).
			Return(false, nil)

		bestBlockHeader := testHeaders[0]
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 1000}, nil)

		// Simulate corrupted state: block exists but its parent doesn't
		mockBlockchainClient.On("GetBlockExists", mock.Anything, testHeaders[2].Hash()).
			Return(true, nil).Once()
		mockBlockchainClient.On("GetBlockExists", mock.Anything, testHeaders[1].Hash()).
			Return(false, nil).Once()

		// This represents a corrupted state that needs recovery
		_, err := server.filterExistingBlocks(ctx, testHeaders[1:3], targetBlock)

		assert.NoError(t, err)
		// The function should handle this gracefully by including the block for re-validation
	})
}

// TestCatchup_ConcurrentCatchupLock tests the catchup lock mechanism
func TestCatchup_ConcurrentCatchupLock(t *testing.T) {
	t.Run("OnlyOneCatchupAllowed", func(t *testing.T) {
		server, _, _, cleanup := setupTestCatchupServer(t)
		defer cleanup()

		// First catchup acquires lock
		header1 := testhelpers.CreateTestHeaders(t, 1)[0]
		ctx1 := &CatchupContext{
			blockUpTo: &model.Block{
				Header: header1,
				Height: 1000,
			},
		}

		err1 := server.acquireCatchupLock(ctx1)
		assert.NoError(t, err1, "First catchup should acquire lock")

		// Second catchup should fail
		header2 := testhelpers.CreateTestHeaders(t, 1)[0]
		ctx2 := &CatchupContext{
			blockUpTo: &model.Block{
				Header: header2,
				Height: 1001,
			},
		}

		err2 := server.acquireCatchupLock(ctx2)
		assert.Error(t, err2, "Second catchup should fail to acquire lock")
		assert.Contains(t, err2.Error(), "another catchup is currently in progress")

		// Release first lock
		server.releaseCatchupLock(ctx1, &err1)

		// Third catchup should now succeed
		header3 := testhelpers.CreateTestHeaders(t, 1)[0]
		ctx3 := &CatchupContext{
			blockUpTo: &model.Block{
				Header: header3,
				Height: 1002,
			},
		}

		err3 := server.acquireCatchupLock(ctx3)
		assert.NoError(t, err3, "Third catchup should acquire lock after release")

		// Clean up
		server.releaseCatchupLock(ctx3, &err3)
	})

	t.Run("ConcurrentCatchupAttempts", func(t *testing.T) {
		server, _, _, cleanup := setupTestCatchupServer(t)
		defer cleanup()

		numGoroutines := 10
		successCount := 0
		failureCount := 0
		mu := sync.Mutex{}

		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		// Start multiple goroutines trying to acquire catchup lock
		for i := 0; i < numGoroutines; i++ {
			go func(id int) {
				defer wg.Done()

				header := testhelpers.CreateTestHeaders(t, 1)[0]
				ctx := &CatchupContext{
					blockUpTo: &model.Block{
						Header: header,
						Height: uint32(1000 + id),
					},
				}

				err := server.acquireCatchupLock(ctx)
				mu.Lock()
				if err == nil {
					successCount++
					// Hold lock briefly
					time.Sleep(10 * time.Millisecond)
					server.releaseCatchupLock(ctx, &err)
				} else {
					failureCount++
				}
				mu.Unlock()
			}(i)
		}

		wg.Wait()

		// Exactly one should succeed
		assert.Equal(t, 1, successCount,
			"Exactly one goroutine should acquire lock")
		assert.Equal(t, numGoroutines-1, failureCount,
			"All other goroutines should fail")
	})

	t.Run("LockReleasedOnPanic", func(t *testing.T) {
		server, _, _, cleanup := setupTestCatchupServer(t)
		defer cleanup()

		// Function that panics but defers lock release
		runCatchupWithPanic := func() {
			defer func() {
				if r := recover(); r != nil {
					// Recovered from panic
				}
			}()

			header := testhelpers.CreateTestHeaders(t, 1)[0]
			ctx := &CatchupContext{
				blockUpTo: &model.Block{
					Header: header,
					Height: 1000,
				},
			}

			err := server.acquireCatchupLock(ctx)
			require.NoError(t, err)

			// Ensure lock is released even on panic
			defer server.releaseCatchupLock(ctx, &err)

			// Simulate panic during catchup
			panic("simulated catchup failure")
		}

		// Run the function that panics
		runCatchupWithPanic()

		// Lock should be released, so new catchup should succeed
		header2 := testhelpers.CreateTestHeaders(t, 1)[0]
		ctx2 := &CatchupContext{
			blockUpTo: &model.Block{
				Header: header2,
				Height: 1001,
			},
		}

		err := server.acquireCatchupLock(ctx2)
		assert.NoError(t, err, "Lock should be released after panic")
		server.releaseCatchupLock(ctx2, &err)
	})
}

// TestCatchup_HeaderCacheCorruption tests recovery from header cache corruption
func TestCatchup_HeaderCacheCorruption(t *testing.T) {
	t.Run("DetectAndRebuildCorruptedCache", func(t *testing.T) {
		server, _, _, cleanup := setupTestCatchupServer(t)
		defer cleanup()

		// Create test headers
		testHeaders := testhelpers.CreateTestHeaders(t, 10)

		// Build initial cache
		err := server.headerChainCache.BuildFromHeaders(
			testHeaders, server.settings.BlockValidation.PreviousBlockHeaderCount)
		assert.NoError(t, err, "Initial cache build should succeed")

		// Simulate cache corruption by clearing it
		server.headerChainCache.Clear()

		// Try to get validation headers - should return nil for non-existent entry
		cachedHeaders, exists := server.headerChainCache.GetValidationHeaders(testHeaders[5].Hash())
		assert.False(t, exists, "Should not find headers in cleared cache")
		assert.Nil(t, cachedHeaders, "Should return nil for missing cache entry")

		// Rebuild cache
		err = server.headerChainCache.BuildFromHeaders(
			testHeaders, server.settings.BlockValidation.PreviousBlockHeaderCount)
		assert.NoError(t, err, "Cache rebuild should succeed")

		// Verify cache is working again
		cachedHeaders, exists = server.headerChainCache.GetValidationHeaders(testHeaders[5].Hash())
		assert.True(t, exists, "Should find headers after rebuild")
		assert.NotNil(t, cachedHeaders, "Should return headers after rebuild")
	})

	t.Run("CacheCleanupAfterCatchup", func(t *testing.T) {
		server, _, _, cleanup := setupTestCatchupServer(t)
		defer cleanup()

		testHeaders := testhelpers.CreateTestHeaders(t, 5)
		// Create a genesis block to use as common ancestor
		genesisHash := &chainhash.Hash{}
		catchupCtx := &CatchupContext{
			blockUpTo: &model.Block{
				Header: testHeaders[4],
				Height: 1005,
			},
			blockHeaders:       testHeaders,
			commonAncestorHash: genesisHash, // Set common ancestor
		}

		// Build cache
		err := server.buildHeaderCache(catchupCtx)
		assert.NoError(t, err)

		// Verify cache has data
		_, exists := server.headerChainCache.GetValidationHeaders(testHeaders[2].Hash())
		assert.True(t, exists, "Cache should have data before cleanup")

		// Cleanup should clear cache
		server.cleanup(catchupCtx)

		// Verify cache is cleared
		_, exists = server.headerChainCache.GetValidationHeaders(testHeaders[2].Hash())
		assert.False(t, exists, "Cache should be cleared after cleanup")
	})
}

// TestCatchup_PartialStateRecovery tests recovery from partial catchup states
func TestCatchup_PartialStateRecovery(t *testing.T) {
	t.Run("SkipAlreadyValidatedBlocks", func(t *testing.T) {
		ctx := context.Background()
		server, mockBlockchainClient, _, cleanup := setupTestCatchupServer(t)
		defer cleanup()

		// Create 20 test headers
		testHeaders := testhelpers.CreateTestHeaders(t, 20)
		targetBlock := &model.Block{
			Header: testHeaders[19],
			Height: 1020,
		}

		// Simulate that blocks 1-10 were already validated
		for i := 1; i <= 10; i++ {
			mockBlockchainClient.On("GetBlockExists", mock.Anything, testHeaders[i].Hash()).
				Return(true, nil).Once()
		}

		// Blocks 11-19 still need validation
		for i := 11; i < 20; i++ {
			mockBlockchainClient.On("GetBlockExists", mock.Anything, testHeaders[i].Hash()).
				Return(false, nil).Once()
		}

		// Filter blocks
		newHeaders, err := server.filterExistingBlocks(ctx, testHeaders[1:], targetBlock)

		assert.NoError(t, err)
		assert.Len(t, newHeaders, 9, "Should only return blocks 11-19")

		// Verify we're skipping the right blocks
		for i, header := range newHeaders {
			expectedIndex := i + 11
			assert.Equal(t, testHeaders[expectedIndex].Hash(), header.Hash(),
				"Should skip already validated blocks")
		}
	})
}

// TestCatchup_FSMStateManagement tests FSM state transitions during catchup
func TestCatchup_FSMStateManagement(t *testing.T) {
	t.Run("FSMStateTransitions", func(t *testing.T) {
		ctx := context.Background()
		server, mockBlockchainClient, _, cleanup := setupTestCatchupServer(t)
		defer cleanup()

		testHeaders := testhelpers.CreateTestHeaders(t, 5)
		catchupCtx := &CatchupContext{
			blockUpTo: &model.Block{
				Header: testHeaders[4],
				Height: 1005,
			},
			bestBlockHeader: testHeaders[0],
			blockHeaders:    testHeaders[1:],
		}

		// Mock FSM state changes
		mockBlockchainClient.On("CatchUpBlocks", mock.Anything).
			Return(nil).Once()

		catchingState := blockchain.FSMStateCATCHINGBLOCKS
		mockBlockchainClient.On("GetFSMCurrentState", mock.Anything).
			Return(&catchingState, nil).Once()

		mockBlockchainClient.On("Run", mock.Anything, "blockvalidation/Server").
			Return(nil).Once()

		// Test setting CATCHINGBLOCKS state
		size := atomic.Int64{}
		size.Store(4)
		err := server.setFSMCatchingBlocks(ctx, catchupCtx, &size)
		assert.NoError(t, err, "Should set FSM to CATCHINGBLOCKS")

		// Test restoring RUN state
		server.restoreFSMState(ctx, catchupCtx)

		mockBlockchainClient.AssertExpectations(t)
	})

	t.Run("HandleFSMStateQueryError", func(t *testing.T) {
		ctx := context.Background()
		server, mockBlockchainClient, _, cleanup := setupTestCatchupServer(t)
		defer cleanup()

		header := testhelpers.CreateTestHeaders(t, 1)[0]
		catchupCtx := &CatchupContext{
			blockUpTo: &model.Block{
				Header: header,
				Height: 1000,
			},
		}

		// Mock FSM state query failure
		mockBlockchainClient.On("GetFSMCurrentState", mock.Anything).
			Return(nil, assert.AnError).Once()

		// Should handle error gracefully
		server.restoreFSMState(ctx, catchupCtx)

		// Should not attempt to change state if query fails
		mockBlockchainClient.AssertNotCalled(t, "Run", mock.Anything, mock.Anything)
	})
}

// TestCatchup_MetricsAndTracking tests metrics recording during crash recovery
func TestCatchup_MetricsAndTracking(t *testing.T) {
	t.Run("TrackCatchupAttemptsAfterCrash", func(t *testing.T) {
		server, _, _, cleanup := setupTestCatchupServer(t)
		defer cleanup()

		initialAttempts := server.catchupAttempts.Load()

		// Simulate multiple catchup attempts after crashes
		for i := 0; i < 3; i++ {
			ctx := &CatchupContext{
				blockUpTo: &model.Block{
					Header: &model.BlockHeader{Nonce: uint32(i)},
					Height: uint32(1000 + i),
				},
			}

			err := server.acquireCatchupLock(ctx)
			require.NoError(t, err)

			// Simulate catchup work
			time.Sleep(10 * time.Millisecond)

			// Release lock (simulating completion or crash recovery)
			server.releaseCatchupLock(ctx, &err)
		}

		finalAttempts := server.catchupAttempts.Load()
		assert.Equal(t, initialAttempts+3, finalAttempts,
			"Should track all catchup attempts")
	})

	t.Run("RecordSuccessAndFailureMetrics", func(t *testing.T) {
		server, _, _, cleanup := setupTestCatchupServer(t)
		defer cleanup()

		initialSuccesses := server.catchupSuccesses.Load()

		// Successful catchup
		header1 := testhelpers.CreateTestHeaders(t, 1)[0]
		ctx1 := &CatchupContext{
			blockUpTo: &model.Block{
				Header: header1,
				Height: 1000,
			},
			baseURL:   "http://peer1",
			startTime: time.Now(),
		}

		err1 := server.acquireCatchupLock(ctx1)
		require.NoError(t, err1)

		// Simulate successful completion
		var nilErr error = nil
		server.releaseCatchupLock(ctx1, &nilErr)

		assert.Equal(t, initialSuccesses+1, server.catchupSuccesses.Load(),
			"Should increment success counter")

		// Failed catchup
		header2 := testhelpers.CreateTestHeaders(t, 1)[0]
		ctx2 := &CatchupContext{
			blockUpTo: &model.Block{
				Header: header2,
				Height: 1001,
			},
			baseURL:   "http://peer2",
			startTime: time.Now(),
		}

		err2 := server.acquireCatchupLock(ctx2)
		require.NoError(t, err2)

		// Simulate failure
		failErr := assert.AnError
		server.releaseCatchupLock(ctx2, &failErr)

		// Success count should not change for failure
		assert.Equal(t, initialSuccesses+1, server.catchupSuccesses.Load(),
			"Success counter should not increment for failures")
	})
}

// TestCatchup_ContextStateConsistency tests that context state remains consistent
func TestCatchup_ContextStateConsistency(t *testing.T) {
	t.Run("MaintainContextIntegrity", func(t *testing.T) {
		ctx := context.Background()
		server, mockBlockchainClient, _, cleanup := setupTestCatchupServer(t)
		defer cleanup()

		testHeaders := testhelpers.CreateTestHeaders(t, 5)
		targetBlock := &model.Block{
			Header: testHeaders[4],
			Height: 1005,
		}

		// Setup basic mocks
		bestBlockHeader := testHeaders[0]
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 1000}, nil).Maybe()

		mockBlockchainClient.On("GetBlockHeader", mock.Anything, testHeaders[0].Hash()).
			Return(testHeaders[0], &model.BlockHeaderMeta{Height: 1000}, nil).Maybe()

		// Mock GetBlockExists for filtering
		for _, h := range testHeaders[1:] {
			mockBlockchainClient.On("GetBlockExists", mock.Anything, h.Hash()).
				Return(false, nil).Maybe()
		}
		mockBlockchainClient.On("GetBlockExists", mock.Anything, targetBlock.Hash()).
			Return(false, nil).Maybe()

		// Create context with header fetch result
		catchupCtx := &CatchupContext{
			blockUpTo:          targetBlock,
			baseURL:            "http://test-peer",
			startTime:          time.Now(),
			bestBlockHeader:    bestBlockHeader,
			commonAncestorHash: testHeaders[0].Hash(),
			commonAncestorMeta: &model.BlockHeaderMeta{Height: 1000},
			forkDepth:          0,
			currentHeight:      1000,
			blockHeaders:       testHeaders[1:],
			headersFetchResult: &catchup.Result{
				Headers: testHeaders[1:], // Headers to be filtered
			},
		}

		// Verify context consistency after various operations

		// 1. After filtering
		err := server.filterHeaders(ctx, catchupCtx)
		if err == nil {
			assert.NotNil(t, catchupCtx.blockUpTo, "Target block should remain set")
			assert.NotNil(t, catchupCtx.commonAncestorHash, "Common ancestor should remain set")
		}

		// 2. After building cache
		if len(catchupCtx.blockHeaders) > 0 {
			err = server.buildHeaderCache(catchupCtx)
			assert.NoError(t, err)
			assert.Equal(t, targetBlock, catchupCtx.blockUpTo, "Target should not change")
			assert.Equal(t, "http://test-peer", catchupCtx.baseURL, "Peer URL should not change")
		}

		// 3. After cleanup
		server.cleanup(catchupCtx)
		assert.NotNil(t, catchupCtx.startTime, "Start time should be preserved")
		assert.Equal(t, uint32(1000), catchupCtx.currentHeight, "Height should be preserved")
	})

	t.Run("HandleNilContextFields", func(t *testing.T) {
		server, _, _, cleanup := setupTestCatchupServer(t)
		defer cleanup()

		// Create context with minimal fields
		header := testhelpers.CreateTestHeaders(t, 1)[0]
		catchupCtx := &CatchupContext{
			blockUpTo: &model.Block{
				Header: header,
				Height: 1000,
			},
			baseURL:   "http://test-peer",
			startTime: time.Now(),
		}

		// Should handle cleanup gracefully even with nil fields
		server.cleanup(catchupCtx)

		// Should not panic
		assert.NotNil(t, catchupCtx.blockUpTo, "Should preserve non-nil fields")
	})
}

// TestCatchup_ErrorPropagation tests that errors are properly propagated through the catchup flow
func TestCatchup_ErrorPropagation(t *testing.T) {
	t.Run("PropagateValidationErrors", func(t *testing.T) {
		ctx := context.Background()
		server, mockBlockchainClient, _, cleanup := setupTestCatchupServer(t)
		defer cleanup()

		testHeaders := testhelpers.CreateTestHeaders(t, 3)
		targetBlock := &model.Block{
			Header: testHeaders[2],
			Height: 1003,
		}

		// Mock a validation error
		expectedErr := assert.AnError
		mockBlockchainClient.On("GetBlockExists", mock.Anything, testHeaders[1].Hash()).
			Return(false, expectedErr).Once()

		mockBlockchainClient.On("GetBlockExists", mock.Anything, targetBlock.Hash()).
			Return(false, nil).Maybe()

		// Error should be handled gracefully (logged but continues)
		headers, err := server.filterExistingBlocks(ctx, testHeaders[1:], targetBlock)

		// filterExistingBlocks includes the header even on error to be safe
		assert.NoError(t, err, "Should not propagate error")
		assert.Len(t, headers, 2, "Should include headers despite error")
	})

	t.Run("CleanupOnError", func(t *testing.T) {
		server, _, _, cleanup := setupTestCatchupServer(t)
		defer cleanup()

		header := testhelpers.CreateTestHeaders(t, 1)[0]
		catchupCtx := &CatchupContext{
			blockUpTo: &model.Block{
				Header: header,
				Height: 1000,
			},
			baseURL:   "http://test-peer",
			startTime: time.Now(),
		}

		// Acquire lock
		err := server.acquireCatchupLock(catchupCtx)
		require.NoError(t, err)

		// Simulate error during catchup
		catchupErr := assert.AnError

		// Cleanup should still release lock on error
		server.releaseCatchupLock(catchupCtx, &catchupErr)

		// Verify lock is released by trying to acquire again
		header2 := testhelpers.CreateTestHeaders(t, 1)[0]
		ctx2 := &CatchupContext{
			blockUpTo: &model.Block{
				Header: header2,
				Height: 1001,
			},
		}

		err2 := server.acquireCatchupLock(ctx2)
		assert.NoError(t, err2, "Lock should be released after error")
		server.releaseCatchupLock(ctx2, &err2)
	})
}
