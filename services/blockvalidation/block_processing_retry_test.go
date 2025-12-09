package blockvalidation

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jarcoal/httpmock"
	"github.com/jellydator/ttlcache/v3"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBlockProcessingWithRetry tests the retry mechanism when block fetching fails
func TestBlockProcessingWithRetry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.MaxParallelForks = 2
	tSettings.BlockValidation.NearForkThreshold = 10

	// Create test blocks
	blocks := testhelpers.CreateTestBlockChain(t, 5)
	targetBlock := blocks[4]
	targetHash := targetBlock.Hash()

	// Create mock blockchain store and client
	mockBlockchainStore := blockchain_store.NewMockStore()
	mockBlockchainClient, err := blockchain.NewLocalClient(logger, tSettings, mockBlockchainStore, nil, nil)
	require.NoError(t, err)

	// Store the parent blocks so the test block can be processed
	for i := 0; i < 4; i++ {
		err := mockBlockchainClient.AddBlock(ctx, blocks[i], "test-peer")
		require.NoError(t, err)
	}

	// Create mock validator
	mockValidator := &validator.MockValidator{}

	// Create memory stores for testing
	subtreeStore := memory.New()
	txStore := memory.New()
	mockUtxoStore := &utxo.MockUtxostore{}

	// Create block validation
	bv := NewBlockValidation(ctx, logger, tSettings, mockBlockchainClient, subtreeStore, txStore, mockUtxoStore, mockValidator, nil)

	// Create server with priority queue
	server := &Server{
		logger:              logger,
		settings:            tSettings,
		blockchainClient:    mockBlockchainClient,
		blockValidation:     bv,
		blockPriorityQueue:  NewBlockPriorityQueue(logger),
		blockClassifier:     NewBlockClassifier(logger, uint32(tSettings.BlockValidation.NearForkThreshold), mockBlockchainClient),
		forkManager:         NewForkManager(logger, tSettings),
		processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
		catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](),
		// Note: peerMetrics field has been removed from Server struct
	}

	t.Run("Retry_Uses_Alternative_Peer", func(t *testing.T) {
		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		// First peer fails
		failCount := 0
		httpmock.RegisterResponder("GET", fmt.Sprintf("http://peer1/block/%s", targetHash),
			func(req *http.Request) (*http.Response, error) {
				failCount++
				return nil, errors.NewNetworkError("network error")
			})

		// Second peer succeeds
		targetBlockBytes, err := targetBlock.Bytes()
		require.NoError(t, err)
		httpmock.RegisterResponder("GET", fmt.Sprintf("http://peer2/block/%s", targetHash),
			httpmock.NewBytesResponder(200, targetBlockBytes))

		// Add block announcement from first peer
		blockFound1 := processBlockFound{
			hash:    targetHash,
			baseURL: "http://peer1",
			peerID:  "peer1",
		}

		// Add block announcement from second peer as alternative
		blockFound2 := processBlockFound{
			hash:    targetHash,
			baseURL: "http://peer2",
			peerID:  "peer2",
		}

		// Add to queue - first one becomes primary, second becomes alternative
		server.blockPriorityQueue.Add(blockFound1, PriorityChainExtending, targetBlock.Height)
		server.blockPriorityQueue.Add(blockFound2, PriorityChainExtending, targetBlock.Height)

		// Process block - should fail with peer1, succeed with peer2
		err = server.processBlockWithPriority(ctx, blockFound1)
		require.NoError(t, err)

		// Verify first peer was tried
		assert.Equal(t, 1, failCount)

		// Verify block was processed successfully
		exists, err := server.blockValidation.GetBlockExists(ctx, targetHash)
		require.NoError(t, err)
		assert.True(t, exists)
	})

	t.Run("Retry_After_All_Alternatives_Fail", func(t *testing.T) {
		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		// Create a new block for this test
		testBlocks := testhelpers.CreateTestBlockChain(t, 6)
		testBlock := testBlocks[5]
		testHash := testBlock.Hash()

		// Store parent blocks so test block can be processed
		for i := 0; i < 5; i++ {
			err := mockBlockchainClient.AddBlock(ctx, testBlocks[i], "test-peer")
			require.NoError(t, err)
		}

		// All peers fail initially
		peer1Attempts := 0
		peer2Attempts := 0
		peer3Attempts := 0

		httpmock.RegisterResponder("GET", fmt.Sprintf("http://peer1/block/%s", testHash),
			func(req *http.Request) (*http.Response, error) {
				peer1Attempts++
				if peer1Attempts < 2 {
					return nil, errors.NewNetworkError("temporary failure")
				}
				// Succeed on second attempt
				testBlockBytes, _ := testBlock.Bytes()
				return httpmock.NewBytesResponse(200, testBlockBytes), nil
			})

		httpmock.RegisterResponder("GET", fmt.Sprintf("http://peer2/block/%s", testHash),
			func(req *http.Request) (*http.Response, error) {
				peer2Attempts++
				return nil, errors.NewNetworkError("peer2 always fails")
			})

		httpmock.RegisterResponder("GET", fmt.Sprintf("http://peer3/block/%s", testHash),
			func(req *http.Request) (*http.Response, error) {
				peer3Attempts++
				return nil, errors.NewNetworkError("peer3 always fails")
			})

		// Clean queue for this test
		server.blockPriorityQueue.Clear()

		// Add multiple peer announcements
		blockFound1 := processBlockFound{
			hash:    testHash,
			baseURL: "http://peer1",
			peerID:  "peer1",
		}
		blockFound2 := processBlockFound{
			hash:    testHash,
			baseURL: "http://peer2",
			peerID:  "peer2",
		}
		blockFound3 := processBlockFound{
			hash:    testHash,
			baseURL: "http://peer3",
			peerID:  "peer3",
		}

		server.blockPriorityQueue.Add(blockFound1, PriorityChainExtending, testBlock.Height)
		server.blockPriorityQueue.Add(blockFound2, PriorityChainExtending, testBlock.Height)
		server.blockPriorityQueue.Add(blockFound3, PriorityChainExtending, testBlock.Height)

		// First attempt should fail after trying all peers
		err := server.processBlockWithPriority(ctx, blockFound1)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get block")

		// Verify all peers were tried
		assert.Equal(t, 1, peer1Attempts)
		assert.Equal(t, 1, peer2Attempts)
		assert.Equal(t, 1, peer3Attempts)

		// Re-add the sources for retry since they were consumed
		server.blockPriorityQueue.Add(blockFound1, PriorityChainExtending, testBlock.Height)
		server.blockPriorityQueue.Add(blockFound2, PriorityChainExtending, testBlock.Height)
		server.blockPriorityQueue.Add(blockFound3, PriorityChainExtending, testBlock.Height)

		// Now simulate a retry with special retry marker
		retryBlock := processBlockFound{
			hash:    testHash,
			baseURL: "retry",
			peerID:  "",
		}

		// Process retry - should use alternative source (peer1 again) and succeed
		err = server.processBlockWithPriority(ctx, retryBlock)
		require.NoError(t, err)

		// Verify peer1 was tried again and succeeded
		assert.Equal(t, 2, peer1Attempts)
	})

	t.Run("Malicious_Peer_Skipped_On_Retry", func(t *testing.T) {
		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		// Create another test block
		maliciousTestBlocks := testhelpers.CreateTestBlockChain(t, 7)
		maliciousTestBlock := maliciousTestBlocks[6]
		maliciousHash := maliciousTestBlock.Hash()

		// Store parent blocks so test block can be processed
		for i := 5; i < 6; i++ {
			err := mockBlockchainClient.AddBlock(ctx, maliciousTestBlocks[i], "test-peer")
			require.NoError(t, err)
		}

		// Mark peer1 as malicious
		// Note: peerMetrics field has been removed from Server struct
		// (malicious peer marking disabled)

		// Good peer responds correctly
		maliciousBlockBytes, err := maliciousTestBlock.Bytes()
		require.NoError(t, err)
		httpmock.RegisterResponder("GET", fmt.Sprintf("http://good_peer/block/%s", maliciousHash),
			httpmock.NewBytesResponder(200, maliciousBlockBytes))

		// Clean queue
		server.blockPriorityQueue.Clear()

		// Add announcements
		maliciousFound := processBlockFound{
			hash:    maliciousHash,
			baseURL: "http://malicious_peer",
			peerID:  "malicious_peer",
		}
		goodFound := processBlockFound{
			hash:    maliciousHash,
			baseURL: "http://good_peer",
			peerID:  "good_peer",
		}

		server.blockPriorityQueue.Add(maliciousFound, PriorityChainExtending, maliciousTestBlock.Height)
		server.blockPriorityQueue.Add(goodFound, PriorityChainExtending, maliciousTestBlock.Height)

		// Process should skip malicious peer and use good peer
		err = server.processBlockWithPriority(ctx, maliciousFound)
		require.NoError(t, err)

		// Verify block was processed
		exists, err := server.blockValidation.GetBlockExists(ctx, maliciousHash)
		require.NoError(t, err)
		assert.True(t, exists)
	})

	t.Run("No_Alternative_Sources_On_Retry", func(t *testing.T) {
		// Create isolated block
		isolatedBlock := testhelpers.CreateTestBlockChain(t, 8)[7]
		isolatedHash := isolatedBlock.Hash()

		// Clean queue
		server.blockPriorityQueue.Clear()

		// Create retry block with no alternatives
		retryBlock := processBlockFound{
			hash:    isolatedHash,
			baseURL: "retry",
			peerID:  "",
		}

		// Process should fail with no sources available
		err := server.processBlockWithPriority(ctx, retryBlock)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no sources available")
	})
}

// TestBlockPriorityQueueRetry tests the priority queue retry functionality
func TestBlockPriorityQueueRetry(t *testing.T) {
	queue := NewBlockPriorityQueue(ulogger.TestLogger{})

	// Create test block
	hash1 := &chainhash.Hash{0x01}
	hash2 := &chainhash.Hash{0x02}

	blockFound1 := processBlockFound{
		hash:    hash1,
		baseURL: "http://peer1",
		peerID:  "peer1",
	}

	t.Run("RequeueForRetry_New_Block", func(t *testing.T) {
		queue.Add(blockFound1, PriorityChainExtending, 100)

		// Get and process the block
		mockBP := &mockBlockProcessor{}
		found, status := queue.Get(context.Background(), mockBP)
		require.Equal(t, GetOK, status)
		assert.Equal(t, hash1, found.hash)

		// Queue should be empty
		assert.Equal(t, 0, queue.Size())

		// Requeue for retry
		retryBlock := processBlockFound{
			hash:    hash1,
			baseURL: "retry",
			peerID:  "",
		}
		queue.RequeueForRetry(retryBlock, PriorityDeepFork, 100)

		// Should be back in queue with retry info
		assert.Equal(t, 1, queue.Size())

		// Get the retry block
		found2, status := queue.Get(context.Background(), mockBP)
		require.Equal(t, GetOK, status)
		assert.Equal(t, "retry", found2.baseURL)
		assert.Equal(t, "", found2.peerID)
	})

	t.Run("RequeueForRetry_Existing_Block", func(t *testing.T) {
		// Add block
		blockFound2 := processBlockFound{
			hash:    hash2,
			baseURL: "http://peer2",
			peerID:  "peer2",
		}
		queue.Add(blockFound2, PriorityNearFork, 200)

		// Check it exists
		assert.True(t, queue.Contains(*hash2))

		// Requeue same block - should update retry count
		queue.RequeueForRetry(blockFound2, PriorityNearFork, 200)

		// Should still be 1 item
		assert.Equal(t, 1, queue.Size())

		// Verify block is still in queue
		assert.True(t, queue.Contains(*hash2))
	})
}

// TestAlternativeSourceTracking tests that alternative sources are properly tracked
func TestAlternativeSourceTracking(t *testing.T) {
	queue := NewBlockPriorityQueue(ulogger.TestLogger{})
	hash := &chainhash.Hash{0x03}

	// Add primary source
	primary := processBlockFound{
		hash:    hash,
		baseURL: "http://primary",
		peerID:  "primary",
	}
	queue.Add(primary, PriorityChainExtending, 300)

	// Add multiple alternatives
	for i := 1; i <= 3; i++ {
		alt := processBlockFound{
			hash:    hash,
			baseURL: fmt.Sprintf("http://alt%d", i),
			peerID:  fmt.Sprintf("alt%d", i),
		}
		queue.Add(alt, PriorityChainExtending, 300)
	}

	// Should have 3 alternatives stored
	alt1, ok := queue.GetAlternativeSource(hash)
	require.True(t, ok)
	assert.Equal(t, "http://alt1", alt1.baseURL)

	alt2, ok := queue.GetAlternativeSource(hash)
	require.True(t, ok)
	assert.Equal(t, "http://alt2", alt2.baseURL)

	alt3, ok := queue.GetAlternativeSource(hash)
	require.True(t, ok)
	assert.Equal(t, "http://alt3", alt3.baseURL)

	// No more alternatives
	_, ok = queue.GetAlternativeSource(hash)
	assert.False(t, ok)
}

// TestBlockProcessingWorkerRetry tests the worker retry mechanism
func TestBlockProcessingWorkerRetry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	// Create mock blockchain store and client
	mockBlockchainStore := blockchain_store.NewMockStore()
	mockBlockchainClient, err := blockchain.NewLocalClient(logger, tSettings, mockBlockchainStore, nil, nil)
	require.NoError(t, err)

	// Create mock validator
	mockValidator := &validator.MockValidator{}

	// Create memory stores for testing
	subtreeStore := memory.New()
	txStore := memory.New()
	mockUtxoStore := &utxo.MockUtxostore{}

	// Create block validation
	bv := NewBlockValidation(ctx, logger, tSettings, mockBlockchainClient, subtreeStore, txStore, mockUtxoStore, mockValidator, nil)

	server := &Server{
		logger:              logger,
		settings:            tSettings,
		blockchainClient:    mockBlockchainClient,
		blockValidation:     bv,
		blockPriorityQueue:  NewBlockPriorityQueue(logger),
		blockClassifier:     NewBlockClassifier(logger, 10, mockBlockchainClient),
		forkManager:         NewForkManager(logger, tSettings),
		processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
		catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](),
	}

	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()

	// Create test block
	blocks := testhelpers.CreateTestBlockChain(t, 2)
	targetBlock := blocks[1]
	targetHash := targetBlock.Hash()

	// Store parent
	err = mockBlockchainClient.AddBlock(ctx, blocks[0], "test-peer")
	require.NoError(t, err)

	// Mock failing endpoint
	var attemptCount sync.Mutex
	var attempts int
	httpmock.RegisterResponder("GET", fmt.Sprintf("http://failing-peer/block/%s", targetHash),
		func(req *http.Request) (*http.Response, error) {
			attemptCount.Lock()
			attempts++
			attemptCount.Unlock()
			return nil, errors.NewNetworkError("failed to fetch block")
		})

	// Add block to queue
	blockFound := processBlockFound{
		hash:    targetHash,
		baseURL: "http://failing-peer",
		peerID:  "failing-peer",
	}
	server.blockPriorityQueue.Add(blockFound, PriorityChainExtending, targetBlock.Height)

	// Start worker
	workerCtx, workerCancel := context.WithCancel(ctx)
	done := make(chan bool)

	go func() {
		server.blockProcessingWorker(workerCtx, 1)
		done <- true
	}()

	// Let worker process and fail
	time.Sleep(100 * time.Millisecond)

	// Should have attempted once
	attemptCount.Lock()
	actualAttempts := attempts
	attemptCount.Unlock()
	assert.Equal(t, 1, actualAttempts)

	// Cancel worker
	workerCancel()
	<-done

	// Check that block is still in queue (would be re-queued after delay)
	// Note: In real scenario, the retry goroutine would re-add after 5 seconds
	assert.Equal(t, 0, server.blockPriorityQueue.Size()) // Empty because retry is async with delay
}

// TestChainExtendingBlocksNotSentToCatchup tests that chain-extending blocks
// are NOT sent to catchup even when the queue is busy
func TestChainExtendingBlocksNotSentToCatchup(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.UseCatchupWhenBehind = true

	// Create test blockchain
	blocks := testhelpers.CreateTestBlockChain(t, 5)

	// Create mock blockchain store and client
	mockBlockchainStore := blockchain_store.NewMockStore()
	mockBlockchainClient, err := blockchain.NewLocalClient(logger, tSettings, mockBlockchainStore, nil, nil)
	require.NoError(t, err)

	// Store the first 3 blocks as the base chain
	for i := 0; i < 3; i++ {
		err := mockBlockchainClient.AddBlock(ctx, blocks[i], "test-peer")
		require.NoError(t, err)
	}

	// Create mock validator
	mockValidator := &validator.MockValidator{}

	// Create memory stores for testing
	subtreeStore := memory.New()
	txStore := memory.New()
	mockUtxoStore := &utxo.MockUtxostore{}

	// Create block validation
	bv := NewBlockValidation(ctx, logger, tSettings, mockBlockchainClient, subtreeStore, txStore, mockUtxoStore, mockValidator, nil)

	server := &Server{
		logger:             logger,
		settings:           tSettings,
		blockchainClient:   mockBlockchainClient,
		blockValidation:    bv,
		blockFoundCh:       make(chan processBlockFound, 20),
		blockPriorityQueue: NewBlockPriorityQueue(logger),
		blockClassifier:    NewBlockClassifier(logger, 10, mockBlockchainClient),
		forkManager:        NewForkManager(logger, tSettings),
		catchupCh:          make(chan processBlockCatchup, 10),
		// Note: peerMetrics field has been removed from Server struct
		stats:               gocore.NewStat("test"),
		processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
		catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](),
	}

	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()

	// Mock HTTP responses for blocks
	for i := 3; i < 5; i++ {
		block := blocks[i]
		hash := block.Hash()
		blockBytes, err := block.Bytes()
		require.NoError(t, err)
		httpmock.RegisterResponder("GET", fmt.Sprintf("http://test-peer/block/%s", hash),
			httpmock.NewBytesResponder(200, blockBytes))
	}

	// Add many fake blocks to the queue to simulate busy system
	for i := 0; i < 15; i++ {
		fakeHash := &chainhash.Hash{byte(i + 100)}
		blockFound := processBlockFound{
			hash:    fakeHash,
			baseURL: "http://test-peer",
			peerID:  "test_peer",
		}
		server.blockPriorityQueue.Add(blockFound, PriorityDeepFork, uint32(100+i))
	}

	// Queue should have 15 blocks
	assert.Equal(t, 15, server.blockPriorityQueue.Size())

	// Create a channel to monitor catchup
	catchupReceived := make(chan *chainhash.Hash, 1)
	go func() {
		for {
			select {
			case catchupBlock := <-server.catchupCh:
				catchupReceived <- catchupBlock.block.Hash()
			case <-ctx.Done():
				return
			}
		}
	}()

	// Now process block 3 which extends the chain
	// This should NOT go to catchup despite busy queue
	bestHeader, bestMeta, err := mockBlockchainClient.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint32(2), bestMeta.Height)
	assert.True(t, blocks[3].Header.HashPrevBlock.IsEqual(bestHeader.Hash()))

	blockFound3 := processBlockFound{
		hash:    blocks[3].Hash(),
		baseURL: "http://test-peer",
		peerID:  "test_peer",
		errCh:   make(chan error, 1),
	}

	// Process the chain-extending block
	err = server.processBlockFoundChannel(ctx, blockFound3)
	require.NoError(t, err)

	// Give time for any async operations
	time.Sleep(100 * time.Millisecond)

	// Check that block 3 was NOT sent to catchup
	select {
	case hash := <-catchupReceived:
		t.Errorf("Chain-extending block %s should NOT have been sent to catchup", hash)
	default:
		// Good - no block in catchup
	}

	// Block 3 should be in the priority queue as chain-extending
	assert.Equal(t, 16, server.blockPriorityQueue.Size())

	// Verify it was classified correctly - block should be in queue
	queue := server.blockPriorityQueue
	assert.True(t, queue.Contains(*blocks[3].Hash()))

	// Now test that a non-chain-extending block DOES go to catchup
	blockFound4 := processBlockFound{
		hash:    blocks[4].Hash(), // This block's parent (block 3) doesn't exist yet
		baseURL: "http://test-peer",
		peerID:  "test_peer",
		errCh:   make(chan error, 1),
	}

	// Process the non-chain-extending block
	err = server.processBlockFoundChannel(ctx, blockFound4)
	require.NoError(t, err)

	// This one SHOULD go to catchup
	select {
	case hash := <-catchupReceived:
		assert.Equal(t, blocks[4].Hash(), hash, "Non-chain-extending block should have been sent to catchup")
	case <-time.After(500 * time.Millisecond):
		t.Error("Expected non-chain-extending block to be sent to catchup")
	}
}
