package blockvalidation

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockvalidation/catchup"
	"github.com/bitcoin-sv/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestCatchup_PartialNetworkFailure tests catchup behavior with 50% packet loss
func TestCatchup_PartialNetworkFailure(t *testing.T) {
	t.Run("RetryWithPacketLoss", func(t *testing.T) {
		t.Skip("Skipping flaky test that hangs - needs investigation")
		ctx, cancel := testhelpers.CreateTestContext(t, 30*time.Second)
		defer cancel()

		config := testhelpers.DefaultTestServerConfig()
		config.MaxRetries = 5
		server, mockBlockchainClient, mockUTXOStore, cleanup := setupTestCatchupServerWithConfig(t, config)
		defer cleanup()

		// Mock UTXO store block height
		mockUTXOStore.On("GetBlockHeight").Return(uint32(1000))

		// Use consecutive mainnet headers for proper chain linkage
		testHeaders := testhelpers.GetMainnetHeadersRange(t, 0, 10)
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

		// Mock GetBlockHeaders for common ancestor finding
		mockBlockchainClient.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).
			Return([]*model.BlockHeader{bestBlockHeader}, []*model.BlockHeaderMeta{{Height: 1000, ID: 1}}, nil).Maybe()

		// Mock GetBlockHeader for common ancestor finding
		mockBlockchainClient.On("GetBlockHeader", mock.Anything, bestBlockHeader.Hash()).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 1000, ID: 1}, nil).Maybe()

		// Mock GetBlockExists for best block header (it already exists)
		mockBlockchainClient.On("GetBlockExists", mock.Anything, bestBlockHeader.Hash()).
			Return(true, nil).Maybe()

		// Mock that headers don't exist locally
		for _, header := range testHeaders[1:] {
			mockBlockchainClient.On("GetBlockExists", mock.Anything, header.Hash()).
				Return(false, nil).Maybe()
			mockBlockchainClient.On("GetBlockHeader", mock.Anything, header.Hash()).
				Return(header, &model.BlockHeaderMeta{Height: 1001}, nil).Maybe()
		}

		// Setup HTTP mocks with flaky response
		httpMock := testhelpers.NewHTTPMockSetup(t)
		defer httpMock.Deactivate()

		// Register flaky response that fails 50% of the time
		httpMock.RegisterFlakeyResponse("http://unreliable-peer", 1, testHeaders[1:])
		httpMock.Activate()

		// Execute catchup
		result, _, err := server.catchupGetBlockHeaders(ctx, targetBlock, "http://unreliable-peer")

		// Should eventually succeed after retries
		assert.NoError(t, err)
		assert.NotNil(t, result)

		// The request should have succeeded eventually
		// Note: RegisterFlakeyResponse doesn't track counts, so we can't verify retry count directly
	})

	t.Run("EventualSuccessAfterMultipleFailures", func(t *testing.T) {
		t.Skip("Skipping flaky test that hangs - needs investigation")
		ctx, cancel := testhelpers.CreateTestContext(t, 30*time.Second)
		defer cancel()

		config := testhelpers.DefaultTestServerConfig()
		// Increase iteration timeout to allow for retries with exponential backoff
		// With 3 failures and exponential backoff, we need at least 7-10 seconds
		config.IterationTimeout = 15
		server, mockBlockchainClient, mockUTXOStore, cleanup := setupTestCatchupServerWithConfig(t, config)
		defer cleanup()

		// Mock UTXO store block height
		mockUTXOStore.On("GetBlockHeight").Return(uint32(1000))

		server.settings.BlockValidation.CatchupMaxRetries = 10

		// Use consecutive mainnet headers for proper chain linkage
		testHeaders := testhelpers.GetMainnetHeadersRange(t, 0, 5)
		targetBlock := &model.Block{
			Header: testHeaders[4],
			Height: 1005,
		}

		mockBlockchainClient.On("GetBlockExists", mock.Anything, targetBlock.Hash()).
			Return(false, nil)

		bestBlockHeader := testHeaders[0]
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 1000}, nil)

		mockBlockchainClient.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
			Return([]*chainhash.Hash{bestBlockHeader.Hash()}, nil)

		// Mock GetBlockHeaders for common ancestor finding
		mockBlockchainClient.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).
			Return([]*model.BlockHeader{bestBlockHeader}, []*model.BlockHeaderMeta{{Height: 1000, ID: 1}}, nil).Maybe()

		// Mock GetBlockHeader for common ancestor finding
		mockBlockchainClient.On("GetBlockHeader", mock.Anything, bestBlockHeader.Hash()).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 1000, ID: 1}, nil).Maybe()

		// Mock GetBlockExists for all headers
		for _, header := range testHeaders {
			mockBlockchainClient.On("GetBlockExists", mock.Anything, header.Hash()).
				Return(false, nil).Maybe()
		}

		// Setup HTTP mocks
		httpMock := testhelpers.NewHTTPMockSetup(t)
		defer httpMock.Deactivate()

		// Register flaky response that fails first 3 times
		httpMock.RegisterFlakeyResponse("http://flaky-peer", 3, testHeaders[1:])
		httpMock.Activate()

		// Should succeed after retries
		result, _, err := server.catchupGetBlockHeaders(ctx, targetBlock, "http://flaky-peer")

		assert.NoError(t, err)
		assert.NotNil(t, result)
	})
}

// TestCatchup_ConnectionDropMidTransfer tests behavior when connection drops during transfer
func TestCatchup_ConnectionDropMidTransfer(t *testing.T) {
	t.Run("RecoverFromPartialTransfer", func(t *testing.T) {
		t.Skip("Skipping flaky test that hangs - needs investigation")
		ctx, cancel := testhelpers.CreateTestContext(t, 30*time.Second)
		defer cancel()

		server, mockBlockchainClient, mockUTXOStore, cleanup := setupTestCatchupServer(t)
		defer cleanup()

		// Mock UTXO store block height
		mockUTXOStore.On("GetBlockHeight").Return(uint32(1000))

		// Initialize circuit breaker
		server.peerCircuitBreakers = catchup.NewPeerCircuitBreakers(catchup.CircuitBreakerConfig{
			FailureThreshold:    3,
			SuccessThreshold:    2,
			Timeout:             time.Minute,
			MaxHalfOpenRequests: 1,
		})

		// Use consecutive mainnet headers for proper chain linkage
		testHeaders := testhelpers.GetMainnetHeadersRange(t, 0, 100)
		targetBlock := &model.Block{
			Header: testHeaders[99],
			Height: 1100,
		}

		bestBlockHeader := testHeaders[0]

		mockBlockchainClient.On("GetBlockExists", mock.Anything, targetBlock.Hash()).
			Return(false, nil)

		// Mock GetBlockExists - best block header exists, others don't
		mockBlockchainClient.On("GetBlockExists", mock.Anything, bestBlockHeader.Hash()).
			Return(true, nil).Maybe()

		for _, header := range testHeaders[1:] {
			mockBlockchainClient.On("GetBlockExists", mock.Anything, header.Hash()).
				Return(false, nil).Maybe()
		}
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 1000}, nil)

		mockBlockchainClient.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
			Return([]*chainhash.Hash{bestBlockHeader.Hash()}, nil)

		// Mock GetBlockHeaders for common ancestor finding
		mockBlockchainClient.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).
			Return([]*model.BlockHeader{bestBlockHeader}, []*model.BlockHeaderMeta{{Height: 1000, ID: 1}}, nil).Maybe()

		// Mock GetBlockHeader for common ancestor finding
		mockBlockchainClient.On("GetBlockHeader", mock.Anything, bestBlockHeader.Hash()).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 1000, ID: 1}, nil).Maybe()

		// Mock GetBlockExists for best block header (it already exists)
		mockBlockchainClient.On("GetBlockExists", mock.Anything, bestBlockHeader.Hash()).
			Return(true, nil).Maybe()

		// Mock GetBlockExists for all other headers
		for _, header := range testHeaders[1:] {
			mockBlockchainClient.On("GetBlockExists", mock.Anything, header.Hash()).
				Return(false, nil).Maybe()
		}

		// Setup HTTP mocks
		httpMock := testhelpers.NewHTTPMockSetup(t)
		defer httpMock.Deactivate()

		// Simulate partial transfer on first attempt
		fullHeaders := testHeaders[1:100] // All headers

		// Register flaky response that fails first, then succeeds
		httpMock.RegisterFlakeyResponse("http://dropping-peer", 1, fullHeaders)
		httpMock.Activate()

		// First attempt should fail, circuit breaker records failure
		_, _, err := server.catchupGetBlockHeaders(ctx, targetBlock, "http://dropping-peer")

		// Should eventually succeed (either through retry or would try alternate peer)
		if err != nil {
			// Circuit breaker should have recorded the failure
			breaker := server.peerCircuitBreakers.GetBreaker("http://dropping-peer")
			assert.NotNil(t, breaker)
			state, _, _, _ := breaker.GetStats()
			assert.NotEqual(t, catchup.StateClosed, state, "Circuit breaker should not be closed after failure")
			// Check circuit breaker state using helper
			AssertCircuitBreakerState(t, server, "http://dropping-peer", catchup.StateOpen)
		}
	})

	t.Run("CircuitBreakerOpensAfterRepeatedDrops", func(t *testing.T) {
		t.Skip("Skipping flaky test that hangs - needs investigation")
		ctx, cancel := testhelpers.CreateTestContext(t, 30*time.Second)
		defer cancel()

		config := testhelpers.DefaultTestServerConfig()
		config.MaxRetries = 0 // No retries for this test
		server, mockBlockchainClient, mockUTXOStore, cleanup := setupTestCatchupServerWithConfig(t, config)
		defer cleanup()

		// Mock UTXO store block height
		mockUTXOStore.On("GetBlockHeight").Return(uint32(1000))

		// Initialize circuit breaker with low threshold
		server.peerCircuitBreakers = catchup.NewPeerCircuitBreakers(catchup.CircuitBreakerConfig{
			FailureThreshold:    2,
			SuccessThreshold:    2,
			Timeout:             time.Second,
			MaxHalfOpenRequests: 1,
		})

		// Use consecutive mainnet headers for proper chain linkage
		testHeaders := testhelpers.GetMainnetHeadersRange(t, 0, 10)
		targetBlock := &model.Block{
			Header: testHeaders[9],
			Height: 1010,
		}

		mockBlockchainClient.On("GetBlockExists", mock.Anything, targetBlock.Hash()).
			Return(false, nil)

		bestBlockHeader := testHeaders[0]
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 1000}, nil).Times(3)

		mockBlockchainClient.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
			Return([]*chainhash.Hash{bestBlockHeader.Hash()}, nil).Times(3)

		// Mock GetBlockHeaders for common ancestor finding
		mockBlockchainClient.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).
			Return([]*model.BlockHeader{bestBlockHeader}, []*model.BlockHeaderMeta{{Height: 1000, ID: 1}}, nil).Maybe()

		// Mock GetBlockHeader for common ancestor finding
		mockBlockchainClient.On("GetBlockHeader", mock.Anything, bestBlockHeader.Hash()).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 1000, ID: 1}, nil).Maybe()

		// Mock GetBlockExists for best block header (it already exists)
		mockBlockchainClient.On("GetBlockExists", mock.Anything, bestBlockHeader.Hash()).
			Return(true, nil).Maybe()

		// Mock GetBlockExists for all other headers
		for _, header := range testHeaders[1:] {
			mockBlockchainClient.On("GetBlockExists", mock.Anything, header.Hash()).
				Return(false, nil).Maybe()
		}

		// Setup HTTP mocks
		httpMock := testhelpers.NewHTTPMockSetup(t)
		defer httpMock.Deactivate()

		// Always fail to simulate persistent connection drops
		httpMock.RegisterErrorResponse("http://bad-peer", errors.NewError("connection reset by peer"))
		httpMock.Activate()

		// First failure with timeout
		ctx1, cancel1 := context.WithTimeout(ctx, 2*time.Second)
		_, _, err1 := server.catchupGetBlockHeaders(ctx1, targetBlock, "http://bad-peer")
		cancel1()
		assert.Error(t, err1)

		// Second failure should open circuit breaker
		ctx2, cancel2 := context.WithTimeout(ctx, 2*time.Second)
		_, _, err2 := server.catchupGetBlockHeaders(ctx2, targetBlock, "http://bad-peer")
		cancel2()
		assert.Error(t, err2)

		// Third attempt should be blocked by circuit breaker
		ctx3, cancel3 := context.WithTimeout(ctx, 2*time.Second)
		_, _, err3 := server.catchupGetBlockHeaders(ctx3, targetBlock, "http://bad-peer")
		cancel3()
		assert.Error(t, err3)
		assert.Contains(t, err3.Error(), "circuit breaker open")

		// Verify circuit breaker is open
		breaker := server.peerCircuitBreakers.GetBreaker("http://bad-peer")
		state, _, _, _ := breaker.GetStats()
		assert.Equal(t, catchup.StateOpen, state)
	})
}

// TestCatchup_FlappingPeer tests behavior with peers that alternate between success and failure
func TestCatchup_FlappingPeer(t *testing.T) {
	t.Run("CircuitBreakerStabilizes", func(t *testing.T) {
		t.Skip("Skipping flaky test that hangs - needs investigation")
		ctx, cancel := testhelpers.CreateTestContext(t, 30*time.Second)
		defer cancel()

		config := testhelpers.DefaultTestServerConfig()
		config.CircuitBreakerConfig = &catchup.CircuitBreakerConfig{
			FailureThreshold:    3,
			SuccessThreshold:    2,
			Timeout:             time.Second * 5,
			MaxHalfOpenRequests: 1,
		}
		server, mockBlockchainClient, _, cleanup := setupTestCatchupServerWithConfig(t, config)
		defer cleanup()

		// Use consecutive mainnet headers for proper chain linkage
		testHeaders := testhelpers.GetMainnetHeadersRange(t, 0, 10)
		targetBlock := &model.Block{
			Header: testHeaders[9],
			Height: 1010,
		}

		mockBlockchainClient.On("GetBlockExists", mock.Anything, targetBlock.Hash()).
			Return(false, nil)

		bestBlockHeader := testHeaders[0]
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 1000}, nil)

		mockBlockchainClient.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
			Return([]*chainhash.Hash{bestBlockHeader.Hash()}, nil)

		// Mock GetBlockHeaders for common ancestor finding
		mockBlockchainClient.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).
			Return([]*model.BlockHeader{bestBlockHeader}, []*model.BlockHeaderMeta{{Height: 1000, ID: 1}}, nil).Maybe()

		// Mock GetBlockHeader for common ancestor - use mock.Anything for hash
		mockBlockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 1000}, nil).Maybe()

		// Mock GetBlockExists for all headers
		for _, header := range testHeaders {
			mockBlockchainClient.On("GetBlockExists", mock.Anything, header.Hash()).
				Return(false, nil).Maybe()
		}

		// Setup HTTP mocks
		httpMock := testhelpers.NewHTTPMockSetup(t)
		defer httpMock.Deactivate()

		// Alternate between success and failure
		requestCount := int32(0)
		httpMock.RegisterResponse(
			`=~^http://flapping-peer/headers_from_common_ancestor/.*`,
			&testhelpers.HTTPMockResponse{
				StatusCode: 200,
				Validator: func(req *http.Request) error {
					count := atomic.AddInt32(&requestCount, 1)
					// Alternate: fail, succeed, fail, succeed...
					if count%2 == 1 {
						return errors.NewError("connection timeout")
					}
					return nil
				},
				Body: testhelpers.HeadersToBytes(testHeaders[1:]),
			},
		)
		httpMock.Activate()

		// Make multiple requests to test reputation system
		successCount := 0
		failCount := 0

		for i := 0; i < 6; i++ {
			_, _, err := server.catchupGetBlockHeaders(ctx, targetBlock, "http://flapping-peer")
			if err != nil {
				failCount++
			} else {
				successCount++
			}

			// Small delay between attempts
			time.Sleep(100 * time.Millisecond)
		}

		// Should have mix of successes and failures (or all successes if retries worked)
		assert.Greater(t, successCount, 0, "Should have some successes")
		// Note: With retries, we might not see failures at the catchup level

		// Check peer metrics exist
		peerMetric := server.peerMetrics.PeerMetrics["http://flapping-peer"]
		if peerMetric != nil {
			// If we have metrics, reputation might be affected
			if failCount > 0 {
				assert.LessOrEqual(t, peerMetric.ReputationScore, float64(100), "Reputation should be affected by failures")
			}
		}

		// Circuit breaker should eventually stabilize
		breaker := server.peerCircuitBreakers.GetBreaker("http://flapping-peer")
		finalState, _, _, _ := breaker.GetStats()
		t.Logf("Final circuit breaker state: %v, reputation: %.2f", finalState, peerMetric.ReputationScore)
	})

	t.Run("PeerEventuallyMarkedUnreliable", func(t *testing.T) {
		t.Skip("Skipping flaky test that hangs - needs investigation")
		ctx, cancel := testhelpers.CreateTestContext(t, 30*time.Second)
		defer cancel()

		server, mockBlockchainClient, mockUTXOStore, cleanup := setupTestCatchupServer(t)
		defer cleanup()

		// Mock UTXO store block height
		mockUTXOStore.On("GetBlockHeight").Return(uint32(1000))

		server.peerCircuitBreakers = catchup.NewPeerCircuitBreakers(catchup.CircuitBreakerConfig{
			FailureThreshold:    5,
			SuccessThreshold:    2,
			Timeout:             time.Second * 10,
			MaxHalfOpenRequests: 1,
		})
		server.peerMetrics = &catchup.CatchupMetrics{
			PeerMetrics: make(map[string]*catchup.PeerCatchupMetrics),
		}

		// Use consecutive mainnet headers for proper chain linkage
		testHeaders := testhelpers.GetMainnetHeadersRange(t, 0, 5)
		targetBlock := &model.Block{
			Header: testHeaders[4],
			Height: 1005,
		}

		mockBlockchainClient.On("GetBlockExists", mock.Anything, targetBlock.Hash()).
			Return(false, nil)

		bestBlockHeader := testHeaders[0]
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 1000}, nil)

		mockBlockchainClient.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
			Return([]*chainhash.Hash{bestBlockHeader.Hash()}, nil)

		// Mock GetBlockHeaders for common ancestor finding
		mockBlockchainClient.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).
			Return([]*model.BlockHeader{bestBlockHeader}, []*model.BlockHeaderMeta{{Height: 1000, ID: 1}}, nil).Maybe()

		// Mock GetBlockHeader for common ancestor - use mock.Anything for hash
		mockBlockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 1000}, nil).Maybe()

		// Mock GetBlockExists for all headers
		for _, header := range testHeaders {
			mockBlockchainClient.On("GetBlockExists", mock.Anything, header.Hash()).
				Return(false, nil).Maybe()
		}

		// Setup HTTP mocks
		httpMock := testhelpers.NewHTTPMockSetup(t)
		defer httpMock.Deactivate()

		// Start with success, then increasingly fail
		requestCount := int32(0)
		httpMock.RegisterResponse(
			`=~^http://degrading-peer/headers_from_common_ancestor/.*`,
			&testhelpers.HTTPMockResponse{
				StatusCode: 200,
				Validator: func(req *http.Request) error {
					count := atomic.AddInt32(&requestCount, 1)
					// Progressive degradation: success rate decreases over time
					// First 2 succeed, then increasingly fail
					if count > 2 && count%5 != 0 { // 20% success rate after initial period
						return errors.NewError("service unavailable")
					}
					return nil
				},
				Body: testhelpers.HeadersToBytes(testHeaders[1:]),
			},
		)
		httpMock.Activate()

		// Make multiple attempts with individual timeouts
		for i := 0; i < 10; i++ {
			// Create a timeout for each individual request to prevent hanging
			reqCtx, reqCancel := context.WithTimeout(ctx, 2*time.Second)
			_, _, _ = server.catchupGetBlockHeaders(reqCtx, targetBlock, "http://degrading-peer")
			reqCancel()
			time.Sleep(50 * time.Millisecond)
		}

		// Check peer is marked as unreliable
		peerMetric := server.peerMetrics.PeerMetrics["http://degrading-peer"]
		require.NotNil(t, peerMetric)

		// Log the actual metrics for debugging
		t.Logf("Peer metrics - Reputation: %.2f, Failed: %d, Successful: %d, Total: %d",
			peerMetric.ReputationScore, peerMetric.FailedRequests, peerMetric.SuccessfulRequests, peerMetric.TotalRequests)

		// With the retry logic, the peer might still maintain a decent reputation
		// but should have recorded failures
		assert.Greater(t, peerMetric.FailedRequests, int64(0), "Should have recorded some failures")
		// Total requests should be at least the number of attempts
		assert.GreaterOrEqual(t, peerMetric.TotalRequests, int64(10), "Should have made at least 10 requests")
	})
}

// TestCatchup_NetworkPartition tests behavior during network partition scenarios
func TestCatchup_NetworkPartition(t *testing.T) {
	t.Run("HandleConflictingChainsFromMultiplePeers", func(t *testing.T) {
		ctx, cancel := testhelpers.CreateTestContext(t, 30*time.Second)
		defer cancel()

		server, mockBlockchainClient, mockUTXOStore, cleanup := setupTestCatchupServer(t)
		defer cleanup()

		// Mock UTXO store block height
		mockUTXOStore.On("GetBlockHeight").Return(uint32(1000))

		server.peerMetrics = &catchup.CatchupMetrics{
			PeerMetrics: make(map[string]*catchup.PeerCatchupMetrics),
		}

		// Create common base
		// Use consecutive mainnet headers
		commonHeaders := testhelpers.GetMainnetHeadersRange(t, 0, 3)

		// Mock GetBlockExists for common headers
		for _, header := range commonHeaders {
			mockBlockchainClient.On("GetBlockExists", mock.Anything, header.Hash()).
				Return(false, nil).Maybe()
		}

		// Create three different chain extensions
		chains := make([][]*model.BlockHeader, 3)
		for chainIdx := 0; chainIdx < 3; chainIdx++ {
			// Create synthetic chain branching from common headers
			// Each chain is unique due to different mining nonces
			chainHeaders, err := testhelpers.CreateAdversarialFork(commonHeaders, 2, 5)
			require.NoError(t, err, "Failed to create adversarial fork")

			chains[chainIdx] = chainHeaders

			// Mock GetBlockExists for all headers in this chain
			for _, header := range chainHeaders {
				mockBlockchainClient.On("GetBlockExists", mock.Anything, header.Hash()).
					Return(false, nil).Maybe()
			}
		}

		// Try to sync to different targets from different peers
		for peerIdx, chain := range chains {
			targetBlock := &model.Block{
				Header: chain[4],
				Height: 1008,
			}

			mockBlockchainClient.On("GetBlockExists", mock.Anything, targetBlock.Hash()).
				Return(false, nil).Once()

			mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).
				Return(commonHeaders[2], &model.BlockHeaderMeta{Height: 1003}, nil).Once()

			mockBlockchainClient.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
				Return([]*chainhash.Hash{commonHeaders[2].Hash()}, nil).Once()

			// Mock common ancestor finding - use mock.Anything for the hash since different chains are created
			mockBlockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
				Return(commonHeaders[2], &model.BlockHeaderMeta{Height: 1003}, nil).Maybe()

			// Setup HTTP mocks for this peer
			httpMock := testhelpers.NewHTTPMockSetup(t)
			defer httpMock.Deactivate()

			peerURL := fmt.Sprintf("http://peer-%d", peerIdx)
			chainToReturn := chain

			httpMock.RegisterHeaderResponse(peerURL, chainToReturn)
			httpMock.Activate()

			// Attempt catchup from this peer
			result, _, err := server.catchupGetBlockHeaders(ctx, targetBlock, peerURL)

			if err == nil && result != nil {
				// Record successful chain info
				peerMetric := server.peerMetrics.PeerMetrics[peerURL]
				if peerMetric == nil {
					peerMetric = &catchup.PeerCatchupMetrics{
						PeerURL: peerURL,
					}
					server.peerMetrics.PeerMetrics[peerURL] = peerMetric
				}
				peerMetric.SuccessfulRequests++
			}
		}

		// All three peers should have metrics
		assert.Len(t, server.peerMetrics.PeerMetrics, 3)

		// Each peer provided a different valid chain
		for i := 0; i < 3; i++ {
			peerURL := fmt.Sprintf("http://peer-%d", i)
			metric := server.peerMetrics.PeerMetrics[peerURL]
			assert.NotNil(t, metric)
			assert.Greater(t, metric.SuccessfulRequests, int64(0))
		}
	})
}

// TestCatchup_NetworkLatencyHandling tests behavior under various latency conditions
func TestCatchup_NetworkLatencyHandling(t *testing.T) {
	t.Run("HandleHighLatencyPeer", func(t *testing.T) {
		ctx, cancel := testhelpers.CreateTestContext(t, 30*time.Second)
		defer cancel()

		server, mockBlockchainClient, mockUTXOStore, cleanup := setupTestCatchupServer(t)
		defer cleanup()

		// Mock UTXO store block height
		mockUTXOStore.On("GetBlockHeight").Return(uint32(1000))

		// Set reasonable timeout
		server.settings.BlockValidation.CatchupOperationTimeout = 5 // 5 seconds

		// Use consecutive mainnet headers for proper chain linkage
		testHeaders := testhelpers.GetMainnetHeadersRange(t, 0, 10)
		targetBlock := &model.Block{
			Header: testHeaders[9],
			Height: 1010,
		}

		mockBlockchainClient.On("GetBlockExists", mock.Anything, targetBlock.Hash()).
			Return(false, nil)

		bestBlockHeader := testHeaders[0]
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 1000}, nil)

		mockBlockchainClient.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
			Return([]*chainhash.Hash{bestBlockHeader.Hash()}, nil)

		// Mock GetBlockHeaders for common ancestor finding
		mockBlockchainClient.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).
			Return([]*model.BlockHeader{bestBlockHeader}, []*model.BlockHeaderMeta{{Height: 1000, ID: 1}}, nil).Maybe()

		// Mock GetBlockHeader for common ancestor finding
		mockBlockchainClient.On("GetBlockHeader", mock.Anything, bestBlockHeader.Hash()).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 1000, ID: 1}, nil).Maybe()

		// Mock GetBlockExists for best block header (it already exists)
		mockBlockchainClient.On("GetBlockExists", mock.Anything, bestBlockHeader.Hash()).
			Return(true, nil).Maybe()

		// Mock GetBlockExists for all other headers
		for _, header := range testHeaders[1:] {
			mockBlockchainClient.On("GetBlockExists", mock.Anything, header.Hash()).
				Return(false, nil).Maybe()
		}

		// Setup HTTP mocks
		httpMock := testhelpers.NewHTTPMockSetup(t)
		defer httpMock.Deactivate()

		// Simulate high latency peer
		httpMock.RegisterResponse(
			`=~^http://slow-peer/headers_from_common_ancestor/.*`,
			&testhelpers.HTTPMockResponse{
				StatusCode: 200,
				Delay:      2 * time.Second,
				Body:       testhelpers.HeadersToBytes(testHeaders[1:]),
			},
		)
		httpMock.Activate()

		start := time.Now()
		result, _, err := server.catchupGetBlockHeaders(ctx, targetBlock, "http://slow-peer")
		elapsed := time.Since(start)

		// Should succeed but take time
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.GreaterOrEqual(t, elapsed, 2*time.Second, "Should experience latency")
		assert.Less(t, elapsed, 6*time.Second, "Should not timeout")
	})

	t.Run("TimeoutVerySlowPeer", func(t *testing.T) {
		ctx, cancel := testhelpers.CreateTestContext(t, 30*time.Second)
		defer cancel()

		server, mockBlockchainClient, mockUTXOStore, cleanup := setupTestCatchupServer(t)
		defer cleanup()

		// Mock UTXO store block height
		mockUTXOStore.On("GetBlockHeight").Return(uint32(1000))

		// Set short timeout for iteration to test timeout behavior
		server.settings.BlockValidation.CatchupIterationTimeout = 1 // 1 second

		// Use consecutive mainnet headers for proper chain linkage
		testHeaders := testhelpers.GetMainnetHeadersRange(t, 0, 5)
		targetBlock := &model.Block{
			Header: testHeaders[4],
			Height: 1005,
		}

		mockBlockchainClient.On("GetBlockExists", mock.Anything, targetBlock.Hash()).
			Return(false, nil)

		bestBlockHeader := testHeaders[0]
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 1000}, nil)

		mockBlockchainClient.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
			Return([]*chainhash.Hash{bestBlockHeader.Hash()}, nil)

		// Mock GetBlockHeaders for common ancestor finding
		mockBlockchainClient.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).
			Return([]*model.BlockHeader{bestBlockHeader}, []*model.BlockHeaderMeta{{Height: 1000, ID: 1}}, nil).Maybe()

		// Mock GetBlockHeader for common ancestor finding
		mockBlockchainClient.On("GetBlockHeader", mock.Anything, bestBlockHeader.Hash()).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 1000, ID: 1}, nil).Maybe()

		// Mock GetBlockExists for best block header (it already exists)
		mockBlockchainClient.On("GetBlockExists", mock.Anything, bestBlockHeader.Hash()).
			Return(true, nil).Maybe()

		// Mock GetBlockExists for all other headers
		for _, header := range testHeaders[1:] {
			mockBlockchainClient.On("GetBlockExists", mock.Anything, header.Hash()).
				Return(false, nil).Maybe()
		}

		// Setup HTTP mocks
		httpMock := testhelpers.NewHTTPMockSetup(t)
		defer httpMock.Deactivate()

		// Simulate extremely slow peer
		httpMock.RegisterResponse(
			`=~^http://very-slow-peer/headers_from_common_ancestor/.*`,
			&testhelpers.HTTPMockResponse{
				StatusCode: 200,
				Delay:      3 * time.Second,
				Body:       testhelpers.HeadersToBytes(testHeaders[1:]),
			},
		)
		httpMock.Activate()

		// Should timeout due to slow response
		_, _, err := server.catchupGetBlockHeaders(ctx, targetBlock, "http://very-slow-peer")

		assert.Error(t, err)
		// Should get a timeout error for slow peer response
		assert.Contains(t, err.Error(), "timed out")
	})
}

// TestCatchup_ConcurrentNetworkRequests tests handling of concurrent network operations
func TestCatchup_ConcurrentNetworkRequests(t *testing.T) {
	t.Run("HandleConcurrentPeerRequests", func(t *testing.T) {
		ctx, cancel := testhelpers.CreateTestContext(t, 30*time.Second)
		defer cancel()

		server, mockBlockchainClient, mockUTXOStore, cleanup := setupTestCatchupServer(t)
		defer cleanup()

		// Mock UTXO store block height
		mockUTXOStore.On("GetBlockHeight").Return(uint32(1000))

		server.peerMetrics = &catchup.CatchupMetrics{
			PeerMetrics: make(map[string]*catchup.PeerCatchupMetrics),
		}

		numPeers := 5
		// Use consecutive mainnet headers for proper chain linkage
		testHeaders := testhelpers.GetMainnetHeadersRange(t, 0, 20)
		targetBlock := &model.Block{
			Header: testHeaders[19],
			Height: 1020,
		}

		// Setup mocks for concurrent requests
		mockBlockchainClient.On("GetBlockExists", mock.Anything, targetBlock.Hash()).
			Return(false, nil)

		bestBlockHeader := testHeaders[0]
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 1000}, nil)

		mockBlockchainClient.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
			Return([]*chainhash.Hash{bestBlockHeader.Hash()}, nil)

		// Mock GetBlockHeaders for common ancestor finding
		mockBlockchainClient.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).
			Return([]*model.BlockHeader{bestBlockHeader}, []*model.BlockHeaderMeta{{Height: 1000, ID: 1}}, nil).Maybe()

		// Mock GetBlockHeader for common ancestor finding
		mockBlockchainClient.On("GetBlockHeader", mock.Anything, bestBlockHeader.Hash()).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 1000, ID: 1}, nil).Maybe()

		// Mock GetBlockExists for best block header (it already exists)
		mockBlockchainClient.On("GetBlockExists", mock.Anything, bestBlockHeader.Hash()).
			Return(true, nil).Maybe()

		// Mock GetBlockExists for all other headers
		for _, header := range testHeaders[1:] {
			mockBlockchainClient.On("GetBlockExists", mock.Anything, header.Hash()).
				Return(false, nil).Maybe()
		}

		// Setup HTTP mocks
		httpMock := testhelpers.NewHTTPMockSetup(t)
		defer httpMock.Deactivate()

		// Register responders for multiple peers
		responseDelays := []time.Duration{
			100 * time.Millisecond,
			200 * time.Millisecond,
			150 * time.Millisecond,
			300 * time.Millisecond,
			50 * time.Millisecond,
		}

		for i := 0; i < numPeers; i++ {
			peerURL := fmt.Sprintf("http://peer-%d", i)
			delay := responseDelays[i]

			httpMock.RegisterResponse(
				fmt.Sprintf(`=~^%s/headers_from_common_ancestor/.*`, peerURL),
				&testhelpers.HTTPMockResponse{
					StatusCode: 200,
					Delay:      delay,
					Body:       testhelpers.HeadersToBytes(testHeaders[1:]),
				},
			)
		}
		httpMock.Activate()

		// Execute concurrent requests
		var wg sync.WaitGroup
		results := make([]bool, numPeers)

		for i := 0; i < numPeers; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()

				peerURL := fmt.Sprintf("http://peer-%d", idx)
				result, _, err := server.catchupGetBlockHeaders(ctx, targetBlock, peerURL)
				results[idx] = err == nil && result != nil
			}(i)
		}

		wg.Wait()

		// All peers should succeed
		successCount := 0
		for _, success := range results {
			if success {
				successCount++
			}
		}
		assert.Equal(t, numPeers, successCount, "All concurrent requests should succeed")

		// Check metrics were recorded for all peers
		assert.Len(t, server.peerMetrics.PeerMetrics, numPeers)

		// Fastest peer should have the best response time
		var fastestPeer string
		var fastestTime time.Duration = time.Hour

		for peerURL, metric := range server.peerMetrics.PeerMetrics {
			if metric.AverageResponseTime < fastestTime {
				fastestTime = metric.AverageResponseTime
				fastestPeer = peerURL
			}
		}

		assert.Equal(t, "http://peer-4", fastestPeer, "Peer 4 should be fastest")
	})
}
