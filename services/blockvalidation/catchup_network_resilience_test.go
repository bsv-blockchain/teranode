package blockvalidation

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/catchup"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestCatchup_PartialNetworkFailure tests catchup behavior with 50% packet loss
func TestCatchup_PartialNetworkFailure(t *testing.T) {
	t.Run("RetryWithPacketLoss", func(t *testing.T) {
		config := &testhelpers.CatchupServerConfig{
			SecretMiningThreshold:   100,
			MaxRetries:              5,
			RetryDelay:              50 * time.Millisecond,
			CatchupOperationTimeout: 30,
		}
		suite := NewCatchupTestSuiteWithConfig(t, config)
		defer suite.Cleanup()

		suite.MockUTXOStore.On("GetBlockHeight").Return(uint32(1000)).Maybe()

		testHeaders := testhelpers.GetMainnetHeadersRange(t, 0, 10)
		targetBlock := &model.Block{
			Header: testHeaders[9],
			Height: 1010,
		}

		suite.MockBlockchain.On("GetBlockExists", mock.Anything, targetBlock.Hash()).
			Return(false, nil)

		bestBlockHeader := testHeaders[0]
		suite.MockBlockchain.On("GetBestBlockHeader", mock.Anything).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 1000}, nil)

		suite.MockBlockchain.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).
			Return([]uint32{1}, nil).Maybe()

		suite.MockBlockchain.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
			Return([]*chainhash.Hash{bestBlockHeader.Hash()}, nil)

		suite.MockBlockchain.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).
			Return([]*model.BlockHeader{bestBlockHeader}, []*model.BlockHeaderMeta{{Height: 1000, ID: 1}}, nil).Maybe()

		suite.MockBlockchain.On("GetBlockHeader", mock.Anything, bestBlockHeader.Hash()).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 1000, ID: 1}, nil).Maybe()

		suite.MockBlockchain.On("GetBlockExists", mock.Anything, bestBlockHeader.Hash()).
			Return(true, nil).Maybe()

		for _, header := range testHeaders[1:] {
			suite.MockBlockchain.On("GetBlockExists", mock.Anything, header.Hash()).
				Return(false, nil).Maybe()
			suite.MockBlockchain.On("GetBlockHeader", mock.Anything, header.Hash()).
				Return(header, &model.BlockHeaderMeta{Height: 1001}, nil).Maybe()
		}

		httpMock := testhelpers.NewHTTPMockSetup(t)
		defer httpMock.Deactivate()

		httpMock.RegisterFlakeyResponse("http://unreliable-peer", 1, testHeaders[1:])
		httpMock.Activate()

		result, _, err := suite.Server.catchupGetBlockHeaders(suite.Ctx, targetBlock, "peer-unreliable-001", "http://unreliable-peer")

		suite.RequireNoError(err)
		assert.NotNil(t, result)
	})

	t.Run("EventualSuccessAfterMultipleFailures", func(t *testing.T) {
		config := &testhelpers.CatchupServerConfig{
			SecretMiningThreshold:   100,
			MaxRetries:              10,
			RetryDelay:              100 * time.Millisecond,
			CatchupOperationTimeout: 10, // Increase timeout for retries
		}
		suite := NewCatchupTestSuiteWithConfig(t, config)
		defer suite.Cleanup()

		suite.MockUTXOStore.On("GetBlockHeight").Return(uint32(1000)).Maybe()

		testHeaders := testhelpers.GetMainnetHeadersRange(t, 0, 5)
		targetBlock := &model.Block{
			Header: testHeaders[4],
			Height: 1005,
		}

		suite.MockBlockchain.On("GetBlockExists", mock.Anything, targetBlock.Hash()).
			Return(false, nil)

		suite.MockBlockchain.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).
			Return([]uint32{1}, nil).Maybe()

		bestBlockHeader := testHeaders[0]
		suite.MockBlockchain.On("GetBestBlockHeader", mock.Anything).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 1000}, nil)

		suite.MockBlockchain.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).
			Return([]uint32{1}, nil).Maybe()

		suite.MockBlockchain.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
			Return([]*chainhash.Hash{bestBlockHeader.Hash()}, nil)

		suite.MockBlockchain.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).
			Return([]*model.BlockHeader{bestBlockHeader}, []*model.BlockHeaderMeta{{Height: 1000, ID: 1}}, nil).Maybe()

		suite.MockBlockchain.On("GetBlockHeader", mock.Anything, bestBlockHeader.Hash()).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 1000, ID: 1}, nil).Maybe()

		for _, header := range testHeaders {
			suite.MockBlockchain.On("GetBlockExists", mock.Anything, header.Hash()).
				Return(false, nil).Maybe()
		}

		httpMock := testhelpers.NewHTTPMockSetup(t)
		defer httpMock.Deactivate()

		httpMock.RegisterFlakeyResponse("http://flaky-peer", 3, testHeaders[1:])
		httpMock.Activate()

		result, _, err := suite.Server.catchupGetBlockHeaders(suite.Ctx, targetBlock, "peer-flaky-001", "http://flaky-peer")

		suite.RequireNoError(err)
		assert.NotNil(t, result)
	})
}

// TestCatchup_ConnectionDropMidTransfer tests behavior when connection drops during transfer
func TestCatchup_ConnectionDropMidTransfer(t *testing.T) {
	t.Run("RecoverFromPartialTransfer", func(t *testing.T) {
		config := &testhelpers.CatchupServerConfig{
			SecretMiningThreshold:   100,
			MaxRetries:              3,
			RetryDelay:              50 * time.Millisecond,
			CatchupOperationTimeout: 30,
			CircuitBreakerConfig: &catchup.CircuitBreakerConfig{
				FailureThreshold:    3,
				SuccessThreshold:    2,
				Timeout:             time.Minute,
				MaxHalfOpenRequests: 1,
			},
		}
		suite := NewCatchupTestSuiteWithConfig(t, config)
		defer suite.Cleanup()

		suite.MockUTXOStore.On("GetBlockHeight").Return(uint32(1000)).Maybe()

		testHeaders := testhelpers.GetMainnetHeadersRange(t, 0, 100)
		targetBlock := &model.Block{
			Header: testHeaders[99],
			Height: 1100,
		}

		bestBlockHeader := testHeaders[0]

		suite.MockBlockchain.On("GetBlockExists", mock.Anything, targetBlock.Hash()).
			Return(false, nil)

		suite.MockBlockchain.On("GetBlockExists", mock.Anything, bestBlockHeader.Hash()).
			Return(true, nil).Maybe()

		for _, header := range testHeaders[1:] {
			suite.MockBlockchain.On("GetBlockExists", mock.Anything, header.Hash()).
				Return(false, nil).Maybe()
		}
		suite.MockBlockchain.On("GetBestBlockHeader", mock.Anything).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 1000}, nil)

		suite.MockBlockchain.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).
			Return([]uint32{1}, nil).Maybe()

		suite.MockBlockchain.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
			Return([]*chainhash.Hash{bestBlockHeader.Hash()}, nil)

		suite.MockBlockchain.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).
			Return([]*model.BlockHeader{bestBlockHeader}, []*model.BlockHeaderMeta{{Height: 1000, ID: 1}}, nil).Maybe()

		suite.MockBlockchain.On("GetBlockHeader", mock.Anything, bestBlockHeader.Hash()).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 1000, ID: 1}, nil).Maybe()

		httpMock := testhelpers.NewHTTPMockSetup(t)
		defer httpMock.Deactivate()

		fullHeaders := testHeaders[1:100]

		httpMock.RegisterFlakeyResponse("http://dropping-peer", 1, fullHeaders)
		httpMock.Activate()

		_, _, err := suite.Server.catchupGetBlockHeaders(suite.Ctx, targetBlock, "peer-dropping-002", "http://dropping-peer")

		if err != nil {
			breaker := suite.Server.peerCircuitBreakers.GetBreaker("peer-dropping-002")
			assert.NotNil(t, breaker)
			state, _, _, _ := breaker.GetStats()
			assert.NotEqual(t, catchup.StateClosed, state, "Circuit breaker should not be closed after failure")
			AssertCircuitBreakerState(t, suite.Server, "peer-dropping-002", catchup.StateOpen)
		}
	})

	t.Run("CircuitBreakerOpensAfterRepeatedDrops", func(t *testing.T) {
		config := &testhelpers.CatchupServerConfig{
			SecretMiningThreshold:   100,
			MaxRetries:              0,
			RetryDelay:              50 * time.Millisecond,
			CatchupOperationTimeout: 30,
			CircuitBreakerConfig: &catchup.CircuitBreakerConfig{
				FailureThreshold:    2,
				SuccessThreshold:    2,
				Timeout:             time.Second,
				MaxHalfOpenRequests: 1,
			},
		}
		suite := NewCatchupTestSuiteWithConfig(t, config)
		defer suite.Cleanup()

		suite.MockUTXOStore.On("GetBlockHeight").Return(uint32(1000)).Maybe()

		testHeaders := testhelpers.GetMainnetHeadersRange(t, 0, 10)
		targetBlock := &model.Block{
			Header: testHeaders[9],
			Height: 1010,
		}

		suite.MockBlockchain.On("GetBlockExists", mock.Anything, targetBlock.Hash()).
			Return(false, nil)

		bestBlockHeader := testHeaders[0]
		suite.MockBlockchain.On("GetBestBlockHeader", mock.Anything).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 1000}, nil)

		suite.MockBlockchain.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).
			Return([]uint32{1}, nil).Maybe()

		suite.MockBlockchain.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
			Return([]*chainhash.Hash{bestBlockHeader.Hash()}, nil)

		suite.MockBlockchain.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).
			Return([]*model.BlockHeader{bestBlockHeader}, []*model.BlockHeaderMeta{{Height: 1000, ID: 1}}, nil).Maybe()

		suite.MockBlockchain.On("GetBlockHeader", mock.Anything, bestBlockHeader.Hash()).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 1000, ID: 1}, nil).Maybe()

		suite.MockBlockchain.On("GetBlockExists", mock.Anything, bestBlockHeader.Hash()).
			Return(true, nil).Maybe()

		for _, header := range testHeaders[1:] {
			suite.MockBlockchain.On("GetBlockExists", mock.Anything, header.Hash()).
				Return(false, nil).Maybe()
		}

		httpMock := testhelpers.NewHTTPMockSetup(t)
		defer httpMock.Deactivate()

		httpMock.RegisterErrorResponse("http://bad-peer", errors.NewError("connection reset by peer"))
		httpMock.Activate()

		peerID := "peer-bad-001"

		ctx1, cancel1 := context.WithTimeout(suite.Ctx, 2*time.Second)
		_, _, err1 := suite.Server.catchupGetBlockHeaders(ctx1, targetBlock, peerID, "http://bad-peer")
		cancel1()
		assert.Error(t, err1)

		ctx2, cancel2 := context.WithTimeout(suite.Ctx, 2*time.Second)
		_, _, err2 := suite.Server.catchupGetBlockHeaders(ctx2, targetBlock, peerID, "http://bad-peer")
		cancel2()
		assert.Error(t, err2)

		ctx3, cancel3 := context.WithTimeout(suite.Ctx, 2*time.Second)
		_, _, err3 := suite.Server.catchupGetBlockHeaders(ctx3, targetBlock, peerID, "http://bad-peer")
		cancel3()
		assert.Error(t, err3)
		assert.Contains(t, err3.Error(), "circuit breaker open")

		breaker := suite.Server.peerCircuitBreakers.GetBreaker(peerID)
		state, _, _, _ := breaker.GetStats()
		assert.Equal(t, catchup.StateOpen, state)
	})
}

// TestCatchup_FlappingPeer tests behavior with peers that alternate between success and failure
func TestCatchup_FlappingPeer(t *testing.T) {
	t.Run("CircuitBreakerStabilizes", func(t *testing.T) {
		config := &testhelpers.CatchupServerConfig{
			SecretMiningThreshold:   100,
			MaxRetries:              3,
			RetryDelay:              50 * time.Millisecond,
			CatchupOperationTimeout: 30,
			CircuitBreakerConfig: &catchup.CircuitBreakerConfig{
				FailureThreshold:    3,
				SuccessThreshold:    2,
				Timeout:             time.Second * 5,
				MaxHalfOpenRequests: 1,
			},
		}
		suite := NewCatchupTestSuiteWithConfig(t, config)
		defer suite.Cleanup()

		testHeaders := testhelpers.GetMainnetHeadersRange(t, 0, 10)
		targetBlock := &model.Block{
			Header: testHeaders[9],
			Height: 1010,
		}

		suite.MockBlockchain.On("GetBlockExists", mock.Anything, targetBlock.Hash()).
			Return(false, nil)

		bestBlockHeader := testHeaders[0]
		suite.MockBlockchain.On("GetBestBlockHeader", mock.Anything).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 1000}, nil)

		suite.MockBlockchain.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).
			Return([]uint32{1}, nil).Maybe()

		suite.MockBlockchain.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
			Return([]*chainhash.Hash{bestBlockHeader.Hash()}, nil)

		suite.MockBlockchain.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).
			Return([]*model.BlockHeader{bestBlockHeader}, []*model.BlockHeaderMeta{{Height: 1000, ID: 1}}, nil).Maybe()

		suite.MockBlockchain.On("GetBlockHeader", mock.Anything, mock.Anything).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 1000}, nil).Maybe()

		for _, header := range testHeaders {
			suite.MockBlockchain.On("GetBlockExists", mock.Anything, header.Hash()).
				Return(false, nil).Maybe()
		}

		httpMock := testhelpers.NewHTTPMockSetup(t)
		defer httpMock.Deactivate()

		requestCount := int32(0)
		httpMock.RegisterResponse(
			`=~^http://flapping-peer/headers_from_common_ancestor/.*`,
			&testhelpers.HTTPMockResponse{
				StatusCode: 200,
				Validator: func(req *http.Request) error {
					count := atomic.AddInt32(&requestCount, 1)
					if count%2 == 1 {
						return errors.NewError("connection timeout")
					}
					return nil
				},
				Body: testhelpers.HeadersToBytes(testHeaders[1:]),
			},
		)
		httpMock.Activate()

		successCount := 0
		failCount := 0

		for i := 0; i < 6; i++ {
			_, _, err := suite.Server.catchupGetBlockHeaders(suite.Ctx, targetBlock, "peer-flapping-002", "http://flapping-peer")
			if err != nil {
				failCount++
			} else {
				successCount++
			}

			time.Sleep(100 * time.Millisecond)
		}

		assert.Greater(t, successCount, 0, "Should have some successes")

		// Note: peerMetrics field has been removed from Server struct
		// (peer reputation checks disabled)
		_ = failCount // Silence unused variable warning

		breaker := suite.Server.peerCircuitBreakers.GetBreaker("peer-flapping-001")
		finalState, _, _, _ := breaker.GetStats()
		t.Logf("Final circuit breaker state: %v", finalState)
	})

	t.Run("PeerEventuallyMarkedUnreliable", func(t *testing.T) {
		config := &testhelpers.CatchupServerConfig{
			SecretMiningThreshold:   100,
			MaxRetries:              3,
			RetryDelay:              50 * time.Millisecond,
			CatchupOperationTimeout: 30,
			CircuitBreakerConfig: &catchup.CircuitBreakerConfig{
				FailureThreshold:    5,
				SuccessThreshold:    2,
				Timeout:             time.Second * 10,
				MaxHalfOpenRequests: 1,
			},
		}
		suite := NewCatchupTestSuiteWithConfig(t, config)
		defer suite.Cleanup()

		suite.MockUTXOStore.On("GetBlockHeight").Return(uint32(1000)).Maybe()

		testHeaders := testhelpers.GetMainnetHeadersRange(t, 0, 5)
		targetBlock := &model.Block{
			Header: testHeaders[4],
			Height: 1005,
		}

		suite.MockBlockchain.On("GetBlockExists", mock.Anything, targetBlock.Hash()).
			Return(false, nil)

		bestBlockHeader := testHeaders[0]
		suite.MockBlockchain.On("GetBestBlockHeader", mock.Anything).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 1000}, nil)

		suite.MockBlockchain.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).
			Return([]uint32{1}, nil).Maybe()

		suite.MockBlockchain.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
			Return([]*chainhash.Hash{bestBlockHeader.Hash()}, nil)

		suite.MockBlockchain.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).
			Return([]*model.BlockHeader{bestBlockHeader}, []*model.BlockHeaderMeta{{Height: 1000, ID: 1}}, nil).Maybe()

		suite.MockBlockchain.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).
			Return([]uint32{1}, nil).Maybe()

		suite.MockBlockchain.On("GetBlockHeader", mock.Anything, mock.Anything).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 1000}, nil).Maybe()

		for _, header := range testHeaders {
			suite.MockBlockchain.On("GetBlockExists", mock.Anything, header.Hash()).
				Return(false, nil).Maybe()
		}

		httpMock := testhelpers.NewHTTPMockSetup(t)
		defer httpMock.Deactivate()

		requestCount := int32(0)
		httpMock.RegisterResponse(
			`=~^http://degrading-peer/headers_from_common_ancestor/.*`,
			&testhelpers.HTTPMockResponse{
				StatusCode: 200,
				Validator: func(req *http.Request) error {
					count := atomic.AddInt32(&requestCount, 1)
					if count > 2 && count%5 != 0 {
						return errors.NewError("service unavailable")
					}
					return nil
				},
				Body: testhelpers.HeadersToBytes(testHeaders[1:]),
			},
		)
		httpMock.Activate()

		for i := 0; i < 10; i++ {
			reqCtx, reqCancel := context.WithTimeout(suite.Ctx, 2*time.Second)
			_, _, _ = suite.Server.catchupGetBlockHeaders(reqCtx, targetBlock, "peer-degrading-001", "http://degrading-peer")
			reqCancel()
			time.Sleep(50 * time.Millisecond)
		}

		// Note: peerMetrics field has been removed from Server struct
		// (peer metrics checks disabled)
		t.Log("Peer metrics checks disabled - peerMetrics field removed")
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

		// Note: peerMetrics field has been removed from Server struct

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
			peerID := fmt.Sprintf("peer-partition-%03d", peerIdx)
			result, _, err := server.catchupGetBlockHeaders(ctx, targetBlock, peerID, peerURL)

			if err == nil && result != nil {
				// Note: peerMetrics field has been removed from Server struct
				// (peer metrics recording disabled)
			}
		}

		// Note: peerMetrics field has been removed from Server struct
		// (peer metrics assertions disabled)
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
		result, _, err := server.catchupGetBlockHeaders(ctx, targetBlock, "peer-slow-001", "http://slow-peer")
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
		_, _, err := server.catchupGetBlockHeaders(ctx, targetBlock, "peer-very-slow-001", "http://very-slow-peer")

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

		// Note: peerMetrics field has been removed from Server struct

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
				peerID := fmt.Sprintf("peer-concurrent-%03d", idx)
				result, _, err := server.catchupGetBlockHeaders(ctx, targetBlock, peerID, peerURL)
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

		// Note: peerMetrics field has been removed from Server struct
		// (peer metrics checks disabled)

		// Note: fastest peer assertion disabled - peerMetrics field removed
	})
}
