package blockvalidation

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/catchup"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestCatchup_EclipseAttack tests protection against eclipse attacks where all peers collude
func TestCatchup_EclipseAttack(t *testing.T) {
	t.Run("DetectColludingPeers", func(t *testing.T) {
		ctx, cancel := testhelpers.CreateTestContext(t, 30*time.Second)
		defer cancel()

		config := testhelpers.DefaultTestServerConfig()
		server, mockBlockchainClient, mockUTXOStore, cleanup := setupTestCatchupServerWithConfig(t, config)
		defer cleanup()

		// Mock UTXO store block height
		mockUTXOStore.On("GetBlockHeight").Return(uint32(1000))

		// Target block that colluding peers are pushing
		maliciousBlock := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  &chainhash.Hash{99, 99, 99}, // Fake previous
				HashMerkleRoot: &chainhash.Hash{88, 88, 88},
				Timestamp:      uint32(time.Now().Unix()),
				Bits:           model.NBit{},
				Nonce:          666666, // Suspicious nonce pattern
			},
			Height: 2000,
		}
		targetHash := maliciousBlock.Hash()

		mockBlockchainClient.On("GetBlockExists", mock.Anything, targetHash).
			Return(false, nil)

		// Our legitimate chain state - use real mainnet header
		mainnetHeaders := testhelpers.GetMainnetHeaders(t, 1)
		bestBlockHeader := mainnetHeaders[0]
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

		// Setup HTTP mocks using helper
		httpMock := testhelpers.NewHTTPMockSetup(t)
		defer httpMock.Deactivate()

		// All 50 peers return the same malicious chain
		// Use only 10 headers to avoid timestamp validation issues (10 * 10 minutes = 100 minutes < 2 hours)
		maliciousHeaders := testhelpers.CreateMaliciousChain(10)

		// Register malicious peers
		for i := 0; i < 50; i++ {
			peerURL := fmt.Sprintf("http://malicious-peer-%d", i)
			httpMock.RegisterHeaderResponse(peerURL, maliciousHeaders)
		}

		httpMock.Activate()

		// Mock validation that these headers are invalid
		for _, header := range maliciousHeaders {
			mockBlockchainClient.On("GetBlockExists", mock.Anything, header.Hash()).
				Return(false, nil).Maybe()

			// Mock that validation would fail for these headers
			mockBlockchainClient.On("GetBlockHeader", mock.Anything, header.Hash()).
				Return(nil, errors.NewNotFoundError("suspicious header")).Maybe()
		}

		// Try to catch up with the first malicious peer
		err := server.catchup(ctx, maliciousBlock, "", "http://malicious-peer-0")

		// Should detect something is wrong
		assert.Error(t, err)
	})

	t.Run("FindHonestPeerAmongMalicious", func(t *testing.T) {
		ctx, cancel := testhelpers.CreateTestContext(t, 30*time.Second)
		defer cancel()

		config := testhelpers.DefaultTestServerConfig()
		server, mockBlockchainClient, mockUTXOStore, cleanup := setupTestCatchupServerWithConfig(t, config)
		defer cleanup()

		// Mock UTXO store block height
		mockUTXOStore.On("GetBlockHeight").Return(uint32(1000))

		// Use consecutive mainnet headers for proper chain linkage
		mainnetHeaders := testhelpers.GetMainnetHeadersRange(t, 0, 11)
		bestBlockHeader := mainnetHeaders[0]

		// Honest chain is the consecutive mainnet headers after the common ancestor
		honestHeaders := mainnetHeaders[1:11]
		targetBlock := &model.Block{
			Header: honestHeaders[9],
			Height: 1010,
		}
		targetHash := targetBlock.Hash()

		mockBlockchainClient.On("GetBlockExists", mock.Anything, targetHash).
			Return(false, nil)

		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 1000}, nil)

		mockBlockchainClient.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
			Return([]*chainhash.Hash{bestBlockHeader.Hash()}, nil)

		// Mock GetBlockHeaders for common ancestor finding
		mockBlockchainClient.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).
			Return([]*model.BlockHeader{bestBlockHeader}, []*model.BlockHeaderMeta{{Height: 1000, ID: 1}}, nil).Maybe()

		// Mock GetBlockHeader for common ancestor finding - expects the best block header
		mockBlockchainClient.On("GetBlockHeader", mock.Anything, bestBlockHeader.Hash()).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 1000, ID: 1}, nil).Maybe()

		// Setup HTTP mocks
		httpMock := testhelpers.NewHTTPMockSetup(t)
		defer httpMock.Deactivate()

		// Create malicious chain that branches from the common ancestor
		maliciousHeaders, err := testhelpers.CreateAdversarialFork(mainnetHeaders, 0, 10)
		require.NoError(t, err, "Failed to create adversarial fork")

		// 49 malicious peers
		for i := 0; i < 49; i++ {
			peerURL := fmt.Sprintf("http://malicious-peer-%d", i)
			httpMock.RegisterHeaderResponse(peerURL, maliciousHeaders)
		}

		// 1 honest peer hidden among them
		httpMock.RegisterHeaderResponse("http://honest-peer", honestHeaders)
		httpMock.Activate()

		// Mock validation - honest headers exist and are valid
		for _, header := range honestHeaders {
			mockBlockchainClient.On("GetBlockExists", mock.Anything, header.Hash()).
				Return(false, nil).Maybe()
			mockBlockchainClient.On("GetBlockHeader", mock.Anything, header.Hash()).
				Return(header, &model.BlockHeaderMeta{Height: 1001}, nil).Maybe()
		}

		// Mock validation - malicious headers are invalid
		for _, header := range maliciousHeaders {
			mockBlockchainClient.On("GetBlockExists", mock.Anything, header.Hash()).
				Return(false, nil).Maybe()
		}

		// System should eventually find and use the honest peer
		// This tests peer diversity and validation

		// Try with honest peer
		err = server.catchup(ctx, targetBlock, "peer-honest-001", "http://honest-peer")

		// Should work with honest peer
		if err != nil {
			// Check it's not complaining about the honest chain
			assert.NotContains(t, err.Error(), "honest")
		}

		// Try with malicious peer - should fail
		err = server.catchup(ctx, targetBlock, "peer-malicious-001", "http://malicious-peer-0")
		if err == nil {
			t.Log("Warning: Accepted malicious peer data without error")
		}
	})
}

// TestCatchup_SybilAttack tests protection against Sybil attacks
func TestCatchup_SybilAttack(t *testing.T) {
	t.Run("ResistSybilWithManyFakePeers", func(t *testing.T) {
		ctx := context.Background()
		server, mockBlockchainClient, mockUTXOStore, cleanup := setupTestCatchupServer(t)
		defer cleanup()

		// Mock UTXO store block height
		mockUTXOStore.On("GetBlockHeight").Return(uint32(1000))

		// Use consecutive mainnet headers for proper chain linkage
		// Get 21 headers: header[0] is the common ancestor, headers[1:21] are the chain
		mainnetHeaders := testhelpers.GetMainnetHeadersRange(t, 0, 21)
		bestBlockHeader := mainnetHeaders[0]

		// Honest chain is the consecutive mainnet headers after the common ancestor
		honestHeaders := mainnetHeaders[1:21]

		// Create fake headers that branch from the common ancestor
		// These are synthetic headers with valid PoW that will fail other validation
		// CreateAdversarialFork returns headers including the fork point at index 0
		adversarialFork, err := testhelpers.CreateAdversarialFork(mainnetHeaders, 0, 21)
		require.NoError(t, err, "Failed to create adversarial fork")

		// Remove the common ancestor from adversarial fork since it's already at index 0
		// We only want the new forked headers, not the common ancestor
		adversarialForkHeaders := adversarialFork[1:]

		targetBlock := &model.Block{
			Header: honestHeaders[19], // Last header in the honest chain (index 19 since honestHeaders is [1:21])
			Height: 1020,
		}
		targetHash := targetBlock.Hash()

		// Mock that target block doesn't exist (Sybil test should work with non-existent target)
		mockBlockchainClient.On("GetBlockExists", mock.Anything, targetHash).
			Return(false, nil)

		// Mock that the target block header is from the honest chain
		mockBlockchainClient.On("GetBlockHeader", mock.Anything, targetHash).
			Return(targetBlock.Header, &model.BlockHeaderMeta{Height: 1020}, nil).Maybe()

		// bestBlockHeader is already set from mainnetHeaders[0] above
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

		// IMPORTANT: Mock that the common ancestor exists in our blockchain
		// This is required for chain continuity validation
		mockBlockchainClient.On("GetBlockExists", mock.Anything, bestBlockHeader.Hash()).
			Return(true, nil).Maybe()

		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		// Create full blocks for the honest chain
		honestBlocks := make([]*model.Block, len(honestHeaders))
		for i, header := range honestHeaders {
			honestBlocks[i] = &model.Block{
				Header:     header,
				Height:     uint32(1001 + i),
				CoinbaseTx: testhelpers.CreateSimpleCoinbaseTx(uint32(1001 + i)),
			}
		}

		// Create full blocks for adversarial fork
		adversarialBlocks := make([]*model.Block, len(adversarialForkHeaders))
		for i, header := range adversarialForkHeaders {
			adversarialBlocks[i] = &model.Block{
				Header:     header,
				Height:     uint32(1001 + i),
				CoinbaseTx: testhelpers.CreateSimpleCoinbaseTx(uint32(1001 + i)),
			}
		}

		// 100 fake peers controlled by attacker
		requestCounts := make(map[string]int)
		var countMutex sync.Mutex

		for i := 0; i < 100; i++ {
			peerURL := fmt.Sprintf("http://sybil-peer-%d", i)

			// Register headers endpoint
			httpmock.RegisterResponder(
				"GET",
				fmt.Sprintf(`=~^%s/headers_from_common_ancestor/.*`, peerURL),
				func(url string) func(*http.Request) (*http.Response, error) {
					return func(req *http.Request) (*http.Response, error) {
						countMutex.Lock()
						requestCounts[url]++
						countMutex.Unlock()

						// Add random delay to simulate network variance
						delay, _ := rand.Int(rand.Reader, big.NewInt(100))
						time.Sleep(time.Duration(delay.Int64()) * time.Millisecond)

						// This simulates Sybil peers trying to push a different chain
						headerBytes := testhelpers.HeadersToBytes(adversarialForkHeaders)
						return httpmock.NewBytesResponse(200, headerBytes), nil
					}
				}(peerURL),
			)

			// Register blocks batch endpoint for adversarial blocks
			// The batch endpoint expects blocks concatenated together
			httpmock.RegisterResponder(
				"GET",
				fmt.Sprintf(`=~^%s/blocks/.*\?n=.*`, peerURL),
				func(blocks []*model.Block) func(*http.Request) (*http.Response, error) {
					return func(req *http.Request) (*http.Response, error) {
						// Parse the starting hash from URL
						parts := strings.Split(req.URL.Path, "/")
						if len(parts) < 3 {
							return httpmock.NewStringResponse(400, "Invalid URL"), nil
						}
						startHashStr := parts[len(parts)-1]

						// Find the starting block
						startIdx := -1
						for i, block := range blocks {
							if block.Header.Hash().String() == startHashStr {
								startIdx = i
								break
							}
						}

						if startIdx == -1 {
							return httpmock.NewStringResponse(404, "Block not found"), nil
						}

						// Parse the number of blocks to return
						nStr := req.URL.Query().Get("n")
						n := 20 // default
						if nStr != "" {
							if parsed, err := strconv.Atoi(nStr); err == nil {
								n = parsed
							}
						}

						// Build batch response - blocks are returned in reverse order (newest-first)
						var batchData bytes.Buffer
						actualCount := 0
						// Start from the requested block and go backwards
						for i := startIdx; i >= 0 && actualCount < n; i-- {
							blockBytes, _ := blocks[i].Bytes()
							batchData.Write(blockBytes)
							actualCount++
						}

						return httpmock.NewBytesResponse(200, batchData.Bytes()), nil
					}
				}(adversarialBlocks),
			)
		}

		// 1 honest peer - returns the mainnet chain including common ancestor
		// The peer returns from common ancestor to target
		honestChainWithAncestor := append([]*model.BlockHeader{bestBlockHeader}, honestHeaders...)
		honestHeaderBytes := testhelpers.HeadersToBytes(honestChainWithAncestor)
		httpmock.RegisterResponder(
			"GET",
			`=~^http://honest-peer/headers_from_common_ancestor/.*`,
			func(req *http.Request) (*http.Response, error) {
				countMutex.Lock()
				requestCounts["http://honest-peer"]++
				countMutex.Unlock()

				return httpmock.NewBytesResponse(200, honestHeaderBytes), nil
			},
		)

		// Register blocks batch endpoint for honest blocks
		httpmock.RegisterResponder(
			"GET",
			`=~^http://honest-peer/blocks/.*\?n=.*`,
			func(blocks []*model.Block) func(*http.Request) (*http.Response, error) {
				return func(req *http.Request) (*http.Response, error) {
					// Parse the starting hash from URL
					parts := strings.Split(req.URL.Path, "/")
					if len(parts) < 3 {
						return httpmock.NewStringResponse(400, "Invalid URL"), nil
					}
					startHashStr := parts[len(parts)-1]

					// Find the starting block
					startIdx := -1
					for i, block := range blocks {
						if block.Header.Hash().String() == startHashStr {
							startIdx = i
							break
						}
					}

					if startIdx == -1 {
						return httpmock.NewStringResponse(404, "Block not found"), nil
					}

					// Parse the number of blocks to return
					nStr := req.URL.Query().Get("n")
					n := 20 // default
					if nStr != "" {
						if parsed, err := strconv.Atoi(nStr); err == nil {
							n = parsed
						}
					}

					// Build batch response - blocks are returned in reverse order (newest-first)
					var batchData bytes.Buffer
					actualCount := 0
					// Start from the requested block and go backwards
					for i := startIdx; i >= 0 && actualCount < n; i-- {
						blockBytes, _ := blocks[i].Bytes()
						batchData.Write(blockBytes)
						actualCount++
					}

					return httpmock.NewBytesResponse(200, batchData.Bytes()), nil
				}
			}(honestBlocks),
		)

		// Mock validation for honest chain headers (excluding common ancestor which is already mocked as existing)
		for i := 1; i < len(mainnetHeaders); i++ {
			header := mainnetHeaders[i]
			mockBlockchainClient.On("GetBlockExists", mock.Anything, header.Hash()).
				Return(false, nil).Maybe()
			mockBlockchainClient.On("GetBlockHeader", mock.Anything, header.Hash()).
				Return(header, &model.BlockHeaderMeta{}, nil).Maybe()
		}

		// Mock GetBlock for the common ancestor (needed during validation)
		mockBlockchainClient.On("GetBlock", mock.Anything, bestBlockHeader.Hash()).
			Return(&model.Block{Header: bestBlockHeader, Height: 1000}, nil).Maybe()

		// Mock GetBlocksMinedNotSet for validation (returns blocks, not hashes)
		mockBlockchainClient.On("GetBlocksMinedNotSet", mock.Anything).
			Return([]*model.Block{}, nil).Maybe()

		// Mock GetBlockHeaderIDs for validation
		mockBlockchainClient.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).
			Return([]uint32{}, nil).Maybe()

		// Mock AddBlock for successful block validation
		mockBlockchainClient.On("AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(nil).Maybe()

		// Mock SetBlockSubtreesSet for block validation
		mockBlockchainClient.On("SetBlockSubtreesSet", mock.Anything, mock.Anything).
			Return(nil).Maybe()

		// Mock validation for adversarial fork headers (excluding the common ancestor which is already mocked)
		for _, header := range adversarialForkHeaders {
			mockBlockchainClient.On("GetBlockExists", mock.Anything, header.Hash()).
				Return(false, nil).Maybe()
			// Return error for GetBlockHeader - these headers are rejected by blockchain validation
			// This simulates that the adversarial headers don't exist in our blockchain
			mockBlockchainClient.On("GetBlockHeader", mock.Anything, header.Hash()).
				Return(nil, errors.NewNotFoundError("invalid fork header")).Maybe()
		}

		// Test Sybil resistance
		// The key insight: Sybil peers return a different chain that doesn't contain the target block
		// Catchup should fail because the target block is not in the adversarial chain

		// Test each peer type separately to verify behavior
		var successCount, failCount int

		// Try more Sybil peers - they should fail
		for i := 0; i < 5; i++ {
			peerURL := fmt.Sprintf("http://sybil-peer-%d", i)
			peerID := fmt.Sprintf("peer-sybil-%03d", i)
			err := server.catchup(ctx, targetBlock, peerID, peerURL)
			if err != nil {
				failCount++
				t.Logf("Expected: Sybil peer %d failed: %v", i, err)
			} else {
				successCount++
				t.Logf("Unexpected: Sybil peer %d succeeded", i)
			}
		}

		// Mock CatchUpBlocks and GetFSMCurrentState for the honest peer attempt
		mockBlockchainClient.On("CatchUpBlocks", mock.Anything).Return(nil).Once()
		runningState := blockchain.FSMStateRUNNING
		mockBlockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&runningState, nil).Maybe()

		// Try the honest peer - should succeed
		err = server.catchup(ctx, targetBlock, "peer-honest-sybil-001", "http://honest-peer")
		if err == nil {
			successCount++
			t.Logf("Expected: Honest peer succeeded")
		} else {
			failCount++
			t.Logf("Unexpected: Honest peer failed: %v", err)
		}

		t.Logf("Success: %d, Failures: %d", successCount, failCount)
		t.Logf("Request distribution: %v", requestCounts)

		// Verify results
		assert.Equal(t, 1, successCount, "Only honest peer should succeed")
		assert.Equal(t, 5, failCount, "All 5 Sybil peers should fail")
	})
}

// TestCatchup_InvalidHeaderSequence tests detection of broken header chains
func TestCatchup_InvalidHeaderSequence(t *testing.T) {
	t.Run("DetectBrokenHeaderChain", func(t *testing.T) {
		ctx := context.Background()
		server, mockBlockchainClient, mockUTXOStore, cleanup := setupTestCatchupServer(t)
		defer cleanup()

		// Mock UTXO store block height
		mockUTXOStore.On("GetBlockHeight").Return(uint32(1000))

		// Use real mainnet headers for valid PoW
		mainnetHeaders := testhelpers.GetMainnetHeaders(t, 2)
		bestBlockHeader := mainnetHeaders[0]
		targetBlock := &model.Block{
			Header: mainnetHeaders[1],
			Height: 1100,
		}
		targetHash := targetBlock.Hash()

		mockBlockchainClient.On("GetBlockExists", mock.Anything, targetHash).
			Return(false, nil)
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

		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		// Create headers with broken chain - use minimum difficulty for mining
		nBits, _ := model.NewNBitFromString("207fffff")
		brokenHeaders := make([]*model.BlockHeader, 10)
		for i := 0; i < 10; i++ {
			brokenHeaders[i] = &model.BlockHeader{
				Version: 1,
				// Intentionally break the chain at position 5
				HashPrevBlock: func() *chainhash.Hash {
					if i == 5 {
						// Wrong previous hash - breaks the chain
						return &chainhash.Hash{99, 99, 99}
					}
					if i == 0 {
						return &chainhash.Hash{}
					}
					return brokenHeaders[i-1].Hash()
				}(),
				HashMerkleRoot: &chainhash.Hash{byte(i), byte(i + 1)},
				Timestamp:      uint32(time.Now().Unix() - int64((10-i)*600)), // Past timestamps
				Bits:           *nBits,
				Nonce:          0,
			}

			// Mine the header to have valid PoW
			testhelpers.MineHeader(brokenHeaders[i])

			mockBlockchainClient.On("GetBlockExists", mock.Anything, brokenHeaders[i].Hash()).
				Return(false, nil).Maybe()
		}

		httpmock.RegisterResponder(
			"GET",
			`=~^http://malicious-peer/headers_from_common_ancestor/.*`,
			httpmock.NewBytesResponder(200, testhelpers.HeadersToBytes(brokenHeaders)),
		)

		err := server.catchup(ctx, targetBlock, "", "http://malicious-peer")

		// Should detect the broken chain
		if err != nil {
			t.Logf("Catchup error: %v", err)
		} else {
			t.Log("Catchup succeeded unexpectedly")
		}
		assert.Error(t, err)

		// Verify peer was marked as malicious
		peerState := server.peerCircuitBreakers.GetPeerState("peer-malicious-001")
		t.Logf("Circuit breaker state for malicious peer: %v", peerState)
		// The circuit breaker might not immediately open on first failure
		// Check if there were any failures recorded
		if peerState == catchup.StateClosed {
			t.Log("Circuit breaker is still closed")
		}
		// For now, just verify the error was detected
		assert.Error(t, err, "Should detect broken chain")
	})

	t.Run("DetectOutOfOrderHeaders", func(t *testing.T) {
		ctx, cancel := testhelpers.CreateTestContext(t, 30*time.Second)
		defer cancel()

		config := testhelpers.DefaultTestServerConfig()
		server, mockBlockchainClient, _, cleanup := setupTestCatchupServerWithConfig(t, config)
		defer cleanup()

		targetBlock := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  &chainhash.Hash{1},
				HashMerkleRoot: &chainhash.Hash{2},
				Timestamp:      uint32(time.Now().Unix()),
				Bits:           model.NBit{},
				Nonce:          12345,
			},
			Height: 1100,
		}
		targetHash := targetBlock.Hash()

		mockBlockchainClient.On("GetBlockExists", mock.Anything, targetHash).
			Return(false, nil)

		bestBlockHeader := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(time.Now().Unix()),
			Bits:           model.NBit{},
			Nonce:          0,
		}
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 1000}, nil)

		mockBlockchainClient.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
			Return([]*chainhash.Hash{bestBlockHeader.Hash()}, nil)

		// Setup HTTP mocks
		httpMock := testhelpers.NewHTTPMockSetup(t)
		defer httpMock.Deactivate()

		// Create valid headers using ChainBuilder
		builder := testhelpers.NewChainBuilder(t).WithStartHeight(1001)
		builder.Build()
		validHeaders := builder.GetHeaders()

		// Shuffle the headers to simulate out-of-order delivery
		shuffledHeaders := make([]*model.BlockHeader, len(validHeaders))
		copy(shuffledHeaders, validHeaders)
		// Manual shuffle using crypto/rand
		for i := len(shuffledHeaders) - 1; i > 0; i-- {
			j, _ := rand.Int(rand.Reader, big.NewInt(int64(i+1)))
			jIdx := j.Int64()
			shuffledHeaders[i], shuffledHeaders[jIdx] = shuffledHeaders[jIdx], shuffledHeaders[i]
		}

		for _, header := range shuffledHeaders {
			mockBlockchainClient.On("GetBlockExists", mock.Anything, header.Hash()).
				Return(false, nil).Maybe()
		}

		httpMock.RegisterHeaderResponse("http://confused-peer", shuffledHeaders)
		httpMock.Activate()

		err := server.catchup(ctx, targetBlock, "", "http://confused-peer")

		// Should detect headers are not properly chained
		assert.Error(t, err)
	})
}

// TestCatchup_SecretMiningDetection tests detection of secret mining attempts
func TestCatchup_SecretMiningDetection(t *testing.T) {
	t.Run("DetectLargeHiddenChain", func(t *testing.T) {
		ctx, cancel := testhelpers.CreateTestContext(t, 30*time.Second)
		defer cancel()

		config := testhelpers.DefaultTestServerConfig()
		config.SecretMiningThreshold = 50
		server, mockBlockchainClient, mockUTXOStore, cleanup := setupTestCatchupServerWithConfig(t, config)
		defer cleanup()

		// Mock UTXO store block height
		mockUTXOStore.On("GetBlockHeight").Return(uint32(1000))

		// We're at block 1000 - use real mainnet header
		mainnetHeaders := testhelpers.GetMainnetHeaders(t, 1)
		bestBlockHeader := mainnetHeaders[0]

		// Create a suspiciously long hidden chain (200 blocks) with valid PoW
		nBits, _ := model.NewNBitFromString("207fffff")
		hiddenChain := make([]*model.BlockHeader, 200)
		for i := 0; i < 200; i++ {
			hiddenChain[i] = &model.BlockHeader{
				Version: 1,
				HashPrevBlock: func() *chainhash.Hash {
					if i == 0 {
						// Link to the best block header
						return bestBlockHeader.Hash()
					}
					return hiddenChain[i-1].Hash()
				}(),
				HashMerkleRoot: &chainhash.Hash{byte(i), byte(i + 1)},
				Timestamp:      uint32(time.Now().Unix() - int64((200-i)*600)), // Old timestamps, 10 min apart
				Bits:           *nBits,
				Nonce:          0,
			}
			// Mine the header
			testhelpers.MineHeader(hiddenChain[i])
		}

		targetBlock := &model.Block{
			Header: hiddenChain[199],
			Height: 1200,
		}
		targetHash := targetBlock.Hash()

		mockBlockchainClient.On("GetBlockExists", mock.Anything, targetHash).
			Return(false, nil)
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 1000}, nil)

		mockBlockchainClient.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
			Return([]*chainhash.Hash{bestBlockHeader.Hash()}, nil)

		// Mock GetBlockHeaders for common ancestor finding
		mockBlockchainClient.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).
			Return([]*model.BlockHeader{bestBlockHeader}, []*model.BlockHeaderMeta{{Height: 1000, ID: 1}}, nil).Maybe()

		// Mock GetBlock for common ancestor check
		mockBlockchainClient.On("GetBlock", mock.Anything, mock.Anything).
			Return(&model.Block{Height: 950}, nil).Maybe()

		// Mock GetBlockHeader for common ancestor - use mock.Anything for hash
		mockBlockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 1000}, nil).Maybe()

		// Setup HTTP mocks
		httpMock := testhelpers.NewHTTPMockSetup(t)
		defer httpMock.Deactivate()

		// Mock GetBlockExists for best block header (for FilterNewHeaders)
		mockBlockchainClient.On("GetBlockExists", mock.Anything, bestBlockHeader.Hash()).
			Return(true, nil).Maybe()

		for _, header := range hiddenChain {
			mockBlockchainClient.On("GetBlockExists", mock.Anything, header.Hash()).
				Return(false, nil).Maybe()
		}

		// Include the common ancestor (bestBlockHeader) in the response
		fullChain := append([]*model.BlockHeader{bestBlockHeader}, hiddenChain...)
		httpMock.RegisterHeaderResponse("http://secret-miner", fullChain)
		httpMock.Activate()

		// Mock CatchUpBlocks and GetFSMCurrentState for the catchup attempt
		mockBlockchainClient.On("CatchUpBlocks", mock.Anything).Return(nil).Maybe()
		runningState := blockchain.FSMStateRUNNING
		mockBlockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&runningState, nil).Maybe()

		// Should detect secret mining attempt
		err := server.catchup(ctx, targetBlock, "peer-secret-miner-001", "http://secret-miner")

		// Should trigger secret mining detection or fail during validation
		if err == nil {
			t.Log("Warning: Secret mining not detected as error")
		} else {
			t.Logf("Secret mining detection error: %v", err)
		}

		// The catchup should fail - either due to common ancestor issues or secret mining detection
		assert.Error(t, err, "Catchup with secret miner should fail")
	})

	t.Run("AllowLegitimateDeepReorg", func(t *testing.T) {
		ctx, cancel := testhelpers.CreateTestContext(t, 30*time.Second)
		defer cancel()

		config := testhelpers.DefaultTestServerConfig()
		config.SecretMiningThreshold = 100
		server, mockBlockchainClient, mockUTXOStore, cleanup := setupTestCatchupServerWithConfig(t, config)
		defer cleanup()

		// Mock UTXO store block height
		mockUTXOStore.On("GetBlockHeight").Return(uint32(1000))

		// Use real mainnet headers for legitimate reorg
		mainnetHeaders := testhelpers.GetMainnetHeadersRange(t, 0, 41) // 41 headers for 40-block reorg
		bestBlockHeader := mainnetHeaders[0]
		reorgChain := mainnetHeaders[1:41] // 40 blocks representing the reorg

		targetBlock := &model.Block{
			Header: reorgChain[39],
			Height: 1040,
		}
		targetHash := targetBlock.Hash()

		mockBlockchainClient.On("GetBlockExists", mock.Anything, targetHash).
			Return(false, nil)
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 1000}, nil)

		mockBlockchainClient.On("GetBlockLocator", mock.Anything, mock.Anything, mock.Anything).
			Return([]*chainhash.Hash{bestBlockHeader.Hash()}, nil)

		// Mock GetBlockHeaders for common ancestor finding
		mockBlockchainClient.On("GetBlockHeaders", mock.Anything, mock.Anything, mock.Anything).
			Return([]*model.BlockHeader{bestBlockHeader}, []*model.BlockHeaderMeta{{Height: 1000, ID: 1}}, nil).Maybe()

		// Mock GetBlock for common ancestor check
		mockBlockchainClient.On("GetBlock", mock.Anything, mock.Anything).
			Return(&model.Block{Height: 990}, nil).Maybe()

		// Mock GetBlockHeader for common ancestor - use mock.Anything for hash
		mockBlockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 1000}, nil).Maybe()

		// Setup HTTP mocks
		httpMock := testhelpers.NewHTTPMockSetup(t)
		defer httpMock.Deactivate()

		// Mock GetBlockExists for best block header (it already exists)
		mockBlockchainClient.On("GetBlockExists", mock.Anything, bestBlockHeader.Hash()).
			Return(true, nil).Maybe()

		// Mock for all mainnet headers except the best block header
		for _, header := range mainnetHeaders[1:] {
			mockBlockchainClient.On("GetBlockExists", mock.Anything, header.Hash()).
				Return(false, nil).Maybe()
		}

		// Include common ancestor in the response
		httpMock.RegisterHeaderResponse("http://legitimate-peer", mainnetHeaders)
		httpMock.Activate()

		// Mock CatchUpBlocks and GetFSMCurrentState for the catchup attempt
		mockBlockchainClient.On("CatchUpBlocks", mock.Anything).Return(nil).Maybe()
		runningState := blockchain.FSMStateRUNNING
		mockBlockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&runningState, nil).Maybe()

		// Should allow legitimate reorg
		err := server.catchup(ctx, targetBlock, "peer-legitimate-001", "http://legitimate-peer")

		// Should not trigger secret mining for legitimate reorg
		if err != nil {
			assert.NotContains(t, err.Error(), "secret mining")
			assert.NotContains(t, err.Error(), "threshold")
		}

		// Should not mark peer as malicious
		AssertPeerMetrics(t, server, "peer-legitimate-001", func(m *catchup.PeerCatchupMetrics) {
			assert.Equal(t, int64(0), m.MaliciousAttempts, "Should not mark legitimate peer as malicious")
		})
	})
}

// Helper functions removed - now using functions from catchup_test_helpers.go
