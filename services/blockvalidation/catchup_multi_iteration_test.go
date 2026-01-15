package blockvalidation

import (
	"context"
	"net/http"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestCatchup_MultiIterationNoDuplicates verifies that multi-iteration catchup
// does not create duplicate headers in the final result.
// This tests the bug fix where GetBlockHeadersFromOldest includes the starting block,
// causing duplicates at iteration boundaries (e.g., at position 9999/10000).
func TestCatchup_MultiIterationNoDuplicates(t *testing.T) {
	t.Run("ExactlyTwoIterations_10000Headers", func(t *testing.T) {
		ctx := context.Background()
		server, mockBlockchainClient, _, cleanup := setupTestCatchupServer(t)
		defer cleanup()

		// Create exactly 10,001 headers (0-10000) to test the boundary
		// This will require exactly 2 iterations with maxBlockHeadersPerRequest=10000
		numHeaders := 10_001
		allHeaders := testhelpers.CreateTestHeaders(t, numHeaders)

		targetBlock := &model.Block{
			Header: allHeaders[numHeaders-1],
			Height: uint32(numHeaders - 1),
		}

		// Mock GetBlockExists for target - not in our chain
		mockBlockchainClient.On("GetBlockExists", mock.Anything, targetBlock.Header.Hash()).
			Return(false, nil).Once()

		// Mock best block (we're at block 0)
		bestBlockHeader := allHeaders[0]
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 0}, nil)

		// Mock initial block locator from genesis
		mockBlockchainClient.On("GetBlockLocator", mock.Anything, bestBlockHeader.Hash(), uint32(0)).
			Return([]*chainhash.Hash{bestBlockHeader.Hash()}, nil)

		// Mock GetBlockExists for all headers
		mockBlockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).
			Return(false, nil).Maybe()

		// Mock GetBlockHeader for common ancestor
		mockBlockchainClient.On("GetBlockHeader", mock.Anything, allHeaders[0].Hash()).
			Return(allHeaders[0], &model.BlockHeaderMeta{Height: 0}, nil).Maybe()

		// Mock GetBlockHeader for any other blocks
		mockBlockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
			Return(nil, nil, errors.NewNotFoundError("block not found")).Maybe()

		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		iterationCount := 0
		httpmock.RegisterResponder(
			"GET",
			`=~^http://test-peer/headers_from_common_ancestor/.*`,
			func(req *http.Request) (*http.Response, error) {
				iterationCount++

				var responseHeaders []byte
				switch iterationCount {
				case 1:
					// First iteration: return common ancestor (0) and headers 1-10000
					// This simulates GetBlockHeadersFromOldest behavior (includes starting block)
					for i := 0; i < 10_000 && i < numHeaders; i++ {
						responseHeaders = append(responseHeaders, allHeaders[i].Bytes()...)
					}
				case 2:
					// Second iteration: return from header 9999 (the last from iteration 1)
					// This simulates GetBlockHeadersFromOldest including the starting block
					for i := 9_999; i < numHeaders; i++ {
						responseHeaders = append(responseHeaders, allHeaders[i].Bytes()...)
					}
				default:
					// Should not reach here
					t.Errorf("Unexpected iteration %d", iterationCount)
				}

				return httpmock.NewBytesResponse(200, responseHeaders), nil
			},
		)

		// Execute catchup
		result, _, err := server.catchupGetBlockHeaders(ctx, targetBlock, "peer-test-001", "http://test-peer")

		// Verify results
		require.NoError(t, err)
		require.NotNil(t, result)

		t.Logf("Total iterations: %d", iterationCount)
		t.Logf("Headers retrieved: %d", len(result.Headers))
		t.Logf("Expected headers: %d", numHeaders)

		// Verify we made 2 iterations
		assert.Equal(t, 2, iterationCount, "Should require exactly 2 iterations")

		// Verify we got all headers (including common ancestor)
		assert.Equal(t, numHeaders, len(result.Headers), "Should retrieve all headers")

		// CRITICAL: Verify no duplicate headers by checking each header hash is unique
		seenHashes := make(map[chainhash.Hash]int)
		for i, header := range result.Headers {
			hash := *header.Hash()
			if prevIndex, exists := seenHashes[hash]; exists {
				t.Errorf("Duplicate header found at index %d (previously seen at index %d): %s",
					i, prevIndex, hash.String())
			}
			seenHashes[hash] = i
		}

		// Verify chain continuity - each header should link to the previous
		for i := 1; i < len(result.Headers); i++ {
			prevHash := result.Headers[i-1].Hash()
			currentPrevHash := result.Headers[i].HashPrevBlock

			assert.True(t, currentPrevHash.IsEqual(prevHash),
				"Chain broken at position %d: block %s expects prev %s but previous block is %s",
				i, result.Headers[i].Hash().String(), currentPrevHash.String(), prevHash.String())
		}

		// Verify the critical boundary at position 9999/10000
		if len(result.Headers) > 10_000 {
			prevHash := result.Headers[9_999].Hash()
			currentPrevHash := result.Headers[10_000].HashPrevBlock

			assert.True(t, currentPrevHash.IsEqual(prevHash),
				"Chain broken at critical boundary 9999/10000: block %s expects prev %s but previous block is %s",
				result.Headers[10_000].Hash().String(), currentPrevHash.String(), prevHash.String())

			// Verify they are NOT the same block (no duplicate)
			assert.NotEqual(t, result.Headers[9_999].Hash().String(), result.Headers[10_000].Hash().String(),
				"Headers at position 9999 and 10000 should not be duplicates")
		}

		// Verify we reached the target
		assert.True(t, result.ReachedTarget, "Should reach target block")
	})

	t.Run("ThreeIterations_25000Headers", func(t *testing.T) {
		ctx := context.Background()
		server, mockBlockchainClient, _, cleanup := setupTestCatchupServer(t)
		defer cleanup()

		// Create 25,000 headers to test 3 iterations
		numHeaders := 25_000
		allHeaders := testhelpers.CreateTestHeaders(t, numHeaders)

		targetBlock := &model.Block{
			Header: allHeaders[numHeaders-1],
			Height: uint32(numHeaders - 1),
		}

		mockBlockchainClient.On("GetBlockExists", mock.Anything, targetBlock.Header.Hash()).
			Return(false, nil).Once()

		bestBlockHeader := allHeaders[0]
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 0}, nil)

		mockBlockchainClient.On("GetBlockLocator", mock.Anything, bestBlockHeader.Hash(), uint32(0)).
			Return([]*chainhash.Hash{bestBlockHeader.Hash()}, nil)

		mockBlockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).
			Return(false, nil).Maybe()

		mockBlockchainClient.On("GetBlockHeader", mock.Anything, allHeaders[0].Hash()).
			Return(allHeaders[0], &model.BlockHeaderMeta{Height: 0}, nil).Maybe()

		mockBlockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
			Return(nil, nil, errors.NewNotFoundError("block not found")).Maybe()

		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		iterationCount := 0
		httpmock.RegisterResponder(
			"GET",
			`=~^http://test-peer/headers_from_common_ancestor/.*`,
			func(req *http.Request) (*http.Response, error) {
				iterationCount++

				var responseHeaders []byte
				var startIdx, endIdx int

				switch iterationCount {
				case 1:
					// Iteration 1: headers 0-9999 (10,000 headers)
					startIdx = 0
					endIdx = 10_000
				case 2:
					// Iteration 2: headers 9999-19999 (10,001 headers including duplicate)
					startIdx = 9_999
					endIdx = 20_000
				case 3:
					// Iteration 3: headers 19999-24999 (5,001 headers including duplicate)
					startIdx = 19_999
					endIdx = numHeaders
				default:
					t.Errorf("Unexpected iteration %d", iterationCount)
				}

				for i := startIdx; i < endIdx && i < numHeaders; i++ {
					responseHeaders = append(responseHeaders, allHeaders[i].Bytes()...)
				}

				return httpmock.NewBytesResponse(200, responseHeaders), nil
			},
		)

		// Execute catchup
		result, _, err := server.catchupGetBlockHeaders(ctx, targetBlock, "peer-test-001", "http://test-peer")

		// Verify results
		require.NoError(t, err)
		require.NotNil(t, result)

		t.Logf("Total iterations: %d", iterationCount)
		t.Logf("Headers retrieved: %d", len(result.Headers))

		// Verify we made 3 iterations
		assert.Equal(t, 3, iterationCount, "Should require exactly 3 iterations")

		// Verify we got all headers
		assert.Equal(t, numHeaders, len(result.Headers), "Should retrieve all headers")

		// Verify no duplicate headers
		seenHashes := make(map[chainhash.Hash]int)
		for i, header := range result.Headers {
			hash := *header.Hash()
			if prevIndex, exists := seenHashes[hash]; exists {
				t.Errorf("Duplicate header found at index %d (previously seen at index %d): %s",
					i, prevIndex, hash.String())
			}
			seenHashes[hash] = i
		}

		// Verify chain continuity across all boundaries
		for i := 1; i < len(result.Headers); i++ {
			prevHash := result.Headers[i-1].Hash()
			currentPrevHash := result.Headers[i].HashPrevBlock

			assert.True(t, currentPrevHash.IsEqual(prevHash),
				"Chain broken at position %d: block %s expects prev %s but previous block is %s",
				i, result.Headers[i].Hash().String(), currentPrevHash.String(), prevHash.String())
		}

		// Verify specific iteration boundaries
		boundaries := []int{10_000, 20_000}
		for _, boundary := range boundaries {
			if len(result.Headers) > boundary {
				prevHash := result.Headers[boundary-1].Hash()
				currentPrevHash := result.Headers[boundary].HashPrevBlock

				assert.True(t, currentPrevHash.IsEqual(prevHash),
					"Chain broken at boundary %d: block %s expects prev %s but previous block is %s",
					boundary, result.Headers[boundary].Hash().String(), currentPrevHash.String(), prevHash.String())

				assert.NotEqual(t, result.Headers[boundary-1].Hash().String(), result.Headers[boundary].Hash().String(),
					"Headers at position %d and %d should not be duplicates", boundary-1, boundary)
			}
		}
	})

	t.Run("SingleHeaderSecondIteration", func(t *testing.T) {
		// Edge case: second iteration returns only 1 header (which would be a duplicate)
		ctx := context.Background()
		server, mockBlockchainClient, _, cleanup := setupTestCatchupServer(t)
		defer cleanup()

		numHeaders := 10_001
		allHeaders := testhelpers.CreateTestHeaders(t, numHeaders)

		targetBlock := &model.Block{
			Header: allHeaders[numHeaders-1],
			Height: uint32(numHeaders - 1),
		}

		mockBlockchainClient.On("GetBlockExists", mock.Anything, targetBlock.Header.Hash()).
			Return(false, nil).Once()

		bestBlockHeader := allHeaders[0]
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).
			Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 0}, nil)

		mockBlockchainClient.On("GetBlockLocator", mock.Anything, bestBlockHeader.Hash(), uint32(0)).
			Return([]*chainhash.Hash{bestBlockHeader.Hash()}, nil)

		mockBlockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).
			Return(false, nil).Maybe()

		mockBlockchainClient.On("GetBlockHeader", mock.Anything, allHeaders[0].Hash()).
			Return(allHeaders[0], &model.BlockHeaderMeta{Height: 0}, nil).Maybe()

		mockBlockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
			Return(nil, nil, errors.NewNotFoundError("block not found")).Maybe()

		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		iterationCount := 0
		httpmock.RegisterResponder(
			"GET",
			`=~^http://test-peer/headers_from_common_ancestor/.*`,
			func(req *http.Request) (*http.Response, error) {
				iterationCount++

				var responseHeaders []byte
				switch iterationCount {
				case 1:
					// First iteration: return headers 0-9999
					for i := 0; i < 10_000; i++ {
						responseHeaders = append(responseHeaders, allHeaders[i].Bytes()...)
					}
				case 2:
					// Second iteration: return only header 9999 (duplicate of last from iteration 1)
					responseHeaders = append(responseHeaders, allHeaders[9_999].Bytes()...)
				default:
					t.Errorf("Unexpected iteration %d", iterationCount)
				}

				return httpmock.NewBytesResponse(200, responseHeaders), nil
			},
		)

		// Execute catchup
		result, _, err := server.catchupGetBlockHeaders(ctx, targetBlock, "peer-test-001", "http://test-peer")

		// Verify results
		require.NoError(t, err)
		require.NotNil(t, result)

		t.Logf("Headers retrieved: %d", len(result.Headers))
		t.Logf("Stop reason: %s", result.StopReason)

		// The single header from iteration 2 should be skipped as it's a duplicate,
		// and since all headers were duplicates, catchup should stop
		assert.Equal(t, 10_000, len(result.Headers), "Should have exactly 10,000 headers (duplicate skipped)")
		assert.Contains(t, result.StopReason, "duplicates", "Should stop because all headers were duplicates")

		// Verify no duplicates
		seenHashes := make(map[chainhash.Hash]bool)
		for _, header := range result.Headers {
			hash := *header.Hash()
			assert.False(t, seenHashes[hash], "Found duplicate header: %s", hash.String())
			seenHashes[hash] = true
		}
	})
}

// TestCatchup_HeaderChainCacheWithMultiIteration tests that the header chain cache
// correctly handles headers from multi-iteration catchup without duplicates
func TestCatchup_HeaderChainCacheWithMultiIteration(t *testing.T) {
	ctx := context.Background()
	server, mockBlockchainClient, _, cleanup := setupTestCatchupServer(t)
	defer cleanup()

	// Create 15,000 headers to span 2 iterations
	numHeaders := 15_000
	allHeaders := testhelpers.CreateTestHeaders(t, numHeaders)

	targetBlock := &model.Block{
		Header: allHeaders[numHeaders-1],
		Height: uint32(numHeaders - 1),
	}

	// Setup mocks
	mockBlockchainClient.On("GetBlockExists", mock.Anything, targetBlock.Header.Hash()).
		Return(false, nil).Once()

	bestBlockHeader := allHeaders[0]
	mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).
		Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 0}, nil)

	mockBlockchainClient.On("GetBlockLocator", mock.Anything, bestBlockHeader.Hash(), uint32(0)).
		Return([]*chainhash.Hash{bestBlockHeader.Hash()}, nil)

	mockBlockchainClient.On("GetBlockExists", mock.Anything, mock.Anything).
		Return(false, nil).Maybe()

	mockBlockchainClient.On("GetBlockHeader", mock.Anything, allHeaders[0].Hash()).
		Return(allHeaders[0], &model.BlockHeaderMeta{Height: 0}, nil).Maybe()

	mockBlockchainClient.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(nil, nil, errors.NewNotFoundError("block not found")).Maybe()

	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()

	iterationCount := 0
	httpmock.RegisterResponder(
		"GET",
		`=~^http://test-peer/headers_from_common_ancestor/.*`,
		func(req *http.Request) (*http.Response, error) {
			iterationCount++

			var responseHeaders []byte
			switch iterationCount {
			case 1:
				// Iteration 1: headers 0-9999
				for i := 0; i < 10_000; i++ {
					responseHeaders = append(responseHeaders, allHeaders[i].Bytes()...)
				}
			case 2:
				// Iteration 2: headers 9999-14999 (includes duplicate at start)
				for i := 9_999; i < numHeaders; i++ {
					responseHeaders = append(responseHeaders, allHeaders[i].Bytes()...)
				}
			}

			return httpmock.NewBytesResponse(200, responseHeaders), nil
		},
	)

	// Execute catchup
	result, _, err := server.catchupGetBlockHeaders(ctx, targetBlock, "peer-test-001", "http://test-peer")

	require.NoError(t, err)
	require.NotNil(t, result)

	// Verify chain can be validated by building cache
	// This tests the original error scenario
	cache := server.headerChainCache
	cache.Clear()

	err = cache.BuildFromHeaders(result.Headers, 10)
	assert.NoError(t, err, "Header chain cache should build successfully without chain breaks")

	// Verify cache stats
	totalHeaders, validationSets := cache.GetCacheStats()
	assert.Equal(t, numHeaders, totalHeaders, "Cache should contain all headers")
	assert.Equal(t, numHeaders, validationSets, "Cache should have validation sets for all headers")

	// Verify we can retrieve validation headers for a block at the boundary
	if len(result.Headers) > 10_000 {
		blockAt10k := result.Headers[10_000]
		validationHeaders, found := cache.GetValidationHeaders(blockAt10k.Hash())
		assert.True(t, found, "Should find validation headers for block at position 10000")
		assert.NotNil(t, validationHeaders, "Validation headers should not be nil")
	}
}
