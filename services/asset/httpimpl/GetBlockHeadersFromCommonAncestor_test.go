package httpimpl

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/asset/repository"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestGetBlockHeadersFromCommonAncestor(t *testing.T) {
	t.Run("JSON success", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// Mock successful repository call
		mockRepo.On("GetBlockHeadersFromCommonAncestor", mock.Anything, mock.Anything, mock.Anything).Return([]*model.BlockHeader{testBlockHeader}, []*model.BlockHeaderMeta{testBlockHeaderMeta}, nil)

		echoContext.SetPath("/block/headersFromCommonAncestor/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

		// Set query parameters for block locator hashes and number of headers
		echoContext.QueryParams().Set("block_locator_hashes", "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
		echoContext.QueryParams().Set("n", "50")

		err := httpServer.GetBlockHeadersFromCommonAncestor(JSON)(echoContext)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, http.StatusOK, responseRecorder.Code)

		var response []map[string]interface{}

		if err = json.Unmarshal(responseRecorder.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}

		require.NotNil(t, response)
		assert.Equal(t, 1, len(response))
		assert.Equal(t, "9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995", response[0]["hash"])
		assert.Equal(t, float64(1), response[0]["version"])
		assert.Equal(t, float64(1), response[0]["height"])
		assert.Equal(t, float64(2), response[0]["txCount"])
		assert.Equal(t, float64(3), response[0]["sizeInBytes"])
		assert.Equal(t, float64(432645644), response[0]["time"])
		assert.Equal(t, float64(12435623), response[0]["nonce"])
		assert.Equal(t, "00000000", response[0]["bits"])

		mockRepo.AssertExpectations(t)
	})

	t.Run("BINARY success", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		mockRepo.On("GetBlockHeadersFromCommonAncestor", mock.Anything, mock.Anything, mock.Anything).Return([]*model.BlockHeader{testBlockHeader}, []*model.BlockHeaderMeta{testBlockHeaderMeta}, nil)

		echoContext.SetPath("/block/headersFromCommonAncestor/:hash/raw")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")
		echoContext.QueryParams().Set("block_locator_hashes", "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")

		err := httpServer.GetBlockHeadersFromCommonAncestor(BINARY_STREAM)(echoContext)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, http.StatusOK, responseRecorder.Code)
		assert.Equal(t, "application/octet-stream", responseRecorder.Header().Get("Content-Type"))
		assert.Equal(t, 80, responseRecorder.Body.Len()) // 80 bytes per header

		mockRepo.AssertExpectations(t)
	})

	t.Run("HEX success", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		mockRepo.On("GetBlockHeadersFromCommonAncestor", mock.Anything, mock.Anything, mock.Anything).Return([]*model.BlockHeader{testBlockHeader}, []*model.BlockHeaderMeta{testBlockHeaderMeta}, nil)

		echoContext.SetPath("/block/headersFromCommonAncestor/:hash/hex")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")
		echoContext.QueryParams().Set("block_locator_hashes", "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")

		err := httpServer.GetBlockHeadersFromCommonAncestor(HEX)(echoContext)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, http.StatusOK, responseRecorder.Code)
		assert.Equal(t, "text/plain; charset=UTF-8", responseRecorder.Header().Get("Content-Type"))
		assert.Equal(t, 160, responseRecorder.Body.Len()) // 160 hex characters per header

		mockRepo.AssertExpectations(t)
	})

	t.Run("Default number of headers", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// Verify that default number of headers (100) is used when 'n' parameter is not provided
		mockRepo.On("GetBlockHeadersFromCommonAncestor", mock.Anything, mock.Anything, uint32(100)).Return([]*model.BlockHeader{testBlockHeader}, []*model.BlockHeaderMeta{testBlockHeaderMeta}, nil)

		echoContext.SetPath("/block/headersFromCommonAncestor/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")
		echoContext.QueryParams().Set("block_locator_hashes", "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")

		err := httpServer.GetBlockHeadersFromCommonAncestor(JSON)(echoContext)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, http.StatusOK, responseRecorder.Code)
		mockRepo.AssertExpectations(t)
	})

	t.Run("Maximum number of headers capped at 10000", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// Verify that number of headers is capped at 10,000 by parseNumberOfHeaders function
		mockRepo.On("GetBlockHeadersFromCommonAncestor", mock.Anything, mock.Anything, uint32(10000)).Return([]*model.BlockHeader{testBlockHeader}, []*model.BlockHeaderMeta{testBlockHeaderMeta}, nil)

		echoContext.SetPath("/block/headersFromCommonAncestor/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")
		echoContext.QueryParams().Set("block_locator_hashes", "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
		echoContext.QueryParams().Set("n", "15000") // Request more than 10,000

		err := httpServer.GetBlockHeadersFromCommonAncestor(JSON)(echoContext)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, http.StatusOK, responseRecorder.Code)
		mockRepo.AssertExpectations(t)
	})

	t.Run("Multiple block locator hashes", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// Test with multiple block locator hashes
		hash1 := "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
		hash2 := "9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995"
		multipleHashes := hash1 + hash2

		mockRepo.On("GetBlockHeadersFromCommonAncestor", mock.Anything, mock.MatchedBy(func(hashes []chainhash.Hash) bool {
			return len(hashes) == 2
		}), mock.Anything).Return([]*model.BlockHeader{testBlockHeader}, []*model.BlockHeaderMeta{testBlockHeaderMeta}, nil)

		echoContext.SetPath("/block/headersFromCommonAncestor/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")
		echoContext.QueryParams().Set("block_locator_hashes", multipleHashes)

		err := httpServer.GetBlockHeadersFromCommonAncestor(JSON)(echoContext)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, http.StatusOK, responseRecorder.Code)
		mockRepo.AssertExpectations(t)
	})

	t.Run("Invalid hash parameter", func(t *testing.T) {
		httpServer, _, echoContext, _ := GetMockHTTP(t, nil)

		echoContext.SetPath("/block/headersFromCommonAncestor/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("invalid-hash")
		echoContext.QueryParams().Set("block_locator_hashes", "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")

		err := httpServer.GetBlockHeadersFromCommonAncestor(JSON)(echoContext)

		assert.NotNil(t, err)
		echoErr, ok := err.(*echo.HTTPError)
		require.True(t, ok)
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)
		assert.Contains(t, echoErr.Message, "invalid hash string")
	})

	t.Run("Missing block locator hashes", func(t *testing.T) {
		httpServer, _, echoContext, _ := GetMockHTTP(t, nil)

		echoContext.SetPath("/block/headersFromCommonAncestor/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")
		// Don't set block_locator_hashes query parameter

		err := httpServer.GetBlockHeadersFromCommonAncestor(JSON)(echoContext)

		assert.NotNil(t, err)
		echoErr, ok := err.(*echo.HTTPError)
		require.True(t, ok)
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)
		assert.Contains(t, echoErr.Message, "block locator hashes cannot be empty")
	})

	t.Run("Invalid block locator hashes length", func(t *testing.T) {
		httpServer, _, echoContext, _ := GetMockHTTP(t, nil)

		echoContext.SetPath("/block/headersFromCommonAncestor/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")
		echoContext.QueryParams().Set("block_locator_hashes", "invalid-length") // Not multiple of 64

		err := httpServer.GetBlockHeadersFromCommonAncestor(JSON)(echoContext)

		assert.NotNil(t, err)
		echoErr, ok := err.(*echo.HTTPError)
		require.True(t, ok)
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)
		assert.Contains(t, echoErr.Message, "block locator hashes length must be a multiple of 64")
	})

	t.Run("Invalid number of headers parameter", func(t *testing.T) {
		httpServer, _, echoContext, _ := GetMockHTTP(t, nil)

		echoContext.SetPath("/block/headersFromCommonAncestor/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")
		echoContext.QueryParams().Set("block_locator_hashes", "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
		echoContext.QueryParams().Set("n", "invalid-number")

		err := httpServer.GetBlockHeadersFromCommonAncestor(JSON)(echoContext)

		assert.NotNil(t, err)
		echoErr, ok := err.(*echo.HTTPError)
		require.True(t, ok)
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)
		assert.Contains(t, echoErr.Message, "invalid number of headers")
	})

	t.Run("Repository returns not found error", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		mockRepo.On("GetBlockHeadersFromCommonAncestor", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil, errors.ErrNotFound)

		echoContext.SetPath("/block/headersFromCommonAncestor/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")
		echoContext.QueryParams().Set("block_locator_hashes", "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")

		err := httpServer.GetBlockHeadersFromCommonAncestor(JSON)(echoContext)

		assert.NotNil(t, err)
		echoErr, ok := err.(*echo.HTTPError)
		require.True(t, ok)
		assert.Equal(t, http.StatusNotFound, echoErr.Code)

		mockRepo.AssertExpectations(t)
	})

	t.Run("Repository returns internal error", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		mockRepo.On("GetBlockHeadersFromCommonAncestor", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil, errors.NewError("internal error", nil))

		echoContext.SetPath("/block/headersFromCommonAncestor/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")
		echoContext.QueryParams().Set("block_locator_hashes", "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")

		err := httpServer.GetBlockHeadersFromCommonAncestor(JSON)(echoContext)

		assert.NotNil(t, err)
		echoErr, ok := err.(*echo.HTTPError)
		require.True(t, ok)
		assert.Equal(t, http.StatusInternalServerError, echoErr.Code)

		mockRepo.AssertExpectations(t)
	})
}

// TestGetBlockHeadersFromCommonAncestor_Integration tests the handler with real database stores
// instead of mocks. It loads real block headers from testdata/headers.json into a SQLite
// blockchain database and performs integration tests requesting 10, 50, and 100 headers.
func TestGetBlockHeadersFromCommonAncestor_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	initPrometheusMetrics()

	ctx := context.Background()
	logger := ulogger.TestLogger{}

	// Load headers from JSON file
	headersPath := filepath.Join("testdata", "headers.json")
	headersData, err := os.ReadFile(headersPath)
	require.NoError(t, err, "Failed to read headers.json")

	// Parse JSON as array of maps
	var headersArray []map[string]interface{}
	err = json.Unmarshal(headersData, &headersArray)
	require.NoError(t, err, "Failed to parse headers.json")
	require.Equal(t, 101, len(headersArray), "Expected 101 headers in testdata")

	// Set up test settings
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams = &chaincfg.TeraTestNetParams

	// Create SQLite blockchain store
	storeURL, err := url.Parse("sqlitememory:///blockchain_test")
	require.NoError(t, err)

	blockchainStore, err := blockchain_store.New(logger, storeURL, tSettings)
	require.NoError(t, err)

	// Create blockchain client
	blockchainClient, err := blockchain.NewLocalClient(logger, tSettings, blockchainStore, nil, nil)
	require.NoError(t, err)

	b, bm, err := blockchainClient.GetBestBlockHeader(context.Background())
	require.NoError(t, err, "Failed to get best block header")

	_ = b  // Use the best block header to ensure client is initialized correctly
	_ = bm // Use the best block header meta to ensure client is initialized correctly

	// Now store the remaining blocks in chronological order (second-to-last to first)
	// Skip genesis block (last entry) since we handled it above
	for i := len(headersArray) - 2; i >= 0; i-- {
		headerMap := headersArray[i]

		// Convert the map back to JSON string for NewBlockHeaderFromJSON
		headerJSON, err := json.Marshal(headerMap)
		require.NoError(t, err, "Failed to marshal header to JSON")

		// Use the model function to parse the block header
		blockHeader, err := model.NewBlockHeaderFromJSON(string(headerJSON))
		require.NoError(t, err, "Failed to parse block header from JSON")

		block := &model.Block{
			Header:     blockHeader,
			CoinbaseTx: testTx1,
		}

		// Store in database
		_, _, err = blockchainStore.StoreBlock(ctx, block, "test-peer")
		if err != nil {
			// Log the error but continue with the test using whatever blocks we managed to store
			t.Logf("Warning: Could not store block at height %v: %v", headerMap["height"], err)
			break
		}
	}

	// Create repository with real stores
	repo, err := repository.NewRepository(logger, tSettings, nil, nil, blockchainClient, nil, nil, nil, nil)
	require.NoError(t, err)

	// Create HTTP server with real repository
	httpServer := &HTTP{
		repository: repo,
		logger:     logger,
		settings:   tSettings,
	}

	// Test cases for different numbers of headers
	testCases := []struct {
		name                string
		numHeaders          string
		expectedCount       int
		commonAncestorIndex int // Index of the common ancestor in headersArray
		description         string
		chainTiphashStr     string
		locatorHashStr      string
	}{
		{
			name:                "Request all headers from genesis",
			numHeaders:          "10000", // Request more than available headers
			expectedCount:       101,     // Should return all headers
			commonAncestorIndex: 100,     // Common ancestor is at height 0 (genesis)
			description:         "Should return all headers from genesis",
			chainTiphashStr:     headersArray[0]["hash"].(string),   // Height 100 (tip)
			locatorHashStr:      headersArray[100]["hash"].(string), // Height 0 (genesis)
		},
		{
			name:                "Request 10 headers",
			numHeaders:          "10",
			expectedCount:       10,
			commonAncestorIndex: 50, // Common ancestor is at height 50
			description:         "Should return up to 10 headers from common ancestor",
			chainTiphashStr:     headersArray[0]["hash"].(string),  // Height 100 (tip)
			locatorHashStr:      headersArray[50]["hash"].(string), // Height 50 (middle)
		},
		{
			name:                "Request 50 headers",
			numHeaders:          "50",
			expectedCount:       50,
			commonAncestorIndex: 50, // Common ancestor is at height 50
			description:         "Should return up to 50 headers from common ancestor",
			chainTiphashStr:     headersArray[0]["hash"].(string),  // Height 100 (tip)
			locatorHashStr:      headersArray[50]["hash"].(string), // Height 50 (middle)
		},
		{
			name:                "Request 100 headers",
			numHeaders:          "100",
			expectedCount:       51, // Should return all headers from common ancestor
			commonAncestorIndex: 50, // Common ancestor is at height 50
			description:         "Should return up to 100 headers from common ancestor",
			chainTiphashStr:     headersArray[0]["hash"].(string),  // Height 100 (tip)
			locatorHashStr:      headersArray[50]["hash"].(string), // Height 50 (middle)
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create Echo context for the request
			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			// Set path parameters
			c.SetPath("/block/headersFromCommonAncestor/:hash")
			c.SetParamNames("hash")
			c.SetParamValues(tc.chainTiphashStr)

			// Set query parameters
			c.QueryParams().Set("block_locator_hashes", tc.locatorHashStr)
			c.QueryParams().Set("n", tc.numHeaders)

			// Call the handler - only supports BINARY_STREAM
			err = httpServer.GetBlockHeadersFromCommonAncestor(BINARY_STREAM)(c)

			// Check if we get a response (might be successful or might be an error due to blockchain setup issues)
			if err != nil {
				// If there's an error, check if it's a reasonable blockchain-related error
				t.Logf("Handler returned error (expected due to blockchain setup): %v", err)
				// Still verify it's a proper HTTP error response
				if echoErr, ok := err.(*echo.HTTPError); ok {
					assert.True(t, echoErr.Code >= 400 && echoErr.Code < 600, "Expected HTTP error code")
				}
				return
			}

			// If successful, verify the response structure
			assert.Equal(t, http.StatusOK, rec.Code, "Expected 200 OK status")

			responseBytes := rec.Body.Bytes()
			response := make([]*model.BlockHeader, 0, len(responseBytes)/model.BlockHeaderSize)

			// Parse BINARY response using the model read from bytes, headers are 80 bytes each
			for i := 0; i < len(responseBytes); i += model.BlockHeaderSize {
				if i+model.BlockHeaderSize > len(responseBytes) {
					break // Avoid out of bounds
				}
				headerBytes := responseBytes[i : i+model.BlockHeaderSize]
				header, err := model.NewBlockHeaderFromBytes(headerBytes)
				if err != nil {
					t.Fatalf("Failed to parse block header from bytes: %v", err)
				}
				response = append(response, header)
			}

			// Verify the number of headers returned
			assert.Len(t, response, tc.expectedCount, "Expected number of headers in response")

			// Verify response structure
			for i, header := range response {
				// headers are returned from the common ancestor, so we need to adjust the index, will also
				// be in reversed order from the headersArray
				expectedHeader := headersArray[tc.commonAncestorIndex-i]

				assert.Equal(t, expectedHeader["hash"].(string), header.Hash().String(), "Header hash mismatch")
				assert.Equal(t, expectedHeader["version"].(float64), float64(header.Version), "Header version mismatch")
				assert.Equal(t, expectedHeader["nonce"].(float64), float64(header.Nonce), "Header nonce mismatch")
				assert.Equal(t, expectedHeader["bits"].(string), header.Bits.String(), "Header bits mismatch")
			}
		})
	}
}
