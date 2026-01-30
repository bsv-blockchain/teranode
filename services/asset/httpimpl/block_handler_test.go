package httpimpl

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestNewBlockHandler tests the NewBlockHandler function
func TestNewBlockHandler(t *testing.T) {
	// Create mock blockchain client
	mockClient := &blockchain.Mock{}
	logger := ulogger.TestLogger{}

	// Call the function to be tested
	handler := NewBlockHandler(mockClient, nil, logger)

	// Assert that the handler is not nil and has the correct properties
	assert.NotNil(t, handler)
	assert.Equal(t, mockClient, handler.blockchainClient)
	assert.Equal(t, logger, handler.logger)
}

// setupBlockHandlerTest creates a test environment for block handler tests
func setupBlockHandlerTest(_ *testing.T, requestBody string) (*BlockHandler, *blockchain.Mock, echo.Context, *httptest.ResponseRecorder) {
	mockBlockchainClient := &blockchain.Mock{}
	mockBlockValidationClient := &blockvalidation.Mock{}
	logger := ulogger.TestLogger{}
	handler := NewBlockHandler(mockBlockchainClient, mockBlockValidationClient, logger)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(requestBody))

	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	return handler, mockBlockchainClient, c, rec
}

// TestHandleBlockOperationWithInvalidJSON tests the handleBlockOperation function with invalid JSON
func TestHandleBlockOperationWithInvalidJSON(t *testing.T) {
	// Setup test with invalid JSON
	const requestBody = `{invalid-json}`
	handler, _, c, _ := setupBlockHandlerTest(t, requestBody)

	// Create a test operation that should not be called
	operation := func(ctx echo.Context, blockHash *chainhash.Hash) error {
		t.Fatal("Operation should not be called")
		return nil
	}

	// Call the function to be tested
	err := handler.handleBlockOperation(c, "test", operation)

	// Assert results
	httpErr, ok := err.(*echo.HTTPError)
	require.True(t, ok)
	assert.Equal(t, http.StatusBadRequest, httpErr.Code)
	assert.Contains(t, httpErr.Message, "Invalid request body")
}

// TestHandleBlockOperation tests the handleBlockOperation function
func TestHandleBlockOperation(t *testing.T) {
	// This hash is used throughout the tests
	const validBlockHash = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f" // Bitcoin genesis block hash

	t.Run("Success case", func(t *testing.T) {
		// Setup test
		requestBody := `{"blockHash": "` + validBlockHash + `"}`
		handler, mockClient, c, rec := setupBlockHandlerTest(t, requestBody)

		// Mock the blockchain client responses
		blockHash, _ := chainhash.NewHashFromStr(validBlockHash)
		mockClient.On("GetBlockExists", mock.Anything, blockHash).Return(true, nil)

		// Create a test operation
		operationCalled := false
		testOperation := func(ctx echo.Context, hash *chainhash.Hash) error {
			operationCalled = true

			assert.Equal(t, blockHash, hash)

			return nil
		}

		// Call the function to be tested
		err := handler.handleBlockOperation(c, "test", testOperation)

		// Assert results
		assert.NoError(t, err)
		assert.True(t, operationCalled)
		assert.Equal(t, http.StatusOK, rec.Code)

		// Verify response
		var response map[string]interface{}
		err = json.Unmarshal(rec.Body.Bytes(), &response)
		require.NoError(t, err)
		assert.True(t, response["success"].(bool))
		assert.Equal(t, "Block test successfully", response["message"])
	})

	t.Run("Invalid request body", func(t *testing.T) {
		// Setup test with invalid JSON
		handler, _, c, _ := setupBlockHandlerTest(t, "{invalid json")

		// Create a test operation
		testOperation := func(ctx echo.Context, hash *chainhash.Hash) error {
			t.Fatal("Operation should not be called")
			return nil
		}

		// Call the function to be tested
		err := handler.handleBlockOperation(c, "test", testOperation)

		// Assert results
		httpErr, ok := err.(*echo.HTTPError)
		require.True(t, ok)
		assert.Equal(t, http.StatusBadRequest, httpErr.Code)
		assert.Contains(t, httpErr.Message, "Invalid request body")
	})

	t.Run("Missing block hash", func(t *testing.T) {
		// Setup test with empty block hash
		handler, _, c, _ := setupBlockHandlerTest(t, `{"blockHash": ""}`)

		// Create a test operation
		testOperation := func(ctx echo.Context, hash *chainhash.Hash) error {
			t.Fatal("Operation should not be called")
			return nil
		}

		// Call the function to be tested
		err := handler.handleBlockOperation(c, "test", testOperation)

		// Assert results
		httpErr, ok := err.(*echo.HTTPError)
		require.True(t, ok)
		assert.Equal(t, http.StatusBadRequest, httpErr.Code)
		assert.Equal(t, "BlockHash is required", httpErr.Message)
	})

	t.Run("Invalid block hash format", func(t *testing.T) {
		// Setup test with invalid block hash format
		handler, _, c, _ := setupBlockHandlerTest(t, `{"blockHash": "invalid-hash"}`)

		// Create a test operation
		testOperation := func(ctx echo.Context, hash *chainhash.Hash) error {
			t.Fatal("Operation should not be called")
			return nil
		}

		// Call the function to be tested
		err := handler.handleBlockOperation(c, "test", testOperation)

		// Assert results
		httpErr, ok := err.(*echo.HTTPError)
		require.True(t, ok)
		assert.Equal(t, http.StatusBadRequest, httpErr.Code)
		assert.Contains(t, httpErr.Message, "Invalid block hash format")
	})

	t.Run("Block does not exist", func(t *testing.T) {
		// Setup test
		requestBody := `{"blockHash": "` + validBlockHash + `"}`
		handler, mockClient, c, _ := setupBlockHandlerTest(t, requestBody)

		// Mock the blockchain client responses
		blockHash, _ := chainhash.NewHashFromStr(validBlockHash)
		mockClient.On("GetBlockExists", mock.Anything, blockHash).Return(false, nil)

		// Create a test operation
		testOperation := func(ctx echo.Context, hash *chainhash.Hash) error {
			t.Fatal("Operation should not be called")
			return nil
		}

		// Call the function to be tested
		err := handler.handleBlockOperation(c, "test", testOperation)

		// Assert results
		httpErr, ok := err.(*echo.HTTPError)
		require.True(t, ok)
		assert.Equal(t, http.StatusNotFound, httpErr.Code)
		assert.Contains(t, httpErr.Message, "Block with hash")
	})

	t.Run("Error checking if block exists", func(t *testing.T) {
		// Setup test
		requestBody := `{"blockHash": "` + validBlockHash + `"}`
		handler, mockClient, c, _ := setupBlockHandlerTest(t, requestBody)

		// Mock the blockchain client responses
		blockHash, _ := chainhash.NewHashFromStr(validBlockHash)
		mockClient.On("GetBlockExists", mock.Anything, blockHash).Return(false, errors.ErrServiceError)

		// Create a test operation
		testOperation := func(ctx echo.Context, hash *chainhash.Hash) error {
			t.Fatal("Operation should not be called")
			return nil
		}

		// Call the function to be tested
		err := handler.handleBlockOperation(c, "test", testOperation)

		// Assert results
		httpErr, ok := err.(*echo.HTTPError)
		require.True(t, ok)
		assert.Equal(t, http.StatusInternalServerError, httpErr.Code)
		assert.Contains(t, httpErr.Message, "Error checking if block exists")
	})

	t.Run("Operation error", func(t *testing.T) {
		// Setup test
		requestBody := `{"blockHash": "` + validBlockHash + `"}`
		handler, mockClient, c, _ := setupBlockHandlerTest(t, requestBody)

		// Mock the blockchain client responses
		blockHash, _ := chainhash.NewHashFromStr(validBlockHash)
		mockClient.On("GetBlockExists", mock.Anything, blockHash).Return(true, nil)

		// Create a test operation that returns an error
		testOperation := func(ctx echo.Context, hash *chainhash.Hash) error {
			return errors.ErrServiceError
		}

		// Call the function to be tested
		err := handler.handleBlockOperation(c, "test", testOperation)

		// Assert results
		httpErr, ok := err.(*echo.HTTPError)
		require.True(t, ok)
		assert.Equal(t, http.StatusInternalServerError, httpErr.Code)
		assert.Contains(t, httpErr.Message, "Failed to test block")
	})
}

// TestInvalidateBlockWithEmptyRequest tests the InvalidateBlock method with an empty request
func TestInvalidateBlockWithEmptyRequest(t *testing.T) {
	// Setup test with an empty request body
	handler, _, c, _ := setupBlockHandlerTest(t, "{}")

	// Call the function to be tested
	err := handler.InvalidateBlock(c)

	// Assert results
	httpErr, ok := err.(*echo.HTTPError)
	require.True(t, ok)
	assert.Equal(t, http.StatusBadRequest, httpErr.Code)
	assert.Contains(t, httpErr.Message, "BlockHash is required")
}

// TestInvalidateBlockWithInvalidHash tests the InvalidateBlock method with an invalid block hash
func TestInvalidateBlockWithInvalidHash(t *testing.T) {
	// Setup test with an invalid block hash
	const requestBody = `{"blockHash": "invalid-hash"}`
	handler, _, c, _ := setupBlockHandlerTest(t, requestBody)

	// Call the function to be tested
	err := handler.InvalidateBlock(c)

	// Assert results
	httpErr, ok := err.(*echo.HTTPError)
	require.True(t, ok)
	assert.Equal(t, http.StatusBadRequest, httpErr.Code)
	assert.Contains(t, httpErr.Message, "Invalid block hash format")
}

// TestInvalidateBlock tests the InvalidateBlock method
func TestInvalidateBlock(t *testing.T) {
	const validBlockHash = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f" // Bitcoin genesis block hash

	t.Run("Success case", func(t *testing.T) {
		// Setup test
		requestBody := `{"blockHash": "` + validBlockHash + `"}`
		handler, mockClient, c, rec := setupBlockHandlerTest(t, requestBody)

		// Mock the blockchain client responses
		blockHash, _ := chainhash.NewHashFromStr(validBlockHash)
		mockClient.On("GetBlockExists", mock.Anything, blockHash).Return(true, nil)
		mockClient.On("InvalidateBlock", mock.Anything, blockHash).Return([]chainhash.Hash{}, nil)

		// Call the function to be tested
		err := handler.InvalidateBlock(c)

		// Assert results
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)

		// Verify response
		var response map[string]interface{}
		err = json.Unmarshal(rec.Body.Bytes(), &response)
		require.NoError(t, err)
		assert.True(t, response["success"].(bool))
		assert.Equal(t, "Block invalidate successfully", response["message"])

		// Verify that the mock was called with the correct arguments
		mockClient.AssertCalled(t, "InvalidateBlock", mock.Anything, blockHash)
	})

	t.Run("Error from InvalidateBlock", func(t *testing.T) {
		// Setup test
		requestBody := `{"blockHash": "` + validBlockHash + `"}`
		handler, mockClient, c, _ := setupBlockHandlerTest(t, requestBody)

		// Mock the blockchain client responses
		blockHash, _ := chainhash.NewHashFromStr(validBlockHash)
		mockClient.On("GetBlockExists", mock.Anything, blockHash).Return(true, nil)
		mockClient.On("InvalidateBlock", mock.Anything, blockHash).Return([]chainhash.Hash{}, errors.ErrServiceError)

		// Call the function to be tested
		err := handler.InvalidateBlock(c)

		// Assert results
		httpErr, ok := err.(*echo.HTTPError)
		require.True(t, ok)
		assert.Equal(t, http.StatusInternalServerError, httpErr.Code)
		assert.Contains(t, httpErr.Message, "Failed to invalidate block")

		// Verify that the mock was called with the correct arguments
		mockClient.AssertCalled(t, "InvalidateBlock", mock.Anything, blockHash)
	})
}

// TestRevalidateBlockWithEmptyRequest tests the RevalidateBlock method with an empty request
func TestRevalidateBlockWithEmptyRequest(t *testing.T) {
	// Setup test with an empty request body
	handler, _, c, _ := setupBlockHandlerTest(t, "{}")

	// Call the function to be tested
	err := handler.RevalidateBlock(c)

	// Assert results
	httpErr, ok := err.(*echo.HTTPError)
	require.True(t, ok)
	assert.Equal(t, http.StatusBadRequest, httpErr.Code)
	assert.Contains(t, httpErr.Message, "BlockHash is required")
}

// TestRevalidateBlockWithInvalidHash tests the RevalidateBlock method with an invalid block hash
func TestRevalidateBlockWithInvalidHash(t *testing.T) {
	// Setup test with an invalid block hash
	const requestBody = `{"blockHash": "invalid-hash"}`
	handler, _, c, _ := setupBlockHandlerTest(t, requestBody)

	// Call the function to be tested
	err := handler.RevalidateBlock(c)

	// Assert results
	httpErr, ok := err.(*echo.HTTPError)
	require.True(t, ok)
	assert.Equal(t, http.StatusBadRequest, httpErr.Code)
	assert.Contains(t, httpErr.Message, "Invalid block hash format")
}

// TestRevalidateBlock tests the RevalidateBlock method
func TestRevalidateBlock(t *testing.T) {
	const validBlockHash = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f" // Bitcoin genesis block hash

	t.Run("Success case", func(t *testing.T) {
		// Setup test
		requestBody := `{"blockHash": "` + validBlockHash + `"}`
		handler, mockClient, c, rec := setupBlockHandlerTest(t, requestBody)

		handler.blockvalidationClient.(*blockvalidation.Mock).On("RevalidateBlock", mock.Anything, mock.Anything).
			Return(nil)

		// Mock the blockchain client responses
		blockHash, _ := chainhash.NewHashFromStr(validBlockHash)
		mockClient.On("GetBlock", mock.Anything, blockHash).Return(&model.Block{}, nil)
		mockClient.On("GetBlockExists", mock.Anything, blockHash).Return(true, nil)
		mockClient.On("RevalidateBlock", mock.Anything, blockHash).Return(nil)

		// Call the function to be tested
		err := handler.RevalidateBlock(c)

		// Assert results
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)

		// Verify response
		var response map[string]interface{}
		err = json.Unmarshal(rec.Body.Bytes(), &response)
		require.NoError(t, err)
		assert.True(t, response["success"].(bool))
		assert.Equal(t, "Block revalidate successfully", response["message"])
	})

	t.Run("Error from RevalidateBlock", func(t *testing.T) {
		// Setup test
		requestBody := `{"blockHash": "` + validBlockHash + `"}`
		handler, mockClient, c, _ := setupBlockHandlerTest(t, requestBody)

		handler.blockvalidationClient.(*blockvalidation.Mock).On("RevalidateBlock", mock.Anything, mock.Anything).
			Return(errors.NewBlockInvalidError("Failed to revalidate block"))

		// Mock the blockchain client responses
		blockHash, _ := chainhash.NewHashFromStr(validBlockHash)
		mockClient.On("GetBlock", mock.Anything, blockHash).Return(&model.Block{}, nil)
		mockClient.On("GetBlockExists", mock.Anything, blockHash).Return(true, nil)
		mockClient.On("RevalidateBlock", mock.Anything, blockHash).Return(errors.ErrServiceError)

		// Call the function to be tested
		err := handler.RevalidateBlock(c)

		// Assert results
		httpErr, ok := err.(*echo.HTTPError)
		require.True(t, ok)
		assert.Equal(t, http.StatusInternalServerError, httpErr.Code)
		assert.Contains(t, httpErr.Message, "Failed to revalidate block")
	})
}

// TestGetLastNInvalidBlocksWithInvalidCount tests the GetLastNInvalidBlocks method with an invalid count parameter
func TestGetLastNInvalidBlocksWithInvalidCount(t *testing.T) {
	// Setup test
	handler, _, c, _ := setupBlockHandlerTest(t, "")

	// Create a request with an invalid count query parameter
	req := httptest.NewRequest(http.MethodGet, "/api/v1/blocks/invalid?count=invalid", nil)
	c.SetRequest(req)

	// Call the function to be tested
	err := handler.GetLastNInvalidBlocks(c)

	// Assert results
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Invalid count parameter")
}

// TestGetLastNInvalidBlocksWithInvalidOffset tests the GetLastNInvalidBlocks method with an invalid offset parameter
func TestGetLastNInvalidBlocksWithInvalidOffset(t *testing.T) {
	// Setup test
	handler, _, c, _ := setupBlockHandlerTest(t, "")

	// Create a request with an invalid offset query parameter
	req := httptest.NewRequest(http.MethodGet, "/api/v1/blocks/invalid?count=5&offset=invalid", nil)
	c.SetRequest(req)

	// Call the function to be tested
	err := handler.GetLastNInvalidBlocks(c)

	// Assert results
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Invalid offset parameter")
}

// TestGetLastNInvalidBlocksWithNegativeCount tests the GetLastNInvalidBlocks method with a negative count parameter
func TestGetLastNInvalidBlocksWithNegativeCount(t *testing.T) {
	// Setup test
	handler, _, c, _ := setupBlockHandlerTest(t, "")

	// Create a request with a negative count query parameter
	req := httptest.NewRequest(http.MethodGet, "/api/v1/blocks/invalid?count=-5", nil)
	c.SetRequest(req)

	// Call the function to be tested
	err := handler.GetLastNInvalidBlocks(c)

	// Assert results
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Count must be a positive number")
}

// TestGetLastNInvalidBlocks tests the GetLastNInvalidBlocks method
func TestGetLastNInvalidBlocks(t *testing.T) {
	// Define test data
	const (
		validBlockHash1 = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
		validBlockHash2 = "00000000839a8e6886ab5951d76f411475428afc90947ee320161bbf18eb6048"
	)

	t.Run("Success case with default count", func(t *testing.T) {
		// Setup test
		handler, mockClient, c, rec := setupBlockHandlerTest(t, "")

		// Create a request with no query parameters (should use default count=10)
		req := httptest.NewRequest(http.MethodGet, "/api/v1/blocks/invalid", nil)
		c.SetRequest(req)

		// Create test data for the mock response
		blockHash1, _ := chainhash.NewHashFromStr(validBlockHash1)
		blockHash2, _ := chainhash.NewHashFromStr(validBlockHash2)

		// Create proper 80-byte block headers
		// A block header consists of: Version(4) + PrevBlock(32) + MerkleRoot(32) + Timestamp(4) + Bits(4) + Nonce(4) = 80 bytes
		blockHeader1 := make([]byte, 80)
		blockHeader2 := make([]byte, 80)

		// Set version (first 4 bytes)
		version := uint32(1)
		blockHeader1[0] = byte(version)
		blockHeader1[1] = byte(version >> 8)
		blockHeader1[2] = byte(version >> 16)
		blockHeader1[3] = byte(version >> 24)
		copy(blockHeader2[0:4], blockHeader1[0:4])

		// Set previous block hash (next 32 bytes)
		// For genesis block, prev hash is all zeros
		// For block 1, prev hash is the hash of genesis block
		copy(blockHeader2[4:36], blockHash1.CloneBytes())

		// Set merkle root (next 32 bytes) - using the block hashes for simplicity
		copy(blockHeader1[36:68], blockHash1.CloneBytes())
		copy(blockHeader2[36:68], blockHash2.CloneBytes())

		// Set timestamp (next 4 bytes)
		timestamp1Unix := uint32(1231006505) // Genesis block timestamp
		timestamp2Unix := uint32(1231469665) // Block 1 timestamp
		blockHeader1[68] = byte(timestamp1Unix)
		blockHeader1[69] = byte(timestamp1Unix >> 8)
		blockHeader1[70] = byte(timestamp1Unix >> 16)
		blockHeader1[71] = byte(timestamp1Unix >> 24)
		blockHeader2[68] = byte(timestamp2Unix)
		blockHeader2[69] = byte(timestamp2Unix >> 8)
		blockHeader2[70] = byte(timestamp2Unix >> 16)
		blockHeader2[71] = byte(timestamp2Unix >> 24)

		// Set bits/difficulty (next 4 bytes)
		bits := uint32(0x1d00ffff) // Initial difficulty
		blockHeader1[72] = byte(bits)
		blockHeader1[73] = byte(bits >> 8)
		blockHeader1[74] = byte(bits >> 16)
		blockHeader1[75] = byte(bits >> 24)
		copy(blockHeader2[72:76], blockHeader1[72:76])

		// Set nonce (last 4 bytes)
		nonce1 := uint32(2083236893) // Genesis block nonce
		nonce2 := uint32(2573394689) // Block 1 nonce
		blockHeader1[76] = byte(nonce1)
		blockHeader1[77] = byte(nonce1 >> 8)
		blockHeader1[78] = byte(nonce1 >> 16)
		blockHeader1[79] = byte(nonce1 >> 24)
		blockHeader2[76] = byte(nonce2)
		blockHeader2[77] = byte(nonce2 >> 8)
		blockHeader2[78] = byte(nonce2 >> 16)
		blockHeader2[79] = byte(nonce2 >> 24)

		// Create timestamp for SeenAt
		timestamp1 := timestamppb.New(time.Unix(1231006505, 0))
		timestamp2 := timestamppb.New(time.Unix(1231469665, 0))

		mockBlocks := []*model.BlockInfo{
			{BlockHeader: blockHeader1, Height: 0, SeenAt: timestamp1, TransactionCount: 1},
			{BlockHeader: blockHeader2, Height: 1, SeenAt: timestamp2, TransactionCount: 2},
		}

		// Mock the blockchain client response
		mockClient.On("GetLastNInvalidBlocks", mock.Anything, int64(11)).Return(mockBlocks, nil)

		// Call the function to be tested
		err := handler.GetLastNInvalidBlocks(c)

		// Assert results
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)

		// Verify response
		var response map[string]interface{}
		err = json.Unmarshal(rec.Body.Bytes(), &response)
		require.NoError(t, err)
		assert.True(t, response["success"].(bool))
		assert.Equal(t, float64(2), response["count"])
		assert.Len(t, response["blocks"], 2)

		// Verify that the mock was called with the correct arguments
		mockClient.AssertCalled(t, "GetLastNInvalidBlocks", mock.Anything, int64(11))
	})

	t.Run("Success case with custom count", func(t *testing.T) {
		// Setup test
		handler, mockClient, c, rec := setupBlockHandlerTest(t, "")

		// Create a request with count=5 query parameter
		req := httptest.NewRequest(http.MethodGet, "/api/v1/blocks/invalid?count=5", nil)
		c.SetRequest(req)
		// Set query parameter
		q := req.URL.Query()
		q.Add("count", "5")
		req.URL.RawQuery = q.Encode()

		// Create test data for the mock response
		blockHash1, _ := chainhash.NewHashFromStr(validBlockHash1)

		// Create proper 80-byte block header
		// A block header consists of: Version(4) + PrevBlock(32) + MerkleRoot(32) + Timestamp(4) + Bits(4) + Nonce(4) = 80 bytes
		blockHeader1 := make([]byte, 80)

		// Set version (first 4 bytes)
		version := uint32(1)
		blockHeader1[0] = byte(version)
		blockHeader1[1] = byte(version >> 8)
		blockHeader1[2] = byte(version >> 16)
		blockHeader1[3] = byte(version >> 24)

		// Set merkle root (next 32 bytes) - using the block hash for simplicity
		copy(blockHeader1[36:68], blockHash1.CloneBytes())

		// Set timestamp (next 4 bytes)
		timestamp1Unix := uint32(1231006505) // Genesis block timestamp
		blockHeader1[68] = byte(timestamp1Unix)
		blockHeader1[69] = byte(timestamp1Unix >> 8)
		blockHeader1[70] = byte(timestamp1Unix >> 16)
		blockHeader1[71] = byte(timestamp1Unix >> 24)

		// Set bits/difficulty (next 4 bytes)
		bits := uint32(0x1d00ffff) // Initial difficulty
		blockHeader1[72] = byte(bits)
		blockHeader1[73] = byte(bits >> 8)
		blockHeader1[74] = byte(bits >> 16)
		blockHeader1[75] = byte(bits >> 24)

		// Set nonce (last 4 bytes)
		nonce1 := uint32(2083236893) // Genesis block nonce
		blockHeader1[76] = byte(nonce1)
		blockHeader1[77] = byte(nonce1 >> 8)
		blockHeader1[78] = byte(nonce1 >> 16)
		blockHeader1[79] = byte(nonce1 >> 24)

		// Create timestamp for SeenAt
		timestamp1 := timestamppb.New(time.Unix(1231006505, 0))

		mockBlocks := []*model.BlockInfo{
			{BlockHeader: blockHeader1, Height: 0, SeenAt: timestamp1, TransactionCount: 1},
		}

		// Mock the blockchain client response
		mockClient.On("GetLastNInvalidBlocks", mock.Anything, int64(6)).Return(mockBlocks, nil)

		// Call the function to be tested
		err := handler.GetLastNInvalidBlocks(c)

		// Assert results
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)

		// Verify response
		var response map[string]interface{}
		err = json.Unmarshal(rec.Body.Bytes(), &response)
		require.NoError(t, err)
		assert.True(t, response["success"].(bool))
		assert.Equal(t, float64(1), response["count"])
		assert.Len(t, response["blocks"], 1)

		// Verify that the mock was called with the correct arguments
		mockClient.AssertCalled(t, "GetLastNInvalidBlocks", mock.Anything, int64(6))
	})

	t.Run("Success case with offset pagination", func(t *testing.T) {
		// Setup test
		handler, mockClient, c, rec := setupBlockHandlerTest(t, "")

		// Request page 2 (offset=5, count=5)
		req := httptest.NewRequest(http.MethodGet, "/api/v1/blocks/invalid?count=5&offset=5", nil)
		c.SetRequest(req)
		q := req.URL.Query()
		q.Add("count", "5")
		q.Add("offset", "5")
		req.URL.RawQuery = q.Encode()

		// Create test data for the mock response
		blockHash1, _ := chainhash.NewHashFromStr(validBlockHash1)
		blockHeader1 := make([]byte, 80)
		version := uint32(1)
		blockHeader1[0] = byte(version)
		blockHeader1[1] = byte(version >> 8)
		blockHeader1[2] = byte(version >> 16)
		blockHeader1[3] = byte(version >> 24)
		copy(blockHeader1[36:68], blockHash1.CloneBytes())
		timestamp1Unix := uint32(1231006505)
		blockHeader1[68] = byte(timestamp1Unix)
		blockHeader1[69] = byte(timestamp1Unix >> 8)
		blockHeader1[70] = byte(timestamp1Unix >> 16)
		blockHeader1[71] = byte(timestamp1Unix >> 24)
		bits := uint32(0x1d00ffff)
		blockHeader1[72] = byte(bits)
		blockHeader1[73] = byte(bits >> 8)
		blockHeader1[74] = byte(bits >> 16)
		blockHeader1[75] = byte(bits >> 24)
		nonce1 := uint32(2083236893)
		blockHeader1[76] = byte(nonce1)
		blockHeader1[77] = byte(nonce1 >> 8)
		blockHeader1[78] = byte(nonce1 >> 16)
		blockHeader1[79] = byte(nonce1 >> 24)

		timestamp1 := timestamppb.New(time.Unix(1231006505, 0))
		mockBlocks := make([]*model.BlockInfo, 0, 11)
		for i := 0; i < 11; i++ {
			mockBlocks = append(mockBlocks, &model.BlockInfo{BlockHeader: blockHeader1, Height: uint32(i), SeenAt: timestamp1, TransactionCount: 1})
		}

		// Handler will fetch offset+count+1 = 11
		mockClient.On("GetLastNInvalidBlocks", mock.Anything, int64(11)).Return(mockBlocks, nil)

		// Call the function to be tested
		err := handler.GetLastNInvalidBlocks(c)

		// Assert results
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)

		var response map[string]interface{}
		err = json.Unmarshal(rec.Body.Bytes(), &response)
		require.NoError(t, err)
		assert.True(t, response["success"].(bool))
		assert.Equal(t, float64(5), response["count"])
		assert.Equal(t, float64(5), response["offset"])
		assert.Equal(t, float64(5), response["limit"])
		assert.Equal(t, true, response["hasMore"].(bool))
		assert.Len(t, response["blocks"], 5)

		mockClient.AssertCalled(t, "GetLastNInvalidBlocks", mock.Anything, int64(11))
	})

	t.Run("Invalid count parameter", func(t *testing.T) {
		// Setup test
		handler, _, c, _ := setupBlockHandlerTest(t, "")

		// Create a request with invalid count parameter
		req := httptest.NewRequest(http.MethodGet, "/api/v1/blocks/invalid?count=invalid", nil)
		c.SetRequest(req)
		// Set query parameter
		q := req.URL.Query()
		q.Add("count", "invalid")
		req.URL.RawQuery = q.Encode()

		// Call the function to be tested
		err := handler.GetLastNInvalidBlocks(c)

		// Assert results
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Invalid count parameter")
	})

	t.Run("Negative count parameter", func(t *testing.T) {
		// Setup test
		handler, _, c, _ := setupBlockHandlerTest(t, "")

		// Create a request with negative count parameter
		req := httptest.NewRequest(http.MethodGet, "/api/v1/blocks/invalid?count=-5", nil)
		c.SetRequest(req)
		// Set query parameter
		q := req.URL.Query()
		q.Add("count", "-5")
		req.URL.RawQuery = q.Encode()

		// Call the function to be tested
		err := handler.GetLastNInvalidBlocks(c)

		// Assert results
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Count must be a positive number")
	})

	t.Run("Error from GetLastNInvalidBlocks", func(t *testing.T) {
		// Setup test
		handler, mockClient, c, _ := setupBlockHandlerTest(t, "")

		// Create a request with default count
		req := httptest.NewRequest(http.MethodGet, "/api/v1/blocks/invalid", nil)
		c.SetRequest(req)

		// Mock the blockchain client response with error
		mockClient.On("GetLastNInvalidBlocks", mock.Anything, int64(11)).Return(nil, errors.ErrServiceError)

		// Call the function to be tested
		err := handler.GetLastNInvalidBlocks(c)

		// Assert results
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Failed to retrieve invalid blocks")

		// Verify that the mock was called with the correct arguments
		mockClient.AssertCalled(t, "GetLastNInvalidBlocks", mock.Anything, int64(11))
	})
}
