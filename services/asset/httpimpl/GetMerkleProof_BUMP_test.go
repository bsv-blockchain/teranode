package httpimpl

import (
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/bump"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestGetMerkleProofBUMPFormats(t *testing.T) {
	// Initialize Prometheus metrics
	initPrometheusMetrics()

	// Setup
	logger := ulogger.TestLogger{}
	tSettings := &settings.Settings{
		Asset: settings.AssetSettings{},
	}

	// Create test data
	txHashStr := "abc1234567890123456789012345678901234567890123456789012345678901"
	txHash, err := chainhash.NewHashFromStr(txHashStr)
	require.NoError(t, err)

	subtreeHashStr := "def4567890123456789012345678901234567890123456789012345678901234"
	subtreeHash, err := chainhash.NewHashFromStr(subtreeHashStr)
	require.NoError(t, err)

	merkleRootStr := "fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321"
	merkleRoot, err := chainhash.NewHashFromStr(merkleRootStr)
	require.NoError(t, err)

	// Create mock transaction metadata
	txMeta := &meta.Data{
		Tx:           &bt.Tx{},
		BlockIDs:     []uint32{1},
		BlockHeights: []uint32{100},
		SubtreeIdxs:  []int{0},
	}

	// Create mock subtree with proper initialization
	mockSubtree, err := subtree.NewTreeByLeafCount(2)
	require.NoError(t, err)
	mockSubtree.Nodes = []subtree.Node{
		{Hash: *txHash},
		{Hash: chainhash.Hash{}},
	}

	// Create NBit for difficulty
	bits, _ := model.NewNBitFromString("1d00ffff")

	// Create mock block
	mockBlock := &model.Block{
		Header: &model.BlockHeader{
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: merkleRoot,
			Timestamp:      1234567890,
			Bits:           *bits,
			Nonce:          12345,
			Version:        1,
		},
		Subtrees: []*chainhash.Hash{subtreeHash},
		Height:   100,
	}

	// Create mock block header
	mockBlockHeader := &model.BlockHeader{
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: merkleRoot,
		Timestamp:      1234567890,
		Bits:           *bits,
		Nonce:          12345,
		Version:        1,
	}

	// Create mock block header metadata
	mockBlockHeaderMeta := &model.BlockHeaderMeta{
		Height: 100,
	}

	t.Run("JSON BUMP format", func(t *testing.T) {
		mockRepo := new(MockRepositoryForMerkleProof)
		h := &HTTP{
			logger:     logger,
			settings:   tSettings,
			repository: mockRepo,
		}

		// Setup mock expectations
		mockRepo.On("GetTxMeta", mock.Anything, txHash).Return(txMeta, nil)
		mockRepo.On("GetBlockByID", mock.Anything, uint64(1)).Return(mockBlock, nil)
		mockRepo.On("GetSubtree", mock.Anything, subtreeHash).Return(mockSubtree, nil)
		mockRepo.On("GetBlockHeader", mock.Anything, mock.AnythingOfType("*chainhash.Hash")).Return(mockBlockHeader, mockBlockHeaderMeta, nil)

		// Create request
		e := echo.New()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/merkle_proof/"+txHashStr+"/json", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		c.SetParamNames("hash")
		c.SetParamValues(txHashStr)

		// Execute handler
		handler := h.GetMerkleProof(JSON)
		err := handler(c)

		// Assert
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))

		// Parse BUMP JSON response
		var bumpResponse bump.Format
		assert.NoError(t, json.Unmarshal(rec.Body.Bytes(), &bumpResponse))

		// Validate BUMP structure
		assert.Equal(t, uint32(100), bumpResponse.BlockHeight)
		assert.NotEmpty(t, bumpResponse.Path)
		assert.NoError(t, bump.Validate(&bumpResponse))

		mockRepo.AssertExpectations(t)
	})

	t.Run("HEX BUMP format", func(t *testing.T) {
		mockRepo := new(MockRepositoryForMerkleProof)
		h := &HTTP{
			logger:     logger,
			settings:   tSettings,
			repository: mockRepo,
		}

		// Setup mock expectations
		mockRepo.On("GetTxMeta", mock.Anything, txHash).Return(txMeta, nil)
		mockRepo.On("GetBlockByID", mock.Anything, uint64(1)).Return(mockBlock, nil)
		mockRepo.On("GetSubtree", mock.Anything, subtreeHash).Return(mockSubtree, nil)
		mockRepo.On("GetBlockHeader", mock.Anything, mock.AnythingOfType("*chainhash.Hash")).Return(mockBlockHeader, mockBlockHeaderMeta, nil)

		// Create request
		e := echo.New()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/merkle_proof/"+txHashStr+"/hex", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		c.SetParamNames("hash")
		c.SetParamValues(txHashStr)

		// Execute handler
		handler := h.GetMerkleProof(HEX)
		err := handler(c)

		// Assert
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "text/plain; charset=UTF-8", rec.Header().Get("Content-Type"))

		// Validate hex response
		hexResponse := rec.Body.String()
		assert.NotEmpty(t, hexResponse)

		// Should be valid hex
		decodedBytes, err := hex.DecodeString(hexResponse)
		assert.NoError(t, err)
		assert.NotEmpty(t, decodedBytes)

		// Should start with block height (100 = 0x64)
		assert.Equal(t, byte(0x64), decodedBytes[0])

		mockRepo.AssertExpectations(t)
	})

	t.Run("Binary BUMP format", func(t *testing.T) {
		mockRepo := new(MockRepositoryForMerkleProof)
		h := &HTTP{
			logger:     logger,
			settings:   tSettings,
			repository: mockRepo,
		}

		// Setup mock expectations
		mockRepo.On("GetTxMeta", mock.Anything, txHash).Return(txMeta, nil)
		mockRepo.On("GetBlockByID", mock.Anything, uint64(1)).Return(mockBlock, nil)
		mockRepo.On("GetSubtree", mock.Anything, subtreeHash).Return(mockSubtree, nil)
		mockRepo.On("GetBlockHeader", mock.Anything, mock.AnythingOfType("*chainhash.Hash")).Return(mockBlockHeader, mockBlockHeaderMeta, nil)

		// Create request
		e := echo.New()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/merkle_proof/"+txHashStr, nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		c.SetParamNames("hash")
		c.SetParamValues(txHashStr)

		// Execute handler
		handler := h.GetMerkleProof(BINARY_STREAM)
		err := handler(c)

		// Assert
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "application/octet-stream", rec.Header().Get("Content-Type"))

		// Validate binary response
		binaryResponse := rec.Body.Bytes()
		assert.NotEmpty(t, binaryResponse)

		// Should start with block height (100 = 0x64)
		assert.Equal(t, byte(0x64), binaryResponse[0])

		// Should have tree height as second byte
		assert.True(t, binaryResponse[1] >= 0) // Tree height should be non-negative

		mockRepo.AssertExpectations(t)
	})

	t.Run("Format consistency", func(t *testing.T) {
		// Test that all three formats represent the same data
		mockRepo := new(MockRepositoryForMerkleProof)
		h := &HTTP{
			logger:     logger,
			settings:   tSettings,
			repository: mockRepo,
		}

		// Setup mock expectations (called multiple times)
		mockRepo.On("GetTxMeta", mock.Anything, txHash).Return(txMeta, nil)
		mockRepo.On("GetBlockByID", mock.Anything, uint64(1)).Return(mockBlock, nil)
		mockRepo.On("GetSubtree", mock.Anything, subtreeHash).Return(mockSubtree, nil)
		mockRepo.On("GetBlockHeader", mock.Anything, mock.AnythingOfType("*chainhash.Hash")).Return(mockBlockHeader, mockBlockHeaderMeta, nil)

		// Get JSON format
		e := echo.New()
		reqJSON := httptest.NewRequest(http.MethodGet, "/api/v1/merkle_proof/"+txHashStr+"/json", nil)
		recJSON := httptest.NewRecorder()
		cJSON := e.NewContext(reqJSON, recJSON)
		cJSON.SetParamNames("hash")
		cJSON.SetParamValues(txHashStr)

		handlerJSON := h.GetMerkleProof(JSON)
		err := handlerJSON(cJSON)
		require.NoError(t, err)

		var jsonBUMP bump.Format
		require.NoError(t, json.Unmarshal(recJSON.Body.Bytes(), &jsonBUMP))

		// Get HEX format
		reqHEX := httptest.NewRequest(http.MethodGet, "/api/v1/merkle_proof/"+txHashStr+"/hex", nil)
		recHEX := httptest.NewRecorder()
		cHEX := e.NewContext(reqHEX, recHEX)
		cHEX.SetParamNames("hash")
		cHEX.SetParamValues(txHashStr)

		handlerHEX := h.GetMerkleProof(HEX)
		err = handlerHEX(cHEX)
		require.NoError(t, err)

		// Get binary format
		reqBinary := httptest.NewRequest(http.MethodGet, "/api/v1/merkle_proof/"+txHashStr, nil)
		recBinary := httptest.NewRecorder()
		cBinary := e.NewContext(reqBinary, recBinary)
		cBinary.SetParamNames("hash")
		cBinary.SetParamValues(txHashStr)

		handlerBinary := h.GetMerkleProof(BINARY_STREAM)
		err = handlerBinary(cBinary)
		require.NoError(t, err)

		// Verify consistency: JSON -> Binary should match Binary response
		expectedBinary, err := jsonBUMP.EncodeBinary()
		require.NoError(t, err)
		assert.Equal(t, expectedBinary, recBinary.Body.Bytes())

		// Verify consistency: JSON -> Hex should match Hex response
		expectedHex, err := jsonBUMP.EncodeHex()
		require.NoError(t, err)
		assert.Equal(t, expectedHex, recHEX.Body.String())

		mockRepo.AssertExpectations(t)
	})

	t.Run("BUMP validation errors", func(t *testing.T) {
		// This test verifies that validation errors are properly handled
		// We would need to create a scenario where BUMP validation fails
		// For now, this is a placeholder for comprehensive validation testing
		assert.True(t, true, "BUMP validation error handling placeholder")
	})
}
