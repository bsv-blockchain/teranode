package httpimpl

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

var (
	transactionMeta = &meta.Data{
		Tx:          nil,
		TxInpoints:  subtree.TxInpoints{ParentTxHashes: []chainhash.Hash{*testBlockHeader.Hash()}, Idxs: [][]uint32{{1}}},
		BlockIDs:    []uint32{1, 2, 3},
		SubtreeIdxs: []int{0, 0, 0}, // Add subtree indices
		Fee:         123,
		SizeInBytes: 321,
		IsCoinbase:  false,
		LockTime:    500000,
	}
)

func TestGetTransactionMeta(t *testing.T) {
	initPrometheusMetrics()

	t.Run("JSON success", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetTxMeta", mock.Anything, mock.Anything).Return(transactionMeta, nil)

		// Mock GetBlockByID calls for each block ID
		// Create blocks with proper headers and subtrees
		subtreeHash1, _ := chainhash.NewHashFromStr("1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
		block1 := &model.Block{
			Header: &model.BlockHeader{
				Version:        testBlockHeader.Version,
				HashPrevBlock:  testBlockHeader.HashPrevBlock,
				HashMerkleRoot: testBlockHeader.HashMerkleRoot,
				Timestamp:      testBlockHeader.Timestamp,
				Bits:           testBlockHeader.Bits,
				Nonce:          testBlockHeader.Nonce,
			},
			Subtrees: []*chainhash.Hash{subtreeHash1}, // Add a subtree to match subtreeIdxs
		}
		mockRepo.On("GetBlockByID", uint64(1)).Return(block1, nil)
		mockRepo.On("GetBlockByID", uint64(2)).Return(block1, nil)
		mockRepo.On("GetBlockByID", uint64(3)).Return(block1, nil)

		// set echo context
		echoContext.SetPath("/tx/meta/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

		// Call GetTransactionMeta handler
		err := httpServer.GetTransactionMeta(JSON)(echoContext)
		if err != nil {
			t.Fatal(err)
		}

		// Check response status code
		assert.Equal(t, http.StatusOK, responseRecorder.Code)

		// Check response body
		var response map[string]interface{}
		if err = json.Unmarshal(responseRecorder.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}

		parentTxHashes := response["parentTxHashes"].([]interface{})

		// Check response fields
		require.NotNil(t, response)
		assert.Nil(t, response["tx"])
		assert.Equal(t, []interface{}{"9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995"}, parentTxHashes)
		assert.Equal(t, []interface{}{float64(1), float64(2), float64(3)}, response["blockIDs"])

		// Verify blockHashes were added and they match the expected hash
		blockHashes := response["blockHashes"].([]interface{})
		assert.Equal(t, 3, len(blockHashes))
		expectedHash := block1.Header.Hash().String()
		assert.Equal(t, []interface{}{expectedHash, expectedHash, expectedHash}, blockHashes)

		// Verify subtreeHashes were added
		subtreeHashes := response["subtreeHashes"].([]interface{})
		assert.Equal(t, 3, len(subtreeHashes))
		expectedSubtreeHash := subtreeHash1.String()
		assert.Equal(t, []interface{}{expectedSubtreeHash, expectedSubtreeHash, expectedSubtreeHash}, subtreeHashes)

		assert.Equal(t, float64(123), response["fee"])
		assert.Equal(t, float64(321), response["sizeInBytes"])
		assert.Equal(t, false, response["isCoinbase"])
		assert.Equal(t, float64(500000), response["lockTime"])
	})

	t.Run("Invalid hash length", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetTxMeta", mock.Anything, mock.Anything).Return(transactionMeta, nil)

		// set echo context
		echoContext.SetPath("/tx/meta/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("invalid")

		// Call GetTransactionMeta handler
		err := httpServer.GetTransactionMeta(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)

		// Check response body
		assert.Equal(t, "INVALID_ARGUMENT (1): invalid hash length", echoErr.Message)
	})

	t.Run("Invalid hash character", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetTxMeta", mock.Anything, mock.Anything).Return(transactionMeta, nil)

		// set echo context
		echoContext.SetPath("/tx/meta/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("sd45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea99y")

		// Call GetTransactionMeta handler
		err := httpServer.GetTransactionMeta(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)

		// Check response body
		assert.Equal(t, "INVALID_ARGUMENT (1): invalid hash string", echoErr.Message)
	})

	t.Run("Repository error", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetTxMeta", mock.Anything, mock.Anything).Return(nil, errors.NewProcessingError("error getting transaction meta"))

		// set echo context
		echoContext.SetPath("/tx/meta/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

		// Call GetTransactionMeta handler
		err := httpServer.GetTransactionMeta(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusInternalServerError, echoErr.Code)

		// Check response body
		assert.Equal(t, "PROCESSING (4): error getting transaction meta", echoErr.Message)
	})

	t.Run("Repository not found", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetTxMeta", mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundError("transaction meta not found"))

		// set echo context
		echoContext.SetPath("/tx/meta/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

		// Call GetTransactionMeta handler
		err := httpServer.GetTransactionMeta(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusNotFound, echoErr.Code)

		// Check response body
		assert.Equal(t, "NOT_FOUND (3): transaction meta not found", echoErr.Message)
	})

	t.Run("Invalid read mode", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetTxMeta", mock.Anything, mock.Anything).Return(transactionMeta, nil)

		// Mock GetBlockByID calls - they'll still be called before the read mode check
		subtreeHash1, _ := chainhash.NewHashFromStr("1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
		block1 := &model.Block{
			Header: &model.BlockHeader{
				Version:        testBlockHeader.Version,
				HashPrevBlock:  testBlockHeader.HashPrevBlock,
				HashMerkleRoot: testBlockHeader.HashMerkleRoot,
				Timestamp:      testBlockHeader.Timestamp,
				Bits:           testBlockHeader.Bits,
				Nonce:          testBlockHeader.Nonce,
			},
			Subtrees: []*chainhash.Hash{subtreeHash1},
		}
		mockRepo.On("GetBlockByID", uint64(1)).Return(block1, nil)
		mockRepo.On("GetBlockByID", uint64(2)).Return(block1, nil)
		mockRepo.On("GetBlockByID", uint64(3)).Return(block1, nil)

		// set echo context
		echoContext.SetPath("/tx/meta/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

		var (
			invalidReadMode ReadMode = 999
		)

		// Call GetTransactionMeta handler
		err := httpServer.GetTransactionMeta(invalidReadMode)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)

		// Check response body
		assert.Equal(t, "INVALID_ARGUMENT (1): bad read mode", echoErr.Message)
	})
}
