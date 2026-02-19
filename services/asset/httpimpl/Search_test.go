package httpimpl

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestSearch(t *testing.T) {
	initPrometheusMetrics()

	t.Run("Search by block hash success", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetBlockHeader", mock.Anything, mock.Anything).Return(testBlockHeader, testBlockHeaderMeta, nil)

		// set echo context
		echoContext.SetPath("/search")
		echoContext.QueryParams().Set("q", "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")

		// Call Search handler
		err := httpServer.Search(echoContext)
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

		// Check response fields
		require.NotNil(t, response)
		assert.Equal(t, "block", response["type"])
		assert.Equal(t, "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f", response["hash"])
	})

	t.Run("Search by block height success", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetBlockByHeight", mock.Anything, mock.Anything).Return(testBlock, nil)

		// set echo context
		echoContext.SetPath("/search")
		echoContext.QueryParams().Set("q", "1000")

		// Call Search handler
		err := httpServer.Search(echoContext)
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

		// Check response fields
		require.NotNil(t, response)
		assert.Equal(t, "block", response["type"])
		assert.Equal(t, testBlock.Hash().String(), response["hash"])
	})

	t.Run("Search missing query parameter", func(t *testing.T) {
		httpServer, _, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// set echo context
		echoContext.SetPath("/search")

		// Call Search handler
		err := httpServer.Search(echoContext)
		require.NoError(t, err)

		// Check response status code
		assert.Equal(t, http.StatusBadRequest, echoContext.Response().Status)

		// Check response body
		response := responseRecorder.Body.Bytes()

		var responseJSON map[string]interface{}
		err = json.Unmarshal(response, &responseJSON)
		require.NoError(t, err)

		assert.Equal(t, float64(400), responseJSON["status"])
		assert.Equal(t, float64(1), responseJSON["code"])
		assert.Equal(t, "INVALID_ARGUMENT (1): missing query parameter", responseJSON["error"])
	})

	t.Run("Search invalid hash", func(t *testing.T) {
		httpServer, _, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// set echo context
		echoContext.SetPath("/search")
		echoContext.QueryParams().Set("q", "invalidhash")

		// Call Search handler
		err := httpServer.Search(echoContext)
		require.NoError(t, err)

		// Check response status code
		assert.Equal(t, http.StatusBadRequest, echoContext.Response().Status)

		// Check response body
		response := responseRecorder.Body.Bytes()

		var responseJSON map[string]interface{}
		err = json.Unmarshal(response, &responseJSON)
		require.NoError(t, err)

		// Check response fields
		assert.Equal(t, float64(400), responseJSON["status"])
		assert.Equal(t, float64(1), responseJSON["code"])
		assert.Equal(t, "INVALID_ARGUMENT (1): query must be a valid hash or block height", responseJSON["error"])
	})

	t.Run("Search invalid hash format", func(t *testing.T) {
		httpServer, _, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// set echo context
		echoContext.SetPath("/search")
		echoContext.QueryParams().Set("q", "s00000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26t")

		// Call Search handler
		err := httpServer.Search(echoContext)
		require.NoError(t, err)

		// Check response status code
		assert.Equal(t, http.StatusBadRequest, echoContext.Response().Status)

		// Check response body
		response := responseRecorder.Body.Bytes()

		var responseJSON map[string]interface{}
		err = json.Unmarshal(response, &responseJSON)
		require.NoError(t, err)

		// Check response fields
		assert.Equal(t, float64(400), responseJSON["status"])
		assert.Equal(t, float64(1), responseJSON["code"])
		assert.Equal(t, "INVALID_ARGUMENT (1): error reading hash", responseJSON["error"])
	})

	t.Run("Search nothing found", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetBlockHeader", mock.Anything, mock.Anything).Return(nil, nil, errors.NewBlockNotFoundError("block header not found"))
		mockRepo.On("GetTransactionMeta", mock.Anything, mock.Anything).Return(nil, errors.NewTxNotFoundError("transaction not found"))
		mockRepo.On("GetSubtreeExists", mock.Anything, mock.Anything).Return(false, errors.NewNotFoundError("subtree does not exist"))
		mockRepo.On("GetUtxo", mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundError("utxo does not found"))

		// set echo context
		echoContext.SetPath("/search")
		echoContext.QueryParams().Set("q", "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")

		// Call Search handler - errors from search return as a 400 OK with the error in the body
		err := httpServer.Search(echoContext)
		require.NoError(t, err)

		// Check response status code
		assert.Equal(t, http.StatusNotFound, echoContext.Response().Status)

		// Check response body
		response := responseRecorder.Body.Bytes()

		var responseJSON map[string]interface{}
		err = json.Unmarshal(response, &responseJSON)
		require.NoError(t, err)

		assert.Equal(t, float64(404), responseJSON["status"])
		assert.Equal(t, float64(3), responseJSON["code"])
		assert.Equal(t, "NOT_FOUND (3): no matching entity found", responseJSON["error"])
	})

	t.Run("Search invalid query format", func(t *testing.T) {
		httpServer, _, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// set echo context
		echoContext.SetPath("/search")
		echoContext.QueryParams().Set("q", "-1")

		// Call Search handler
		err := httpServer.Search(echoContext)
		require.NoError(t, err)

		// Check response status code
		assert.Equal(t, http.StatusBadRequest, echoContext.Response().Status)

		// Check response body
		response := responseRecorder.Body.Bytes()

		var responseJSON map[string]interface{}
		err = json.Unmarshal(response, &responseJSON)
		require.NoError(t, err)

		// Check response fields
		assert.Equal(t, float64(400), responseJSON["status"])
		assert.Equal(t, float64(1), responseJSON["code"])
		assert.Equal(t, "INVALID_ARGUMENT (1): block height must be greater than or equal to 0", responseJSON["error"])
	})

	t.Run("Search find transaction", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetBlockHeader", mock.Anything, mock.Anything).Return(nil, nil, errors.NewBlockNotFoundError("block header not found"))
		mockRepo.On("GetTransactionMeta", mock.Anything, mock.Anything).Return(testTxMeta, nil)

		// set echo context
		echoContext.SetPath("/search")
		echoContext.QueryParams().Set("q", "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")

		// Call Search handler
		err := httpServer.Search(echoContext)
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

		// Check response fields
		require.NotNil(t, response)
		assert.Equal(t, "tx", response["type"])
		assert.Equal(t, "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f", response["hash"])
	})

	t.Run("Search find subtree", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetBlockHeader", mock.Anything, mock.Anything).Return(nil, nil, errors.NewBlockNotFoundError("block header not found"))
		mockRepo.On("GetTransactionMeta", mock.Anything, mock.Anything).Return(nil, errors.NewTxNotFoundError("transaction not found"))
		mockRepo.On("GetSubtreeExists", mock.Anything, mock.Anything).Return(true, nil)

		// set echo context
		echoContext.SetPath("/search")
		echoContext.QueryParams().Set("q", "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")

		// Call Search handler
		err := httpServer.Search(echoContext)
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

		// Check response fields
		require.NotNil(t, response)
		assert.Equal(t, "subtree", response["type"])
		assert.Equal(t, "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f", response["hash"])
	})

	t.Run("Search utxo not supported", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// set mock response - UTXO search is no longer supported
		mockRepo.On("GetBlockHeader", mock.Anything, mock.Anything).Return(nil, nil, errors.NewBlockNotFoundError("block header not found"))
		mockRepo.On("GetTransactionMeta", mock.Anything, mock.Anything).Return(nil, errors.NewTxNotFoundError("transaction not found"))
		mockRepo.On("GetSubtreeExists", mock.Anything, mock.Anything).Return(false, errors.NewNotFoundError("subtree does not exist"))
		// GetUtxo should not be called anymore

		// set echo context
		echoContext.SetPath("/search")
		echoContext.QueryParams().Set("q", "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")

		// Call Search handler
		err := httpServer.Search(echoContext)
		if err != nil {
			t.Fatal(err)
		}

		// Check response status code - should be not found since UTXO search is not supported
		assert.Equal(t, http.StatusNotFound, responseRecorder.Code)

		// Check response body
		var response map[string]interface{}
		if err = json.Unmarshal(responseRecorder.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}

		// Check error response
		require.NotNil(t, response)
		assert.Equal(t, float64(404), response["status"])
		assert.Equal(t, float64(errors.ERR_NOT_FOUND), response["code"])
		assert.Contains(t, response["error"], "no matching entity found")
	})

	t.Run("Search by block height error", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetBlockByHeight", mock.Anything, mock.Anything).Return(nil, errors.NewBlockNotFoundError("block not found"))

		// set echo context
		echoContext.SetPath("/search")
		echoContext.QueryParams().Set("q", "1000")

		// Call Search handler
		err := httpServer.Search(echoContext)
		if err != nil {
			t.Fatal(err)
		}

		// Check response status code
		assert.Equal(t, http.StatusNotFound, responseRecorder.Code)

		// Check response body
		var response map[string]interface{}
		if err = json.Unmarshal(responseRecorder.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}

		// Check response fields
		require.NotNil(t, response)
		assert.Equal(t, float64(404), response["status"])
		assert.Equal(t, float64(3), response["code"])
		assert.Equal(t, "NOT_FOUND (3): block not found", response["error"])
	})
}
