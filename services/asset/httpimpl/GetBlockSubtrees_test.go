package httpimpl

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestGetBlockSubtrees(t *testing.T) {
	initPrometheusMetrics()

	t.Run("JSON success", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetBlockByHash", mock.Anything, mock.Anything).Return(testBlock, nil)
		mockRepo.On("GetSubtreeHead", mock.Anything, mock.Anything).Return(testSubtree, 4, nil)

		// set echo context
		echoContext.SetPath("/block/subtrees/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")

		// Call GetBlockSubtrees handler
		err := httpServer.GetBlockSubtrees(JSON)(echoContext)
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
		assert.Equal(t, float64(0), response["pagination"].(map[string]interface{})["offset"])
		assert.Equal(t, float64(20), response["pagination"].(map[string]interface{})["limit"])
		assert.Equal(t, float64(len(testBlock.Subtrees)), response["pagination"].(map[string]interface{})["totalRecords"])

		// subtree data
		subtree := response["data"].([]interface{})[0].(map[string]interface{})
		assert.Equal(t, testSubtree.Fees, uint64(subtree["fee"].(float64)))
		assert.Equal(t, testSubtree.SizeInBytes, uint64(subtree["size"].(float64)))
		assert.Equal(t, "b042f298deabcebbf15355aa3a13c7d7cfe96c44ac4f492735f936f8e50d06f6", subtree["hash"])
		assert.Equal(t, len(testSubtree.Nodes), int(subtree["txCount"].(float64)))
	})

	t.Run("JSON success - without subtree head", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetBlockByHash", mock.Anything, mock.Anything).Return(testBlock, nil)
		mockRepo.On("GetSubtreeHead", mock.Anything, mock.Anything).Return(nil, 0, errors.NewStorageError("subtree head not found"))

		// set echo context
		echoContext.SetPath("/block/subtrees/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")

		// Call GetBlockSubtrees handler
		err := httpServer.GetBlockSubtrees(JSON)(echoContext)
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
		assert.Equal(t, float64(0), response["pagination"].(map[string]interface{})["offset"])
		assert.Equal(t, float64(20), response["pagination"].(map[string]interface{})["limit"])
		assert.Equal(t, float64(len(testBlock.Subtrees)), response["pagination"].(map[string]interface{})["totalRecords"])

		// subtree data
		subtree := response["data"].([]interface{})[0].(map[string]interface{})
		assert.Equal(t, "b042f298deabcebbf15355aa3a13c7d7cfe96c44ac4f492735f936f8e50d06f6", subtree["hash"])

		// this info is missing if the subtree head is not found
		assert.Equal(t, uint64(0), uint64(subtree["fee"].(float64)))
		assert.Equal(t, uint64(0), uint64(subtree["size"].(float64)))
		assert.Equal(t, 0, int(subtree["txCount"].(float64)))
	})

	t.Run("Invalid hash length", func(t *testing.T) {
		httpServer, _, echoContext, _ := GetMockHTTP(t, nil)

		// set echo context
		echoContext.SetPath("/block/subtrees/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("invalidhashlength")

		// Call GetBlockSubtrees handler
		err := httpServer.GetBlockSubtrees(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)

		// Check response body
		assert.Equal(t, "INVALID_ARGUMENT (1): invalid hash length", echoErr.Message)
	})

	t.Run("Invalid hash string", func(t *testing.T) {
		httpServer, _, echoContext, _ := GetMockHTTP(t, nil)

		// set echo context
		echoContext.SetPath("/block/subtrees/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("s042f298deabcebbf15355aa3a13c7d7cfe96c44ac4f492735f936f8e50d06ft")

		// Call GetBlockSubtrees handler
		err := httpServer.GetBlockSubtrees(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)

		// Check response body
		assert.Equal(t, "INVALID_ARGUMENT (1): invalid hash string -> UNKNOWN (0): encoding/hex: invalid byte: U+0073 's'", echoErr.Message)
	})

	t.Run("Block not found", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetBlockByHash", mock.Anything, mock.Anything).Return(nil, errors.NewStorageError("block not found"))

		// set echo context
		echoContext.SetPath("/block/subtrees/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")

		// Call GetBlockSubtrees handler
		err := httpServer.GetBlockSubtrees(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusNotFound, echoErr.Code)

		// Check response body
		assert.Equal(t, "STORAGE_ERROR (69): block not found", echoErr.Message)
	})

	t.Run("Repository error", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetBlockByHash", mock.Anything, mock.Anything).Return(nil, errors.NewStorageError("repository error"))

		// set echo context
		echoContext.SetPath("/block/subtrees/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")

		// Call GetBlockSubtrees handler
		err := httpServer.GetBlockSubtrees(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusInternalServerError, echoErr.Code)

		// Check response body
		assert.Equal(t, "STORAGE_ERROR (69): repository error", echoErr.Message)
	})

	t.Run("Invalid limit parameter", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetBlockByHash", mock.Anything, mock.Anything).Return(testBlock, nil)
		mockRepo.On("GetSubtreeHead", mock.Anything, mock.Anything).Return(testSubtree, 4, nil)

		// set echo context
		echoContext.SetPath("/block/subtrees/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
		echoContext.QueryParams().Set("limit", "invalid")

		// Call GetBlocks handler
		err := httpServer.GetBlockSubtrees(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)

		// Check response body
		assert.Equal(t, "INVALID_ARGUMENT (1): invalid limit format -> UNKNOWN (0): strconv.Atoi: parsing \"invalid\": invalid syntax", echoErr.Message)
	})

	t.Run("Invalid read mode", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetBlockByHash", mock.Anything, mock.Anything).Return(testBlock, nil)
		mockRepo.On("GetSubtreeHead", mock.Anything, mock.Anything).Return(testSubtree, 4, nil)

		// set echo context
		echoContext.SetPath("/block/subtrees/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")

		var (
			invalidReadMode ReadMode = 999
		)

		// Call GetBlockSubtrees handler
		err := httpServer.GetBlockSubtrees(invalidReadMode)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)

		// Check response body
		assert.Equal(t, "INVALID_ARGUMENT (1): bad read mode", echoErr.Message)
	})
}
