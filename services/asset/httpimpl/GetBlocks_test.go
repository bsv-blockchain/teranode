package httpimpl

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestGetBlocks(t *testing.T) {
	initPrometheusMetrics()

	t.Run("JSON success", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		blockInfos := []*model.BlockInfo{testBlockInfo}

		// set mock response
		mockRepo.On("GetBestBlockHeader", mock.Anything).Return(testBlockHeader, testBlockHeaderMeta, nil)
		mockRepo.On("GetLastNBlocks", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(blockInfos, nil)

		// set echo context
		echoContext.SetPath("/blocks")
		echoContext.QueryParams().Set("offset", "0")
		echoContext.QueryParams().Set("limit", "20")

		// Call GetBlocks handler
		err := httpServer.GetBlocks(echoContext)
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
		assert.Equal(t, float64(2), response["pagination"].(map[string]interface{})["totalRecords"])

		assert.Equal(t, 1, len(response["data"].([]interface{})))
		blocks := response["data"].([]interface{})
		block := blocks[0].(map[string]interface{})
		assert.Equal(t, float64(123), block["height"])
		assert.Equal(t, "9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995", block["hash"])
		assert.Equal(t, "0000000000000000000000000000000000000000000000000000000000000000", block["previousblockhash"])
		assert.Equal(t, float64(321), block["coinbaseValue"])
		assert.Equal(t, float64(2), block["transactionCount"])
		assert.Equal(t, float64(12345), block["size"])
		assert.Equal(t, "miner", block["miner"])
	})

	t.Run("Invalid offset parameter", func(t *testing.T) {
		httpServer, _, echoContext, _ := GetMockHTTP(t, nil)

		// set echo context
		echoContext.SetPath("/blocks")
		echoContext.QueryParams().Set("offset", "invalid")

		// Call GetBlocks handler
		err := httpServer.GetBlocks(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)

		// Check response body
		assert.Equal(t, "INVALID_ARGUMENT (1): invalid offset format -> UNKNOWN (0): strconv.Atoi: parsing \"invalid\": invalid syntax", echoErr.Message)
	})

	t.Run("Invalid limit parameter", func(t *testing.T) {
		httpServer, _, echoContext, _ := GetMockHTTP(t, nil)

		// set echo context
		echoContext.SetPath("/blocks")
		echoContext.QueryParams().Set("limit", "invalid")

		// Call GetBlocks handler
		err := httpServer.GetBlocks(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)

		// Check response body
		assert.Equal(t, "INVALID_ARGUMENT (1): invalid limit format -> UNKNOWN (0): strconv.Atoi: parsing \"invalid\": invalid syntax", echoErr.Message)
	})

	t.Run("No blocks found", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetBestBlockHeader", mock.Anything).Return(testBlockHeader, testBlockHeaderMeta, nil)
		mockRepo.On("GetLastNBlocks", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundError("blocks not found"))

		// set echo context
		echoContext.SetPath("/blocks")
		echoContext.QueryParams().Set("offset", "0")
		echoContext.QueryParams().Set("limit", "20")

		// Call GetBlocks handler
		err := httpServer.GetBlocks(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusNotFound, echoErr.Code)

		// Check response body
		assert.Equal(t, "NOT_FOUND (3): blocks not found", echoErr.Message)
	})

	t.Run("Best block header retrieval failure", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetBestBlockHeader", mock.Anything).Return(nil, nil, errors.NewStorageError("error getting best block header"))

		// set echo context
		echoContext.SetPath("/blocks")
		echoContext.QueryParams().Set("offset", "0")
		echoContext.QueryParams().Set("limit", "20")

		// Call GetBlocks handler
		err := httpServer.GetBlocks(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusInternalServerError, echoErr.Code)

		// Check response body
		assert.Equal(t, "PROCESSING (4): error getting best block header -> STORAGE_ERROR (69): error getting best block header", echoErr.Message)
	})
}
