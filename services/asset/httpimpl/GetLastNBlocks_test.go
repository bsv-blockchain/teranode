package httpimpl

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestGetLastNBlocks(t *testing.T) {
	initPrometheusMetrics()

	t.Run("Valid request with default parameters", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetLastNBlocks", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*model.BlockInfo{testBlockInfo}, nil)

		// set echo context
		echoContext.SetPath("/blocks/last")

		// Call GetLastNBlocks handler
		err := httpServer.GetLastNBlocks(echoContext)
		if err != nil {
			t.Fatal(err)
		}

		// Check response status code
		assert.Equal(t, http.StatusOK, responseRecorder.Code)

		// Check response body
		var response []map[string]interface{}
		err = json.Unmarshal(responseRecorder.Body.Bytes(), &response)
		require.NoError(t, err)

		checkResponse(t, response)
	})

	t.Run("Valid request with parameters", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetLastNBlocks", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*model.BlockInfo{testBlockInfo}, nil)

		// set echo context
		echoContext.SetPath("/blocks/last")
		echoContext.QueryParams().Set("n", "20")
		echoContext.QueryParams().Set("fromHeight", "100000")
		echoContext.QueryParams().Set("includeOrphans", "true")

		// Call GetLastNBlocks handler
		err := httpServer.GetLastNBlocks(echoContext)
		if err != nil {
			t.Fatal(err)
		}

		// Check response status code
		assert.Equal(t, http.StatusOK, responseRecorder.Code)

		// Check response body
		var response []map[string]interface{}
		err = json.Unmarshal(responseRecorder.Body.Bytes(), &response)
		require.NoError(t, err)

		checkResponse(t, response)
	})

	t.Run("Invalid 'n' parameter", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetLastNBlocks", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*model.BlockInfo{testBlockInfo}, nil)

		// set echo context
		echoContext.SetPath("/blocks/last")
		echoContext.QueryParams().Set("n", "invalid")

		// Call GetLastNBlocks handler
		err := httpServer.GetLastNBlocks(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)

		// Check response body
		assert.Equal(t, "INVALID_ARGUMENT (1): invalid 'n' parameter -> UNKNOWN (0): strconv.ParseInt: parsing \"invalid\": invalid syntax", echoErr.Message)
	})

	t.Run("Invalid 'fromHeight' parameter", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetLastNBlocks", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*model.BlockInfo{testBlockInfo}, nil)

		// set echo context
		echoContext.SetPath("/blocks/last")
		echoContext.QueryParams().Set("fromHeight", "invalid")

		// Call GetLastNBlocks handler
		err := httpServer.GetLastNBlocks(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)

		// Check response body
		assert.Equal(t, "INVALID_ARGUMENT (1): invalid 'fromHeight' parameter -> UNKNOWN (0): strconv.ParseUint: parsing \"invalid\": invalid syntax", echoErr.Message)
	})

	t.Run("Repository error", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetLastNBlocks", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.NewStorageError("error getting last N blocks"))

		// set echo context
		echoContext.SetPath("/blocks/last")

		// Call GetLastNBlocks handler
		err := httpServer.GetLastNBlocks(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusInternalServerError, echoErr.Code)

		// Check response body
		assert.Equal(t, "STORAGE_ERROR (59): error getting last N blocks", echoErr.Message)
	})

	t.Run("No blocks found", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetLastNBlocks", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundError("blocks not found"))

		// set echo context
		echoContext.SetPath("/blocks/last")

		// Call GetLastNBlocks handler
		err := httpServer.GetLastNBlocks(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusNotFound, echoErr.Code)

		// Check response body
		assert.Equal(t, "NOT_FOUND (3): blocks not found", echoErr.Message)
	})
}

func checkResponse(t *testing.T, response []map[string]interface{}) {
	require.NotNil(t, response)
	assert.Equal(t, 1, len(response))
	assert.Equal(t, "9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995", response[0]["hash"])
	assert.Equal(t, float64(123), response[0]["height"])
	assert.Equal(t, float64(2), response[0]["transactionCount"])
	assert.Equal(t, float64(12345), response[0]["size"])
	assert.Equal(t, "1983-09-17T11:20:44.000Z", response[0]["timestamp"])
	assert.Equal(t, "miner", response[0]["miner"])
	assert.Equal(t, float64(321), response[0]["coinbaseValue"])
}
