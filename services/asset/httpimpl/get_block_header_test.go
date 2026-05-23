package httpimpl

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestGetBlockHeader(t *testing.T) {
	initPrometheusMetrics()

	t.Run("JSON success", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetBlockHeader", mock.Anything, mock.Anything).Return(testBlockHeader, testBlockHeaderMeta, nil)

		// set echo context
		echoContext.SetPath("/block/header/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

		// Call GetBlockHeader handler
		err := httpServer.GetBlockHeader(JSON)(echoContext)
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
		assert.Equal(t, "9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995", response["hash"])
		assert.Equal(t, float64(1), response["version"])
		assert.Equal(t, float64(1), response["height"])
		assert.Equal(t, float64(2), response["txCount"])
		assert.Equal(t, float64(3), response["sizeInBytes"])
		assert.Equal(t, float64(432645644), response["time"])
		assert.Equal(t, float64(12435623), response["nonce"])
		assert.Equal(t, "00000000", response["bits"])
		assert.Equal(t, "Miner", response["miner"])
		assert.Equal(t, "0000000000000000000000000000000000000000000000000000000000000000", response["previousblockhash"])
		assert.Equal(t, "0000000000000000000000000000000000000000000000000000000000000000", response["merkleroot"])
	})

	t.Run("Binary success", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetBlockHeader", mock.Anything, mock.Anything).Return(testBlockHeader, testBlockHeaderMeta, nil)

		// set echo context
		echoContext.SetPath("/block/header/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

		// Call GetBlockHeader handler
		err := httpServer.GetBlockHeader(BINARY_STREAM)(echoContext)
		if err != nil {
			t.Fatal(err)
		}

		// Check response status code
		assert.Equal(t, http.StatusOK, responseRecorder.Code)

		// Check response body
		assert.Equal(t, 80, len(responseRecorder.Body.Bytes()))

		// unmarshal into block header
		blockHeaderFromRec, err := model.NewBlockHeaderFromBytes(responseRecorder.Body.Bytes())
		require.NoError(t, err)

		// Check response fields
		require.NotNil(t, blockHeaderFromRec)
		assert.Equal(t, testBlockHeader.Version, blockHeaderFromRec.Version)
		assert.Equal(t, testBlockHeader.Timestamp, blockHeaderFromRec.Timestamp)
		assert.Equal(t, testBlockHeader.Nonce, blockHeaderFromRec.Nonce)
		assert.Equal(t, testBlockHeader.Bits, blockHeaderFromRec.Bits)
		assert.Equal(t, testBlockHeader.HashPrevBlock, blockHeaderFromRec.HashPrevBlock)
		assert.Equal(t, testBlockHeader.HashMerkleRoot, blockHeaderFromRec.HashMerkleRoot)
	})

	t.Run("Hex success", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetBlockHeader", mock.Anything, mock.Anything).Return(testBlockHeader, testBlockHeaderMeta, nil)

		// set echo context
		echoContext.SetPath("/block/header/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

		// Call GetBlockHeader handler
		err := httpServer.GetBlockHeader(HEX)(echoContext)
		if err != nil {
			t.Fatal(err)
		}

		// Check response status code
		assert.Equal(t, http.StatusOK, responseRecorder.Code)
		assert.Equal(t, "text/plain; charset=UTF-8", responseRecorder.Header().Get("Content-Type"))

		// Check response body
		assert.Equal(t, 160, len(responseRecorder.Body.Bytes()))
	})

	t.Run("Invalid hash", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetBlockHeader", mock.Anything, mock.Anything).Return(testBlockHeader, testBlockHeaderMeta, nil)

		// set echo context
		echoContext.SetPath("/block/header/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("invalid")

		// Call GetBlockHeader handler
		err := httpServer.GetBlockHeader(JSON)(echoContext)
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
		mockRepo.On("GetBlockHeader", mock.Anything, mock.Anything).Return(testBlockHeader, testBlockHeaderMeta, nil)

		// set echo context
		echoContext.SetPath("/block/header/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("sd45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea99y")

		// Call GetBlockHeader handler
		err := httpServer.GetBlockHeader(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)

		// Check response body
		assert.Equal(t, "PROCESSING (4): invalid hash string -> UNKNOWN (0): encoding/hex: invalid byte: U+0073 's'", echoErr.Message)
	})

	t.Run("Repository error", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetBlockHeader", mock.Anything, mock.Anything).Return(nil, nil, errors.NewProcessingError("error getting block header"))

		// set echo context
		echoContext.SetPath("/block/header/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

		// Call GetBlockHeader handler
		err := httpServer.GetBlockHeader(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusInternalServerError, echoErr.Code)

		// Check response body
		assert.Equal(t, "PROCESSING (4): error getting block header", echoErr.Message)
	})

	t.Run("Repository not found", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetBlockHeader", mock.Anything, mock.Anything).Return(nil, nil, errors.NewNotFoundError("block header not found"))

		// set echo context
		echoContext.SetPath("/block/header/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

		// Call GetBlockHeader handler
		err := httpServer.GetBlockHeader(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusNotFound, echoErr.Code)

		// Check response body
		assert.Equal(t, "NOT_FOUND (3): block header not found", echoErr.Message)
	})

	t.Run("Invalid read mode", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetBlockHeader", mock.Anything, mock.Anything).Return(testBlockHeader, testBlockHeaderMeta, nil)

		// set echo context
		echoContext.SetPath("/block/header/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

		var (
			invalidReadMode ReadMode = 999
		)

		// Call GetBlockHeader handler
		err := httpServer.GetBlockHeader(invalidReadMode)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)

		// Check response body
		assert.Equal(t, "INVALID_ARGUMENT (1): bad read mode", echoErr.Message)
	})

	t.Run("JSON success with valid ProcessedAt", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// Create test data with valid ProcessedAt
		testBlockHeaderMetaWithProcessedAt := &model.BlockHeaderMeta{
			Height:      1,
			TxCount:     2,
			SizeInBytes: 3,
			Miner:       "Miner",
			ProcessedAt: func() *time.Time { t := time.Unix(1609459200, 0).UTC(); return &t }(), // Valid timestamp: 2021-01-01 UTC
		}

		// set mock response
		mockRepo.On("GetBlockHeader", mock.Anything, mock.Anything).Return(testBlockHeader, testBlockHeaderMetaWithProcessedAt, nil)

		// set echo context
		echoContext.SetPath("/block/header/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

		// Call GetBlockHeader handler
		err := httpServer.GetBlockHeader(JSON)(echoContext)
		if err != nil {
			t.Fatal(err)
		}

		// Check response status code
		assert.Equal(t, http.StatusOK, responseRecorder.Code)

		// Check response body contains processed_at
		var response map[string]interface{}
		if err = json.Unmarshal(responseRecorder.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}

		// Should have processed_at field
		assert.Contains(t, response, "processed_at")
		assert.Equal(t, "2021-01-01T00:00:00Z", response["processed_at"])
	})

	t.Run("JSON success with invalid ProcessedAt - negative timestamp", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// Create test data with invalid ProcessedAt (year way before 0)
		testBlockHeaderMetaWithInvalidProcessedAt := &model.BlockHeaderMeta{
			Height:      1,
			TxCount:     2,
			SizeInBytes: 3,
			Miner:       "Miner",
			ProcessedAt: func() *time.Time { t := time.Unix(-999999999999, 0); return &t }(), // Invalid: year -29686
		}

		// set mock response
		mockRepo.On("GetBlockHeader", mock.Anything, mock.Anything).Return(testBlockHeader, testBlockHeaderMetaWithInvalidProcessedAt, nil)

		// set echo context
		echoContext.SetPath("/block/header/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

		// Call GetBlockHeader handler - should not fail even with invalid timestamp
		err := httpServer.GetBlockHeader(JSON)(echoContext)
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

		// Should not contain processed_at field or should be null
		processedAt, exists := response["processed_at"]
		if exists {
			assert.Nil(t, processedAt, "ProcessedAt should be null for invalid timestamps")
		}
	})

	t.Run("JSON success with invalid ProcessedAt - far future timestamp", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// Create test data with invalid ProcessedAt (year way beyond 9999)
		testBlockHeaderMetaWithInvalidProcessedAt := &model.BlockHeaderMeta{
			Height:      1,
			TxCount:     2,
			SizeInBytes: 3,
			Miner:       "Miner",
			ProcessedAt: func() *time.Time { t := time.Unix(999999999999, 0); return &t }(), // Invalid: year 33658
		}

		// set mock response
		mockRepo.On("GetBlockHeader", mock.Anything, mock.Anything).Return(testBlockHeader, testBlockHeaderMetaWithInvalidProcessedAt, nil)

		// set echo context
		echoContext.SetPath("/block/header/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

		// Call GetBlockHeader handler - should not fail even with invalid timestamp
		err := httpServer.GetBlockHeader(JSON)(echoContext)
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

		// Should not contain processed_at field or should be null
		processedAt, exists := response["processed_at"]
		if exists {
			assert.Nil(t, processedAt, "ProcessedAt should be null for invalid timestamps")
		}
	})
}
