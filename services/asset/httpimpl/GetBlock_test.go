package httpimpl

import (
	"encoding/hex"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestGetBlockByHeight(t *testing.T) {
	initPrometheusMetrics()

	t.Run("JSON success", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetBlockByHeight", mock.Anything, mock.Anything).Return(testBlock, nil)

		// set echo context
		echoContext.SetPath("/block/height/:height")
		echoContext.SetParamNames("height")
		echoContext.SetParamValues("0")

		// Call GetBlockByHeight handler
		err := httpServer.GetBlockByHeight(JSON)(echoContext)
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
		assert.Equal(t, float64(100), response["height"])
		assert.Equal(t, float64(123), response["transaction_count"])
		assert.Equal(t, float64(321), response["size_in_bytes"])
		assert.Equal(t, float64(666), response["id"])
		assert.Equal(t, "9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995", response["nextblock"])

		subtrees := response["subtrees"].([]interface{})
		assert.Equal(t, 1, len(subtrees))
		assert.Equal(t, "b042f298deabcebbf15355aa3a13c7d7cfe96c44ac4f492735f936f8e50d06f6", subtrees[0])

		header := response["header"].(map[string]interface{})
		assert.Equal(t, float64(1), header["version"])
		assert.Equal(t, "0000000000000000000000000000000000000000000000000000000000000000", header["hash_prev_block"])
		assert.Equal(t, "0000000000000000000000000000000000000000000000000000000000000000", header["hash_merkle_root"])
		assert.Equal(t, float64(432645644), header["timestamp"])
		assert.Equal(t, "00000000", header["bits"])
		assert.Equal(t, float64(12435623), header["nonce"])
	})

	t.Run("Binary success", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetBlockByHeight", mock.Anything, mock.Anything).Return(testBlock, nil)

		// set echo context
		echoContext.SetPath("/block/height/:height")
		echoContext.SetParamNames("height")
		echoContext.SetParamValues("0")

		// Call GetBlockByHeight handler
		err := httpServer.GetBlockByHeight(BINARY_STREAM)(echoContext)
		if err != nil {
			t.Fatal(err)
		}

		// Check response status code
		assert.Equal(t, http.StatusOK, responseRecorder.Code)

		// Check response body
		assert.Equal(t, 229, len(responseRecorder.Body.Bytes()))

		// check block data
		assert.Equal(t, testBlockBytes, responseRecorder.Body.Bytes())
	})

	t.Run("Hex success", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetBlockByHeight", mock.Anything, mock.Anything).Return(testBlock, nil)
		mockRepo.On("GetBlockByHeight", mock.Anything, mock.Anything).Return(nil, errors.ErrNotFound)

		// set echo context
		echoContext.SetPath("/block/height/:height")
		echoContext.SetParamNames("height")
		echoContext.SetParamValues("0")

		// Call GetBlockByHeight handler
		err := httpServer.GetBlockByHeight(HEX)(echoContext)
		if err != nil {
			t.Fatal(err)
		}

		// Check response status code
		assert.Equal(t, http.StatusOK, responseRecorder.Code)
		assert.Equal(t, "text/plain; charset=UTF-8", responseRecorder.Header().Get("Content-Type"))

		// Check response body
		blockHex := responseRecorder.Body.String()
		assert.Equal(t, 229*2, len(blockHex))
		assert.Equal(t, hex.EncodeToString(testBlockBytes), blockHex)
	})

	t.Run("Invalid height", func(t *testing.T) {
		httpServer, _, echoContext, _ := GetMockHTTP(t, nil)

		// set echo context
		echoContext.SetPath("/block/height/:height")
		echoContext.SetParamNames("height")
		echoContext.SetParamValues("invalid")

		// Call GetBlockByHeight handler
		err := httpServer.GetBlockByHeight(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)

		// Check response body
		assert.Equal(t, "INVALID_ARGUMENT (1): invalid height parameter -> UNKNOWN (0): strconv.ParseUint: parsing \"invalid\": invalid syntax", echoErr.Message)
	})

	t.Run("Repository error", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetBlockByHeight", mock.Anything, mock.Anything).Return(nil, errors.NewStorageError("error getting block"))

		// set echo context
		echoContext.SetPath("/block/height/:height")
		echoContext.SetParamNames("height")
		echoContext.SetParamValues("0")

		// Call GetBlockByHeight handler
		err := httpServer.GetBlockByHeight(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusInternalServerError, echoErr.Code)

		// Check response body
		assert.Equal(t, "STORAGE_ERROR (69): error getting block", echoErr.Message)
	})

	t.Run("Repository not found", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetBlockByHeight", mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundError("block not found"))

		// set echo context
		echoContext.SetPath("/block/height/:height")
		echoContext.SetParamNames("height")
		echoContext.SetParamValues("0")

		// Call GetBlockByHeight handler
		err := httpServer.GetBlockByHeight(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusNotFound, echoErr.Code)

		// Check response body
		assert.Equal(t, "NOT_FOUND (3): block not found", echoErr.Message)
	})

	t.Run("Invalid read mode", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetBlockByHeight", mock.Anything, mock.Anything).Return(testBlock, nil)

		// set echo context
		echoContext.SetPath("/block/height/:height")
		echoContext.SetParamNames("height")
		echoContext.SetParamValues("0")

		var (
			invalidReadMode ReadMode = 999
		)

		// Call GetBlockByHeight handler
		err := httpServer.GetBlockByHeight(invalidReadMode)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)

		// Check response body
		assert.Equal(t, "INVALID_ARGUMENT (1): bad read mode", echoErr.Message)
	})
}

func TestGetBlockByHash(t *testing.T) {
	initPrometheusMetrics()

	t.Run("JSON success", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetBlockByHash", mock.Anything, mock.Anything).Return(testBlock, nil).Once()
		mockRepo.On("GetBlockByHeight", mock.Anything, mock.Anything).Return(testBlock, nil).Once()

		// set echo context
		echoContext.SetPath("/block/hash/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")

		// Call GetBlockByHash handler
		err := httpServer.GetBlockByHash(JSON)(echoContext)
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
		assert.Equal(t, float64(100), response["height"])
		assert.Equal(t, float64(123), response["transaction_count"])
		assert.Equal(t, float64(321), response["size_in_bytes"])
		assert.Equal(t, float64(666), response["id"])
		assert.Equal(t, "9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995", response["nextblock"])

		subtrees := response["subtrees"].([]interface{})
		assert.Equal(t, 1, len(subtrees))
		assert.Equal(t, "b042f298deabcebbf15355aa3a13c7d7cfe96c44ac4f492735f936f8e50d06f6", subtrees[0])

		header := response["header"].(map[string]interface{})
		assert.Equal(t, float64(1), header["version"])
		assert.Equal(t, "0000000000000000000000000000000000000000000000000000000000000000", header["hash_prev_block"])
		assert.Equal(t, "0000000000000000000000000000000000000000000000000000000000000000", header["hash_merkle_root"])
		assert.Equal(t, float64(432645644), header["timestamp"])
		assert.Equal(t, "00000000", header["bits"])
		assert.Equal(t, float64(12435623), header["nonce"])
	})

	t.Run("Binary success", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetBlockByHash", mock.Anything, mock.Anything).Return(testBlock, nil)

		// set echo context
		echoContext.SetPath("/block/hash/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")

		// Call GetBlockByHash handler
		err := httpServer.GetBlockByHash(BINARY_STREAM)(echoContext)
		if err != nil {
			t.Fatal(err)
		}

		// Check response status code
		assert.Equal(t, http.StatusOK, responseRecorder.Code)

		// Check response body
		assert.Equal(t, 229, len(responseRecorder.Body.Bytes()))

		// check block data
		assert.Equal(t, testBlockBytes, responseRecorder.Body.Bytes())
	})

	t.Run("Hex success", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetBlockByHash", mock.Anything, mock.Anything).Return(testBlock, nil)

		// set echo context
		echoContext.SetPath("/block/hash/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")

		// Call GetBlockByHash handler
		err := httpServer.GetBlockByHash(HEX)(echoContext)
		if err != nil {
			t.Fatal(err)
		}

		// Check response status code
		assert.Equal(t, http.StatusOK, responseRecorder.Code)
		assert.Equal(t, "text/plain; charset=UTF-8", responseRecorder.Header().Get("Content-Type"))

		// Check response body
		blockHex := responseRecorder.Body.String()
		assert.Equal(t, 229*2, len(blockHex))
		assert.Equal(t, hex.EncodeToString(testBlockBytes), blockHex)
	})

	t.Run("Invalid hash length", func(t *testing.T) {
		httpServer, _, echoContext, _ := GetMockHTTP(t, nil)

		// set echo context
		echoContext.SetPath("/block/hash/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("invalidhashlength")

		// Call GetBlockByHash handler
		err := httpServer.GetBlockByHash(JSON)(echoContext)
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
		echoContext.SetPath("/block/hash/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("s00000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26t")

		// Call GetBlockByHash handler
		err := httpServer.GetBlockByHash(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)

		// Check response body
		assert.Equal(t, "INVALID_ARGUMENT (1): invalid hash string -> UNKNOWN (0): encoding/hex: invalid byte: U+0073 's'", echoErr.Message)
	})

	t.Run("Repository error", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetBlockByHash", mock.Anything, mock.Anything).Return(nil, errors.NewStorageError("error getting block"))

		// set echo context
		echoContext.SetPath("/block/hash/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")

		// Call GetBlockByHash handler
		err := httpServer.GetBlockByHash(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusInternalServerError, echoErr.Code)

		// Check response body
		assert.Equal(t, "STORAGE_ERROR (69): error getting block", echoErr.Message)
	})

	t.Run("Repository not found", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetBlockByHash", mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundError("block not found"))

		// set echo context
		echoContext.SetPath("/block/hash/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")

		// Call GetBlockByHash handler
		err := httpServer.GetBlockByHash(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusNotFound, echoErr.Code)

		// Check response body
		assert.Equal(t, "NOT_FOUND (3): block not found", echoErr.Message)
	})

	t.Run("Invalid read mode", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetBlockByHash", mock.Anything, mock.Anything).Return(testBlock, nil)

		// set echo context
		echoContext.SetPath("/block/hash/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")

		var (
			invalidReadMode ReadMode = 999
		)

		// Call GetBlockByHash handler
		err := httpServer.GetBlockByHash(invalidReadMode)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)

		// Check response body
		assert.Equal(t, "INVALID_ARGUMENT (1): bad read mode", echoErr.Message)
	})
}
