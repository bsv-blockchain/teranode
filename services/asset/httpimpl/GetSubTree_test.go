package httpimpl

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestGetSubtree(t *testing.T) {
	initPrometheusMetrics()

	t.Run("Valid JSON response", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		mockRepo.On("GetSubtree", mock.Anything, mock.Anything).Return(testSubtree, nil)

		// set echo context
		echoContext.SetPath("/subtree/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

		// Call GetSubtree handler
		err := httpServer.GetSubtree(JSON)(echoContext)
		require.NoError(t, err)

		// Check response status code
		assert.Equal(t, http.StatusOK, echoContext.Response().Status)

		// Check response body
		var response map[string]any
		err = json.Unmarshal(responseRecorder.Body.Bytes(), &response)
		require.NoError(t, err)

		// Check response structure (now paginated)
		require.NotNil(t, response)
		require.NotNil(t, response["data"], "Expected data field in response")
		require.NotNil(t, response["pagination"], "Expected pagination field in response")

		// Check data fields
		data := response["data"].(map[string]any)
		assert.Equal(t, float64(2), data["Height"])
		assert.Equal(t, float64(6), data["Fees"])
		assert.Equal(t, float64(6), data["SizeInBytes"])
		assert.Equal(t, "0000000000000000000000000000000000000000000000000000000000000000", data["FeeHash"])
		assert.Equal(t, 4, len(data["Nodes"].([]any)))
		assert.Nil(t, data["ConflictingNodes"])

		// Check pagination fields
		pagination := response["pagination"].(map[string]any)
		assert.Equal(t, float64(0), pagination["offset"])
		assert.Equal(t, float64(20), pagination["limit"])
		assert.Equal(t, float64(4), pagination["totalRecords"])
	})

	t.Run("Valid BINARY response", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// create a buffer to write the subtree to
		subtreeBytes, err := testSubtree.Serialize()
		require.NoError(t, err)

		reader := bytes.NewReader(subtreeBytes)

		mockRepo.On("GetSubtreeTxIDsReader", mock.Anything, mock.Anything).Return(io.NopCloser(reader), nil)

		// set echo context
		echoContext.SetPath("/subtree/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

		// Call GetSubtree handler
		err = httpServer.GetSubtree(BINARY_STREAM)(echoContext)
		require.NoError(t, err)

		// Check response status code
		assert.Equal(t, http.StatusOK, echoContext.Response().Status)

		// Check response body
		subtreeBytesFromResponse := responseRecorder.Body.Bytes()

		// read bytes into 32 byte hashes until end
		buf := make([]byte, 32)
		hashes := make([]chainhash.Hash, 0)
		responseReader := bytes.NewReader(subtreeBytesFromResponse)

		for {
			n, err := responseReader.Read(buf)
			if err == io.EOF {
				break
			}

			require.NoError(t, err)

			assert.Equal(t, 32, n)

			hashes = append(hashes, chainhash.Hash(buf))
		}

		for i, hash := range hashes {
			expectedHash, err := chainhash.NewHashFromStr(fmt.Sprintf("%x", i))
			require.NoError(t, err)
			assert.Equal(t, expectedHash.String(), hash.String())
		}
	})

	t.Run("Valid HEX response", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// create a buffer to write the subtree to
		subtreeBytes, err := testSubtree.Serialize()
		require.NoError(t, err)

		reader := bytes.NewReader(subtreeBytes)

		mockRepo.On("GetSubtreeTxIDsReader", mock.Anything, mock.Anything).Return(io.NopCloser(reader), nil)

		// set echo context
		echoContext.SetPath("/subtree/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

		// Call GetSubtree handler
		err = httpServer.GetSubtree(HEX)(echoContext)
		require.NoError(t, err)

		// Check response status code
		assert.Equal(t, http.StatusOK, echoContext.Response().Status)
		assert.Equal(t, "text/plain; charset=UTF-8", responseRecorder.Header().Get("Content-Type"))

		// Check response body
		subtreeBytesFromResponse := responseRecorder.Body.Bytes()
		subtreeNodes, err := testSubtree.SerializeNodes()
		require.NoError(t, err)

		assert.Equal(t, hex.EncodeToString(subtreeNodes), string(subtreeBytesFromResponse))
	})

	t.Run("Subtree not found", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetSubtree", mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundError("subtree could not found"))

		// set echo context
		echoContext.SetPath("/subtree/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

		// Call GetSubtree handler
		err := httpServer.GetSubtree(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusNotFound, echoErr.Code)

		// Check response body
		assert.Equal(t, "NOT_FOUND (3): subtree could not found", echoErr.Message)
	})

	t.Run("SubtreeReader not found", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetSubtreeTxIDsReader", mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundError("subtree could not found"))

		// set echo context
		echoContext.SetPath("/subtree/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

		// Call GetSubtree handler
		err := httpServer.GetSubtree(BINARY_STREAM)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusNotFound, echoErr.Code)

		// Check response body
		assert.Equal(t, "NOT_FOUND (3): subtree could not found", echoErr.Message)
	})

	t.Run("invalid subtree reader", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// create a buffer to write the subtree to
		reader := bytes.NewReader([]byte{0x01, 0x02, 0x03, 0x04})

		mockRepo.On("GetSubtreeTxIDsReader", mock.Anything, mock.Anything).Return(io.NopCloser(reader), nil)

		// set echo context
		echoContext.SetPath("/subtree/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

		// Call GetSubtree handler
		err := httpServer.GetSubtree(BINARY_STREAM)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusInternalServerError, echoErr.Code)

		// Check response body
		assert.Equal(t, "unable to read subtree root information: unexpected EOF", echoErr.Message)
	})

	t.Run("Invalid hash length", func(t *testing.T) {
		httpServer, _, echoContext, _ := GetMockHTTP(t, nil)

		// set echo context
		echoContext.SetPath("/subtree/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("invalidhash")

		// Call GetSubtree handler
		err := httpServer.GetSubtree(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)

		// Check response body
		assert.Equal(t, "INVALID_ARGUMENT (1): invalid hash length", echoErr.Message)
	})

	t.Run("Invalid hash format", func(t *testing.T) {
		httpServer, _, echoContext, _ := GetMockHTTP(t, nil)

		// set echo context
		echoContext.SetPath("/subtree/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("sd45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea99y")

		// Call GetSubtree handler
		err := httpServer.GetSubtree(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)

		// Check response body
		assert.Equal(t, "INVALID_ARGUMENT (1): invalid hash string -> UNKNOWN (0): encoding/hex: invalid byte: U+0073 's'", echoErr.Message)
	})

	t.Run("Invalid read mode", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// create a buffer to write the subtree to
		subtreeBytes, err := testSubtree.Serialize()
		require.NoError(t, err)

		reader := bytes.NewReader(subtreeBytes)

		mockRepo.On("GetSubtreeTxIDsReader", mock.Anything, mock.Anything).Return(io.NopCloser(reader), nil)

		// set echo context
		echoContext.SetPath("/subtree/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

		var invalidReadMode ReadMode = 999

		// Call GetSubtree handler
		err = httpServer.GetSubtree(invalidReadMode)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)

		// Check response body
		assert.Equal(t, "INVALID_ARGUMENT (1): bad read mode", echoErr.Message)
	})
}

func TestCalculateSpeed(t *testing.T) {
	b := make([]byte, 1024)
	duration := 1 * time.Second
	sizeInKB := float64(len(b)) / 1024

	res := fmt.Sprintf("%.2f kB): in %s (%.2f kB/sec)", sizeInKB, duration, calculateSpeed(duration, sizeInKB))
	assert.Equal(t, "1.00 kB): in 1s (1.00 kB/sec)", res)
}
