package httpimpl

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestGetTransaction(t *testing.T) {
	initPrometheusMetrics()

	t.Run("JSON success", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetTransaction", mock.Anything, mock.Anything).Return(testTX1RawBytes, nil)

		// set echo context
		echoContext.SetPath("/tx/hash/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

		// Call GetTransaction handler
		err := httpServer.GetTransaction(JSON)(echoContext)
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

		tx1, err := bt.NewTxFromString(testTX1Raw)
		require.NoError(t, err)

		// Check response fields
		require.NotNil(t, response)
		assert.Equal(t, testTX1Hash.String(), response["txid"])
		assert.Equal(t, testTX1Raw, response["hex"])
		assert.Equal(t, float64(1), response["version"])
		assert.Equal(t, float64(0), response["lockTime"])

		// check inputs
		inputs := response["inputs"].([]interface{})
		require.Len(t, inputs, 1)
		input := inputs[0].(map[string]interface{})
		assert.Equal(t, tx1.Inputs[0].UnlockingScript.String(), input["unlockingScript"])
		assert.Equal(t, tx1.Inputs[0].PreviousTxIDChainHash().String(), input["txid"])
		assert.Equal(t, float64(tx1.Inputs[0].PreviousTxOutIndex), input["vout"])
		assert.Equal(t, float64(tx1.Inputs[0].SequenceNumber), input["sequence"])

		// check outputs
		outputs := response["outputs"].([]interface{})
		require.Len(t, outputs, 1)
		output := outputs[0].(map[string]interface{})
		assert.Equal(t, float64(tx1.Outputs[0].Satoshis), output["satoshis"])
		assert.Equal(t, tx1.Outputs[0].LockingScript.String(), output["lockingScript"])
	})

	t.Run("Binary success", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetTransaction", mock.Anything, mock.Anything).Return(testTX1RawBytes, nil)

		// set echo context
		echoContext.SetPath("/tx/hash/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

		// Call GetTransaction handler
		err := httpServer.GetTransaction(BINARY_STREAM)(echoContext)
		if err != nil {
			t.Fatal(err)
		}

		// Check response status code
		assert.Equal(t, http.StatusOK, responseRecorder.Code)

		// Check response body
		assert.Equal(t, testTX1RawBytes, responseRecorder.Body.Bytes())
	})

	t.Run("Hex success", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetTransaction", mock.Anything, mock.Anything).Return(testTX1RawBytes, nil)

		// set echo context
		echoContext.SetPath("/tx/hash/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

		// Call GetTransaction handler
		err := httpServer.GetTransaction(HEX)(echoContext)
		if err != nil {
			t.Fatal(err)
		}

		// Check response status code
		assert.Equal(t, http.StatusOK, responseRecorder.Code)
		assert.Equal(t, "text/plain; charset=UTF-8", responseRecorder.Header().Get("Content-Type"))

		// Check response body
		assert.Equal(t, testTX1Raw, responseRecorder.Body.String())
	})

	t.Run("JSON with invalid tx bytes", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetTransaction", mock.Anything, mock.Anything).Return(append([]byte{0x10, 0x11, 0x12}, testTX1RawBytes...), nil)

		// set echo context
		echoContext.SetPath("/tx/hash/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

		// Call GetTransaction handler
		err := httpServer.GetTransaction(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusInternalServerError, echoErr.Code)
		assert.Equal(t, "PROCESSING (4): error parsing transaction -> UNKNOWN (0): nLockTime length must be 4 bytes long", echoErr.Message)
	})

	t.Run("Invalid hash length", func(t *testing.T) {
		httpServer, _, echoContext, _ := GetMockHTTP(t, nil)

		// set echo context
		echoContext.SetPath("/tx/hash/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("invalid")

		// Call GetTransaction handler
		err := httpServer.GetTransaction(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)

		// Check response body
		assert.Equal(t, "INVALID_ARGUMENT (1): invalid hash length", echoErr.Message)
	})

	t.Run("Invalid hash character", func(t *testing.T) {
		httpServer, _, echoContext, _ := GetMockHTTP(t, nil)

		// set echo context
		echoContext.SetPath("/tx/hash/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("sd45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea99y")

		// Call GetTransaction handler
		err := httpServer.GetTransaction(JSON)(echoContext)
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
		mockRepo.On("GetTransaction", mock.Anything, mock.Anything).Return(nil, errors.NewProcessingError("error getting transaction"))

		// set echo context
		echoContext.SetPath("/tx/hash/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

		// Call GetTransaction handler
		err := httpServer.GetTransaction(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusInternalServerError, echoErr.Code)

		// Check response body
		assert.Equal(t, "PROCESSING (4): error getting transaction", echoErr.Message)
	})

	t.Run("Repository not found", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetTransaction", mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundError("transaction not found"))

		// set echo context
		echoContext.SetPath("/tx/hash/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

		// Call GetTransaction handler
		err := httpServer.GetTransaction(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusNotFound, echoErr.Code)

		// Check response body
		assert.Equal(t, "NOT_FOUND (3): transaction not found", echoErr.Message)
	})

	t.Run("Invalid read mode", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetTransaction", mock.Anything, mock.Anything).Return([]byte{0x00, 0x01, 0x02}, nil)

		// set echo context
		echoContext.SetPath("/tx/hash/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

		var (
			invalidReadMode ReadMode = 999
		)

		// Call GetTransaction handler
		err := httpServer.GetTransaction(invalidReadMode)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)

		// Check response body
		assert.Equal(t, "INVALID_ARGUMENT (1): bad read mode", echoErr.Message)
	})
}
