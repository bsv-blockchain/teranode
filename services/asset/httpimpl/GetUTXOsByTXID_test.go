package httpimpl

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestGetUTXOsByTxID(t *testing.T) {
	initPrometheusMetrics()

	t.Run("Valid transaction hash", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetTransaction", mock.Anything, mock.Anything).Return(testTX1RawBytes, nil).Once()
		mockRepo.On("GetUtxo", mock.Anything, mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil).Once()

		// set echo context
		echoContext.SetPath("/utxos/txid/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues(testTX1Hash.String())

		// Call GetUTXOsByTxID handler
		err := httpServer.GetUTXOsByTxID(JSON)(echoContext)
		if err != nil {
			t.Fatal(err)
		}

		// Check response status code
		assert.Equal(t, http.StatusOK, responseRecorder.Code)

		// Check response body
		var response []map[string]interface{}
		if err = json.Unmarshal(responseRecorder.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}

		tx1, err := bt.NewTxFromBytes(testTX1RawBytes)
		require.NoError(t, err)

		// Check response fields
		require.NotNil(t, response)
		assert.Equal(t, testTX1Hash.String(), response[0]["txid"])
		assert.Equal(t, float64(0), response[0]["vout"])
		assert.Equal(t, tx1.Outputs[0].LockingScript.String(), response[0]["lockingScript"])
		assert.Equal(t, float64(tx1.Outputs[0].Satoshis), response[0]["satoshis"])
		assert.Equal(t, "29e0a7bea903237deb8c905aa6b578aac8a8d8a70b8d3c332812e1c9f728fa6e", response[0]["utxoHash"])
		assert.Equal(t, "OK", response[0]["status"])
	})

	t.Run("Invalid transaction hash length", func(t *testing.T) {
		httpServer, _, echoContext, _ := GetMockHTTP(t, nil)

		// set echo context
		echoContext.SetPath("/utxos/txid/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("short")

		// Call GetUTXOsByTxID handler
		err := httpServer.GetUTXOsByTxID(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusInternalServerError, echoErr.Code)

		// Check response body
		assert.Equal(t, "INVALID_ARGUMENT (1): invalid transaction hash length", echoErr.Message)
	})

	t.Run("Transaction not found", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetTransaction", mock.Anything, mock.Anything).Return(nil, errors.NewTxNotFoundError("transaction not found"))

		// set echo context
		echoContext.SetPath("/utxos/txid/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("29e0a7bea903237deb8c905aa6b578aac8a8d8a70b8d3c332812e1c9f728fa6e")

		// Call GetUTXOsByTxID handler
		err := httpServer.GetUTXOsByTxID(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusNotFound, echoErr.Code)

		// Check response body
		assert.Equal(t, "TX_NOT_FOUND (30): transaction not found", echoErr.Message)
	})

	t.Run("Invalid transaction hash format", func(t *testing.T) {
		httpServer, _, echoContext, _ := GetMockHTTP(t, nil)

		// set echo context
		echoContext.SetPath("/utxos/txid/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("s9e0a7bea903237deb8c905aa6b578aac8a8d8a70b8d3c332812e1c9f728fa6t")

		// Call GetUTXOsByTxID handler
		err := httpServer.GetUTXOsByTxID(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusInternalServerError, echoErr.Code)

		// Check response body
		assert.Equal(t, "INVALID_ARGUMENT (1): invalid transaction hash format -> UNKNOWN (0): encoding/hex: invalid byte: U+0073 's'", echoErr.Message)
	})

	t.Run("Repository error", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetTransaction", mock.Anything, mock.Anything).Return(nil, errors.NewStorageError("error getting transaction"))

		// set echo context
		echoContext.SetPath("/utxos/txid/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("29e0a7bea903237deb8c905aa6b578aac8a8d8a70b8d3c332812e1c9f728fa6e")

		// Call GetUTXOsByTxID handler
		err := httpServer.GetUTXOsByTxID(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusInternalServerError, echoErr.Code)

		// Check response body
		assert.Equal(t, "STORAGE_ERROR (69): error getting transaction", echoErr.Message)
	})
}
