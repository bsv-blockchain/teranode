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

// TestGetUTXOsByTxIDOutputBudget covers asset_maxUTXOsPerTx. The output count of
// the requested transaction drives one store lookup and one retained item each,
// so a transaction with an extreme output count makes a single request
// disproportionately expensive. The setting defaults to 0 (unlimited).
func TestGetUTXOsByTxIDOutputBudget(t *testing.T) {
	initPrometheusMetrics()

	tx := bt.NewTx()
	for i := 0; i < 3; i++ {
		require.NoError(t, tx.AddP2PKHOutputFromAddress("1BitcoinEaterAddressDontSendf59kuE", 1000))
	}

	txBytes := tx.Bytes()
	txHash := tx.TxIDChainHash()

	t.Run("unset serves every output", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		mockRepo.On("GetTransaction", mock.Anything, mock.Anything).Return(txBytes, nil).Once()
		mockRepo.On("GetUtxo", mock.Anything, mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil)

		echoContext.SetPath("/utxos/:hash/json")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues(txHash.String())

		require.NoError(t, httpServer.GetUTXOsByTxID(JSON)(echoContext))
		require.Equal(t, http.StatusOK, responseRecorder.Code)

		var response []map[string]interface{}
		require.NoError(t, json.Unmarshal(responseRecorder.Body.Bytes(), &response))
		require.Len(t, response, 3)
	})

	t.Run("set rejects before any lookup", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)
		httpServer.settings.Asset.MaxUTXOsPerTx = 2

		mockRepo.On("GetTransaction", mock.Anything, mock.Anything).Return(txBytes, nil).Once()
		mockRepo.On("GetUtxo", mock.Anything, mock.Anything).Return(&utxo.SpendResponse{Status: int(utxo.Status_OK)}, nil)

		echoContext.SetPath("/utxos/:hash/json")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues(txHash.String())

		err := httpServer.GetUTXOsByTxID(JSON)(echoContext)

		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))
		require.Equal(t, http.StatusRequestEntityTooLarge, echoErr.Code)

		mockRepo.AssertNotCalled(t, "GetUtxo", mock.Anything, mock.Anything)
	})
}
