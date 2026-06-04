package httpimpl

import (
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestGetUTXO(t *testing.T) {
	initPrometheusMetrics()

	t.Run("Valid UTXO JSON response", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// set mock responses
		mockRepo.On("GetUtxo", mock.Anything, mock.Anything).Return(&utxo.SpendResponse{
			Status:       1,
			SpendingData: spendpkg.NewSpendingData(&chainhash.Hash{}, 0),
			LockTime:     1234567,
		}, nil)

		// set echo context with vout query parameter
		q := make(url.Values)
		q.Set("vout", "0")
		echoContext.SetPath("/utxo/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")
		echoContext.Request().URL.RawQuery = q.Encode()

		// Call GetUTXO handler
		err := httpServer.GetUTXO(JSON)(echoContext)
		require.NoError(t, err)

		// Check response status code
		assert.Equal(t, http.StatusOK, responseRecorder.Code)

		// Check response body
		var response map[string]interface{}
		if err = json.Unmarshal(responseRecorder.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}

		// Check response fields
		require.NotNil(t, response)
		assert.Equal(t, float64(1), response["status"])
		spendingDataMap, ok := response["spendingData"].(map[string]interface{})
		require.True(t, ok)

		assert.Equal(t, "0000000000000000000000000000000000000000000000000000000000000000", spendingDataMap["txId"])
		assert.Equal(t, float64(0), spendingDataMap["vin"])

		assert.Equal(t, float64(1234567), response["lockTime"])
	})

	t.Run("Valid UTXO BINARY response", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// set mock responses
		mockRepo.On("GetUtxo", mock.Anything, mock.Anything).Return(&utxo.SpendResponse{
			Status:       1,
			SpendingData: spendpkg.NewSpendingData(&chainhash.Hash{}, 0),
			LockTime:     1234567,
		}, nil)

		// set echo context with vout query parameter
		q := make(url.Values)
		q.Set("vout", "0")
		echoContext.SetPath("/utxo/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")
		echoContext.Request().URL.RawQuery = q.Encode()

		// Call GetUTXO handler
		err := httpServer.GetUTXO(BINARY_STREAM)(echoContext)
		require.NoError(t, err)

		// Check response status code
		assert.Equal(t, http.StatusOK, responseRecorder.Code)

		// Check response body
		response := utxo.SpendResponse{}
		err = response.FromBytes(responseRecorder.Body.Bytes())
		require.NoError(t, err)

		// Check response fields
		assert.Equal(t, 1, response.Status)
		assert.Equal(t, chainhash.Hash{}, *response.SpendingData.TxID)
		assert.Equal(t, uint32(1234567), response.LockTime)
	})

	t.Run("Valid UTXO HEX response", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// set mock responses
		mockRepo.On("GetUtxo", mock.Anything, mock.Anything).Return(&utxo.SpendResponse{
			Status:       1,
			SpendingData: spendpkg.NewSpendingData(&chainhash.Hash{}, 0),
			LockTime:     1234567,
		}, nil)

		// set echo context with vout query parameter
		q := make(url.Values)
		q.Set("vout", "0")
		echoContext.SetPath("/utxo/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")
		echoContext.Request().URL.RawQuery = q.Encode()

		// Call GetUTXO handler
		err := httpServer.GetUTXO(HEX)(echoContext)
		require.NoError(t, err)

		// Check response status code
		assert.Equal(t, http.StatusOK, responseRecorder.Code)
		assert.Equal(t, "text/plain; charset=UTF-8", responseRecorder.Header().Get("Content-Type"))

		// Check response body
		response := utxo.SpendResponse{}
		responseBytes, err := hex.DecodeString(responseRecorder.Body.String())
		require.NoError(t, err)

		err = response.FromBytes(responseBytes)
		require.NoError(t, err)

		// Check response fields
		assert.Equal(t, 1, response.Status)
		assert.Equal(t, chainhash.Hash{}, *response.SpendingData.TxID)
		assert.Equal(t, uint32(1234567), response.LockTime)
	})

	t.Run("Invalid UTXO hash length", func(t *testing.T) {
		httpServer, _, echoContext, _ := GetMockHTTP(t, nil)

		// set echo context
		echoContext.SetPath("/utxo/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("short")

		// Call GetUTXO handler
		err := httpServer.GetUTXO(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)

		// Check response body
		assert.Equal(t, "INVALID_ARGUMENT (1): invalid hash length", echoErr.Message)
	})

	t.Run("Invalid UTXO hash format", func(t *testing.T) {
		httpServer, _, echoContext, _ := GetMockHTTP(t, nil)

		// set echo context
		echoContext.SetPath("/utxo/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("sd45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea99t")

		// Call GetUTXO handler
		err := httpServer.GetUTXO(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)

		// Check response body
		assert.Equal(t, "INVALID_ARGUMENT (1): invalid hash format -> UNKNOWN (0): encoding/hex: invalid byte: U+0073 's'", echoErr.Message)
	})

	t.Run("UTXO not found", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// set mock responses
		mockRepo.On("GetUtxo", mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundError("UTXO not found"))

		// set echo context with vout query parameter
		q := make(url.Values)
		q.Set("vout", "0")
		echoContext.SetPath("/utxo/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")
		echoContext.Request().URL.RawQuery = q.Encode()

		// Call GetUTXO handler
		err := httpServer.GetUTXO(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusNotFound, echoErr.Code)

		// Check response body
		assert.Equal(t, "NOT_FOUND (3): UTXO not found", echoErr.Message)
	})

	t.Run("UTXO not found status", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// set mock responses
		mockRepo.On("GetUtxo", mock.Anything, mock.Anything).Return(&utxo.SpendResponse{
			Status: int(utxo.Status_NOT_FOUND),
		}, nil)

		// set echo context with vout query parameter
		q := make(url.Values)
		q.Set("vout", "0")
		echoContext.SetPath("/utxo/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")
		echoContext.Request().URL.RawQuery = q.Encode()

		// Call GetUTXO handler
		err := httpServer.GetUTXO(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusNotFound, echoErr.Code)

		// Check response body
		assert.Equal(t, "NOT_FOUND (3): UTXO not found", echoErr.Message)
	})

	t.Run("Repository error", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// set mock responses
		mockRepo.On("GetUtxo", mock.Anything, mock.Anything).Return(nil, errors.NewProcessingError("repository error"))

		// set echo context with vout query parameter
		q := make(url.Values)
		q.Set("vout", "0")
		echoContext.SetPath("/utxo/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")
		echoContext.Request().URL.RawQuery = q.Encode()

		// Call GetUTXO handler
		err := httpServer.GetUTXO(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusInternalServerError, echoErr.Code)

		// Check response body
		assert.Contains(t, echoErr.Message.(string), "repository error")
	})

	t.Run("Invalid read mode", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// set mock responses
		mockRepo.On("GetUtxo", mock.Anything, mock.Anything).Return(&utxo.SpendResponse{
			Status:       1,
			SpendingData: spendpkg.NewSpendingData(&chainhash.Hash{}, 0),
			LockTime:     1234567,
		}, nil)

		// set echo context with vout query parameter
		q := make(url.Values)
		q.Set("vout", "0")
		echoContext.SetPath("/utxo/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")
		echoContext.Request().URL.RawQuery = q.Encode()

		var (
			invalidReadMode ReadMode = 999
		)

		// Call GetUTXO handler
		err := httpServer.GetUTXO(invalidReadMode)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusInternalServerError, echoErr.Code)

		// Check response body
		assert.Equal(t, "INVALID_ARGUMENT (1): bad read mode", echoErr.Message)
	})

	t.Run("Missing vout parameter", func(t *testing.T) {
		httpServer, _, echoContext, _ := GetMockHTTP(t, nil)

		// set echo context WITHOUT vout query parameter
		echoContext.SetPath("/utxo/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

		// Call GetUTXO handler
		err := httpServer.GetUTXO(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)

		// Check response body
		assert.Equal(t, "INVALID_ARGUMENT (1): missing required query parameter: vout", echoErr.Message)
	})

	t.Run("Invalid vout parameter", func(t *testing.T) {
		httpServer, _, echoContext, _ := GetMockHTTP(t, nil)

		// set echo context with invalid vout
		q := make(url.Values)
		q.Set("vout", "invalid")
		echoContext.SetPath("/utxo/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")
		echoContext.Request().URL.RawQuery = q.Encode()

		// Call GetUTXO handler
		err := httpServer.GetUTXO(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)

		// Check response body
		assert.Contains(t, echoErr.Message.(string), "invalid vout format")
	})

	// Transaction-not-found and vout-out-of-range both manifest as a store-level
	// Status_NOT_FOUND now that the handler no longer fetches the full
	// transaction up front. They are reported uniformly as 404 "UTXO not found".
	t.Run("Transaction not found", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		mockRepo.On("GetUtxo", mock.Anything, mock.Anything).Return(&utxo.SpendResponse{
			Status: int(utxo.Status_NOT_FOUND),
		}, nil)

		q := make(url.Values)
		q.Set("vout", "0")
		echoContext.SetPath("/utxo/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")
		echoContext.Request().URL.RawQuery = q.Encode()

		err := httpServer.GetUTXO(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		assert.Equal(t, http.StatusNotFound, echoErr.Code)
		assert.Equal(t, "NOT_FOUND (3): UTXO not found", echoErr.Message)
	})

	t.Run("Vout out of range", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		mockRepo.On("GetUtxo", mock.Anything, mock.Anything).Return(&utxo.SpendResponse{
			Status: int(utxo.Status_NOT_FOUND),
		}, nil)

		q := make(url.Values)
		q.Set("vout", "5")
		echoContext.SetPath("/utxo/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")
		echoContext.Request().URL.RawQuery = q.Encode()

		err := httpServer.GetUTXO(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		assert.Equal(t, http.StatusNotFound, echoErr.Code)
		assert.Equal(t, "NOT_FOUND (3): UTXO not found", echoErr.Message)
	})

	// New: GetUTXO must not call repository.GetTransaction. This guards against
	// regressing the fast-path: the slowness in the old endpoint came from
	// fetching+parsing the full raw tx just to recompute the UTXO hash.
	t.Run("Does not call GetTransaction", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		mockRepo.On("GetUtxo", mock.Anything, mock.Anything).Return(&utxo.SpendResponse{
			Status:   int(utxo.Status_OK),
			LockTime: 42,
		}, nil)

		q := make(url.Values)
		q.Set("vout", "0")
		echoContext.SetPath("/utxo/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")
		echoContext.Request().URL.RawQuery = q.Encode()

		err := httpServer.GetUTXO(JSON)(echoContext)
		require.NoError(t, err)

		mockRepo.AssertNotCalled(t, "GetTransaction", mock.Anything, mock.Anything)
	})
}
