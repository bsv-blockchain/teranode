package httpimpl

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestGetSubtreeTxs(t *testing.T) {
	initPrometheusMetrics()

	t.Run("Valid JSON mode", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		mockRepo.On("GetSubtree", mock.Anything, mock.Anything).Return(testSubtree, nil)
		mockRepo.On("GetTransactionMeta", mock.Anything, mock.Anything).Return(testTxMeta, nil)

		echoContext.SetPath("/subtree/txs/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

		err := httpServer.GetSubtreeTxs(JSON)(echoContext)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, http.StatusOK, responseRecorder.Code)
		assert.Equal(t, echo.MIMEApplicationJSON, responseRecorder.Header().Get("Content-Type"))

		var response map[string]interface{}
		if err = json.Unmarshal(responseRecorder.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}

		require.NotNil(t, response)
		assert.Equal(t, float64(0), response["pagination"].(map[string]interface{})["offset"])
		assert.Equal(t, float64(20), response["pagination"].(map[string]interface{})["limit"])
		assert.Equal(t, float64(4), response["pagination"].(map[string]interface{})["totalRecords"])

		assert.Equal(t, 4, len(response["data"].([]interface{})))

		for i := 0; i < 4; i++ {
			hash, _ := chainhash.NewHashFromStr(fmt.Sprintf("%x", i))
			data := response["data"].([]interface{})[i].(map[string]interface{})
			assert.Equal(t, float64(123), data["fee"])
			assert.Equal(t, float64(321), data["size"])
			assert.Equal(t, hash.String(), data["txid"])
		}
	})

	t.Run("Invalid hash length", func(t *testing.T) {
		httpServer, _, echoContext, _ := GetMockHTTP(t, nil)

		echoContext.SetPath("/subtree/txs/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("short")

		err := httpServer.GetSubtreeTxs(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		assert.Equal(t, http.StatusBadRequest, echoErr.Code)
		assert.Equal(t, "INVALID_ARGUMENT (1): invalid hash length", echoErr.Message)
	})

	t.Run("Invalid hash string", func(t *testing.T) {
		httpServer, _, echoContext, _ := GetMockHTTP(t, nil)

		echoContext.SetPath("/subtree/txs/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("sd45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea99t")

		err := httpServer.GetSubtreeTxs(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		assert.Equal(t, http.StatusBadRequest, echoErr.Code)
		assert.Equal(t, "INVALID_ARGUMENT (1): invalid hash string -> UNKNOWN (0): encoding/hex: invalid byte: U+0073 's'", echoErr.Message)
	})

	t.Run("Invalid offset", func(t *testing.T) {
		httpServer, _, echoContext, _ := GetMockHTTP(t, nil)

		echoContext.SetPath("/subtree/txs/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")
		echoContext.QueryParams().Set("limit", "125")
		echoContext.QueryParams().Set("offset", "invalid")

		err := httpServer.GetSubtreeTxs(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		assert.Equal(t, http.StatusBadRequest, echoErr.Code)
		assert.Equal(t, "INVALID_ARGUMENT (1): invalid offset format -> UNKNOWN (0): strconv.Atoi: parsing \"invalid\": invalid syntax", echoErr.Message)
	})

	t.Run("Invalid limit", func(t *testing.T) {
		httpServer, _, echoContext, _ := GetMockHTTP(t, nil)

		echoContext.SetPath("/subtree/txs/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")
		echoContext.QueryParams().Set("limit", "invalid")
		echoContext.QueryParams().Set("offset", "100")

		err := httpServer.GetSubtreeTxs(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		assert.Equal(t, http.StatusBadRequest, echoErr.Code)
		assert.Equal(t, "INVALID_ARGUMENT (1): invalid limit format -> UNKNOWN (0): strconv.Atoi: parsing \"invalid\": invalid syntax", echoErr.Message)
	})

	t.Run("Subtree not found", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		mockRepo.On("GetSubtree", mock.Anything, mock.Anything).Return(nil, errors.ErrNotFound)

		echoContext.SetPath("/subtree/txs/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

		err := httpServer.GetSubtreeTxs(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		assert.Equal(t, http.StatusNotFound, echoErr.Code)
		assert.Equal(t, "NOT_FOUND (3): not found", echoErr.Message)
	})

	t.Run("Transaction meta not found", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		mockRepo.On("GetSubtree", mock.Anything, mock.Anything).Return(testSubtree, nil)
		mockRepo.On("GetTransactionMeta", mock.Anything, mock.Anything).Return(nil, errors.ErrNotFound)

		echoContext.SetPath("/subtree/txs/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

		err := httpServer.GetSubtreeTxs(JSON)(echoContext)
		require.NoError(t, err)

		assert.Equal(t, http.StatusOK, responseRecorder.Code)
		assert.Equal(t, echo.MIMEApplicationJSON, responseRecorder.Header().Get("Content-Type"))

		var response map[string]interface{}
		if err = json.Unmarshal(responseRecorder.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}

		require.NotNil(t, response)
		assert.Equal(t, float64(0), response["pagination"].(map[string]interface{})["offset"])
		assert.Equal(t, float64(20), response["pagination"].(map[string]interface{})["limit"])
		assert.Equal(t, float64(4), response["pagination"].(map[string]interface{})["totalRecords"])

		assert.Equal(t, 0, len(response["data"].([]interface{})))
	})

	t.Run("Repository error", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		mockRepo.On("GetSubtree", mock.Anything, mock.Anything).Return(nil, errors.NewProcessingError("error getting subtree transactions"))

		echoContext.SetPath("/subtree/txs/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

		err := httpServer.GetSubtreeTxs(JSON)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		assert.Equal(t, http.StatusInternalServerError, echoErr.Code)
		assert.Equal(t, "PROCESSING (4): error getting subtree transactions", echoErr.Message)
	})

	t.Run("Invalid read mode", func(t *testing.T) {
		httpServer, _, echoContext, _ := GetMockHTTP(t, nil)

		echoContext.SetPath("/subtree/txs/:hash")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

		var invalidReadMode ReadMode = 999

		err := httpServer.GetSubtreeTxs(invalidReadMode)(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		assert.Equal(t, http.StatusInternalServerError, echoErr.Code)
		assert.Equal(t, "INVALID_ARGUMENT (1): bad read mode", echoErr.Message)
	})
}
