package httpimpl

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/asset/repository"
	"github.com/bsv-blockchain/teranode/stores/blob"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
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

// TestGetTransaction_MarksItsOwnNotFound pins the header that tells a caller this
// 404 is the handler's verdict on the transaction, not a route that failed to
// match. getrawtransaction answers -5 only for a marked 404; without the mark a
// misconfigured asset URL would read as every transaction being unknown.
func TestGetTransaction_MarksItsOwnNotFound(t *testing.T) {
	initPrometheusMetrics()

	for name, tc := range map[string]struct {
		err    error
		status int
		marked bool
	}{
		"the transaction is not held": {errors.NewNotFoundError("transaction not found"), http.StatusNotFound, true},
		"the repository failed":       {errors.NewProcessingError("error getting transaction"), http.StatusInternalServerError, false},
		// A store fault can say "not found" without being a miss: a misconfigured
		// aerospike namespace does. The class decides the 404, never the text, so
		// it is a fault for every caller, not only for getrawtransaction.
		"a store fault whose text says not found": {
			errors.NewServiceError("aerospike get failed: Namespace not found"), http.StatusInternalServerError, false,
		},
		"a processing error whose text says not found": {
			errors.NewProcessingError("record not found in the index"), http.StatusInternalServerError, false,
		},
		// The repository counts a tx store's blob not-found as a miss, as it does
		// ErrNotFound, so it is the same verdict.
		"the tx store has no blob": {errors.NewBlobNotFoundError("blob not found"), http.StatusNotFound, true},
		// The class decides the 404, not the wording.
		"a blob miss whose text does not say so": {errors.NewBlobNotFoundError("no tx blob for this hash"), http.StatusNotFound, true},
		// A store that calls its not-found a storage failure has failed while it
		// looked; that is a fault, not an answer about the transaction. Its class
		// keeps the 404 for callers that read the status alone, unmarked.
		"a storage error over a not-found": {
			errors.NewStorageError("tx store read failed", errors.NewNotFoundError("not found")), http.StatusNotFound, false,
		},
		// Outside the tx-store miss set, the only set the repository vets against a
		// UTXO fault, so it cannot be told from a fault. Only its text earned it a
		// 404. The repository never returns it: it drops the UTXO store's error and
		// no tx store reports a miss with this class.
		"a tx not-found the repository does not vet": {errors.NewTxNotFoundError("tx not found"), http.StatusInternalServerError, false},
	} {
		t.Run(name, func(t *testing.T) {
			httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)
			mockRepo.On("GetTransaction", mock.Anything, mock.Anything).Return(nil, tc.err)

			echoContext.SetPath("/tx/:hash/hex")
			echoContext.SetParamNames("hash")
			echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

			err := httpServer.GetTransaction(HEX)(echoContext)
			echoErr := &echo.HTTPError{}
			require.True(t, errors.As(err, &echoErr))
			require.Equal(t, tc.status, echoErr.Code)

			if tc.marked {
				require.Equal(t, util.AssetNotFoundTx, responseRecorder.Header().Get(util.AssetNotFoundHeader))
			} else {
				require.Empty(t, responseRecorder.Header().Get(util.AssetNotFoundHeader),
					"only a verdict that the transaction is absent is marked")
			}
		})
	}
}

// TestGetTransaction_MarksAMissBehindAnHTTPTxStore drives the handler over a real
// repository: a UTXO store that does not hold the transaction and an http:// tx store.
// When the blob server answers that it does not hold the blob either, that is a miss in
// both, so the 404 is marked; the tx store's error reaches isTransactionMiss as the
// repository returns it. Anything else answering on the tx store's address, or a blob
// server that fails, is a fault and must not deny the transaction.
func TestGetTransaction_MarksAMissBehindAnHTTPTxStore(t *testing.T) {
	initPrometheusMetrics()

	logger := ulogger.TestLogger{}

	for name, tc := range map[string]struct {
		txStore func(t *testing.T) http.Handler
		status  int
		marked  bool
	}{
		"the blob server does not hold it": {
			txStore: func(t *testing.T) http.Handler {
				memoryURL, err := url.Parse("memory://")
				require.NoError(t, err)

				blobServer, err := blob.NewHTTPBlobServer(logger, memoryURL, "")
				require.NoError(t, err)

				return blobServer
			},
			status: http.StatusNotFound,
			marked: true,
		},
		// A wrong base URL or a proxy: a 404 that is not the blob server's verdict.
		"something else answers 404": {
			txStore: func(*testing.T) http.Handler { return http.NotFoundHandler() },
			status:  http.StatusInternalServerError,
		},
		"the blob server fails": {
			txStore: func(*testing.T) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					http.Error(w, "disk read failed", http.StatusInternalServerError)
				})
			},
			status: http.StatusInternalServerError,
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			tSettings := test.CreateBaseTestSettings(t)

			blobServer := httptest.NewServer(tc.txStore(t))
			t.Cleanup(blobServer.Close)

			txStoreURL, err := url.Parse(blobServer.URL)
			require.NoError(t, err)

			txStore, err := blob.NewStore(logger, txStoreURL)
			require.NoError(t, err)

			utxoStoreURL, err := url.Parse("sqlitememory:///" + t.Name())
			require.NoError(t, err)

			utxoStore, err := utxosql.New(ctx, logger, tSettings, utxoStoreURL)
			require.NoError(t, err)
			t.Cleanup(func() { _ = utxoStore.Close(ctx) })

			repo, err := repository.NewRepository(logger, tSettings, utxoStore, txStore, nil, nil, nil, nil, nil, nil)
			require.NoError(t, err)

			httpServer, _, echoContext, responseRecorder := GetMockHTTP(t, nil)
			httpServer.repository = repo

			echoContext.SetPath("/tx/:hash/hex")
			echoContext.SetParamNames("hash")
			echoContext.SetParamValues("9d45ad79ad3c6baecae872c0e35022d60c3bbbd024ccce06690321ece15ea995")

			err = httpServer.GetTransaction(HEX)(echoContext)
			echoErr := &echo.HTTPError{}
			require.True(t, errors.As(err, &echoErr))
			require.Equal(t, tc.status, echoErr.Code)

			if tc.marked {
				require.Equal(t, util.AssetNotFoundTx, responseRecorder.Header().Get(util.AssetNotFoundHeader))
			} else {
				require.Empty(t, responseRecorder.Header().Get(util.AssetNotFoundHeader))
			}
		})
	}
}
