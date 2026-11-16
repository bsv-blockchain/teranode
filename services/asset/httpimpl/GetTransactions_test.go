package httpimpl

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/asset/repository"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestGetTransactions(t *testing.T) {
	initPrometheusMetrics()

	t.Run("Valid transaction hashes with subtree hash", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		subtreeHash := chainhash.HashH([]byte("subtreeHash"))

		// set mock response
		mockRepo.On("GetTransaction", mock.Anything, mock.Anything).Return(testTX1RawBytes, nil).Once()
		mockRepo.On("GetTransaction", mock.Anything, mock.Anything).Return(testTX2RawBytes, nil).Once()
		mockSubtreeWithoutData(mockRepo, *testTX1Hash, *testTX2Hash)

		transactionHashes := append(testTX1Hash.CloneBytes(), testTX2Hash.CloneBytes()...)

		// Set up the request with subtree hash
		echoContext.SetPath("/subtree/:hash/txs")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues(subtreeHash.String())
		echoContext.Request().Body = io.NopCloser(bytes.NewReader(transactionHashes))

		// Call GetTransactions handler
		err := httpServer.GetTransactions()(echoContext)
		require.NoError(t, err)

		// Check response status code
		assert.Equal(t, http.StatusOK, responseRecorder.Code)

		// Verify transactions using helper function
		verifyTransactions(t, bytes.NewReader(responseRecorder.Body.Bytes()), testTX1Hash.String(), testTX2Hash.String())
	})

	t.Run("With invalid subtree hash", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		subtreeHash := chainhash.HashH([]byte("subtreeHash"))

		// set mock response
		mockRepo.On("GetSubtreeExists", mock.Anything, mock.Anything).Return(false, nil).Once()
		mockRepo.On("GetSubtreeExists", mock.Anything, mock.Anything).Return(true, nil)

		// set echo context
		echoContext.Request().Header.Set(echo.HeaderContentType, echo.MIMEOctetStream)
		echoContext.SetPath("/:hash/txs")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues(subtreeHash.String())

		// Call GetTransactions handler
		err := httpServer.GetTransactions()(echoContext)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "NOT_FOUND")

		echoContext.SetParamValues("test")

		// Call GetTransactions handler
		err = httpServer.GetTransactions()(echoContext)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid subtree hash length")

		echoContext.SetParamValues("testtesttesttesttesttesttesttesttesttesttesttesttesttesttesttest")

		// Call GetTransactions handler
		err = httpServer.GetTransactions()(echoContext)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid subtree hash string")
	})

	t.Run("Invalid transaction hash length", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		subtreeHash := chainhash.HashH([]byte("subtreeHash"))

		mockRepo.On("GetTransaction", mock.Anything, mock.Anything).Return(testTX1RawBytes, nil)
		mockRepo.On("GetSubtreeExists", mock.Anything, mock.Anything).Return(true, nil).Once()
		mockRepo.On("GetSubtreeTransactions", mock.Anything, mock.Anything).Return(make(map[chainhash.Hash]*bt.Tx), nil)

		// set echo context
		echoContext.Request().Header.Set(echo.HeaderContentType, echo.MIMEOctetStream)
		echoContext.SetPath("/subtree/:hash/txs")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues(subtreeHash.String())

		echoContext.Request().Body = io.NopCloser(bytes.NewReader([]byte{0x01, 0x02, 0x03, 0x04}))

		// Call GetTransactions handler
		err := httpServer.GetTransactions()(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// A body whose length isn't a whole multiple of the hash size is a
		// client error, not a transport/server error.
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)
		assert.Equal(t, "INVALID_ARGUMENT (1): request body length is not a multiple of 32 bytes", echoErr.Message)
	})

	t.Run("Transaction not found", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		subtreeHash := chainhash.HashH([]byte("subtreeHash"))

		// set mock response
		mockRepo.On("GetTransaction", mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundError("transaction not found"))
		mockSubtreeWithoutData(mockRepo, *testTX1Hash)

		// set echo context
		echoContext.Request().Header.Set(echo.HeaderContentType, echo.MIMEOctetStream)
		echoContext.SetPath("/subtree/:hash/txs")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues(subtreeHash.String())

		echoContext.Request().Body = io.NopCloser(bytes.NewReader(testTX1Hash.CloneBytes()))

		// Call GetTransactions handler
		err := httpServer.GetTransactions()(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusNotFound, echoErr.Code)

		// Check response body
		assert.Equal(t, "NOT_FOUND (3): transaction not found -> NOT_FOUND (3): transaction not found", echoErr.Message)
	})

	t.Run("Repository error", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		subtreeHash := chainhash.HashH([]byte("subtreeHash"))

		// set mock response
		mockRepo.On("GetTransaction", mock.Anything, mock.Anything).Return(nil, errors.NewStorageError("error getting transaction"))
		mockSubtreeWithoutData(mockRepo, *testTX1Hash)

		// set echo context
		echoContext.Request().Header.Set(echo.HeaderContentType, echo.MIMEOctetStream)
		echoContext.SetPath("/subtree/:hash/txs")
		echoContext.SetParamNames("hash")
		echoContext.SetParamValues(subtreeHash.String())

		echoContext.Request().Body = io.NopCloser(bytes.NewReader(testTX1Hash.CloneBytes()))

		// Call GetTransactions handler
		err := httpServer.GetTransactions()(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusInternalServerError, echoErr.Code)

		// Check response body
		assert.Equal(t, "PROCESSING (4): error getting transaction -> STORAGE_ERROR (69): error getting transaction", echoErr.Message)
	})

	t.Run("missing subtree hash is rejected", func(t *testing.T) {
		httpServer, _, echoContext, _ := GetMockHTTP(t, nil)

		echoContext.Request().Body = io.NopCloser(bytes.NewReader(testTX1Hash.CloneBytes()))

		err := httpServer.GetTransactions()(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))
		assert.Equal(t, http.StatusBadRequest, echoErr.Code)
	})
}

// setSubtreeRoute points echoContext at the real POST /subtree/:hash/txs route.
func setSubtreeRoute(c echo.Context, hash *chainhash.Hash) {
	c.SetPath("/subtree/:hash/txs")
	c.SetParamNames("hash")
	c.SetParamValues(hash.String())
}

// mockSubtreeWithoutData models a subtree whose .subtreeData is not stored yet,
// which is what peer catchup meets on a live subtree: GetSubtreeTransactions
// fails, so every hash falls through to the GetTransaction store lookup, and
// members are the node hashes the membership check streams from the subtree.
func mockSubtreeWithoutData(mockRepo *repository.Mock, members ...chainhash.Hash) {
	mockRepo.On("GetSubtreeExists", mock.Anything, mock.Anything).Return(true, nil).Once()
	mockRepo.On("GetSubtreeTransactions", mock.Anything, mock.Anything).Return(nil, errors.ErrNotFound)
	mockSubtreeNodeHashes(mockRepo, members...)
}

// mockSubtreeNodeHashes makes GetSubtreeNodeHashesReader stream members as the
// subtree's node hashes.
func mockSubtreeNodeHashes(mockRepo *repository.Mock, members ...chainhash.Hash) {
	mockRepo.On("GetSubtreeNodeHashesReader", mock.Anything).Return(io.NopCloser(bytes.NewReader(concatHashes(members))), nil)
}

// distinctHashes returns n different hashes. The store mocks answer any hash, so
// these stand in for n different transactions of a subtree.
func distinctHashes(n int) []chainhash.Hash {
	hashes := make([]chainhash.Hash, n)

	for i := range hashes {
		var seed [8]byte

		binary.LittleEndian.PutUint64(seed[:], uint64(i))
		hashes[i] = chainhash.HashH(seed[:])
	}

	return hashes
}

func concatHashes(hashes []chainhash.Hash) []byte {
	b := make([]byte, 0, len(hashes)*chainhash.HashSize)

	for i := range hashes {
		b = append(b, hashes[i][:]...)
	}

	return b
}

// verifyTransactions verifies that the response contains the expected transactions
func verifyTransactions(t *testing.T, responseBody io.Reader, expectedHashes ...string) {
	t.Helper()

	// Convert expected hashes to a map for easier lookup
	expected := make(map[string]bool)
	for _, h := range expectedHashes {
		expected[h] = true
	}

	// Read transactions from response
	var foundHashes []string

	reader := responseBody

	for {
		tx := &bt.Tx{}

		_, err := tx.ReadFrom(reader)
		if err == io.EOF {
			break
		}

		require.NoError(t, err, "Failed to read transaction from response")

		foundHashes = append(foundHashes, tx.TxIDChainHash().String())
	}

	// Verify we found all expected hashes
	for _, hash := range foundHashes {
		_, exists := expected[hash]
		assert.True(t, exists, "Unexpected transaction hash: %s", hash)
		delete(expected, hash)
	}

	assert.Empty(t, expected, "Did not receive all expected transaction hashes")
}

// TestGetTransactionsCatchupBatch is the regression that matters most on this
// route: POST /subtree/:hash/txs is the peer-catchup path and subtree validation
// posts subtreevalidation_missingTransactionsBatchSize txids (16384 by default)
// in a single request. A rejection here is never retried and demotes the honest
// peer, so a full-size batch must still succeed. This test runs with a zero
// Settings (no record cap); TestGetTransactionsDefaultRecordCap covers the
// shipped 16384 default.
func TestGetTransactionsCatchupBatch(t *testing.T) {
	initPrometheusMetrics()

	const catchupBatchSize = 16384

	httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

	subtreeHash := chainhash.HashH([]byte("catchupSubtree"))
	txMap := make(map[chainhash.Hash]*bt.Tx, catchupBatchSize)
	body := make([]byte, 0, catchupBatchSize*chainhash.HashSize)

	// A subtree never lists a tx twice, so a real batch is catchupBatchSize
	// different transactions: vary the lock time to get distinct txids.
	for i := 0; i < catchupBatchSize; i++ {
		tx, err := bt.NewTxFromBytes(testTX1RawBytes)
		require.NoError(t, err)

		tx.LockTime = uint32(i)
		txMap[*tx.TxIDChainHash()] = tx
		body = append(body, tx.TxIDChainHash().CloneBytes()...)
	}

	require.Len(t, txMap, catchupBatchSize)

	mockRepo.On("GetSubtreeExists", mock.Anything, mock.Anything).Return(true, nil).Once()
	mockRepo.On("GetSubtreeTransactions", mock.Anything, mock.Anything).Return(txMap, nil).Once()

	echoContext.SetPath("/subtree/:hash/txs")
	echoContext.SetParamNames("hash")
	echoContext.SetParamValues(subtreeHash.String())
	echoContext.Request().Body = io.NopCloser(bytes.NewReader(body))

	require.NoError(t, httpServer.GetTransactions()(echoContext))
	require.Equal(t, http.StatusOK, responseRecorder.Code)
	require.Len(t, responseRecorder.Body.Bytes(), catchupBatchSize*len(testTX1RawBytes))
}

// TestGetTransactionsEmptyBodySkipsSubtreeLoad covers the empty-body vector: the
// full subtree transaction map used to be materialized from the path hash alone,
// before a single body byte was read, so a caller paid nothing for it.
func TestGetTransactionsEmptyBodySkipsSubtreeLoad(t *testing.T) {
	initPrometheusMetrics()

	httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

	subtreeHash := chainhash.HashH([]byte("emptyBodySubtree"))

	mockRepo.On("GetSubtreeExists", mock.Anything, mock.Anything).Return(true, nil).Once()
	mockRepo.On("GetSubtreeTransactions", mock.Anything, mock.Anything).Return(make(map[chainhash.Hash]*bt.Tx), nil)

	echoContext.SetPath("/subtree/:hash/txs")
	echoContext.SetParamNames("hash")
	echoContext.SetParamValues(subtreeHash.String())
	echoContext.Request().Body = io.NopCloser(bytes.NewReader(nil))

	require.NoError(t, httpServer.GetTransactions()(echoContext))
	require.Equal(t, http.StatusOK, responseRecorder.Code)
	require.Empty(t, responseRecorder.Body.Bytes())

	mockRepo.AssertNotCalled(t, "GetSubtreeTransactions", mock.Anything, mock.Anything)
}

// TestGetTransactionsRecordBudget checks asset_maxBatchRecords, which defaults to
// 16384 (the catchup batch size); an explicit value below the catchup floor is
// raised to it.
func TestGetTransactionsRecordBudget(t *testing.T) {
	initPrometheusMetrics()

	t.Run("unset accepts any count", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		subtreeHash := chainhash.HashH([]byte("recordBudgetUnset"))
		setSubtreeRoute(echoContext, &subtreeHash)
		hashes := distinctHashes(8)
		mockSubtreeWithoutData(mockRepo, hashes...)
		mockRepo.On("GetTransaction", mock.Anything, mock.Anything).Return(testTX1RawBytes, nil)

		echoContext.Request().Body = io.NopCloser(bytes.NewReader(concatHashes(hashes)))

		require.NoError(t, httpServer.GetTransactions()(echoContext))
		require.Equal(t, http.StatusOK, responseRecorder.Code)
	})

	t.Run("set rejects an over-budget batch", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)
		httpServer.settings.Asset.MaxBatchRecords = 4

		subtreeHash := chainhash.HashH([]byte("recordBudgetOverBudget"))
		setSubtreeRoute(echoContext, &subtreeHash)
		hashes := distinctHashes(5)
		mockSubtreeWithoutData(mockRepo, hashes...)
		mockRepo.On("GetTransaction", mock.Anything, mock.Anything).Return(testTX1RawBytes, nil)

		echoContext.Request().Body = io.NopCloser(bytes.NewReader(concatHashes(hashes)))

		err := httpServer.GetTransactions()(echoContext)

		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))
		require.Equal(t, http.StatusRequestEntityTooLarge, echoErr.Code)

		mockRepo.AssertNotCalled(t, "GetSubtreeTransactions", mock.Anything, mock.Anything)
	})

	t.Run("never enforced below the catchup batch size", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)
		httpServer.settings.Asset.MaxBatchRecords = 4
		httpServer.settings.SubtreeValidation.MissingTransactionsBatchSize = 16384

		subtreeHash := chainhash.HashH([]byte("recordBudgetFloor"))
		setSubtreeRoute(echoContext, &subtreeHash)
		hashes := distinctHashes(64)
		mockSubtreeWithoutData(mockRepo, hashes...)
		mockRepo.On("GetTransaction", mock.Anything, mock.Anything).Return(testTX1RawBytes, nil)

		echoContext.Request().Body = io.NopCloser(bytes.NewReader(concatHashes(hashes)))

		require.NoError(t, httpServer.GetTransactions()(echoContext))
		require.Equal(t, http.StatusOK, responseRecorder.Code)
	})
}

// TestGetTransactionsDefaultRecordCap pins asset_maxBatchRecords' shipped default
// (16384, matching subtreevalidation_missingTransactionsBatchSize's own default):
// a batch one over that default is rejected with 413 before GetSubtreeTransactions
// (and the node-wide permit it takes) is ever reached, and a batch at exactly the
// default succeeds.
func TestGetTransactionsDefaultRecordCap(t *testing.T) {
	initPrometheusMetrics()

	const shippedDefault = 16384

	t.Run("one over the default is rejected before the permit is taken", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)
		httpServer.settings.Asset.MaxBatchRecords = shippedDefault
		httpServer.settings.SubtreeValidation.MissingTransactionsBatchSize = shippedDefault

		subtreeHash := chainhash.HashH([]byte("defaultCapOverBudget"))
		setSubtreeRoute(echoContext, &subtreeHash)
		mockRepo.On("GetSubtreeExists", mock.Anything, mock.Anything).Return(true, nil).Once()

		echoContext.Request().Body = io.NopCloser(bytes.NewReader(bytes.Repeat(testTX1Hash.CloneBytes(), shippedDefault+1)))

		err := httpServer.GetTransactions()(echoContext)

		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))
		require.Equal(t, http.StatusRequestEntityTooLarge, echoErr.Code)

		mockRepo.AssertNotCalled(t, "GetSubtreeTransactions", mock.Anything, mock.Anything)
	})

	t.Run("exactly the default is accepted", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)
		httpServer.settings.Asset.MaxBatchRecords = shippedDefault
		httpServer.settings.SubtreeValidation.MissingTransactionsBatchSize = shippedDefault

		subtreeHash := chainhash.HashH([]byte("defaultCapExact"))
		setSubtreeRoute(echoContext, &subtreeHash)
		hashes := distinctHashes(shippedDefault)
		mockSubtreeWithoutData(mockRepo, hashes...)
		mockRepo.On("GetTransaction", mock.Anything, mock.Anything).Return(testTX1RawBytes, nil)

		echoContext.Request().Body = io.NopCloser(bytes.NewReader(concatHashes(hashes)))

		require.NoError(t, httpServer.GetTransactions()(echoContext))
		require.Equal(t, http.StatusOK, responseRecorder.Code)
	})
}

// TestGetTransactionsResponseByteBudget checks asset_maxBatchResponseBytes. A
// record cap alone does not bound output: each record expands into a full
// transaction.
func TestGetTransactionsResponseByteBudget(t *testing.T) {
	initPrometheusMetrics()

	httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)
	httpServer.settings.Asset.MaxBatchResponseBytes = int64(len(testTX1RawBytes))

	subtreeHash := chainhash.HashH([]byte("responseByteBudget"))
	setSubtreeRoute(echoContext, &subtreeHash)
	hashes := distinctHashes(4)
	mockSubtreeWithoutData(mockRepo, hashes...)
	mockRepo.On("GetTransaction", mock.Anything, mock.Anything).Return(testTX1RawBytes, nil)

	echoContext.Request().Body = io.NopCloser(bytes.NewReader(concatHashes(hashes)))

	err := httpServer.GetTransactions()(echoContext)

	echoErr := &echo.HTTPError{}
	require.True(t, errors.As(err, &echoErr))
	require.Equal(t, http.StatusRequestEntityTooLarge, echoErr.Code)
}

// TestGetTransactionsResponseByteBudgetFloor checks that
// asset_maxBatchResponseBytes, like asset_maxBatchRecords, is never enforced
// below the response size a maximal catchup batch can produce: a value set well
// under subtreevalidation_missingTransactionsBatchSize * catchupResponseBytesPerTx
// must not reject a batch that fits under the floor.
func TestGetTransactionsResponseByteBudgetFloor(t *testing.T) {
	initPrometheusMetrics()

	httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)
	httpServer.settings.Asset.MaxBatchResponseBytes = 1
	httpServer.settings.SubtreeValidation.MissingTransactionsBatchSize = 16384

	subtreeHash := chainhash.HashH([]byte("responseByteBudgetFloor"))
	setSubtreeRoute(echoContext, &subtreeHash)
	hashes := distinctHashes(4)
	mockSubtreeWithoutData(mockRepo, hashes...)
	mockRepo.On("GetTransaction", mock.Anything, mock.Anything).Return(testTX1RawBytes, nil)

	echoContext.Request().Body = io.NopCloser(bytes.NewReader(concatHashes(hashes)))

	require.NoError(t, httpServer.GetTransactions()(echoContext))
	require.Equal(t, http.StatusOK, responseRecorder.Code)
}

// TestGetTransactionsRejectsDuplicateTxids covers one txid repeated up to the
// record cap. Each repeat used to expand into its own copy of the transaction,
// so a 32-byte record bought a full transaction of response, held in memory.
// Honest catchup never sends a txid twice in a batch: checkSubtreeDuplicates
// rejects such a subtree before its missing transactions are fetched.
func TestGetTransactionsRejectsDuplicateTxids(t *testing.T) {
	initPrometheusMetrics()

	const shippedDefault = 16384

	tests := []struct {
		name string
		body []byte
	}{
		{"one txid repeated up to the default record cap", bytes.Repeat(testTX1Hash.CloneBytes(), shippedDefault)},
		{"a repeat that is not adjacent", concatHashes([]chainhash.Hash{*testTX1Hash, *testTX2Hash, *testTX1Hash})},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)
			httpServer.settings.Asset.MaxBatchRecords = shippedDefault
			httpServer.settings.SubtreeValidation.MissingTransactionsBatchSize = shippedDefault

			subtreeHash := chainhash.HashH([]byte("duplicateTxids"))
			setSubtreeRoute(echoContext, &subtreeHash)
			mockSubtreeWithoutData(mockRepo, *testTX1Hash, *testTX2Hash)
			mockRepo.On("GetTransaction", mock.Anything).Return(testTX1RawBytes, nil)

			echoContext.Request().Body = io.NopCloser(bytes.NewReader(tt.body))

			err := httpServer.GetTransactions()(echoContext)

			echoErr := &echo.HTTPError{}
			require.ErrorAs(t, err, &echoErr)
			require.Equal(t, http.StatusBadRequest, echoErr.Code)
			require.Contains(t, echoErr.Message, "duplicate transaction hash")

			// Rejected from the body alone: no permit, no lookup.
			mockRepo.AssertNotCalled(t, "GetSubtreeTransactions", mock.Anything, mock.Anything)
			mockRepo.AssertNotCalled(t, "GetTransaction", mock.Anything)
		})
	}
}

// TestGetTransactionsServesOnlySubtreeMembers covers txids that are not in the
// subtree the path names. The route used to answer any transaction the node
// stores, so a batch of distinct large transactions from anywhere bought the
// same response as one repeated txid.
func TestGetTransactionsServesOnlySubtreeMembers(t *testing.T) {
	initPrometheusMetrics()

	requireNotInSubtree := func(t *testing.T, err error) {
		t.Helper()

		echoErr := &echo.HTTPError{}
		require.ErrorAs(t, err, &echoErr)
		require.Equal(t, http.StatusNotFound, echoErr.Code)
		require.Contains(t, echoErr.Message, "not in subtree")
	}

	t.Run("without subtree data a non-member is refused before any store lookup", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		subtreeHash := chainhash.HashH([]byte("nonMemberNoData"))
		setSubtreeRoute(echoContext, &subtreeHash)
		mockSubtreeWithoutData(mockRepo, *testTX1Hash)
		mockRepo.On("GetTransaction", mock.Anything).Return(testTX2RawBytes, nil)

		echoContext.Request().Body = io.NopCloser(bytes.NewReader(concatHashes([]chainhash.Hash{*testTX1Hash, *testTX2Hash})))

		requireNotInSubtree(t, httpServer.GetTransactions()(echoContext))
		mockRepo.AssertNotCalled(t, "GetTransaction", mock.Anything)
	})

	t.Run("with subtree data a hash missing from it is checked, not looked up in the store", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		subtreeHash := chainhash.HashH([]byte("nonMemberWithData"))
		setSubtreeRoute(echoContext, &subtreeHash)
		mockRepo.On("GetSubtreeExists", mock.Anything, mock.Anything).Return(true, nil).Once()
		mockRepo.On("GetSubtreeTransactions", mock.Anything, mock.Anything).
			Return(map[chainhash.Hash]*bt.Tx{*testTX1Hash: testTx1}, nil)
		mockSubtreeNodeHashes(mockRepo, *testTX1Hash)
		mockRepo.On("GetTransaction", mock.Anything).Return(testTX2RawBytes, nil)

		echoContext.Request().Body = io.NopCloser(bytes.NewReader(concatHashes([]chainhash.Hash{*testTX1Hash, *testTX2Hash})))

		requireNotInSubtree(t, httpServer.GetTransactions()(echoContext))
		mockRepo.AssertNotCalled(t, "GetTransaction", mock.Anything)
	})

	t.Run("a member missing from the subtree data is served from the store", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		subtreeHash := chainhash.HashH([]byte("memberMissingFromData"))
		setSubtreeRoute(echoContext, &subtreeHash)
		mockRepo.On("GetSubtreeExists", mock.Anything, mock.Anything).Return(true, nil).Once()
		mockRepo.On("GetSubtreeTransactions", mock.Anything, mock.Anything).
			Return(map[chainhash.Hash]*bt.Tx{*testTX1Hash: testTx1}, nil)
		mockSubtreeNodeHashes(mockRepo, *testTX1Hash, *testTX2Hash)
		mockRepo.On("GetTransaction", mock.Anything).Return(testTX2RawBytes, nil).Once()

		echoContext.Request().Body = io.NopCloser(bytes.NewReader(concatHashes([]chainhash.Hash{*testTX1Hash, *testTX2Hash})))

		require.NoError(t, httpServer.GetTransactions()(echoContext))
		require.Equal(t, http.StatusOK, responseRecorder.Code)

		expected := append(append([]byte{}, testTX1RawBytes...), testTX2RawBytes...)
		require.Equal(t, expected, responseRecorder.Body.Bytes())
	})

	t.Run("a batch the subtree data covers reads no node hashes", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		subtreeHash := chainhash.HashH([]byte("coveredBySubtreeData"))
		setSubtreeRoute(echoContext, &subtreeHash)
		mockRepo.On("GetSubtreeExists", mock.Anything, mock.Anything).Return(true, nil).Once()
		mockRepo.On("GetSubtreeTransactions", mock.Anything, mock.Anything).
			Return(map[chainhash.Hash]*bt.Tx{*testTX1Hash: testTx1}, nil)

		echoContext.Request().Body = io.NopCloser(bytes.NewReader(testTX1Hash.CloneBytes()))

		require.NoError(t, httpServer.GetTransactions()(echoContext))
		require.Equal(t, http.StatusOK, responseRecorder.Code)
		mockRepo.AssertNotCalled(t, "GetSubtreeNodeHashesReader", mock.Anything)
	})

	t.Run("a store error reading the node hashes is a server error", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		subtreeHash := chainhash.HashH([]byte("nodeHashesStoreError"))
		setSubtreeRoute(echoContext, &subtreeHash)
		mockRepo.On("GetSubtreeExists", mock.Anything, mock.Anything).Return(true, nil).Once()
		mockRepo.On("GetSubtreeTransactions", mock.Anything, mock.Anything).Return(nil, errors.ErrNotFound)
		mockRepo.On("GetSubtreeNodeHashesReader", mock.Anything).Return(nil, errors.NewStorageError("subtree store down"))
		mockRepo.On("GetTransaction", mock.Anything).Return(testTX1RawBytes, nil)

		echoContext.Request().Body = io.NopCloser(bytes.NewReader(testTX1Hash.CloneBytes()))

		err := httpServer.GetTransactions()(echoContext)

		echoErr := &echo.HTTPError{}
		require.ErrorAs(t, err, &echoErr)
		require.Equal(t, http.StatusInternalServerError, echoErr.Code)
		mockRepo.AssertNotCalled(t, "GetTransaction", mock.Anything)
	})

	// The status each failure to read the node hashes maps to. A 503 is retried by
	// the catchup client; a 500 is not, and the client blames the serving peer.
	for _, tc := range []struct {
		name   string
		reader func() (io.ReadCloser, error)
		status int
	}{
		{
			name: "an exhausted stream permit is a retryable 503",
			reader: func() (io.ReadCloser, error) {
				return nil, errors.NewServiceUnavailableError("GetSubtreeNodeHashesReader: permit not acquired")
			},
			status: http.StatusServiceUnavailable,
		},
		{
			name:   "a subtree gone from the store is a 404",
			reader: func() (io.ReadCloser, error) { return nil, errors.ErrNotFound },
			status: http.StatusNotFound,
		},
		{
			// The node-hash reader wraps io.EOF when the file ends before its node
			// count; only a bare io.EOF is the real end of the list.
			name: "a subtree file cut short at a node boundary is a 500, not a non-member",
			reader: func() (io.ReadCloser, error) {
				return io.NopCloser(&errReader{prefix: testTX1Hash.CloneBytes(), err: errors.NewProcessingError("unable to read subtree node", io.EOF)}), nil
			},
			status: http.StatusInternalServerError,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

			subtreeHash := chainhash.HashH([]byte(tc.name))
			setSubtreeRoute(echoContext, &subtreeHash)
			mockRepo.On("GetSubtreeExists", mock.Anything, mock.Anything).Return(true, nil).Once()
			mockRepo.On("GetSubtreeTransactions", mock.Anything, mock.Anything).Return(nil, errors.ErrNotFound)

			reader, readerErr := tc.reader()
			mockRepo.On("GetSubtreeNodeHashesReader", mock.Anything).Return(reader, readerErr)
			mockRepo.On("GetTransaction", mock.Anything).Return(testTX2RawBytes, nil)

			echoContext.Request().Body = io.NopCloser(bytes.NewReader(testTX2Hash.CloneBytes()))

			err := httpServer.GetTransactions()(echoContext)

			echoErr := &echo.HTTPError{}
			require.ErrorAs(t, err, &echoErr)
			require.Equal(t, tc.status, echoErr.Code)
			mockRepo.AssertNotCalled(t, "GetTransaction", mock.Anything)

			if tc.status == http.StatusServiceUnavailable {
				require.Equal(t, "1", responseRecorder.Header().Get("Retry-After"))
			}
		})
	}

	t.Run("the scan stops once every requested hash has been seen", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		subtreeHash := chainhash.HashH([]byte("scanStopsEarly"))
		setSubtreeRoute(echoContext, &subtreeHash)
		mockRepo.On("GetSubtreeExists", mock.Anything, mock.Anything).Return(true, nil).Once()
		mockRepo.On("GetSubtreeTransactions", mock.Anything, mock.Anything).Return(nil, errors.ErrNotFound)

		// Reading past tx1 fails, so the request only succeeds if the scan stops there.
		mockRepo.On("GetSubtreeNodeHashesReader", mock.Anything).Return(io.NopCloser(
			&errReader{prefix: testTX1Hash.CloneBytes(), err: errors.NewStorageError("read past the last requested hash")}), nil)
		mockRepo.On("GetTransaction", mock.Anything).Return(testTX1RawBytes, nil)

		echoContext.Request().Body = io.NopCloser(bytes.NewReader(testTX1Hash.CloneBytes()))

		require.NoError(t, httpServer.GetTransactions()(echoContext))
		require.Equal(t, http.StatusOK, responseRecorder.Code)
	})
}

// TestGetTransactionsPreservesResponseOrder pins the response order to the
// request order, not to whichever concurrent lookup finishes first.
func TestGetTransactionsPreservesResponseOrder(t *testing.T) {
	initPrometheusMetrics()

	httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

	subtreeHash := chainhash.HashH([]byte("preservesResponseOrder"))
	setSubtreeRoute(echoContext, &subtreeHash)
	mockSubtreeWithoutData(mockRepo, *testTX1Hash, *testTX2Hash)

	// tx1 is requested second (see body below) but must finish its lookup
	// first, forcing the two lookups to complete out of request order. A
	// storage path keyed on completion order rather than request index would
	// then put tx1 ahead of tx2 in the response.
	tx1Done := make(chan struct{})

	mockRepo.On("GetTransaction", mock.MatchedBy(func(h *chainhash.Hash) bool {
		return h.IsEqual(testTX1Hash)
	})).Return(testTX1RawBytes, nil).Run(func(mock.Arguments) { close(tx1Done) })
	mockRepo.On("GetTransaction", mock.MatchedBy(func(h *chainhash.Hash) bool {
		return h.IsEqual(testTX2Hash)
	})).Return(testTX2RawBytes, nil).Run(func(mock.Arguments) {
		// Bounded, so a serialised or cancelled fan-out fails on the byte check instead of hanging.
		select {
		case <-tx1Done:
		case <-time.After(5 * time.Second):
		}
	})

	// Request tx2 before tx1: the response must still come back tx2, then tx1.
	body := append(testTX2Hash.CloneBytes(), testTX1Hash.CloneBytes()...)
	echoContext.Request().Body = io.NopCloser(bytes.NewReader(body))

	require.NoError(t, httpServer.GetTransactions()(echoContext))
	require.Equal(t, http.StatusOK, responseRecorder.Code)

	expected := append(append([]byte{}, testTX2RawBytes...), testTX1RawBytes...)
	require.Equal(t, expected, responseRecorder.Body.Bytes())
}

// TestGetTransactionsFromSubtreeStopsProcessingAfterAFailure guards the
// gCtx.Err() check in lookupAndStoreTransaction: once a dispatched lookup has
// failed, goroutines still queued behind the 1024-way fan-out limit must skip
// their own store lookup rather than running it anyway. Without that check,
// every one of a large batch's hashes is looked up regardless of an earlier
// failure.
func TestGetTransactionsFromSubtreeStopsProcessingAfterAFailure(t *testing.T) {
	initPrometheusMetrics()

	const requestedHashes = 5000

	httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

	subtreeHash := chainhash.HashH([]byte("stopsProcessingAfterFailure"))
	setSubtreeRoute(echoContext, &subtreeHash)
	hashes := distinctHashes(requestedHashes)
	mockSubtreeWithoutData(mockRepo, hashes...)

	var calls atomic.Int64

	mockRepo.On("GetTransaction", mock.Anything, mock.Anything).
		Run(func(mock.Arguments) { calls.Add(1) }).
		Return(nil, errors.NewNotFoundError("transaction not found"))

	echoContext.Request().Body = io.NopCloser(bytes.NewReader(concatHashes(hashes)))

	err := httpServer.GetTransactions()(echoContext)

	echoErr := &echo.HTTPError{}
	require.True(t, errors.As(err, &echoErr))
	require.Equal(t, http.StatusNotFound, echoErr.Code)

	require.Less(t, calls.Load(), int64(requestedHashes),
		"once a lookup has already failed, queued goroutines must skip their own store lookup rather than running it anyway")
}

// TestGetTransactionsFromSubtreeReservesBudgetBeforeSerializing guards the
// subtree-map branch's tx.Size() budget check: it must run, and reject an
// over-budget batch with 413, even though the offending transaction comes from
// the subtree-data map rather than the store.
func TestGetTransactionsFromSubtreeReservesBudgetBeforeSerializing(t *testing.T) {
	initPrometheusMetrics()

	httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)
	httpServer.settings.Asset.MaxBatchResponseBytes = 1

	subtreeHash := chainhash.HashH([]byte("reservesBudgetBeforeSerializing"))
	setSubtreeRoute(echoContext, &subtreeHash)

	mockRepo.On("GetSubtreeExists", mock.Anything, mock.Anything).Return(true, nil).Once()
	mockRepo.On("GetSubtreeTransactions", mock.Anything, mock.Anything).
		Return(map[chainhash.Hash]*bt.Tx{*testTX1Hash: testTx1}, nil)

	echoContext.Request().Body = io.NopCloser(bytes.NewReader(testTX1Hash.CloneBytes()))

	err := httpServer.GetTransactions()(echoContext)

	echoErr := &echo.HTTPError{}
	require.True(t, errors.As(err, &echoErr))
	require.Equal(t, http.StatusRequestEntityTooLarge, echoErr.Code)
}

// TestConcatTransactionBytesAllocatesExactly guards against a speculative
// preallocation sized from the requested record count rather than from the
// serialized length. The handler used to reserve a flat 32MB per in-flight
// request before reading the first body byte, on an unauthenticated route.
func TestConcatTransactionBytesAllocatesExactly(t *testing.T) {
	parts := [][]byte{testTX1RawBytes, testTX2RawBytes, testTX1RawBytes}

	concatenated := concatTransactionBytes(parts)

	require.Len(t, concatenated, 2*len(testTX1RawBytes)+len(testTX2RawBytes))
	require.Equal(t, len(concatenated), cap(concatenated),
		"capacity must match the serialized length, not a per-record reservation")
}

// permitWriteProbe records whether the subtree-map permit had already been released
// at the moment the response body started being written.
type permitWriteProbe struct {
	http.ResponseWriter

	released         chan struct{}
	releasedAtWrite  atomic.Bool
	observedAnyWrite atomic.Bool
}

func (p *permitWriteProbe) Write(b []byte) (int, error) {
	p.observedAnyWrite.Store(true)

	select {
	case <-p.released:
		p.releasedAtWrite.Store(true)
	default:
	}

	return p.ResponseWriter.Write(b)
}

// TestGetTransactionsReleasesSubtreePermitBeforeResponseWrite pins the permit to the
// lifetime of the subtree map rather than the lifetime of the response.
//
// asset_concurrency_get_subtree_transactions defaults to 2, while a catching-up peer
// issues subtreevalidation_getMissingTransactions (32) concurrent batch requests. If
// the permit is held across the response write, which is paced by the peer's read
// speed, the node serves two catchup batches at a time and the rest stall on the
// semaphore for the 30s acquire deadline.
func TestGetTransactionsReleasesSubtreePermitBeforeResponseWrite(t *testing.T) {
	initPrometheusMetrics()

	subtreeHash := testSubtree.RootHash()

	body := make([]byte, 0, chainhash.HashSize)
	body = append(body, testTX1Hash.CloneBytes()...)

	httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, bytes.NewReader(body))

	released := make(chan struct{})
	probe := &permitWriteProbe{ResponseWriter: responseRecorder, released: released}
	echoContext.Response().Writer = probe

	txMap := map[chainhash.Hash]*bt.Tx{*testTX1Hash: testTx1}

	mockRepo.On("GetSubtreeExists", mock.Anything, mock.Anything).Return(true, nil).Once()
	// The repository contract guarantees release is sync.Once-guarded, so mirror
	// that here: the handler legitimately calls it once eagerly and once via defer.
	var releaseOnce sync.Once

	mockRepo.On("GetSubtreeTransactions", mock.Anything, mock.Anything).
		Return(txMap, func() { releaseOnce.Do(func() { close(released) }) }, nil).Once()

	echoContext.SetPath("/subtree/:hash/txs")
	echoContext.SetParamNames("hash")
	echoContext.SetParamValues(subtreeHash.String())

	require.NoError(t, httpServer.GetTransactions()(echoContext))

	require.True(t, probe.observedAnyWrite.Load(), "expected the handler to write a response body")
	require.True(t, probe.releasedAtWrite.Load(),
		"subtree map permit must be released once the fan-out is done, not held across the client-paced response write")
}

// TestGetTransactionsFromSubtreeReadsBodyBeforeTakingThePermit is the regression
// test for the slowloris fix: on the subtree path, GetSubtreeTransactions — which
// takes the node-wide asset_concurrency_get_subtree_transactions permit — must not
// be called until the request body has been read to completion. Dispatching while
// reading (as an earlier version of this handler did) would hold that shared,
// low-default (2) permit for the whole client-paced upload, letting a couple of
// slow anonymous uploads pin both permits and starve every other caller of this
// route, including honest peer catchup.
//
// A blocking body (io.Pipe) proves it: GetSubtreeTransactions must not be called
// while the body is still open, and must be called once EOF is supplied.
func TestGetTransactionsFromSubtreeReadsBodyBeforeTakingThePermit(t *testing.T) {
	initPrometheusMetrics()

	pr, pw := io.Pipe()
	httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, pr)

	subtreeHash := chainhash.HashH([]byte("slowloris-subtree"))

	subtreeTransactionsCalled := make(chan struct{})

	mockRepo.On("GetSubtreeExists", mock.Anything, mock.Anything).Return(true, nil).Once()
	mockRepo.On("GetSubtreeTransactions", mock.Anything, mock.Anything).
		Run(func(mock.Arguments) { close(subtreeTransactionsCalled) }).
		Return(map[chainhash.Hash]*bt.Tx{*testTX1Hash: testTx1}, func() {}, nil).Once()

	echoContext.SetPath("/subtree/:hash/txs")
	echoContext.SetParamNames("hash")
	echoContext.SetParamValues(subtreeHash.String())

	done := make(chan error, 1)

	go func() {
		done <- httpServer.GetTransactions()(echoContext)
	}()

	// Write one full hash, then hold the pipe open (no EOF yet): the body is
	// still being uploaded.
	_, err := pw.Write(testTX1Hash.CloneBytes())
	require.NoError(t, err)

	select {
	case <-subtreeTransactionsCalled:
		t.Fatal("GetSubtreeTransactions (and therefore the node-wide permit) must not be taken while the body is still open")
	case <-time.After(200 * time.Millisecond):
		// expected: nothing has happened yet, the handler is still reading the body
	}

	require.NoError(t, pw.Close())

	select {
	case <-subtreeTransactionsCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("GetSubtreeTransactions must be called once the body reaches EOF")
	}

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not return after the body reached EOF")
	}

	require.Equal(t, http.StatusOK, responseRecorder.Code)
}

// TestReadSubtreeBatchHashesInitialCapacityIsCapped pins that nothing a client
// controls sizes the up-front allocation beyond subtreeBatchHashHintCap: not a
// forged Content-Length, and not an operator maxRecords reached through a
// request that sends no Content-Length at all. The slice still grows to hold a
// genuinely large body.
func TestReadSubtreeBatchHashesInitialCapacityIsCapped(t *testing.T) {
	tests := []struct {
		name          string
		contentLength int64
		maxRecords    int
	}{
		{"forged content-length, no record cap", 1 << 30, 0},
		{"no content-length, large record cap", 0, 1_000_000},
		{"forged content-length, large record cap", 1 << 30, 1_000_000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hashes, err := readSubtreeBatchHashes(bytes.NewReader(nil), tt.contentLength, tt.maxRecords)
			require.NoError(t, err)
			require.LessOrEqual(t, cap(hashes), subtreeBatchHashHintCap)
		})
	}

	t.Run("a body larger than the hint still reads in full", func(t *testing.T) {
		n := subtreeBatchHashHintCap + 10
		body := bytes.Repeat([]byte{0x01}, n*chainhash.HashSize)

		hashes, err := readSubtreeBatchHashes(bytes.NewReader(body), 0, 0)
		require.NoError(t, err)
		require.Len(t, hashes, n)
	})
}

// TestReadSubtreeBatchHashesPartialTrailingHashIsBadRequest pins that a body
// whose length is not a whole multiple of the hash size is a client error
// (400), not a server error (500). io.ReadFull returns io.ErrUnexpectedEOF
// (not io.EOF) on a short trailing read, and that must not fall through to
// the generic "error reading request body" 500 path.
func TestReadSubtreeBatchHashesPartialTrailingHashIsBadRequest(t *testing.T) {
	body := append(bytes.Repeat([]byte{0x01}, chainhash.HashSize), 0x02, 0x03, 0x04)

	_, err := readSubtreeBatchHashes(bytes.NewReader(body), 0, 0)
	require.Error(t, err)

	echoErr := &echo.HTTPError{}
	require.ErrorAs(t, err, &echoErr)
	require.Equal(t, http.StatusBadRequest, echoErr.Code)
}

// TestTxSizeMatchesResponseBytes pins the assumption the subtree-map branch of
// the response-byte budget relies on: it reserves tx.Size() before serializing,
// so that must equal the length of the non-extended bytes it then writes.
func TestTxSizeMatchesResponseBytes(t *testing.T) {
	tx := bt.NewTx()
	require.NoError(t, tx.From("a9b84a7e4b1c2f1d3a5e6b7c8d9e0f1a2b3c4d5e6f708192a3b4c5d6e7f80910", 0, "76a91489abcdefabbaabbaabbaabbaabbaabbaabbaabba88ac", 1000))
	require.NoError(t, tx.PayToAddress("1AdZmoAQUw4XCsCihukoHMvNWXcsd8jDN6", 900))

	require.Equal(t, len(tx.Bytes()), tx.Size())
}

// TestGetTransactionsSubtreeMembershipRealRepository runs the membership check
// against a real repository: a validated subtree with no .subtreeData (a live
// subtree), and a UTXO store holding tx1 and tx2, of which only tx1 is in the
// subtree.
func TestGetTransactionsSubtreeMembershipRealRepository(t *testing.T) {
	initPrometheusMetrics()

	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///" + t.Name())
	require.NoError(t, err)

	utxoStore, err := utxosql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = utxoStore.Close(ctx) })

	tx2, err := bt.NewTxFromBytes(testTX2RawBytes)
	require.NoError(t, err)

	_, err = utxoStore.Create(ctx, testTx1, 1)
	require.NoError(t, err)
	_, err = utxoStore.Create(ctx, tx2, 1)
	require.NoError(t, err)

	st, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, st.AddNode(chainhash.HashH([]byte("filler")), 1, 100))
	require.NoError(t, st.AddNode(*testTX1Hash, 1, uint64(testTx1.Size())))

	subtreeBytes, err := st.Serialize()
	require.NoError(t, err)

	subtreeHash := st.RootHash()
	subtreeStore := memory.New()
	require.NoError(t, subtreeStore.Set(ctx, subtreeHash.CloneBytes(), fileformat.FileTypeSubtree, subtreeBytes))

	repo, err := repository.NewRepository(logger, tSettings, utxoStore, memory.New(), nil, nil, subtreeStore, nil, nil, nil)
	require.NoError(t, err)

	request := func(t *testing.T, hashes ...chainhash.Hash) ([]byte, error) {
		t.Helper()

		httpServer, _, echoContext, responseRecorder := GetMockHTTP(t, nil)
		httpServer.repository = repo
		setSubtreeRoute(echoContext, subtreeHash)
		echoContext.Request().Body = io.NopCloser(bytes.NewReader(concatHashes(hashes)))

		err := httpServer.GetTransactions()(echoContext)

		return responseRecorder.Body.Bytes(), err
	}

	t.Run("a member is served", func(t *testing.T) {
		body, err := request(t, *testTX1Hash)
		require.NoError(t, err)

		tx, err := bt.NewTxFromBytes(body)
		require.NoError(t, err)
		require.Equal(t, *testTX1Hash, *tx.TxIDChainHash())
	})

	t.Run("a stored transaction from another subtree is refused", func(t *testing.T) {
		_, err := request(t, *testTX1Hash, *tx2.TxIDChainHash())

		echoErr := &echo.HTTPError{}
		require.ErrorAs(t, err, &echoErr)
		require.Equal(t, http.StatusNotFound, echoErr.Code)
		require.Contains(t, echoErr.Message, "not in subtree")
	})
}
