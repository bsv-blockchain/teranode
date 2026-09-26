package httpimpl

import (
	"bytes"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestGetTransactions(t *testing.T) {
	initPrometheusMetrics()

	t.Run("Valid transaction hashes", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		// Set up mock responses for transaction hashes
		// We use mock.Anything for the hash parameter since the exact value isn't critical for this test
		mockRepo.On("GetTransaction", mock.Anything).Return(testTX1RawBytes, nil).Once()
		mockRepo.On("GetTransaction", mock.Anything).Return(testTX2RawBytes, nil).Once()

		// Create a slice with both transaction hashes
		transactionHashes := append(testTX1Hash.CloneBytes(), testTX2Hash.CloneBytes()...)

		// Set up the request
		echoContext.Request().Body = io.NopCloser(bytes.NewReader(transactionHashes))

		// Call GetTransactions handler
		err := httpServer.GetTransactions()(echoContext)
		require.NoError(t, err)

		// Check response status code
		assert.Equal(t, http.StatusOK, responseRecorder.Code)

		// Verify transactions using helper function
		verifyTransactions(t, bytes.NewReader(responseRecorder.Body.Bytes()), testTX1Hash.String(), testTX2Hash.String())
	})

	t.Run("Valid transaction hashes with subtree hash", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		subtreeHash := chainhash.HashH([]byte("subtreeHash"))

		emptyTxMap := make(map[chainhash.Hash]*bt.Tx)

		// set mock response
		mockRepo.On("GetTransaction", mock.Anything, mock.Anything).Return(testTX1RawBytes, nil).Once()
		mockRepo.On("GetTransaction", mock.Anything, mock.Anything).Return(testTX2RawBytes, nil).Once()
		mockRepo.On("GetSubtreeExists", mock.Anything, mock.Anything).Return(true, nil).Once()
		mockRepo.On("GetSubtreeTransactions", mock.Anything, mock.Anything).Return(emptyTxMap, nil)

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

		mockRepo.On("GetTransaction", mock.Anything, mock.Anything).Return(testTX1RawBytes, nil)

		// set echo context
		echoContext.Request().Header.Set(echo.HeaderContentType, echo.MIMEOctetStream)

		echoContext.Request().Body = io.NopCloser(bytes.NewReader([]byte{0x01, 0x02, 0x03, 0x04}))

		// Call GetTransactions handler
		err := httpServer.GetTransactions()(echoContext)
		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))

		// Check response status code
		assert.Equal(t, http.StatusInternalServerError, echoErr.Code)

		// Check response body
		assert.Equal(t, "PROCESSING (4): error reading request body -> UNKNOWN (0): unexpected EOF", echoErr.Message)
	})

	t.Run("Transaction not found", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

		// set mock response
		mockRepo.On("GetTransaction", mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundError("transaction not found"))

		// set echo context
		echoContext.Request().Header.Set(echo.HeaderContentType, echo.MIMEOctetStream)

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

		// set mock response
		mockRepo.On("GetTransaction", mock.Anything, mock.Anything).Return(nil, errors.NewStorageError("error getting transaction"))

		// set echo context
		echoContext.Request().Header.Set(echo.HeaderContentType, echo.MIMEOctetStream)

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
// peer, so a full-size batch must still succeed with the shipped defaults.
func TestGetTransactionsCatchupBatch(t *testing.T) {
	initPrometheusMetrics()

	const catchupBatchSize = 16384

	httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

	tx, err := bt.NewTxFromBytes(testTX1RawBytes)
	require.NoError(t, err)

	subtreeHash := chainhash.HashH([]byte("catchupSubtree"))
	txMap := map[chainhash.Hash]*bt.Tx{*testTX1Hash: tx}

	mockRepo.On("GetSubtreeExists", mock.Anything, mock.Anything).Return(true, nil).Once()
	mockRepo.On("GetSubtreeTransactions", mock.Anything, mock.Anything).Return(txMap, nil).Once()

	body := make([]byte, 0, catchupBatchSize*chainhash.HashSize)
	for i := 0; i < catchupBatchSize; i++ {
		body = append(body, testTX1Hash.CloneBytes()...)
	}

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
// 0 (unlimited, today's behaviour) and rejects only once an operator sets it.
func TestGetTransactionsRecordBudget(t *testing.T) {
	initPrometheusMetrics()

	t.Run("unset accepts any count", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

		mockRepo.On("GetTransaction", mock.Anything, mock.Anything).Return(testTX1RawBytes, nil)

		echoContext.Request().Body = io.NopCloser(bytes.NewReader(bytes.Repeat(testTX1Hash.CloneBytes(), 8)))

		require.NoError(t, httpServer.GetTransactions()(echoContext))
		require.Equal(t, http.StatusOK, responseRecorder.Code)
	})

	t.Run("set rejects an over-budget batch", func(t *testing.T) {
		httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)
		httpServer.settings.Asset.MaxBatchRecords = 4

		mockRepo.On("GetTransaction", mock.Anything, mock.Anything).Return(testTX1RawBytes, nil)

		echoContext.Request().Body = io.NopCloser(bytes.NewReader(bytes.Repeat(testTX1Hash.CloneBytes(), 5)))

		err := httpServer.GetTransactions()(echoContext)

		echoErr := &echo.HTTPError{}
		require.True(t, errors.As(err, &echoErr))
		require.Equal(t, http.StatusRequestEntityTooLarge, echoErr.Code)
	})

	t.Run("never enforced below the catchup batch size", func(t *testing.T) {
		httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)
		httpServer.settings.Asset.MaxBatchRecords = 4
		httpServer.settings.SubtreeValidation.MissingTransactionsBatchSize = 16384

		mockRepo.On("GetTransaction", mock.Anything, mock.Anything).Return(testTX1RawBytes, nil)

		echoContext.Request().Body = io.NopCloser(bytes.NewReader(bytes.Repeat(testTX1Hash.CloneBytes(), 64)))

		require.NoError(t, httpServer.GetTransactions()(echoContext))
		require.Equal(t, http.StatusOK, responseRecorder.Code)
	})
}

// TestGetTransactionsResponseByteBudget checks asset_maxBatchResponseBytes. A
// record cap alone does not bound output: duplicate hashes each expand into a
// full transaction.
func TestGetTransactionsResponseByteBudget(t *testing.T) {
	initPrometheusMetrics()

	httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)
	httpServer.settings.Asset.MaxBatchResponseBytes = int64(len(testTX1RawBytes))

	mockRepo.On("GetTransaction", mock.Anything, mock.Anything).Return(testTX1RawBytes, nil)

	echoContext.Request().Body = io.NopCloser(bytes.NewReader(bytes.Repeat(testTX1Hash.CloneBytes(), 4)))

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

	mockRepo.On("GetTransaction", mock.Anything, mock.Anything).Return(testTX1RawBytes, nil)

	echoContext.Request().Body = io.NopCloser(bytes.NewReader(bytes.Repeat(testTX1Hash.CloneBytes(), 4)))

	require.NoError(t, httpServer.GetTransactions()(echoContext))
	require.Equal(t, http.StatusOK, responseRecorder.Code)
}

// TestGetTransactionsPreservesResponseOrder pins the response order to the
// request order, not to whichever concurrent lookup finishes first.
func TestGetTransactionsPreservesResponseOrder(t *testing.T) {
	initPrometheusMetrics()

	httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, nil)

	mockRepo.On("GetTransaction", mock.MatchedBy(func(h *chainhash.Hash) bool {
		return h.IsEqual(testTX1Hash)
	})).Return(testTX1RawBytes, nil)
	mockRepo.On("GetTransaction", mock.MatchedBy(func(h *chainhash.Hash) bool {
		return h.IsEqual(testTX2Hash)
	})).Return(testTX2RawBytes, nil)

	// Request tx2 before tx1: the response must still come back tx2, then tx1.
	body := append(testTX2Hash.CloneBytes(), testTX1Hash.CloneBytes()...)
	echoContext.Request().Body = io.NopCloser(bytes.NewReader(body))

	require.NoError(t, httpServer.GetTransactions()(echoContext))
	require.Equal(t, http.StatusOK, responseRecorder.Code)

	expected := append(append([]byte{}, testTX2RawBytes...), testTX1RawBytes...)
	require.Equal(t, expected, responseRecorder.Body.Bytes())
}

// TestGetTransactionsDispatchesBeforeBodyFullyRead is the memory/shape regression:
// the handler must dispatch each hash's lookup as it is read, not read the whole
// body into a slice before any lookup starts. A blocking body reader proves it:
// the first hash's lookup must run while the second hash has not been supplied
// yet.
func TestGetTransactionsDispatchesBeforeBodyFullyRead(t *testing.T) {
	initPrometheusMetrics()

	pr, pw := io.Pipe()
	httpServer, mockRepo, echoContext, responseRecorder := GetMockHTTP(t, pr)

	firstLookupStarted := make(chan struct{})

	mockRepo.On("GetTransaction", mock.MatchedBy(func(h *chainhash.Hash) bool {
		return h.IsEqual(testTX1Hash)
	})).Run(func(mock.Arguments) {
		close(firstLookupStarted)
	}).Return(testTX1RawBytes, nil).Once()
	mockRepo.On("GetTransaction", mock.MatchedBy(func(h *chainhash.Hash) bool {
		return h.IsEqual(testTX2Hash)
	})).Return(testTX2RawBytes, nil).Once()

	done := make(chan error, 1)

	go func() {
		done <- httpServer.GetTransactions()(echoContext)
	}()

	_, err := pw.Write(testTX1Hash.CloneBytes())
	require.NoError(t, err)

	select {
	case <-firstLookupStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("expected the first hash's lookup to start before the rest of the body was read")
	}

	_, err = pw.Write(testTX2Hash.CloneBytes())
	require.NoError(t, err)
	require.NoError(t, pw.Close())

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not return after the body was fully supplied")
	}

	require.Equal(t, http.StatusOK, responseRecorder.Code)
}

// TestGetTransactionsStopsDispatchingAfterAFailure covers both the early-stop-on-
// cancellation and reserve-before-serialize fixes: once a dispatched lookup has
// failed, the handler must not keep dispatching (and therefore serializing) every
// remaining hash in a large batch. On the previous implementation nothing checked
// the shared context before dispatching or serializing, so every hash in the batch
// was always looked up regardless of an earlier failure.
func TestGetTransactionsStopsDispatchingAfterAFailure(t *testing.T) {
	initPrometheusMetrics()

	const requestedHashes = 5000

	httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

	var calls atomic.Int64

	mockRepo.On("GetTransaction", mock.Anything, mock.Anything).
		Run(func(mock.Arguments) { calls.Add(1) }).
		Return(nil, errors.NewNotFoundError("transaction not found"))

	body := bytes.Repeat(testTX1Hash.CloneBytes(), requestedHashes)
	echoContext.Request().Body = io.NopCloser(bytes.NewReader(body))

	err := httpServer.GetTransactions()(echoContext)

	echoErr := &echo.HTTPError{}
	require.True(t, errors.As(err, &echoErr))
	require.Equal(t, http.StatusNotFound, echoErr.Code)

	require.Less(t, calls.Load(), int64(requestedHashes),
		"the handler must stop reading and dispatching more hashes once a lookup has already failed")
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
// reading (as the plain POST /transactions path does) would hold that shared,
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

// TestTxSizeMatchesResponseBytes pins the assumption the subtree-map branch of
// the response-byte budget relies on: it reserves tx.Size() before serializing,
// so that must equal the length of the non-extended bytes it then writes.
func TestTxSizeMatchesResponseBytes(t *testing.T) {
	tx := bt.NewTx()
	require.NoError(t, tx.From("a9b84a7e4b1c2f1d3a5e6b7c8d9e0f1a2b3c4d5e6f708192a3b4c5d6e7f80910", 0, "76a91489abcdefabbaabbaabbaabbaabbaabbaabbaabba88ac", 1000))
	require.NoError(t, tx.PayToAddress("1AdZmoAQUw4XCsCihukoHMvNWXcsd8jDN6", 900))

	require.Equal(t, len(tx.Bytes()), tx.Size())
}
