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
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Echo's middleware.Recover only wraps the request goroutine. Handlers that fan
// out over errgroup must recover per goroutine or a single bad record takes the
// whole asset process down — and both of these handlers take their keys straight
// from the request body, so the record is attacker-chosen.
//
// The panic these reproduce is not hypothetical: a tx whose .tx external blob is
// absent but whose .outputs blob is present reconstructs with nil *bt.Output
// holes, and every go-bt serialization entry point dereferences them
// (repository.GetTransaction -> txMeta.Tx.ExtendedBytes()).
//
// Without the fix these tests do not fail — they abort the whole test binary,
// which is exactly what they are asserting cannot happen in production.

// panicMessage is what the injected panics carry. customHTTPErrorHandler returns
// echo.HTTPError.Message verbatim to the client, so no fragment of this may reach
// the response body — the panic value belongs in the log only.
const panicMessage = "runtime error: invalid memory address or nil pointer dereference"

// requireNoPanicLeak asserts the recovered panic value did not make it into the
// client-facing message. This is the documented property in GetUTXOs.go, and the
// one a refactor to errors.NewProcessingError("...", r) would silently break.
func requireNoPanicLeak(t *testing.T, httpErr *echo.HTTPError) {
	t.Helper()

	body, ok := httpErr.Message.(string)
	require.True(t, ok, "message must be the string built by the handler, not an opaque value")
	require.NotContains(t, body, panicMessage)
	require.NotContains(t, body, "runtime error")
	require.NotContains(t, body, "nil pointer dereference")
}

// manyOutputTx returns testTX1 widened to n outputs, so a fan-out over it is
// wider than one goroutine.
func manyOutputTx(t *testing.T, n int) *bt.Tx {
	t.Helper()

	tx, err := bt.NewTxFromBytes(testTX1RawBytes)
	require.NoError(t, err)

	template := tx.Outputs[0]
	tx.Outputs = make([]*bt.Output, 0, n)

	for i := 0; i < n; i++ {
		tx.Outputs = append(tx.Outputs, &bt.Output{Satoshis: template.Satoshis, LockingScript: template.LockingScript})
	}

	return tx
}

func TestGetTransactions_PanicInErrgroupGoroutineDoesNotCrashProcess(t *testing.T) {
	initPrometheusMetrics()

	httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

	mockRepo.On("GetTransaction", mock.Anything).Run(func(mock.Arguments) {
		panic(panicMessage)
	}).Return(nil, nil)

	echoContext.Request().Body = io.NopCloser(bytes.NewReader(testTX1Hash.CloneBytes()))

	err := httpServer.GetTransactions()(echoContext)

	require.Error(t, err, "handler must surface an error, not let the panic escape the goroutine")

	httpErr := &echo.HTTPError{}
	require.ErrorAs(t, err, &httpErr)
	require.Equal(t, http.StatusInternalServerError, httpErr.Code)
	requireNoPanicLeak(t, httpErr)
}

// The fan-out cap is the other half of the fix: the output count comes from the
// requested transaction, so without a limit one request for a large-output tx
// spawns one goroutine per output. Assert the ceiling holds instead of trusting
// the SetLimit call to stay in place.
func TestGetUTXOsByTXID_FanOutIsBounded(t *testing.T) {
	initPrometheusMetrics()

	httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

	const outputs = utxosFanoutLimit + 64

	tx := manyOutputTx(t, outputs)

	var (
		inFlight, maxInFlight atomic.Int64
		release               = make(chan struct{})
		releaseOnce           sync.Once
	)

	mockRepo.On("GetTransaction", mock.Anything).Return(tx.Bytes(), nil).Once()
	mockRepo.On("GetUtxo", mock.Anything).Run(func(mock.Arguments) {
		cur := inFlight.Add(1)

		for {
			prev := maxInFlight.Load()
			if cur <= prev || maxInFlight.CompareAndSwap(prev, cur) {
				break
			}
		}

		// Hold the slot so concurrency accumulates. An unbounded fan-out gets
		// every output in flight and releases itself immediately; a bounded one
		// never does, and each wave pays the timeout instead. A fixed sleep is
		// not enough — the mock's own lock staggers entry.
		if cur >= outputs {
			releaseOnce.Do(func() { close(release) })
		}

		select {
		case <-release:
		case <-time.After(500 * time.Millisecond):
		}

		inFlight.Add(-1)
	}).Return(testUtxo, nil)

	echoContext.SetPath("/utxos/:hash/json")
	echoContext.SetParamNames("hash")
	echoContext.SetParamValues(tx.TxID())

	require.NoError(t, httpServer.GetUTXOsByTxID(JSON)(echoContext))

	require.LessOrEqual(t, maxInFlight.Load(), int64(utxosFanoutLimit), "fan-out exceeded utxosFanoutLimit: %d goroutines in flight", maxInFlight.Load())
	require.Greater(t, maxInFlight.Load(), int64(1), "test did not exercise concurrency at all")
}

func TestGetUTXOsByTXID_PanicInErrgroupGoroutineDoesNotCrashProcess(t *testing.T) {
	initPrometheusMetrics()

	httpServer, mockRepo, echoContext, _ := GetMockHTTP(t, nil)

	// Wider than one output, so the panicking index sits alongside siblings that
	// succeed — a single-output fixture would not exercise the fan-out at all.
	const (
		outputs    = 8
		panicOnOut = 3
	)

	tx := manyOutputTx(t, outputs)

	var completed atomic.Int64

	mockRepo.On("GetTransaction", mock.Anything).Return(tx.Bytes(), nil).Once()
	mockRepo.On("GetUtxo", mock.Anything).Run(func(args mock.Arguments) {
		if args.Get(0).(*utxo.Spend).Vout == panicOnOut {
			panic(panicMessage)
		}

		completed.Add(1)
	}).Return(testUtxo, nil)

	echoContext.SetPath("/utxos/:hash/json")
	echoContext.SetParamNames("hash")
	echoContext.SetParamValues(tx.TxID())

	err := httpServer.GetUTXOsByTxID(JSON)(echoContext)

	require.Error(t, err, "handler must surface an error, not let the panic escape the goroutine")

	httpErr := &echo.HTTPError{}
	require.ErrorAs(t, err, &httpErr)
	require.Equal(t, http.StatusInternalServerError, httpErr.Code)
	requireNoPanicLeak(t, httpErr)
	require.Equal(t, int64(outputs-1), completed.Load(), "one output panicking must not abort its siblings")
}
