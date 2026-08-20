package validator

// Tests for the validator client's handling of a back-pressure shed.
//
// ResourceExhausted is overloaded in-band — it is both "transaction exceeds the
// gRPC message limit" (fall back to HTTP) and the shed's THRESHOLD_EXCEEDED —
// and the HTTP fallback the oversize case takes must not flatten the shed's 429
// into a terminal ServiceError: propagation re-wraps anything that is not
// THRESHOLD_EXCEEDED as Internal, which ARC-style broadcasters treat as a hard
// reject.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

func newHTTPFallbackClient(t *testing.T, status int, body string) *Client {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)

	addr, err := url.Parse(server.URL)
	require.NoError(t, err)

	running := atomic.Bool{}
	running.Store(true)

	return &Client{
		logger:            &testLogger{t: t},
		running:           &running,
		validatorHTTPAddr: addr,
	}
}

func fallbackTestTx(t *testing.T) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	tx.Inputs = []*bt.Input{{PreviousTxSatoshis: 1000, PreviousTxOutIndex: 0, SequenceNumber: 1}}
	require.NoError(t, tx.Inputs[0].PreviousTxIDAdd(&chainhash.Hash{}))
	require.NoError(t, tx.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))

	return tx
}

func TestClientHTTPFallbackPreservesShed(t *testing.T) {
	ctx := context.Background()

	t.Run("429 stays retryable", func(t *testing.T) {
		c := newHTTPFallbackClient(t, http.StatusTooManyRequests, "block assembly ingest queue is over the limit")

		err := c.validateTransactionViaHTTP(ctx, fallbackTestTx(t), 100, NewDefaultOptions())
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrThresholdExceeded), "a shed must stay THRESHOLD_EXCEEDED through the HTTP fallback, got: %v", err)
	})

	t.Run("other failures stay terminal", func(t *testing.T) {
		c := newHTTPFallbackClient(t, http.StatusInternalServerError, "boom")

		err := c.validateTransactionViaHTTP(ctx, fallbackTestTx(t), 100, NewDefaultOptions())
		require.Error(t, err)
		require.False(t, errors.Is(err, errors.ErrThresholdExceeded), "a real failure must not be reported as back-pressure, got: %v", err)
		require.True(t, errors.Is(err, errors.ErrServiceError))
	})

	t.Run("success is unchanged", func(t *testing.T) {
		c := newHTTPFallbackClient(t, http.StatusOK, "")

		require.NoError(t, c.validateTransactionViaHTTP(ctx, fallbackTestTx(t), 100, NewDefaultOptions()))
	})
}
