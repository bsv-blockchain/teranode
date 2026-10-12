package propagation

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-batcher/v2/completion"
	bt "github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/propagation/propagation_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// shedStubPropagationServer is a PropagationAPI server whose ProcessTransaction and
// ProcessTransactionBatch always fail with a caller-supplied error, so a test
// controls exactly what crosses the wire on both the single and batch paths.
type shedStubPropagationServer struct {
	propagation_api.UnimplementedPropagationAPIServer

	err error
}

func (s *shedStubPropagationServer) ProcessTransaction(context.Context, *propagation_api.ProcessTransactionRequest) (*propagation_api.EmptyMessage, error) {
	return nil, s.err
}

func (s *shedStubPropagationServer) ProcessTransactionBatch(context.Context, *propagation_api.ProcessTransactionBatchRequest) (*propagation_api.ProcessTransactionBatchResponse, error) {
	return nil, s.err
}

func startShedStubPropagationServer(t *testing.T, serverErr error) propagation_api.PropagationAPIClient {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := grpc.NewServer()
	propagation_api.RegisterPropagationAPIServer(srv, &shedStubPropagationServer{err: serverErr})

	go func() {
		_ = srv.Serve(lis)
	}()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = conn.Close()
		srv.Stop()
	})

	return propagation_api.NewPropagationAPIClient(conn)
}

// countingHTTPPropagation stands up an HTTP endpoint that counts requests, standing
// in for the propagation /tx fallback surface.
func countingHTTPPropagation(t *testing.T) (*url.URL, *atomic.Int64) {
	t.Helper()

	var calls atomic.Int64

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	u, err := url.Parse(srv.URL)
	require.NoError(t, err)

	return u, &calls
}

func shedStubClient(t *testing.T, serverErr error) (*Client, *atomic.Int64) {
	t.Helper()

	httpAddr, calls := countingHTTPPropagation(t)

	// A bare Settings keeps AlwaysUseHTTP false and batchSize zero, so
	// ProcessTransaction takes the direct gRPC path under test rather than the
	// always-HTTP shortcut or the batcher.
	return &Client{
		client:              startShedStubPropagationServer(t, serverErr),
		logger:              ulogger.TestLogger{},
		settings:            &settings.Settings{},
		propagationHTTPAddr: httpAddr,
	}, calls
}

func shedTestTx(t *testing.T) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	require.NoError(t, tx.From("0000000000000000000000000000000000000000000000000000000000000000", 0, "76a914d1a5c9ee12cade94281609fc8f96bbc95db6335488ac", 100_000))
	require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 90_000))

	return tx
}

// TestPropagationClient_ShedDoesNotTriggerHTTPFallback mirrors the validator-side
// guard for propagation's single-transaction path: a block-assembly queue-full shed
// must be surfaced, not re-sent over HTTP against a node that just reported itself
// saturated.
//
// This is DEFENSIVE. The propagation server currently re-wraps a shed as a
// ProcessingError and resolves the status through the public-cause allowlist, which
// does not include ERR_THRESHOLD_EXCEEDED, so a shed collapses to codes.Internal and
// never reaches this branch today. The test pins the client's behaviour for when that
// changes — widening the allowlist is a public-error-boundary change that needs its
// own safety review, and this is the guard that makes it safe to make later.
func TestPropagationClient_ShedDoesNotTriggerHTTPFallback(t *testing.T) {
	c, httpCalls := shedStubClient(t, errors.WrapGRPC(errors.NewThresholdExceededError("block assembly queue full")))

	err := c.ProcessTransaction(context.Background(), shedTestTx(t))

	require.Error(t, err)
	require.ErrorIs(t, err, errors.ErrThresholdExceeded, "the shed must be surfaced to the caller")
	require.Equal(t, int64(0), httpCalls.Load(), "a shed must not be retried over the HTTP fallback")
}

// TestPropagationClient_MessageTooLargeStillFallsBackToHTTP is the non-regression
// twin: a bare transport ResourceExhausted with no project detail is a genuine size
// problem and must still fall back to HTTP.
func TestPropagationClient_MessageTooLargeStillFallsBackToHTTP(t *testing.T) {
	c, httpCalls := shedStubClient(t, status.Error(codes.ResourceExhausted, "grpc: received message larger than max"))

	require.NoError(t, c.ProcessTransaction(context.Background(), shedTestTx(t)))
	require.Equal(t, int64(1), httpCalls.Load(), "an oversized message must still be retried over HTTP, exactly once")
}

// batchOf builds a one-item batch whose completion group the test owns, so
// ProcessTransactionBatch can be driven directly without the batcher.
func batchOf(t *testing.T, tx *bt.Tx) []*batchItem {
	t.Helper()

	return []*batchItem{{
		ctx:   context.Background(),
		tx:    tx,
		group: completion.NewGroup(1),
	}}
}

// TestPropagationClient_BatchShedDoesNotTriggerHTTPFallback is the batch-path twin.
//
// This is the amplification case, and it is the worst of the four sites: treating a
// batch-level shed as an oversized batch turns ONE shed into one full HTTP validation
// per transaction in the batch (up to propagation_sendBatchSize, default 100), aimed
// at a node that has just reported itself saturated. The gate is the same shared
// predicate the single-transaction path uses, so the two cannot drift.
func TestPropagationClient_BatchShedDoesNotTriggerHTTPFallback(t *testing.T) {
	c, httpCalls := shedStubClient(t, errors.WrapGRPC(errors.NewThresholdExceededError("block assembly queue full")))

	err := c.ProcessTransactionBatch(context.Background(), batchOf(t, shedTestTx(t)))

	require.Error(t, err, "the batch failure is propagated to the submitters")
	require.Equal(t, int64(0), httpCalls.Load(), "a batch-level shed must not be amplified into N HTTP validations")
}

// TestPropagationClient_BatchMessageTooLargeStillFallsBackToHTTP is its
// non-regression twin: an oversized batch is exactly what the HTTP fallback exists
// for and must still take it.
func TestPropagationClient_BatchMessageTooLargeStillFallsBackToHTTP(t *testing.T) {
	c, httpCalls := shedStubClient(t, status.Error(codes.ResourceExhausted, "grpc: received message larger than max"))

	require.NoError(t, c.ProcessTransactionBatch(context.Background(), batchOf(t, shedTestTx(t))))
	require.Equal(t, int64(1), httpCalls.Load(), "an oversized batch must still be retried over HTTP, exactly once")
}
