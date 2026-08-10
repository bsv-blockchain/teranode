package validator

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"

	bt "github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/validator/validator_api"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// shedStubServer is a ValidatorAPI server whose ValidateTransaction always fails
// with a caller-supplied error, so a test can choose exactly what crosses the wire.
type shedStubServer struct {
	validator_api.UnimplementedValidatorAPIServer

	err error
}

func (s *shedStubServer) ValidateTransaction(context.Context, *validator_api.ValidateTransactionRequest) (*validator_api.ValidateTransactionResponse, error) {
	return &validator_api.ValidateTransactionResponse{Valid: false}, s.err
}

// startShedStubServer stands up a real gRPC server on a loopback port and returns a
// generated stub client for it. A real client/server pair is the point: the whole
// discrimination rests on the project error code surviving the wire as a status
// detail, and only a transport round-trip proves that.
func startShedStubServer(t *testing.T, serverErr error) validator_api.ValidatorAPIClient {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := grpc.NewServer()
	validator_api.RegisterValidatorAPIServer(srv, &shedStubServer{err: serverErr})

	go func() {
		_ = srv.Serve(lis)
	}()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = conn.Close()
		srv.Stop()
	})

	return validator_api.NewValidatorAPIClient(conn)
}

// countingHTTPValidator stands up an HTTP endpoint that counts requests, standing in
// for the validator's /tx fallback surface.
func countingHTTPValidator(t *testing.T) (*url.URL, *atomic.Int64) {
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

func shedTestTx(t *testing.T) *bt.Tx {
	t.Helper()

	tx, err := bt.NewTxFromBytes(sampleTx)
	require.NoError(t, err)

	return tx
}

// TestValidatorAPI_ShedRoundTripsAsThresholdExceeded is the TRANSPORT round-trip,
// asserted on the generated stub rather than on validator.Client.
//
// The existing coverage constructs the error locally and asserts the server-side
// status code, or round-trips WrapGRPC/UnwrapGRPC in-process. Neither proves the
// project error code survives a real client/server pair — which is exactly what the
// client-side discrimination depends on, since a shed and an oversized message share
// codes.ResourceExhausted and can only be told apart by the reconstructed ERR code.
func TestValidatorAPI_ShedRoundTripsAsThresholdExceeded(t *testing.T) {
	client := startShedStubServer(t, errors.WrapGRPC(errors.NewThresholdExceededError("block assembly queue full")))

	_, err := client.ValidateTransaction(context.Background(), &validator_api.ValidateTransactionRequest{
		TransactionData: sampleTx,
		BlockHeight:     100,
	})
	require.Error(t, err)

	// The gRPC code alone cannot distinguish the two causes...
	require.Equal(t, codes.ResourceExhausted, status.Code(err))

	// ...the status detail is what carries the distinction across the wire. Assert its
	// exact shape, because the whole client-side discrimination is built on it: the
	// status must carry EXACTLY ONE detail, that detail must decode as a TError, and
	// its code must be ERR_THRESHOLD_EXCEEDED.
	//
	// The detail arrives as an *anypb.Any rather than a *TError because WrapGRPC
	// already hands WithDetails an Any and WithDetails wraps it again — so Details()
	// unmarshals the outer Any back to the inner one. That double-wrapping is exactly
	// what UnwrapGRPC's own cast depends on, so pinning it here guards the mechanism
	// and not just the outcome.
	st, ok := status.FromError(err)
	require.True(t, ok, "the shed must arrive as a gRPC status error")

	details := st.Details()
	require.Len(t, details, 1, "a single unwrapped shed must carry exactly one TError detail")

	detailAny, ok := details[0].(*anypb.Any)
	require.True(t, ok, "the detail must be an anypb.Any wrapping the TError")

	var tErr errors.TError
	require.NoError(t, anypb.UnmarshalTo(detailAny, &tErr, proto.UnmarshalOptions{}))
	require.Equal(t, errors.ERR_THRESHOLD_EXCEEDED, tErr.Code,
		"the project error code must survive the wire in the status detail")

	// ...and the reconstructed project error therefore works end to end.
	require.ErrorIs(t, errors.UnwrapGRPC(err), errors.ErrThresholdExceeded,
		"the shed must be recognisable as ErrThresholdExceeded after a real gRPC round-trip")

	require.True(t, errors.Is(errors.UnwrapGRPC(err), errors.ErrThresholdExceeded))
	require.False(t, errors.IsGRPCMessageTooLarge(err), "a shed is not a message-size problem")
}

// TestClient_ShedDoesNotTriggerHTTPFallback is the CLIENT BEHAVIOUR twin: a shed must
// be surfaced to the caller, not re-sent over HTTP. Re-sending drove a second full
// validation against a node that had just reported itself saturated, logged as a
// message-size problem.
//
// The assertion is deliberately on errors.Is and NOT on status.Code: the value
// returned here has been through UnwrapGRPC, making it a plain *errors.Error with no
// GRPCStatus method, so status.Code on it reports codes.Unknown by construction. The
// transport-level status assertion belongs on the stub, above.
func TestClient_ShedDoesNotTriggerHTTPFallback(t *testing.T) {
	httpAddr, httpCalls := countingHTTPValidator(t)

	c := &Client{
		client:            startShedStubServer(t, errors.WrapGRPC(errors.NewThresholdExceededError("block assembly queue full"))),
		logger:            ulogger.TestLogger{},
		validatorHTTPAddr: httpAddr,
	}

	_, err := c.ValidateWithOptions(context.Background(), shedTestTx(t), 100, NewDefaultOptions())

	require.Error(t, err)
	require.ErrorIs(t, err, errors.ErrThresholdExceeded, "the shed must be surfaced to the caller")
	require.Equal(t, int64(0), httpCalls.Load(), "a shed must not be retried over the HTTP fallback")
}

// TestClient_MessageTooLargeStillFallsBackToHTTP is the non-regression twin: a bare
// transport ResourceExhausted with no project detail is a genuine size problem and
// must still fall back to HTTP exactly as before.
func TestClient_MessageTooLargeStillFallsBackToHTTP(t *testing.T) {
	httpAddr, httpCalls := countingHTTPValidator(t)

	c := &Client{
		client:            startShedStubServer(t, status.Error(codes.ResourceExhausted, "grpc: received message larger than max")),
		logger:            ulogger.TestLogger{},
		validatorHTTPAddr: httpAddr,
	}

	_, err := c.ValidateWithOptions(context.Background(), shedTestTx(t), 100, NewDefaultOptions())

	// The HTTP fallback succeeded, so the call reports success without metadata.
	require.NoError(t, err)
	require.Equal(t, int64(1), httpCalls.Load(), "an oversized message must still be retried over HTTP, exactly once")
}

// TestClient_NonStatusErrorIsUnwrappedNotRetried covers the arm most likely to
// regress when an inline status check becomes a helper call: an error that is not a
// gRPC status error at all must keep the original "unwrap and return" behaviour and
// must not reach the HTTP fallback.
func TestClient_NonStatusErrorIsUnwrappedNotRetried(t *testing.T) {
	plain := errors.NewProcessingError("connection reset by peer")

	require.False(t, errors.IsGRPCMessageTooLarge(plain), "a non-status error is not a message-size problem")
	require.False(t, errors.IsGRPCMessageTooLarge(nil), "nil is not a message-size problem")

	httpAddr, httpCalls := countingHTTPValidator(t)

	c := &Client{
		client: &MockValidatorAPIClient{validateTxFunc: func(context.Context, *validator_api.ValidateTransactionRequest) (*validator_api.ValidateTransactionResponse, error) {
			return nil, plain
		}},
		logger:            ulogger.TestLogger{},
		validatorHTTPAddr: httpAddr,
	}

	_, err := c.ValidateWithOptions(context.Background(), shedTestTx(t), 100, NewDefaultOptions())

	require.Error(t, err)
	require.Equal(t, int64(0), httpCalls.Load(), "a non-status transport failure must not be retried over HTTP")
}

// TestClient_BatchShedDoesNotTriggerHTTPFallback guards the batch seam. It is not
// reachable today — ValidateTransactionBatch returns a nil batch-level error and
// carries per-transaction failures in Errors[] — but if a batch-level error ever did
// propagate, treating a shed as a size problem would amplify one shed batch into one
// full HTTP validation per transaction in it, against a saturated node.
func TestClient_BatchShedDoesNotTriggerHTTPFallback(t *testing.T) {
	httpAddr, httpCalls := countingHTTPValidator(t)

	c := &Client{
		logger:            ulogger.TestLogger{},
		validatorHTTPAddr: httpAddr,
	}

	shed := errors.WrapGRPC(errors.NewThresholdExceededError("block assembly queue full"))
	require.False(t, c.shouldAttemptHTTPFallback(shed), "a batch-level shed must not open the HTTP fallback")

	tooLarge := status.Error(codes.ResourceExhausted, "grpc: received message larger than max")
	require.True(t, c.shouldAttemptHTTPFallback(tooLarge), "an oversized batch must still fall back")

	require.Equal(t, int64(0), httpCalls.Load(), "the predicate itself must not issue requests")
}
