package validator

import (
	"context"
	"encoding/binary"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-batcher/v2/completion"
	"github.com/bsv-blockchain/teranode/services/validator/validator_api"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// validBatchMetadata is the minimal byte layout utxometa.NewMetaDataFromBytes
// accepts: 8 bytes fee, 8 bytes size, 1 flag byte, then an 8-byte zero
// parent-hash count. Anything shorter than 17 bytes is rejected outright, which is
// why an item that completes with nil metadata surfaces at the caller as a parse
// error rather than as a success.
func validBatchMetadata() []byte {
	b := make([]byte, 25)
	binary.LittleEndian.PutUint64(b[0:8], 1000)
	binary.LittleEndian.PutUint64(b[8:16], 250)

	return b
}

// TestBatchOversized_NonDefaultOptionsRetriedOverGRPC is the regression guard for
// the aggregate-oversized batch.
//
// A batch can exceed the gRPC message limit in total while every transaction in it
// fits comfortably on its own — that is by far the commoner way to hit the ceiling.
// Retrying such a batch over the HTTP fallback cannot work now that the HTTP
// surface carries transaction bytes only: the items below are the shape legacy
// netsync's below-checkpoint path sends (SkipPolicyChecks, InBlock and the
// candidate times, at a real block height), and the client refuses to put any of
// that on HTTP. Retrying over unary gRPC is what keeps them working, because gRPC
// is the typed transport that carries options.
//
// Three things are asserted, and each one fails on a straight-to-HTTP retry:
// the batch completes, the options arrive at the validator intact, and the HTTP
// endpoint is never touched.
func TestBatchOversized_NonDefaultOptionsRetriedOverGRPC(t *testing.T) {
	httpAddr, httpCalls := countingHTTPValidator(t)

	var (
		mu       sync.Mutex
		unaryReq []*validator_api.ValidateTransactionRequest
	)

	mockClient := &MockValidatorAPIClient{
		// The batch as a whole is too large — but nothing about any single
		// transaction in it is.
		validateBatchFunc: func(context.Context, *validator_api.ValidateTransactionBatchRequest) (*validator_api.ValidateTransactionBatchResponse, error) {
			return nil, status.Error(codes.ResourceExhausted, "grpc: received message larger than max")
		},
		// Each item, sent on its own, is accepted.
		validateTxFunc: func(_ context.Context, in *validator_api.ValidateTransactionRequest) (*validator_api.ValidateTransactionResponse, error) {
			mu.Lock()
			unaryReq = append(unaryReq, in)
			mu.Unlock()

			return &validator_api.ValidateTransactionResponse{Valid: true, Metadata: validBatchMetadata()}, nil
		},
	}

	c := &Client{
		client:            mockClient,
		logger:            &testLogger{t: t},
		validatorHTTPAddr: httpAddr,
	}

	// The option set legacy netsync's quick-validation pre-warm actually sends,
	// at a real block height. Built through the shared builder so the test cannot
	// drift from the wire shape the batch path really produces.
	opts := NewDefaultOptions()
	opts.SkipPolicyChecks = true
	opts.InBlock = true
	opts.SkipTxMetaPublishing = true
	opts.CandidateBlockTime = 1700000000
	opts.CandidateParentMedianTime = 1699999000

	txBytes := createTestTransaction(t).SerializeBytes()

	group := completion.NewGroup(2)
	batch := []*batchItem{
		{req: buildValidateTxRequest(txBytes, 620000, opts), group: group},
		{req: buildValidateTxRequest(txBytes, 620000, opts), group: group},
	}

	c.sendBatchToValidator(context.Background(), batch)

	require.NoError(t, group.Wait(context.Background(), 0))

	for i, item := range batch {
		require.NoError(t, item.result.err,
			"item %d carries non-default options and must survive an aggregate-oversized batch", i)

		// Real metadata, not the nil the HTTP route would have produced — which
		// the caller cannot parse.
		parsed := &meta.Data{}
		require.NoError(t, meta.NewMetaDataFromBytes(item.result.metaData, parsed),
			"item %d must come back with metadata the caller can parse", i)
		require.Equal(t, uint64(1000), parsed.Fee)
	}

	require.Equal(t, int64(0), httpCalls.Load(),
		"an aggregate-oversized batch must be retried over gRPC; HTTP cannot carry these options at all")

	mu.Lock()
	defer mu.Unlock()

	require.Len(t, unaryReq, 2, "every item of the failed batch must be retried individually")

	for i, req := range unaryReq {
		require.Equal(t, uint32(620000), req.BlockHeight, "retry %d lost the block height", i)
		require.True(t, req.GetSkipPolicyChecks(), "retry %d lost SkipPolicyChecks", i)
		require.True(t, req.GetInBlock(), "retry %d lost InBlock", i)
		require.True(t, req.GetSkipTxmetaPublishing(), "retry %d lost SkipTxmetaPublishing", i)
		require.Equal(t, uint32(1700000000), req.GetCandidateBlockTime(), "retry %d lost CandidateBlockTime", i)
		require.Equal(t, uint32(1699999000), req.GetCandidateParentMedianTime(), "retry %d lost CandidateParentMedianTime", i)
	}
}

// TestBatchOversized_ItemTooLargeForGRPCStillUsesHTTP pins the other arm: when the
// individual retry ALSO reports message-too-large, that item genuinely does not fit
// gRPC and the HTTP fallback is the right route. It can only carry default options,
// so this item has them — which is exactly the condition under which the HTTP
// surface accepts anything at all.
func TestBatchOversized_ItemTooLargeForGRPCStillUsesHTTP(t *testing.T) {
	httpAddr, httpCalls := countingHTTPValidator(t)

	mockClient := &MockValidatorAPIClient{
		validateBatchFunc: func(context.Context, *validator_api.ValidateTransactionBatchRequest) (*validator_api.ValidateTransactionBatchResponse, error) {
			return nil, status.Error(codes.ResourceExhausted, "grpc: received message larger than max")
		},
		// The item is too large on its own too.
		validateTxFunc: func(context.Context, *validator_api.ValidateTransactionRequest) (*validator_api.ValidateTransactionResponse, error) {
			return nil, status.Error(codes.ResourceExhausted, "grpc: received message larger than max")
		},
	}

	c := &Client{
		client:            mockClient,
		logger:            &testLogger{t: t},
		validatorHTTPAddr: httpAddr,
	}

	group := completion.NewGroup(1)
	batch := []*batchItem{
		{
			req:   buildValidateTxRequest(createTestTransaction(t).SerializeBytes(), 0, NewDefaultOptions()),
			group: group,
		},
	}

	c.sendBatchToValidator(context.Background(), batch)

	require.NoError(t, group.Wait(context.Background(), 0))

	require.NoError(t, batch[0].result.err)
	require.Equal(t, int64(1), httpCalls.Load(),
		"an item that is itself oversized for gRPC must still reach the HTTP fallback, exactly once")
}
