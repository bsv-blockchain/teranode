package validator

import (
	"context"
	"encoding/binary"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-batcher/v2/completion"
	"github.com/bsv-blockchain/teranode/errors"
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
//
// The `result.err == nil` assertion below records CURRENT BEHAVIOUR, and is not an
// endorsement of it as a contract. The HTTP route returns no metadata, so the item
// completes as a success the caller cannot use: ValidateWithOptions then feeds nil
// to utxometa.NewMetaDataFromBytes and surfaces a parse error. The unary arm of the
// same route is worse — Client.go's non-batch path returns (nil, nil) on an
// HTTP-fallback success, and services/legacy/netsync/manager.go reads Fee off that
// nil pointer, which panics rather than erroring. Neither is fixed here: every
// available fix picks a different return contract for all callers of
// ValidateWithOptions.
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

// TestBatchOversized_RetriedIndividuallyWithoutHTTPAddress pins the case a
// deployment with validator_httpAddress unset actually hits — the default, since
// the setting defaults to "".
//
// The individual retry starts on unary gRPC and never needed an HTTP address, so
// requiring one before opening it failed an aggregate-oversized batch wholesale on
// exactly the deployments where every item would have been recovered. Only an item
// that is ITSELF too large reaches the HTTP send, and handleValidationError gates
// that on the address separately, so the worst case per item is the error the whole
// batch used to get.
func TestBatchOversized_RetriedIndividuallyWithoutHTTPAddress(t *testing.T) {
	mockClient := &MockValidatorAPIClient{
		validateBatchFunc: func(context.Context, *validator_api.ValidateTransactionBatchRequest) (*validator_api.ValidateTransactionBatchResponse, error) {
			return nil, status.Error(codes.ResourceExhausted, "grpc: received message larger than max")
		},
		validateTxFunc: func(context.Context, *validator_api.ValidateTransactionRequest) (*validator_api.ValidateTransactionResponse, error) {
			return &validator_api.ValidateTransactionResponse{Valid: true, Metadata: validBatchMetadata()}, nil
		},
	}

	c := &Client{
		client: mockClient,
		logger: &testLogger{t: t},
		// The shipped default: no HTTP fallback endpoint configured at all.
		validatorHTTPAddr: nil,
	}

	txBytes := createTestTransaction(t).SerializeBytes()

	group := completion.NewGroup(2)
	batch := []*batchItem{
		{req: buildValidateTxRequest(txBytes, 0, NewDefaultOptions()), group: group},
		{req: buildValidateTxRequest(txBytes, 0, NewDefaultOptions()), group: group},
	}

	c.sendBatchToValidator(context.Background(), batch)

	require.NoError(t, group.Wait(context.Background(), 0))

	for i, item := range batch {
		require.NoError(t, item.result.err,
			"item %d must be retried individually even with no validator HTTP address", i)

		parsed := &meta.Data{}
		require.NoError(t, meta.NewMetaDataFromBytes(item.result.metaData, parsed),
			"item %d must come back with metadata the caller can parse", i)
		require.Equal(t, uint64(1000), parsed.Fee)
	}

	// The predicate itself, on a client that has no HTTP address: an oversized
	// batch opens the retry regardless, because the retry is a gRPC one.
	require.True(t,
		c.shouldAttemptHTTPFallback(status.Error(codes.ResourceExhausted, "grpc: received message larger than max")),
		"an oversized batch must open the individual retry with no HTTP address configured")
}

// TestBatchOversized_IndividualRetryVerdictDoesNotStopTheLoop pins the verdict arm
// of the per-item retry.
//
// The loop aborts on a shed or a cancelled context, both of which say the node or
// the caller is gone. A per-item verdict says neither: the transaction is invalid,
// the node is healthy, and every remaining item must still be validated. Widening
// the abort to fire on any error would silently drop them.
func TestBatchOversized_IndividualRetryVerdictDoesNotStopTheLoop(t *testing.T) {
	httpAddr, httpCalls := countingHTTPValidator(t)

	var (
		mu         sync.Mutex
		unaryCalls int
	)

	mockClient := &MockValidatorAPIClient{
		validateBatchFunc: func(context.Context, *validator_api.ValidateTransactionBatchRequest) (*validator_api.ValidateTransactionBatchResponse, error) {
			return nil, status.Error(codes.ResourceExhausted, "grpc: received message larger than max")
		},
		validateTxFunc: func(context.Context, *validator_api.ValidateTransactionRequest) (*validator_api.ValidateTransactionResponse, error) {
			mu.Lock()
			unaryCalls++
			call := unaryCalls
			mu.Unlock()

			if call == 1 {
				return nil, errors.WrapGRPC(errors.NewTxInvalidError("bad-txns-inputs-missingorspent"))
			}

			return &validator_api.ValidateTransactionResponse{Valid: true, Metadata: validBatchMetadata()}, nil
		},
	}

	c := &Client{
		client:            mockClient,
		logger:            &testLogger{t: t},
		validatorHTTPAddr: httpAddr,
	}

	txBytes := createTestTransaction(t).SerializeBytes()

	group := completion.NewGroup(2)
	batch := []*batchItem{
		{req: buildValidateTxRequest(txBytes, 0, NewDefaultOptions()), group: group},
		{req: buildValidateTxRequest(txBytes, 0, NewDefaultOptions()), group: group},
	}

	waitCtx, cancelWait := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelWait()

	c.sendBatchToValidator(waitCtx, batch)
	require.NoError(t, group.Wait(waitCtx, 10*time.Second), "every item of the batch must complete")

	require.ErrorIs(t, batch[0].result.err, errors.ErrTxInvalid, "the verdict must reach its own item")
	require.Nil(t, batch[0].result.metaData)

	require.NoError(t, batch[1].result.err, "a verdict on item 0 must not stop item 1 being validated")

	parsed := &meta.Data{}
	require.NoError(t, meta.NewMetaDataFromBytes(batch[1].result.metaData, parsed))
	require.Equal(t, uint64(1000), parsed.Fee)

	mu.Lock()
	defer mu.Unlock()

	require.Equal(t, 2, unaryCalls, "both items must be retried individually")
	require.Equal(t, int64(0), httpCalls.Load(),
		"a verdict is not a size problem and must never be re-submitted over HTTP")
}

// TestBatchOversized_IndividualRetryStopsOnShedOrCancelledContext pins the abort
// arm: once the node has reported itself saturated, or the caller has gone, the
// remaining items are completed with that same error instead of being sent.
// Continuing would issue a full unary validation per remaining item against a node
// that cannot serve any of them.
func TestBatchOversized_IndividualRetryStopsOnShedOrCancelledContext(t *testing.T) {
	oversized := func(context.Context, *validator_api.ValidateTransactionBatchRequest) (*validator_api.ValidateTransactionBatchResponse, error) {
		return nil, status.Error(codes.ResourceExhausted, "grpc: received message larger than max")
	}

	newBatch := func(t *testing.T, group *completion.Group, n int) []*batchItem {
		t.Helper()

		txBytes := createTestTransaction(t).SerializeBytes()

		batch := make([]*batchItem, 0, n)
		for i := 0; i < n; i++ {
			batch = append(batch, &batchItem{req: buildValidateTxRequest(txBytes, 0, NewDefaultOptions()), group: group})
		}

		return batch
	}

	t.Run("shed", func(t *testing.T) {
		httpAddr, httpCalls := countingHTTPValidator(t)

		var (
			mu         sync.Mutex
			unaryCalls int
		)

		c := &Client{
			client: &MockValidatorAPIClient{
				validateBatchFunc: oversized,
				validateTxFunc: func(context.Context, *validator_api.ValidateTransactionRequest) (*validator_api.ValidateTransactionResponse, error) {
					mu.Lock()
					unaryCalls++
					mu.Unlock()

					return nil, errors.WrapGRPC(errors.NewThresholdExceededError("block assembly queue full"))
				},
			},
			logger:            &testLogger{t: t},
			validatorHTTPAddr: httpAddr,
		}

		group := completion.NewGroup(3)
		batch := newBatch(t, group, 3)

		waitCtx, cancelWait := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancelWait()

		c.sendBatchToValidator(waitCtx, batch)
		require.NoError(t, group.Wait(waitCtx, 10*time.Second), "every item of the batch must complete")

		for i := range batch {
			require.ErrorIs(t, batch[i].result.err, errors.ErrThresholdExceeded,
				"item %d must carry the shed the loop aborted on", i)
		}

		mu.Lock()
		defer mu.Unlock()

		require.Equal(t, 1, unaryCalls,
			"the loop must stop at the first shed, not validate the remaining items against a saturated node")
		require.Equal(t, int64(0), httpCalls.Load(), "a shed must never be re-sent over HTTP")
	})

	t.Run("cancelled context", func(t *testing.T) {
		httpAddr, httpCalls := countingHTTPValidator(t)

		// TWO contexts, deliberately. Group.Wait selects over the completion
		// channel AND ctx.Done(), so waiting on the context the test cancels would
		// let Wait return context.Canceled with both arms ready — passing or
		// failing for the wrong reason. waitCtx is never cancelled by the test;
		// only its child sendCtx is.
		waitCtx, cancelWait := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancelWait()

		sendCtx, cancelSend := context.WithCancel(waitCtx)
		defer cancelSend()

		var (
			mu         sync.Mutex
			unaryCalls int
		)

		c := &Client{
			client: &MockValidatorAPIClient{
				validateBatchFunc: oversized,
				validateTxFunc: func(context.Context, *validator_api.ValidateTransactionRequest) (*validator_api.ValidateTransactionResponse, error) {
					mu.Lock()
					unaryCalls++
					mu.Unlock()

					cancelSend()

					return nil, status.Error(codes.Canceled, "context canceled")
				},
			},
			logger:            &testLogger{t: t},
			validatorHTTPAddr: httpAddr,
		}

		group := completion.NewGroup(3)
		batch := newBatch(t, group, 3)

		c.sendBatchToValidator(sendCtx, batch)
		require.NoError(t, group.Wait(waitCtx, 10*time.Second), "every item of the batch must complete")

		// The propagation contract: the remaining items get the SAME error value,
		// not merely some error. notifyAllBatchItems hands one *errors.Error to
		// every remaining item, so errors.Is matches on identity and an
		// implementation that invented a fresh error per item fails here while
		// still satisfying a bare require.Error.
		require.Error(t, batch[0].result.err)

		for i := 1; i < len(batch); i++ {
			require.ErrorIs(t, batch[i].result.err, batch[0].result.err,
				"item %d must complete with the same error the loop aborted on", i)
		}

		mu.Lock()
		defer mu.Unlock()

		require.Equal(t, 1, unaryCalls,
			"a cancelled context must stop the loop, not produce a doomed round trip per remaining item")
		require.Equal(t, int64(0), httpCalls.Load(), "a cancelled context must not open the HTTP fallback")
	})
}
