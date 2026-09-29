package validator

import (
	"context"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/validator/validator_api"
	bcsql "github.com/bsv-blockchain/teranode/stores/blockchain/sql"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/status"
)

// batchMTPActiveHeight is the CSV activation height these fixtures configure on
// their own settings copy. Pinning it low keeps the MTP fetch range tiny while
// still putting the request above the `blockHeight < CSVHeight` early return in
// EnsureMTPLoaded, so the loader really reaches the blockchain store.
const batchMTPActiveHeight = uint32(2)

// batchMTPValidator delegates MTP loading to a real *Validator — the actual
// loader whose failures this file is about — while controlling
// ValidateWithOptions so successes carry identifiable metadata and siblings can
// be scheduled deterministically. It exists only in tests and changes no
// production interface.
type batchMTPValidator struct {
	*Validator

	ensureCalls   atomic.Int32
	validateCalls atomic.Int32

	// afterEnsure and validateFn are installed before the call that starts
	// workers and are only read by those workers.
	afterEnsure func(height uint32, err error)
	validateFn  func(ctx context.Context, tx *bt.Tx, height uint32, opts *Options) (*meta.Data, error)
}

var _ Interface = (*batchMTPValidator)(nil)

func (v *batchMTPValidator) EnsureMTPLoaded(ctx context.Context, height uint32) error {
	v.ensureCalls.Add(1)

	err := v.Validator.EnsureMTPLoaded(ctx, height)
	if v.afterEnsure != nil {
		v.afterEnsure(height, err)
	}

	return err
}

func (v *batchMTPValidator) ValidateWithOptions(ctx context.Context, tx *bt.Tx, height uint32, opts *Options) (*meta.Data, error) {
	v.validateCalls.Add(1)

	return v.validateFn(ctx, tx, height, opts)
}

// newBatchMTPServer builds a validator server backed by a real local blockchain
// client over a real sqlitememory store. When closeStore is true the store is
// closed before any request runs, so an MTP fetch fails inside the loader
// instead of being padded with zeros. Each call gets its own database and an
// empty MTP cache, so no earlier range fetch can hide the dependency failure.
func newBatchMTPServer(t *testing.T, closeStore bool) (*Server, *batchMTPValidator, map[uint32]*meta.Data) {
	t.Helper()

	logger := ulogger.TestLogger{}

	cfg := test.CreateBaseTestSettings(t)
	cfg.ChainCfgParams.CSVHeight = batchMTPActiveHeight

	// sqlitememory ignores the URL path and allocates a uniquely named in-memory
	// database per store, so fixtures cannot share state.
	store, err := bcsql.New(logger, &url.URL{Scheme: "sqlitememory"}, cfg)
	require.NoError(t, err)

	var closeOnce sync.Once

	closeBlockchainStore := func() {
		closeOnce.Do(func() {
			require.NoError(t, store.Close(context.Background()))
		})
	}

	t.Cleanup(closeBlockchainStore)

	client, err := blockchain.NewLocalClient(logger, cfg, store, nil, nil)
	require.NoError(t, err)

	// Construct the Validator directly rather than via New/Init: only its MTP
	// loader is exercised here, and this avoids unrelated queues, UTXO
	// dependencies and background publishers.
	actual := &Validator{logger: logger, settings: cfg, blockchainClient: client}

	tx, err := bt.NewTxFromBytes(sampleTx)
	require.NoError(t, err)

	// Distinct fees give each height distinct serialized metadata, so an index
	// mix-up cannot pass.
	metadata := map[uint32]*meta.Data{
		0: {Fee: 101, SizeInBytes: uint64(len(sampleTx)), TxInpoints: singleParentInpoints(tx.TxIDChainHash(), 0)},
		1: {Fee: 202, SizeInBytes: uint64(len(sampleTx)), TxInpoints: singleParentInpoints(tx.TxIDChainHash(), 1)},
	}

	wrapper := &batchMTPValidator{Validator: actual}
	wrapper.validateFn = func(_ context.Context, _ *bt.Tx, height uint32, _ *Options) (*meta.Data, error) {
		data, ok := metadata[height]
		if !ok {
			return nil, errors.NewServiceError("unexpected successful validation at height %d", height)
		}

		return data, nil
	}

	server := NewServer(logger, cfg, nil, client, nil, nil, nil, nil, nil)
	server.validator = wrapper

	if closeStore {
		closeBlockchainStore()
	}

	return server, wrapper, metadata
}

func batchMTPRequest(height uint32) *validator_api.ValidateTransactionRequest {
	return &validator_api.ValidateTransactionRequest{TransactionData: sampleTx, BlockHeight: height}
}

// requireBatchMTPError compares a serialized error chain field by field,
// including the originating source location, so a per-item error must come from
// the same producer path as the independently obtained expectation.
func requireBatchMTPError(t *testing.T, want, got *errors.TError) {
	t.Helper()

	for depth := 0; want != nil; depth++ {
		require.NotNil(t, got, "error chain truncated at depth %d", depth)
		require.Equal(t, want.Code, got.Code, "code at depth %d", depth)
		require.Equal(t, want.Message, got.Message, "message at depth %d", depth)
		require.Equal(t, want.Data, got.Data, "data at depth %d", depth)
		require.Equal(t, want.File, got.File, "file at depth %d", depth)
		require.Equal(t, want.Line, got.Line, "line at depth %d", depth)
		require.Equal(t, want.Function, got.Function, "function at depth %d", depth)

		want, got = want.WrappedError, got.WrappedError
	}

	require.Nil(t, got)
}

func requireBatchMTPShape(t *testing.T, response *validator_api.ValidateTransactionBatchResponse, err error, n int) {
	t.Helper()

	require.NoError(t, err)
	require.NotNil(t, response)
	require.True(t, response.Valid)
	require.Len(t, response.Metadata, n)
	require.Len(t, response.Errors, n)
}

func batchMTPMetadataBytes(t *testing.T, data *meta.Data) []byte {
	t.Helper()

	result, err := data.Bytes()
	require.NoError(t, err)

	return result
}

// TestServerBatchMTPFailure pins the behavioural bug: when per-item MTP loading
// fails inside a batch worker, validateTransaction returns a nil response with
// the error, and the worker must not dereference that nil response. The first
// item sits at height zero so batch prewarming succeeds without fetching, and
// the failing item sits at an active-CSV height whose fetch really reaches the
// closed (or cancelled) SQL store.
func TestServerBatchMTPFailure(t *testing.T) {
	for _, tc := range []struct {
		name   string
		closed bool
	}{
		{name: "closed_store", closed: true},
		{name: "canceled_context", closed: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, _, metadata := newBatchMTPServer(t, tc.closed)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			if !tc.closed {
				cancel()
			}

			failing := batchMTPRequest(batchMTPActiveHeight)

			// Derive the expected error from the producer itself, independently
			// of any batch aggregation.
			direct, directErr := server.validateTransaction(ctx, failing)
			require.Nil(t, direct)
			require.ErrorIs(t, directErr, errors.ErrProcessing)
			require.Contains(t, directErr.Error(), "[Validator][EnsureMTPLoaded] failed to fetch MTPs")

			if tc.closed {
				require.Contains(t, directErr.Error(), "database is closed")
			} else {
				require.ErrorIs(t, directErr, context.Canceled)
			}

			response, err := server.ValidateTransactionBatch(ctx, &validator_api.ValidateTransactionBatchRequest{
				Transactions: []*validator_api.ValidateTransactionRequest{batchMTPRequest(0), failing},
			})
			requireBatchMTPShape(t, response, err, 2)

			require.Equal(t, batchMTPMetadataBytes(t, metadata[0]), response.Metadata[0])
			require.Nil(t, response.Errors[0])

			require.Nil(t, response.Metadata[1])
			requireBatchMTPError(t, errors.Wrap(directErr), response.Errors[1])
		})
	}
}

// TestServerBatchMTPMixedResults checks every index of a batch that mixes
// successes, a real loader failure and malformed bytes, and that the failing
// item neither cancels nor reorders its siblings. Scheduling is driven by
// channels: the delayed success is observed while it is still running, after
// the loader failure has already happened.
func TestServerBatchMTPMixedResults(t *testing.T) {
	server, wrapper, metadata := newBatchMTPServer(t, true)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Obtain the expected errors before installing any scheduling hook.
	direct, loaderErr := server.validateTransaction(ctx, batchMTPRequest(batchMTPActiveHeight))
	require.Nil(t, direct)
	require.ErrorIs(t, loaderErr, errors.ErrProcessing)

	malformed := &validator_api.ValidateTransactionRequest{TransactionData: []byte("invalid")}

	_, parseErr := server.validateTransaction(ctx, malformed)
	require.ErrorIs(t, parseErr, errors.ErrTxError)

	slowStarted := make(chan struct{})
	loaderFailed := make(chan struct{})
	siblingContext := make(chan error, 1)

	var failedOnce sync.Once

	wrapper.afterEnsure = func(height uint32, err error) {
		if height != batchMTPActiveHeight || err == nil {
			return
		}

		select {
		case <-slowStarted:
		case <-ctx.Done():
			return
		}

		failedOnce.Do(func() { close(loaderFailed) })
	}

	ordinary := wrapper.validateFn
	wrapper.validateFn = func(callCtx context.Context, tx *bt.Tx, height uint32, opts *Options) (*meta.Data, error) {
		if height == 1 {
			close(slowStarted)

			select {
			case <-loaderFailed:
			case <-callCtx.Done():
				siblingContext <- callCtx.Err()
				return nil, callCtx.Err()
			}

			// Recorded while this sibling is still being validated: the item
			// failure above must not have cancelled its context.
			siblingContext <- callCtx.Err()
		}

		return ordinary(callCtx, tx, height, opts)
	}

	response, err := server.ValidateTransactionBatch(ctx, &validator_api.ValidateTransactionBatchRequest{
		Transactions: []*validator_api.ValidateTransactionRequest{
			batchMTPRequest(0), batchMTPRequest(batchMTPActiveHeight), malformed, batchMTPRequest(1),
		},
	})
	requireBatchMTPShape(t, response, err, 4)
	require.NoError(t, ctx.Err())

	select {
	case observed := <-siblingContext:
		require.NoError(t, observed)
	default:
		t.Fatal("successful sibling did not record its context")
	}

	require.Equal(t, batchMTPMetadataBytes(t, metadata[0]), response.Metadata[0])
	require.Nil(t, response.Errors[0])

	require.Nil(t, response.Metadata[1])
	requireBatchMTPError(t, errors.Wrap(loaderErr), response.Errors[1])

	require.Nil(t, response.Metadata[2])
	requireBatchMTPError(t, errors.Wrap(parseErr), response.Errors[2])

	require.Equal(t, batchMTPMetadataBytes(t, metadata[1]), response.Metadata[3])
	require.Nil(t, response.Errors[3])
}

// TestServerBatchMTPPrewarmFailure keeps the existing contract for the other
// failure stage: when the first item's MTP load fails, the batch returns the
// loader's own error with no response and starts no transaction validation.
func TestServerBatchMTPPrewarmFailure(t *testing.T) {
	for _, closed := range []bool{true, false} {
		name := "canceled_context"
		if closed {
			name = "closed_store"
		}

		t.Run(name, func(t *testing.T) {
			server, wrapper, _ := newBatchMTPServer(t, closed)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			if !closed {
				cancel()
			}

			// A channel rather than a captured variable: the hook must stay
			// safe to observe even if the early return ever regresses and
			// workers start calling it.
			loaded := make(chan error, 4)
			wrapper.afterEnsure = func(_ uint32, err error) {
				select {
				case loaded <- err:
				default:
				}
			}

			response, err := server.ValidateTransactionBatch(ctx, &validator_api.ValidateTransactionBatchRequest{
				Transactions: []*validator_api.ValidateTransactionRequest{
					batchMTPRequest(batchMTPActiveHeight), batchMTPRequest(0),
				},
			})
			require.Nil(t, response)
			require.ErrorIs(t, err, errors.ErrProcessing)
			require.Equal(t, int32(1), wrapper.ensureCalls.Load())
			require.Zero(t, wrapper.validateCalls.Load())

			if !closed {
				require.ErrorIs(t, err, context.Canceled)
			}

			require.Len(t, loaded, 1)
			require.Same(t, <-loaded, err)
		})
	}
}

// TestServerBatchMTPUnaryCompatibility pins that the same MTP load failure keeps
// producing a nil response and the existing wrapped gRPC error on the unary
// endpoint.
func TestServerBatchMTPUnaryCompatibility(t *testing.T) {
	server, _, _ := newBatchMTPServer(t, true)

	req := batchMTPRequest(batchMTPActiveHeight)

	direct, directErr := server.validateTransaction(context.Background(), req)
	require.Nil(t, direct)
	require.ErrorIs(t, directErr, errors.ErrProcessing)

	response, err := server.ValidateTransaction(context.Background(), req)
	require.Nil(t, response)
	require.Error(t, err)

	want := errors.WrapGRPC(directErr)
	require.Equal(t, status.Code(want), status.Code(err))
	require.Equal(t, status.Convert(want).Message(), status.Convert(err).Message())
	requireBatchMTPError(t, errors.Wrap(errors.UnwrapGRPC(want)), errors.Wrap(errors.UnwrapGRPC(err)))
}

// TestServerBatchMTPControls keeps the untouched shapes honest: empty, single
// and heterogeneous-height batches. All heights here are below the configured
// CSV activation, so the closed store proves no MTP fetch is attempted for them.
func TestServerBatchMTPControls(t *testing.T) {
	for _, tc := range []struct {
		name    string
		heights []uint32
	}{
		{name: "empty", heights: nil},
		{name: "single", heights: []uint32{0}},
		{name: "heterogeneous_success", heights: []uint32{0, 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, wrapper, metadata := newBatchMTPServer(t, true)

			req := &validator_api.ValidateTransactionBatchRequest{}
			for _, height := range tc.heights {
				req.Transactions = append(req.Transactions, batchMTPRequest(height))
			}

			response, err := server.ValidateTransactionBatch(context.Background(), req)
			requireBatchMTPShape(t, response, err, len(tc.heights))

			for i, height := range tc.heights {
				require.Equal(t, batchMTPMetadataBytes(t, metadata[height]), response.Metadata[i])
				require.Nil(t, response.Errors[i])
			}

			require.Equal(t, int32(len(tc.heights)), wrapper.validateCalls.Load())

			if len(tc.heights) == 0 {
				require.Zero(t, wrapper.ensureCalls.Load())
			}
		})
	}
}
