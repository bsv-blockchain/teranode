package propagation

import (
	"context"
	"net/http"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/propagation/propagation_api"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/testutil"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// setBlockAssemblyFull drives the real notification that block assembly publishes, so the test
// exercises the same path production uses rather than reaching into client internals.
func setBlockAssemblyFull(t *testing.T, ctx context.Context, client blockchain.ClientI, full bool) {
	t.Helper()

	require.NoError(t, client.SendNotification(ctx, blockchain.NewBlockAssemblyFullNotification(full)))

	require.Equal(t, full, client.IsBlockAssemblyFull())
}

// TestProcessTransactionRejectedWhenBlockAssemblyFull checks the propagation ingress gate.
//
// When block assembly reports it is full, propagation must refuse the transaction and must not
// store it. Storing a transaction it is about to refuse would waste the storage the limit exists
// to protect.
func TestProcessTransactionRejectedWhenBlockAssemblyFull(t *testing.T) {
	tracing.SetupMockTracer()
	initPrometheusMetrics()

	ctx := t.Context()

	validatorInstance, utxoStore := setupRealValidator(t, ctx)

	tSettings := test.CreateBaseTestSettings(t)
	blockchainClient := testutil.NewMemorySQLiteBlockchainClient(ulogger.TestLogger{}, tSettings, t)

	txStore := memory.New()

	ps := &PropagationServer{
		logger:           ulogger.TestLogger{},
		settings:         tSettings,
		validator:        validatorInstance,
		blockchainClient: blockchainClient,
		txStore:          txStore,
	}

	txs := transactions.CreateTestTransactionChainWithCount(t, 3)

	_, _, err := utxoStore.SpendAndCreate(ctx, txs[0], 1, utxo.WithCreateOnly())
	require.NoError(t, err)

	tx := txs[1]

	// Block assembly reports it is full.
	setBlockAssemblyFull(t, ctx, blockchainClient, true)

	_, err = ps.ProcessTransaction(ctx, &propagation_api.ProcessTransactionRequest{
		Tx: tx.ExtendedBytes(),
	})

	require.Error(t, err, "propagation must refuse transactions while block assembly is full")
	require.True(t, ps.isTransientBackpressure(err),
		"a full block assembly is a transient service condition, not a fault in the transaction: %v", err)

	exists, err := txStore.Exists(ctx, tx.TxIDChainHash()[:], "tx")
	require.NoError(t, err)
	require.False(t, exists, "a refused transaction must not be written to the transaction store")

	// Block assembly reports it has room again, and the same transaction is now accepted.
	setBlockAssemblyFull(t, ctx, blockchainClient, false)

	_, err = ps.ProcessTransaction(ctx, &propagation_api.ProcessTransactionRequest{
		Tx: tx.ExtendedBytes(),
	})
	require.NoError(t, err, "propagation must accept transactions once block assembly has room")

	exists, err = txStore.Exists(ctx, tx.TxIDChainHash()[:], "tx")
	require.NoError(t, err)
	require.True(t, exists, "an accepted transaction must be written to the transaction store")
}

// TestIsTransientBackpressure checks the classification that keeps a full block assembly out of the
// error log without also hiding genuine faults.
//
// While block assembly is full, every inbound transaction is refused. All three call sites log
// through logTxProcessingError, so if a refusal were classified as an error the log pipeline would
// take one line per transaction for as long as the condition lasts — amplifying the exact overload
// the limit exists to contain and burying real errors.
//
// The classification must be narrow. ErrServiceUnavailable is not exclusive to the ingress gate: the
// file blob store raises it when a read or write permit times out, and the Aerospike UTXO store
// raises it for an open circuit breaker or a batch timeout. Matching the error class with errors.Is
// would walk the wrap chain into those and demote a disk stall or a tripped circuit breaker to
// debug, where the default log level discards it, and neither the rejection counter nor block
// assembly's transition warning would fire for it.
//
// So the refusal is recognised by its TOP-LEVEL error code. Every path that can surface a downstream
// ErrServiceUnavailable re-wraps it under a different code first (ErrStorageError,
// ErrServiceError, ErrProcessingError), and the gate is the only site returning a bare
// ErrServiceUnavailable, so the top-level code separates them exactly.
//
// The live flag is deliberately not consulted. It used to be ANDed with the error class, which left
// two races: a downstream fault raised after the gate passed but before the flag cleared was demoted
// anyway, and a genuine refusal whose flag cleared before the log call was logged at error level.
// The cases below pin both directions against the flag, so a reintroduced dependency fails here.
func TestIsTransientBackpressure(t *testing.T) {
	fullErr := errors.NewServiceUnavailableError("block assembly is full, not accepting new transactions")

	// What a stalled file blob store looks like by the time it reaches a log site: propagation wraps
	// the store failure in a StorageError, so only the inner code is ErrServiceUnavailable.
	storeOutage := errors.NewStorageError("[ProcessTransaction] failed to save transaction",
		errors.NewServiceUnavailableError("[File] write operation timed out waiting for semaphore permit"))

	// What a tripped Aerospike circuit breaker looks like coming back from the direct validator call,
	// which wraps it in a ProcessingError. This is raised AFTER the gate has already let the
	// transaction through, so it can reach a log site while the flag is set.
	validatorOutage := errors.NewProcessingError("[ProcessTransaction] failed to validate transaction",
		errors.NewServiceUnavailableError("[Aerospike] circuit breaker is open"))

	// What the validator HTTP fallback returns for a 503 from the validator: a ServiceError, again
	// raised after the gate.
	validatorHTTPOutage := errors.NewServiceError(
		"[ProcessTransaction] validator /tx endpoint returned non-OK status: 503, body: overloaded")

	tests := []struct {
		name              string
		err               error
		blockAssemblyFull bool
		expected          bool
	}{
		{name: "nil is not backpressure", err: nil, expected: false},
		{name: "nil is not backpressure while full", err: nil, blockAssemblyFull: true, expected: false},
		{
			name:              "the direct refusal",
			err:               fullErr,
			blockAssemblyFull: true,
			expected:          true,
		},
		{
			name:              "the refusal after the gRPC handler flattens it",
			err:               errors.WrapGRPCPublic(fullErr),
			blockAssemblyFull: true,
			expected:          true,
		},
		{
			name:     "the batch handler admission control rejection, whatever block assembly reports",
			err:      status.Error(codes.Unavailable, "server at capacity"),
			expected: true,
		},
		{
			name:              "a store outage must keep its error level while block assembly has room",
			err:               storeOutage,
			blockAssemblyFull: false,
			expected:          false,
		},
		{
			// The regression case: the gate returns before storeTransaction, but the flag can be set
			// by the time a transaction that already passed the gate fails in the store. Demoting
			// this would silence a disk stall exactly when the node is under pressure.
			name:              "a store outage must keep its error level while block assembly is full",
			err:               storeOutage,
			blockAssemblyFull: true,
			expected:          false,
		},
		{
			name:              "a tripped validator circuit breaker keeps its error level while full",
			err:               validatorOutage,
			blockAssemblyFull: true,
			expected:          false,
		},
		{
			name:              "a 503 from the validator HTTP fallback keeps its error level while full",
			err:               validatorHTTPOutage,
			blockAssemblyFull: true,
			expected:          false,
		},
		{
			// The other direction: the flag can clear between the gate refusing and the caller
			// logging. The refusal is still a refusal, so it must stay at debug.
			name:              "the refusal is still recognised once the flag has cleared",
			err:               fullErr,
			blockAssemblyFull: false,
			expected:          true,
		},
		{
			name:              "the flattened refusal is still recognised once the flag has cleared",
			err:               errors.WrapGRPCPublic(fullErr),
			blockAssemblyFull: false,
			expected:          true,
		},
		{
			name:              "a genuine fault in the transaction still logs as an error",
			err:               errors.NewTxInvalidError("bad script"),
			blockAssemblyFull: true,
			expected:          false,
		},
		{
			name:              "a genuine fault survives the gRPC round trip as an error",
			err:               errors.WrapGRPCPublic(errors.NewTxInvalidError("bad script")),
			blockAssemblyFull: true,
			expected:          false,
		},
		{
			name:              "a storage failure still logs as an error",
			err:               errors.NewStorageError("failed to save transaction"),
			blockAssemblyFull: true,
			expected:          false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blockchainClient := &blockchain.Mock{}
			blockchainClient.BlockAssemblyFull.Store(tt.blockAssemblyFull)

			ps := &PropagationServer{blockchainClient: blockchainClient}

			require.Equal(t, tt.expected, ps.isTransientBackpressure(tt.err))
		})
	}
}

// TestHTTPStatusForBlockAssemblyFull checks what an HTTP submitter is told when the node refuses.
//
// A refusal is temporary and the client fixes it by resubmitting later, so it has to arrive as 503.
// 500 says the node is broken, which stops a load balancer or a client library from backing off and
// retrying, and makes every refusal look like a fault to whatever watches 5xx rates.
func TestHTTPStatusForBlockAssemblyFull(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantStatus int
	}{
		{
			name:       "the block assembly refusal is retryable",
			err:        errors.NewServiceUnavailableError("block assembly is full, not accepting new transactions"),
			wantStatus: http.StatusServiceUnavailable,
		},
		{
			name: "a store shedding load is also retryable",
			err: errors.NewStorageError("failed to save transaction",
				errors.NewServiceUnavailableError("[File] write operation timed out waiting for semaphore permit")),
			wantStatus: http.StatusServiceUnavailable,
		},
		{
			name:       "a genuine storage fault is still a server error",
			err:        errors.NewStorageError("failed to save transaction"),
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:       "a fault in the transaction is still a client error",
			err:        errors.NewTxInvalidError("bad script"),
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.wantStatus, httpStatusForTxError(tt.err))
		})
	}
}

// TestIsTransientBackpressureWithoutBlockchainClient checks the nil-client path, which a propagation
// server configured without a blockchain client takes for every error.
//
// Classification reads the error alone, so it must behave identically with no client at all. That is
// what makes it safe on the paths where the flag has already moved on by the time we log.
func TestIsTransientBackpressureWithoutBlockchainClient(t *testing.T) {
	ps := &PropagationServer{}

	require.True(t, ps.isTransientBackpressure(
		errors.NewServiceUnavailableError("block assembly is full, not accepting new transactions")),
		"the refusal is carried by the error code, so it is recognised without consulting a client")

	require.False(t, ps.isTransientBackpressure(
		errors.NewStorageError("failed to save transaction",
			errors.NewServiceUnavailableError("[File] write operation timed out waiting for semaphore permit"))),
		"a wrapped store outage is a fault, not backpressure, whatever the client reports")

	require.True(t, ps.isTransientBackpressure(status.Error(codes.Unavailable, "server at capacity")),
		"admission control does not depend on block assembly, so it is still backpressure")
}

// TestProcessTransactionAcceptedWhenBlockAssemblyNotFull checks the default. A node that has heard
// nothing from block assembly must accept transactions rather than refusing them.
func TestProcessTransactionAcceptedWhenBlockAssemblyNotFull(t *testing.T) {
	tracing.SetupMockTracer()
	initPrometheusMetrics()

	ctx := t.Context()

	validatorInstance, utxoStore := setupRealValidator(t, ctx)

	tSettings := test.CreateBaseTestSettings(t)
	blockchainClient := testutil.NewMemorySQLiteBlockchainClient(ulogger.TestLogger{}, tSettings, t)

	require.False(t, blockchainClient.IsBlockAssemblyFull(),
		"a client that has heard nothing must default to accepting transactions")

	ps := &PropagationServer{
		logger:           ulogger.TestLogger{},
		settings:         tSettings,
		validator:        validatorInstance,
		blockchainClient: blockchainClient,
		txStore:          memory.New(),
	}

	txs := transactions.CreateTestTransactionChainWithCount(t, 3)

	_, _, err := utxoStore.SpendAndCreate(ctx, txs[0], 1, utxo.WithCreateOnly())
	require.NoError(t, err)

	_, err = ps.ProcessTransaction(ctx, &propagation_api.ProcessTransactionRequest{
		Tx: txs[1].ExtendedBytes(),
	})
	require.NoError(t, err)
}

// BenchmarkIsTransientBackpressure pins the cost of classifying the ingress refusal.
//
// This runs once per refused transaction, at full inbound rate, for as long as block assembly stays
// full — on a node that is already short of memory, which is the whole reason the refusal exists. It
// must not allocate on the path a refusal actually takes.
//
// The refusal is a teranode *Error, which does not implement GRPCStatus(). Reaching for status.Code
// first would therefore fall through to status.FromError, rendering the whole wrapped message and
// allocating a Status before returning Unknown. Matching the top-level code with a type assertion
// keeps the hot path free of both.
func BenchmarkIsTransientBackpressure(b *testing.B) {
	ps := &PropagationServer{}

	refusal := errors.NewServiceUnavailableError(
		"[ProcessTransaction][%s] block assembly is full, not accepting new transactions",
		chainhash.Hash{})

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if !ps.isTransientBackpressure(refusal) {
			b.Fatal("the refusal must classify as backpressure")
		}
	}
}
