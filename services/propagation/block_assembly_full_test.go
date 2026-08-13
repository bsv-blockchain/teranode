package propagation

import (
	"context"
	"net/http"
	"testing"

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
// raises it for an open circuit breaker or a batch timeout. Matching the error class alone would
// demote a disk stall or a tripped circuit breaker to debug, where the default log level discards
// it, and neither the rejection counter nor block assembly's transition warning would fire for it.
// So the refusal is recognised by the class AND the live flag together.
func TestIsTransientBackpressure(t *testing.T) {
	fullErr := errors.NewServiceUnavailableError("block assembly is full, not accepting new transactions")

	// What a stalled file blob store looks like by the time it reaches a log site: propagation wraps
	// the store failure in a StorageError, and errors.Is walks the chain down to the inner code.
	storeOutage := errors.NewStorageError("[ProcessTransaction] failed to save transaction",
		errors.NewServiceUnavailableError("[File] write operation timed out waiting for semaphore permit"))

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
			name:              "the refusal error class alone is not enough while block assembly has room",
			err:               fullErr,
			blockAssemblyFull: false,
			expected:          false,
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
func TestIsTransientBackpressureWithoutBlockchainClient(t *testing.T) {
	ps := &PropagationServer{}

	require.False(t, ps.isTransientBackpressure(
		errors.NewServiceUnavailableError("block assembly is full, not accepting new transactions")),
		"with no blockchain client there is no full condition to recognise, so the error keeps its level")

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
