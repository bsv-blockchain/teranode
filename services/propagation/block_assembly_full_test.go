package propagation

import (
	"context"
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
	require.True(t, isTransientBackpressure(err),
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
// error log.
//
// While block assembly is full, every inbound transaction is refused. All three call sites log
// through logTxProcessingError, so if a refusal were classified as an error the log pipeline would
// take one line per transaction for as long as the condition lasts — amplifying the exact overload
// the limit exists to contain and burying real errors.
func TestIsTransientBackpressure(t *testing.T) {
	fullErr := errors.NewServiceUnavailableError("block assembly is full, not accepting new transactions")

	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{name: "nil is not backpressure", err: nil, expected: false},
		{name: "the direct refusal", err: fullErr, expected: true},
		{
			name:     "the refusal after the gRPC handler flattens it",
			err:      errors.WrapGRPCPublic(fullErr),
			expected: true,
		},
		{
			name:     "the batch handler admission control rejection",
			err:      status.Error(codes.Unavailable, "server at capacity"),
			expected: true,
		},
		{
			name:     "a genuine fault in the transaction still logs as an error",
			err:      errors.NewTxInvalidError("bad script"),
			expected: false,
		},
		{
			name:     "a genuine fault survives the gRPC round trip as an error",
			err:      errors.WrapGRPCPublic(errors.NewTxInvalidError("bad script")),
			expected: false,
		},
		{
			name:     "a storage failure still logs as an error",
			err:      errors.NewStorageError("failed to save transaction"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, isTransientBackpressure(tt.err))
		})
	}
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
