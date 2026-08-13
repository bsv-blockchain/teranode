package propagation

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/services/propagation/propagation_api"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/testutil"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setBlockAssemblyFull drives the real notification that block assembly publishes, so the test
// exercises the same path production uses rather than reaching into client internals.
func setBlockAssemblyFull(t *testing.T, ctx context.Context, client blockchain.ClientI, full bool) {
	t.Helper()

	value := "false"
	if full {
		value = "true"
	}

	require.NoError(t, client.SendNotification(ctx, &blockchain_api.Notification{
		Type: model.NotificationType_BlockAssemblyFull,
		Metadata: &blockchain_api.NotificationMetadata{
			Metadata: map[string]string{"full": value},
		},
	}))

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
	assert.True(t, errors.Is(err, errors.ErrServiceUnavailable),
		"a full block assembly is a transient service condition, not a fault in the transaction: %v", err)

	exists, err := txStore.Exists(ctx, tx.TxIDChainHash()[:], "tx")
	require.NoError(t, err)
	assert.False(t, exists, "a refused transaction must not be written to the transaction store")

	// Block assembly reports it has room again, and the same transaction is now accepted.
	setBlockAssemblyFull(t, ctx, blockchainClient, false)

	_, err = ps.ProcessTransaction(ctx, &propagation_api.ProcessTransactionRequest{
		Tx: tx.ExtendedBytes(),
	})
	require.NoError(t, err, "propagation must accept transactions once block assembly has room")

	exists, err = txStore.Exists(ctx, tx.TxIDChainHash()[:], "tx")
	require.NoError(t, err)
	assert.True(t, exists, "an accepted transaction must be written to the transaction store")
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
