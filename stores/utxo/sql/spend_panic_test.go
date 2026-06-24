package sql

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestSpendPanicRecovery verifies the deferred recover in Spend propagates the
// panic as an error rather than silently returning (nil, nil). Passing a nil
// *bt.Tx triggers a nil pointer dereference inside utxo.GetSpends, exercising
// the deferred recover in the real Spend function.
func TestSpendPanicRecovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, _ := setup(ctx, t)

	spends, err := store.Spend(ctx, nil, store.GetBlockHeight()+1)
	require.Error(t, err, "Spend must surface panic as error, not return (nil, nil)")
	require.Nil(t, spends)
	require.Contains(t, err.Error(), "panic")
}

// TestHandleSpendPanic exercises the helper directly so the panic-recovery
// contract is unit-tested without needing to provoke a real panic.
func TestHandleSpendPanic(t *testing.T) {
	initPrometheusMetrics()

	logger := ulogger.TestLogger{}

	t.Run("nil recovered leaves err untouched", func(t *testing.T) {
		var err error
		handleSpendPanic(nil, &err, logger)
		require.NoError(t, err)
	})

	t.Run("recovered value sets err when nil", func(t *testing.T) {
		var err error
		handleSpendPanic("boom", &err, logger)
		require.Error(t, err)
		require.Contains(t, err.Error(), "panic")
		require.Contains(t, err.Error(), "boom")
	})

	t.Run("existing err is preserved", func(t *testing.T) {
		var (
			original error = errors.NewUnknownError("original failure")
			err            = original
		)
		handleSpendPanic("late panic", &err, logger)
		require.Same(t, original, err)
	})
}
