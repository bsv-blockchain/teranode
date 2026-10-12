package aerospike_test

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestStore_Spend_AbortReturnsNilSlice exercises the group.Wait timeout/abort
// branch of Store.Spend under the race detector. Two guarantees are asserted:
//
//  1. Spend returns a NIL spends slice on abort — it must not hand back the
//     live slice whose *utxo.Spend elements the dispatcher goroutine may still
//     be writing (running on the store ctx, not the caller ctx). Returning the
//     live slice was a data race and could misclassify an in-flight, still
//     Err==nil item as a successful spend.
//
//  2. Run with -race, the internal resolveSpendCompletions(onlyCompleted=true)
//     pass that computes abort-time rollback must not race the dispatcher: it
//     reads a slot only after observing that item's published flag.
func TestStore_Spend_AbortReturnsNilSlice(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(func() {
		deferFn()
	})

	cleanDB(t, client)

	// Create the parent so spendTx's referenced UTXOs exist and are spendable.
	_, err := store.Create(ctx, tx, 101)
	require.NoError(t, err)

	// An extremely short wait forces group.Wait to abort before the batcher can
	// respond, taking the timeout branch while the dispatcher is still in flight.
	tSettings.UtxoStore.SpendWaitTimeout = time.Nanosecond

	spends, err := store.Spend(ctx, spendTx, 101)
	require.Error(t, err)
	require.True(t,
		errors.Is(err, errors.ErrServiceUnavailable) || errors.Is(err, errors.ErrContextCanceled),
		"abort must surface a service-unavailable/context error, got: %v", err)
	require.Nil(t, spends, "Spend must not return the live spends slice on the abort path")
}
