package aerospike_test

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	teranode_aerospike "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/stretchr/testify/require"
)

// readSingleUtxoRecord returns the stored utxos[0] element bytes and the
// spentUtxos counter for the master record holding vout 0 of tx, for a store
// configured with utxoBatchSize == 1.
func readSingleUtxoRecord(t *testing.T, client *uaerospike.Client, tSettings *settings.Settings, store *teranode_aerospike.Store) ([]byte, int) {
	t.Helper()

	batchSize := store.GetUtxoBatchSize()
	require.Equal(t, 1, batchSize, "this helper assumes utxoBatchSize == 1")

	keySource := uaerospike.CalculateKeySource(tx.TxIDChainHash(), 0, batchSize)
	key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), keySource)
	require.NoError(t, err)

	rec, err := client.Get(util.GetAerospikeReadPolicy(tSettings), key, fields.Utxos.String(), fields.SpentUtxos.String())
	require.NoError(t, err)
	require.NotNil(t, rec)

	utxosBin, ok := rec.Bins[fields.Utxos.String()].([]any)
	require.True(t, ok, "utxos bin should be a list, got %T", rec.Bins[fields.Utxos.String()])
	require.GreaterOrEqual(t, len(utxosBin), 1)

	elem, ok := utxosBin[0].([]byte)
	require.True(t, ok, "utxos[0] should be []byte, got %T", utxosBin[0])

	spentUtxos := 0
	if v, ok := rec.Bins[fields.SpentUtxos.String()].(int); ok {
		spentUtxos = v
	}

	return elem, spentUtxos
}

// TestStore_SpendExpressions_CrossBatchDoubleSpend proves first-seen is enforced
// at the store level on the expression spend path, not just within a single
// batch.
//
// Issue #1153: with aerospike_enable_spend_filter_expressions=true (utxoBatchSize==1)
// the spend filter never checked whether the target UTXO was already spent, and
// ListSetOp overwrote the element unconditionally. A second spender in a SEPARATE
// batch therefore passed the filter and silently replaced the first-seen spender.
//
// Each spend below goes through Spend() in its own call (a distinct batch), so the
// in-batch FilterConflictingDuplicateSpendClaims guard does not apply — the store
// itself must reject the double-spend.
func TestStore_SpendExpressions_CrossBatchDoubleSpend(t *testing.T) {
	cases := []struct {
		name              string
		enableExpressions bool
	}{
		// "lua" runs with the flag off, so useExpressionSpend() is false and the new
		// ExpEq clause / FILTERED_OUT->Lua routing is never exercised — it is a parity
		// control proving the reference Lua path behaves identically, not a second test
		// of the fix. "expressions" is the path under test for issue #1153.
		{name: "lua", enableExpressions: false},
		{name: "expressions", enableExpressions: true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			logger := ulogger.NewErrorTestLogger(t)
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.UtxoStore.UtxoBatchSize = 1
			tSettings.UtxoStore.SpendBatcherSize = 1
			tSettings.UtxoStore.SpendBatcherDurationMillis = 5
			tSettings.UtxoStore.SpendWaitTimeout = 10 * time.Second
			tSettings.Aerospike.EnableSpendFilterExpressions = c.enableExpressions

			client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
			t.Cleanup(deferFn)

			cleanDB(t, client)

			_, err := store.Create(ctx, tx, 101)
			require.NoError(t, err)

			// First spender (batch 1) — must succeed.
			firstSpends, err := store.Spend(ctx, spendTx, 200)
			require.NoError(t, err)
			require.Len(t, firstSpends, 1)
			require.NoError(t, firstSpends[0].Err)

			firstSpenderBytes := firstSpends[0].SpendingData.Bytes()
			require.Len(t, firstSpenderBytes, 36)

			// Second, different spender of the SAME outpoint (batch 2) — must be
			// rejected with ErrSpent. spendTx2 is a distinct transaction
			// (GetSpendingTx randomises outputs), so it carries different
			// spending data than spendTx.
			secondSpends, err := store.Spend(ctx, spendTx2, 200)
			require.Error(t, err)
			require.Len(t, secondSpends, 1)
			require.Error(t, secondSpends[0].Err)
			require.ErrorIs(t, secondSpends[0].Err, errors.ErrSpent,
				"a second spender in a separate batch must be rejected as already-spent")

			// The stored spender must STILL be the first-seen one, and spentUtxos
			// must be exactly 1 (the double-spend attempt must not overwrite or
			// re-increment).
			elem, spentUtxos := readSingleUtxoRecord(t, client, tSettings, store)
			require.Len(t, elem, 68, "spent element must be 68 bytes (hash + spendingData)")
			require.Equal(t, firstSpends[0].UTXOHash[:], elem[0:32], "stored utxo hash must be unchanged")
			require.Equal(t, firstSpenderBytes, elem[32:68],
				"stored spender must still be the first-seen spender, not the double-spender")
			require.Equal(t, 1, spentUtxos, "spentUtxos must stay 1 after a rejected double-spend")
		})
	}
}

// TestStore_SpendExpressions_IdempotentRespendNoDoubleIncrement proves that a
// re-spend by the SAME spender in a separate batch is idempotent and does NOT
// double-increment spentUtxos.
//
// Issue #1153 (secondary): the expression path's unconditional AddOp(+1) meant an
// idempotent re-spend in a separate batch double-incremented spentUtxos, so the
// DAH condition spentUtxos==recordUtxos never held and the record leaked (was
// never pruned). With the unspent-element filter clause the re-spend is filtered
// out and routed through the Lua UDF, which recognises the identical spender and
// returns success without incrementing.
func TestStore_SpendExpressions_IdempotentRespendNoDoubleIncrement(t *testing.T) {
	cases := []struct {
		name              string
		enableExpressions bool
	}{
		// "lua" runs with the flag off, so useExpressionSpend() is false and the new
		// ExpEq clause / FILTERED_OUT->Lua routing is never exercised — it is a parity
		// control proving the reference Lua path behaves identically, not a second test
		// of the fix. "expressions" is the path under test for issue #1153.
		{name: "lua", enableExpressions: false},
		{name: "expressions", enableExpressions: true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			logger := ulogger.NewErrorTestLogger(t)
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.UtxoStore.UtxoBatchSize = 1
			tSettings.UtxoStore.SpendBatcherSize = 1
			tSettings.UtxoStore.SpendBatcherDurationMillis = 5
			tSettings.UtxoStore.SpendWaitTimeout = 10 * time.Second
			tSettings.Aerospike.EnableSpendFilterExpressions = c.enableExpressions

			client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
			t.Cleanup(deferFn)

			cleanDB(t, client)

			_, err := store.Create(ctx, tx, 101)
			require.NoError(t, err)

			// First spend — succeeds, spentUtxos -> 1.
			firstSpends, err := store.Spend(ctx, spendTx, 200)
			require.NoError(t, err)
			require.Len(t, firstSpends, 1)
			require.NoError(t, firstSpends[0].Err)

			firstSpenderBytes := firstSpends[0].SpendingData.Bytes()
			require.Len(t, firstSpenderBytes, 36)

			elemAfterFirst, spentAfterFirst := readSingleUtxoRecord(t, client, tSettings, store)
			require.Equal(t, 1, spentAfterFirst)
			require.Equal(t, firstSpenderBytes, elemAfterFirst[32:68])

			// Re-spend by the SAME spender in a separate batch — idempotent success.
			secondSpends, err := store.Spend(ctx, spendTx, 200)
			require.NoError(t, err, "idempotent re-spend by the same spender must succeed")
			require.Len(t, secondSpends, 1)
			require.NoError(t, secondSpends[0].Err)

			elemAfterSecond, spentAfterSecond := readSingleUtxoRecord(t, client, tSettings, store)
			require.Equal(t, 1, spentAfterSecond,
				"spentUtxos must stay 1 after an idempotent re-spend (no double-increment)")
			require.Equal(t, firstSpenderBytes, elemAfterSecond[32:68],
				"stored spending data must be unchanged after an idempotent re-spend")
		})
	}
}
