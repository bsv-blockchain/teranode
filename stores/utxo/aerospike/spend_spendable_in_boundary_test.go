package aerospike_test

import (
	"testing"
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
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

// TestStore_SpendableInBoundary verifies the freeze-window boundary across the
// Aerospike Lua and expression spend paths.
//
// Reference behaviour: the freeze window is closed at start, open at stop —
// i.e. a UTXO becomes spendable AT spendableHeight. The SQL store enforces this
// with `item.blockHeight < *r.spendableIn` (strict <), and svnode's
// CFrozenTXOCheck enforces `nHeight < i.stop`.
//
// Pre-fix:
//   - Lua used `>=` (off-by-one — rejected at the boundary).
//   - Expression rejected ANY record with utxoSpendableIn set (over-strict).
//
// Post-fix:
//   - Lua uses `>` (matches SQL/svnode).
//   - Expression keeps the conservative bin-exists guard for safety, but the
//     result handler retries FILTERED_OUT records through Lua so the boundary
//     check is applied correctly.
func TestStore_SpendableInBoundary(t *testing.T) {
	cases := []struct {
		name              string
		enableExpressions bool
	}{
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

			const spendableHeight uint32 = 200

			boundary := []struct {
				name             string
				currentHeight    uint32
				wantSpendSuccess bool
			}{
				{name: "below_boundary_rejects", currentHeight: spendableHeight - 1, wantSpendSuccess: false},
				{name: "at_boundary_accepts", currentHeight: spendableHeight, wantSpendSuccess: true},
				{name: "above_boundary_accepts", currentHeight: spendableHeight + 1, wantSpendSuccess: true},
			}

			for _, tc := range boundary {
				t.Run(tc.name, func(t *testing.T) {
					cleanDB(t, client)

					_, err := store.Create(ctx, tx, 101)
					require.NoError(t, err)

					injectSpendableIn(t, client, tSettings, store, 0, spendableHeight)

					spends, err := store.Spend(ctx, spendTx, tc.currentHeight)
					if tc.wantSpendSuccess {
						require.NoError(t, err)
						require.Len(t, spends, 1)
						require.NoError(t, spends[0].Err)
						return
					}

					require.Error(t, err)
					require.Len(t, spends, 1)
					require.Error(t, spends[0].Err)
					require.ErrorIs(t, spends[0].Err, errors.ErrFrozen,
						"expected ErrFrozen at currentHeight=%d (spendableHeight=%d)", tc.currentHeight, spendableHeight)
				})
			}
		})
	}
}

// TestStore_SpendableInExpressionParity_PastValueAccepted exercises the property
// called out in the issue's acceptance criteria: a record with utxoSpendableIn
// set whose values are all past must be spendable through BOTH Aerospike paths.
//
// Pre-fix the expression path would reject this spend (it cannot inspect map
// values, only the bin's existence). Post-fix the FILTERED_OUT result is
// retried through Lua, which compares per-offset and accepts the spend.
func TestStore_SpendableInExpressionParity_PastValueAccepted(t *testing.T) {
	cases := []struct {
		name              string
		enableExpressions bool
	}{
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

			// spendableHeight far in the past — currentHeight (300) is well above.
			injectSpendableIn(t, client, tSettings, store, 0, 50)

			spends, err := store.Spend(ctx, spendTx, 300)
			require.NoError(t, err, "spend at currentHeight=300 with spendableHeight=50 must succeed on both paths")
			require.Len(t, spends, 1)
			require.NoError(t, spends[0].Err)
		})
	}
}

// TestStore_SpendableInExpressionParity_FutureValueRejected confirms the safety
// property the conservative guard enforces: a record with utxoSpendableIn[offset]
// strictly greater than currentBlockHeight must NEVER be spent through either
// path. After the T25 retry, the expression path still rejects (via Lua) with
// ErrFrozen, matching SQL and svnode behaviour.
func TestStore_SpendableInExpressionParity_FutureValueRejected(t *testing.T) {
	cases := []struct {
		name              string
		enableExpressions bool
	}{
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

			injectSpendableIn(t, client, tSettings, store, 0, 500)

			spends, err := store.Spend(ctx, spendTx, 100)
			require.Error(t, err, "spend at currentHeight=100 with spendableHeight=500 must fail on both paths")
			require.Len(t, spends, 1)
			require.Error(t, spends[0].Err)
			require.ErrorIs(t, spends[0].Err, errors.ErrFrozen)
		})
	}
}

// injectSpendableIn writes a single-offset utxoSpendableIn map onto the
// Aerospike record that holds the UTXO at the package-level test tx's given
// vout. It uses UPDATE so the rest of the record created by store.Create stays
// intact.
func injectSpendableIn(t *testing.T, client *uaerospike.Client, tSettings *settings.Settings, store *teranode_aerospike.Store, vout uint32, spendableHeight uint32) {
	t.Helper()

	batchSize := store.GetUtxoBatchSize()
	keySource := uaerospike.CalculateKeySource(tx.TxIDChainHash(), vout, batchSize)
	key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), keySource)
	require.NoError(t, err)

	// Offset within the per-batch record. For utxoBatchSize=1 this is always 0.
	offset := int(vout % uint32(batchSize)) // nolint:gosec

	writePolicy := util.GetAerospikeWritePolicy(tSettings, 0)
	writePolicy.RecordExistsAction = aerospike.UPDATE

	bins := aerospike.BinMap{
		fields.UtxoSpendableIn.String(): map[any]any{
			offset: int(spendableHeight),
		},
	}
	err = client.Put(writePolicy, key, bins)
	require.NoError(t, err)
}
