package aerospike_test

import (
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/stretchr/testify/require"
)

// TestFreezeUnfreezeResultHandling exercises the per-record result branches of
// FreezeUTXOs / UnFreezeUTXOs / ReAssignUTXO that only fire on error-shaped
// responses: an already-spent UTXO (SPENT), a missing record (silently skipped
// on both transports — Lua TX_NOT_FOUND on the UDF path, KEY_NOT_FOUND under
// the native path's UPDATE_ONLY policy), and an unfrozen UTXO unfreeze.
func TestFreezeUnfreezeResultHandling(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)

	txHash := chainhash.HashH([]byte("freeze-result-handling-tx"))
	utxoHash := chainhash.HashH([]byte("freeze-result-handling-utxo"))
	spenderTx := chainhash.HashH([]byte("freeze-result-handling-spender"))
	spendingData := spendpkg.NewSpendingData(&spenderTx, 3)

	keySource := uaerospike.CalculateKeySource(&txHash, 0, store.GetUtxoBatchSize())
	key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), keySource)
	require.NoError(t, err)

	// A spent UTXO entry is utxoHash (32) + spendingData (36).
	spentUtxo := make([]byte, 0, 68)
	spentUtxo = append(spentUtxo, utxoHash[:]...)
	spentUtxo = append(spentUtxo, spendingData.Bytes()...)

	require.NoError(t, client.Put(nil, key, aerospike.BinMap{
		fields.Utxos.String():      []interface{}{spentUtxo},
		fields.TotalUtxos.String(): 1,
		fields.SpentUtxos.String(): 1,
	}))

	spend := &utxo.Spend{
		TxID:         &txHash,
		Vout:         0,
		UTXOHash:     &utxoHash,
		SpendingData: spendingData,
	}

	t.Run("unfreeze_unfrozen_utxo_errors", func(t *testing.T) {
		err := store.UnFreezeUTXOs(ctx, []*utxo.Spend{spend}, tSettings)
		require.ErrorIs(t, err, errors.ErrFrozen, "an output with neither marker nor record cannot be unfrozen")
	})

	t.Run("freeze_already_spent_utxo_records_the_window", func(t *testing.T) {
		// The consensus record is a property of the outpoint (#1422): a freeze on an
		// output already spent by a real transaction records the window and leaves the
		// spending data alone, so a late alert still governs a fork block or a re-mine
		// of that spend inside the window.
		spend.FreezeFrom = 500
		spend.FreezeUntil = 600

		require.NoError(t, store.FreezeUTXOs(ctx, []*utxo.Spend{spend}, tSettings))

		rec, err := client.Get(nil, key)
		require.NoError(t, err)

		fromMap, ok := rec.Bins[fields.UtxoFreezeFrom.String()].(map[interface{}]interface{})
		require.True(t, ok, "the freeze record must be written on the spent output")
		require.Equal(t, 500, fromMap[0])

		utxos, ok := rec.Bins[fields.Utxos.String()].([]interface{})
		require.True(t, ok)
		require.Equal(t, spentUtxo, utxos[0].([]byte), "the real spender must be left in place")

		// The identical record again is reported, not silently treated as a change.
		require.ErrorIs(t, store.FreezeUTXOs(ctx, []*utxo.Spend{spend}, tSettings), errors.ErrFrozen)
	})

	t.Run("unfreeze_spent_utxo_with_record_succeeds", func(t *testing.T) {
		require.NoError(t, store.UnFreezeUTXOs(ctx, []*utxo.Spend{spend}, tSettings))

		rec, err := client.Get(nil, key)
		require.NoError(t, err)

		_, found := rec.Bins[fields.UtxoFreezeFrom.String()]
		require.False(t, found, "unfreeze must drop the record from a spent output too")
	})

	t.Run("freeze_missing_record_is_skipped", func(t *testing.T) {
		missingTx := chainhash.HashH([]byte("freeze-missing-record-tx"))
		missingUtxo := chainhash.HashH([]byte("freeze-missing-record-utxo"))
		missingSpend := &utxo.Spend{
			TxID:         &missingTx,
			Vout:         0,
			UTXOHash:     &missingUtxo,
			SpendingData: spendingData,
		}

		// Missing records are a deliberate no-op on freeze: Lua TX_NOT_FOUND
		// on the UDF path, KEY_NOT_FOUND under native UPDATE_ONLY — both must
		// come back as success.
		require.NoError(t, store.FreezeUTXOs(ctx, []*utxo.Spend{missingSpend}, tSettings))
	})

	t.Run("reassign_missing_record_errors", func(t *testing.T) {
		missingTx := chainhash.HashH([]byte("reassign-missing-record-tx"))
		missingUtxo := chainhash.HashH([]byte("reassign-missing-record-utxo"))
		newUtxo := chainhash.HashH([]byte("reassign-missing-record-new-utxo"))

		err := store.ReAssignUTXO(ctx,
			&utxo.Spend{TxID: &missingTx, Vout: 0, UTXOHash: &missingUtxo, SpendingData: spendingData},
			&utxo.Spend{TxID: &missingTx, Vout: 0, UTXOHash: &newUtxo, SpendingData: spendingData},
			tSettings)
		require.Error(t, err)
	})
}
