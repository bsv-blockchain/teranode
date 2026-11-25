package aerospike_test

import (
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
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

	t.Run("freeze_already_spent_utxo_errors", func(t *testing.T) {
		err := store.FreezeUTXOs(ctx, []*utxo.Spend{spend}, tSettings)
		require.Error(t, err)
		// The SPENT errorCode branch fires; the dispatcher reports the spend
		// via the "Already spent" message (spendingData travels in a separate
		// response field, so the message-embedded hex parse falls through to
		// the plain-message error).
		require.Contains(t, err.Error(), "Already spent")
	})

	t.Run("unfreeze_unfrozen_utxo_errors", func(t *testing.T) {
		err := store.UnFreezeUTXOs(ctx, []*utxo.Spend{spend}, tSettings)
		require.Error(t, err)
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

	t.Run("freeze_mismatched_commitment_errors", func(t *testing.T) {
		// An existing, unspent output frozen with the wrong commitment must fail.
		// The record still exists, so this is not the missing-record no-op above.
		unspentTx := chainhash.HashH([]byte("freeze-mismatch-tx"))
		storedUtxo := chainhash.HashH([]byte("freeze-mismatch-stored-utxo"))
		wrongUtxo := chainhash.HashH([]byte("freeze-mismatch-wrong-utxo"))

		unspentKeySource := uaerospike.CalculateKeySource(&unspentTx, 0, store.GetUtxoBatchSize())
		unspentKey, keyErr := aerospike.NewKey(store.GetNamespace(), store.GetName(), unspentKeySource)
		require.NoError(t, keyErr)

		require.NoError(t, client.Put(nil, unspentKey, aerospike.BinMap{
			fields.Utxos.String():      []interface{}{storedUtxo[:]},
			fields.TotalUtxos.String(): 1,
			fields.SpentUtxos.String(): 0,
		}))

		err := store.FreezeUTXOs(ctx, []*utxo.Spend{{TxID: &unspentTx, Vout: 0, UTXOHash: &wrongUtxo}}, tSettings)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to freeze aerospike utxo")

		// The output was not frozen: the correct commitment still freezes it.
		require.NoError(t, store.FreezeUTXOs(ctx, []*utxo.Spend{{TxID: &unspentTx, Vout: 0, UTXOHash: &storedUtxo}}, tSettings))
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
