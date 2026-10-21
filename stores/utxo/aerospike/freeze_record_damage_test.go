package aerospike_test

import (
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	utxo2 "github.com/bsv-blockchain/teranode/test/longtest/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/stretchr/testify/require"
)

// TestFreezeRecordDamageIsRefusedByBothReaders pins that a freeze height outside the
// uint32 range — storage damage, since FreezeUTXOs only ever writes uint32 heights — is
// an error on both readers of the record, never a wrapped height (issue #1422): block
// validation's Go reader reports storage damage, and the Lua spend path refuses the spend
// with FREEZE_RECORD_DAMAGED, which the store surfaces as the same storage error. A fresh
// freeze then overwrites the damaged record, which is how it is repaired.
func TestFreezeRecordDamageIsRefusedByBothReaders(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)

	cleanDB(t, client)

	_, err := store.Create(ctx, tx, 0)
	require.NoError(t, err)

	keySource := uaerospike.CalculateKeySource(tx.TxIDChainHash(), 0, store.GetUtxoBatchSize()) //nolint:gosec
	key, aErr := aerospike.NewKey(store.GetNamespace(), store.GetName(), keySource)
	require.NoError(t, aErr)

	// A negative height in the from map: the shape a wrapped conversion would read as
	// FreezeWindowNever, disarming the freeze.
	_, aErr = client.Operate(nil, key, aerospike.MapPutOp(aerospike.DefaultMapPolicy(), fields.UtxoFreezeFrom.String(), 0, -1))
	require.NoError(t, aErr)

	t.Run("the Go reader reports storage damage", func(t *testing.T) {
		_, err := store.Get(ctx, tx.TxIDChainHash(), fields.UtxoFreezeFrom, fields.UtxoFreezeUntil, fields.UtxoFreezeExp)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrStorageError), "got: %v", err)
	})

	t.Run("the Lua spend path refuses the spend as storage damage", func(t *testing.T) {
		spendTx := utxo2.GetSpendingTx(tx, 0)

		spends, err := store.Spend(ctx, spendTx, 100)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrStorageError), "got: %v", err)
		require.False(t, errors.Is(err, errors.ErrUtxoConsensusFrozen), "damage must never masquerade as a verdict")

		if len(spends) > 0 && spends[0].Err != nil {
			require.True(t, errors.Is(spends[0].Err, errors.ErrStorageError), "got: %v", spends[0].Err)
		}
	})

	t.Run("a fresh freeze repairs the record and both readers agree again", func(t *testing.T) {
		require.NoError(t, store.FreezeUTXOs(ctx, []*utxo.Spend{{
			TxID: tx.TxIDChainHash(), Vout: 0, UTXOHash: utxoHash0,
			FreezeFrom: 500, FreezeUntil: 600,
		}}, tSettings))

		got, err := store.Get(ctx, tx.TxIDChainHash(), fields.UtxoFreezeFrom, fields.UtxoFreezeUntil, fields.UtxoFreezeExp)
		require.NoError(t, err)
		require.Equal(t, uint32(500), got.FreezeRecords[0].From)
		require.Equal(t, uint32(600), got.FreezeRecords[0].Until)

		spendTx := utxo2.GetSpendingTx(tx, 0)

		_, err = store.Spend(ctx, spendTx, 550, utxo.IgnoreFlags{IgnorePolicyFreeze: true})
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrUtxoConsensusFrozen), "the repaired record is judged normally, got: %v", err)
	})
}
