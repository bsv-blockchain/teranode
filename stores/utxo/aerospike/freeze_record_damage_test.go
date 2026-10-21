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
// with FREEZE_RECORD_DAMAGED, which the store surfaces as a permanent processing error
// (never a retryable storage one). A fresh
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

	t.Run("the Lua spend path refuses the spend as permanent damage", func(t *testing.T) {
		spendTx := utxo2.GetSpendingTx(tx, 0)

		spends, err := store.Spend(ctx, spendTx, 100)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrProcessing), "got: %v", err)
		require.False(t, errors.IsRetryableError(err), "damage does not heal by itself, so it must not be retried: %v", err)
		require.False(t, errors.Is(err, errors.ErrUtxoConsensusFrozen), "damage must never masquerade as a verdict")

		require.NotEmpty(t, spends)
		require.Error(t, spends[0].Err)
		require.True(t, errors.Is(spends[0].Err, errors.ErrProcessing), "got: %v", spends[0].Err)
		require.False(t, errors.IsRetryableError(spends[0].Err))
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

// TestFreezeRecordDamageShapesAgree pins that every damaged shape the Go reader refuses is
// refused by the Lua spend path too, so block validation and the spend path cannot reach
// different verdicts on one record (issue #1422): a fractional height, a freeze bin that
// is not a map, and a policy-expiry entry that is not a bool. A string bin is the shape
// that used to fail open in Lua, where indexing a string by offset yields nil.
func TestFreezeRecordDamageShapesAgree(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)

	cases := []struct {
		name   string
		damage func(key *aerospike.Key) error
	}{
		{"fractional from height", func(key *aerospike.Key) error {
			_, err := client.Operate(nil, key, aerospike.MapPutOp(aerospike.DefaultMapPolicy(), fields.UtxoFreezeFrom.String(), 0, 500.5))
			return err
		}},
		{"from bin is not a map", func(key *aerospike.Key) error {
			_, err := client.Operate(nil, key, aerospike.PutOp(aerospike.NewBin(fields.UtxoFreezeFrom.String(), "damaged")))
			return err
		}},
		{"until bin is not a map", func(key *aerospike.Key) error {
			_, err := client.Operate(nil, key,
				aerospike.MapPutOp(aerospike.DefaultMapPolicy(), fields.UtxoFreezeFrom.String(), 0, 500),
				aerospike.PutOp(aerospike.NewBin(fields.UtxoFreezeUntil.String(), 7)))
			return err
		}},
		{"policy-expiry entry is not a bool", func(key *aerospike.Key) error {
			_, err := client.Operate(nil, key,
				aerospike.MapPutOp(aerospike.DefaultMapPolicy(), fields.UtxoFreezeFrom.String(), 0, 500),
				aerospike.MapPutOp(aerospike.DefaultMapPolicy(), fields.UtxoFreezeExp.String(), 0, 1))
			return err
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cleanDB(t, client)

			_, err := store.Create(ctx, tx, 0)
			require.NoError(t, err)

			keySource := uaerospike.CalculateKeySource(tx.TxIDChainHash(), 0, store.GetUtxoBatchSize()) //nolint:gosec
			key, aErr := aerospike.NewKey(store.GetNamespace(), store.GetName(), keySource)
			require.NoError(t, aErr)
			require.NoError(t, tc.damage(key))

			_, err = store.Get(ctx, tx.TxIDChainHash(), fields.UtxoFreezeFrom, fields.UtxoFreezeUntil, fields.UtxoFreezeExp)
			require.ErrorIs(t, err, errors.ErrStorageError, "the Go reader refuses the record")

			spends, err := store.Spend(ctx, utxo2.GetSpendingTx(tx, 0), 100, utxo.IgnoreFlags{IgnorePolicyFreeze: true})
			require.Error(t, err, "the Lua spend path refuses the record too")
			require.NotEmpty(t, spends)
			require.ErrorIs(t, spends[0].Err, errors.ErrProcessing, "got: %v", spends[0].Err)
			require.False(t, errors.IsRetryableError(spends[0].Err))
		})
	}
}
