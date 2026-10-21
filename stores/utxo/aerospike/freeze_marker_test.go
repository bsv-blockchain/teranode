package aerospike_test

import (
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/stretchr/testify/require"
)

// TestFreezeMarkerOnExtraRecords pins how the utxoFreezeRecs marker and the extra
// (pagination) records it names behave together (issue #1422). FreezeUTXOs writes the
// marker before the freeze and without checking that the record exists, since no check
// could be atomic with the write; so a freeze on an extra record is always marked, a
// marker naming a record the store does not hold reads as no record, and only a marked
// record that is really damaged fails the read.
func TestFreezeMarkerOnExtraRecords(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)

	cleanDB(t, client)

	const batchSize = 5

	store.SetUtxoBatchSize(batchSize)

	paged := createTransactionWithOutputs(20) // records 0..3
	txID := paged.TxIDChainHash()

	_, err := store.Create(ctx, paged, 0)
	require.NoError(t, err)

	mainKey, aErr := aerospike.NewKey(store.GetNamespace(), store.GetName(), uaerospike.CalculateKeySourceInternal(txID, 0))
	require.NoError(t, aErr)

	freezeFields := []fields.FieldName{fields.UtxoFreezeFrom, fields.UtxoFreezeUntil, fields.UtxoFreezeExp}

	const vout = 7 // record 1

	utxoHash, err := util.UTXOHashFromOutput(txID, paged.Outputs[vout], vout)
	require.NoError(t, err)

	require.NoError(t, store.FreezeUTXOs(ctx, []*utxo.Spend{{TxID: txID, Vout: vout, UTXOHash: utxoHash, FreezeFrom: 500, FreezeUntil: 600}}, tSettings))

	marker := func(t *testing.T) map[interface{}]interface{} {
		rec, getErr := client.Get(nil, mainKey, fields.UtxoFreezeRecs.String())
		require.NoError(t, getErr)

		m, _ := rec.Bins[fields.UtxoFreezeRecs.String()].(map[interface{}]interface{})

		return m
	}

	t.Run("a freeze on an extra record is marked and read back", func(t *testing.T) {
		require.Contains(t, marker(t), 1)

		got, err := store.Get(ctx, txID, freezeFields...)
		require.NoError(t, err)
		require.Equal(t, uint32(500), got.FreezeRecords[vout].From)
		require.Equal(t, uint32(600), got.FreezeRecords[vout].Until)
	})

	t.Run("a marker naming a record the store does not hold reads as no record", func(t *testing.T) {
		_, opErr := client.Operate(nil, mainKey, aerospike.MapPutOp(aerospike.DefaultMapPolicy(), fields.UtxoFreezeRecs.String(), 9, 1))
		require.NoError(t, opErr)

		got, err := store.Get(ctx, txID, freezeFields...)
		require.NoError(t, err)
		require.Len(t, got.FreezeRecords, 1)
	})

	t.Run("a marker naming a record with no freeze bins reads as no record", func(t *testing.T) {
		_, opErr := client.Operate(nil, mainKey, aerospike.MapPutOp(aerospike.DefaultMapPolicy(), fields.UtxoFreezeRecs.String(), 2, 1))
		require.NoError(t, opErr)

		got, err := store.Get(ctx, txID, freezeFields...)
		require.NoError(t, err)
		require.Len(t, got.FreezeRecords, 1)
	})

	t.Run("a marked record whose freeze data is damaged fails the read as storage damage", func(t *testing.T) {
		extraKey, keyErr := aerospike.NewKey(store.GetNamespace(), store.GetName(), uaerospike.CalculateKeySourceInternal(txID, 3))
		require.NoError(t, keyErr)

		_, opErr := client.Operate(nil, extraKey, aerospike.PutOp(aerospike.NewBin(fields.UtxoFreezeFrom.String(), "damaged")))
		require.NoError(t, opErr)

		_, opErr = client.Operate(nil, mainKey, aerospike.MapPutOp(aerospike.DefaultMapPolicy(), fields.UtxoFreezeRecs.String(), 3, 1))
		require.NoError(t, opErr)

		_, err := store.Get(ctx, txID, freezeFields...)
		require.ErrorIs(t, err, errors.ErrStorageError)
	})

	t.Run("a freeze of a transaction the store does not hold writes no marker", func(t *testing.T) {
		missing := chainhash.HashH([]byte("not-in-store"))

		require.NoError(t, store.FreezeUTXOs(ctx, []*utxo.Spend{{TxID: &missing, Vout: vout, UTXOHash: utxoHash, FreezeFrom: 500}}, tSettings))

		key, keyErr := aerospike.NewKey(store.GetNamespace(), store.GetName(), uaerospike.CalculateKeySourceInternal(&missing, 0))
		require.NoError(t, keyErr)

		exists, existsErr := client.Exists(nil, key)
		require.NoError(t, existsErr)
		require.False(t, exists, "UPDATE_ONLY must not create a main record to hold the marker")
	})
}
