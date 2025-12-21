package aerospike_test

import (
	"testing"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	teranode_aerospike "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	utxo2 "github.com/bsv-blockchain/teranode/test/longtest/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// go test -v -tags test_aerospike ./test/...

func TestAlertSystem(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)

	t.Cleanup(func() {
		deferFn()
	})

	t.Run("FreezeUTXOs", func(t *testing.T) {
		spend := &utxo.Spend{
			TxID:         tx.TxIDChainHash(),
			Vout:         0,
			UTXOHash:     utxoHash0,
			SpendingData: spendpkg.NewSpendingData(tx.TxIDChainHash(), 0),
		}

		// Create a key for the UTXO
		keySource := uaerospike.CalculateKeySource(spend.TxID, spend.Vout, store.GetUtxoBatchSize()) //nolint:gosec
		key, aErr := aerospike.NewKey(store.GetNamespace(), store.GetName(), keySource)
		require.NoError(t, aErr)

		// Insert a mock UTXO record
		bins := aerospike.BinMap{
			fields.Utxos.String(): []interface{}{utxoHash0[:]},
		}
		aErr = client.Put(nil, key, bins)
		require.NoError(t, aErr)

		tSettings := test.CreateBaseTestSettings(t)

		// Call FreezeUTXO
		err := store.FreezeUTXOs(ctx, []*utxo.Spend{spend}, tSettings)
		require.NoError(t, err)

		// Verify the UTXO is frozen
		rec, err := client.Get(nil, key)
		require.NoError(t, err)
		require.NotNil(t, rec)

		utxos, ok := rec.Bins[fields.Utxos.String()].([]interface{})
		require.True(t, ok)
		require.Len(t, utxos, 1)

		frozenUTXO, ok := utxos[0].([]byte)
		require.True(t, ok)
		require.Len(t, frozenUTXO, 68)
		assert.Equal(t, utxoHash0[:], frozenUTXO[:32])
		assert.Equal(t, teranode_aerospike.GetFrozenUTXOBytes(), frozenUTXO[32:68])

		// try to spend the UTXO
		spends, err := store.Spend(ctx, spendTx, store.GetBlockHeight()+1)
		require.Error(t, err)

		var tErr *errors.Error
		require.ErrorAs(t, err, &tErr)
		require.Equal(t, errors.ERR_UTXO_ERROR, tErr.Code())
		require.ErrorIs(t, spends[0].Err, errors.ErrFrozen)
	})

	t.Run("UnFreezeUTXOs", func(t *testing.T) {
		var err error

		spend := &utxo.Spend{
			TxID:         tx.TxIDChainHash(),
			Vout:         0,
			UTXOHash:     utxoHash0,
			SpendingData: spendpkg.NewSpendingData(tx.TxIDChainHash(), 0),
		}

		// Create a key for the UTXO
		keySource := uaerospike.CalculateKeySource(spend.TxID, spend.Vout, store.GetUtxoBatchSize())
		key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), keySource)
		require.NoError(t, err)

		utxoBytes := make([]byte, 68)
		copy(utxoBytes[:32], utxoHash0[:])
		copy(utxoBytes[32:68], teranode_aerospike.GetFrozenUTXOBytes())

		// Insert a mock UTXO record
		bins := aerospike.BinMap{
			fields.Utxos.String():       []interface{}{utxoBytes},
			fields.TotalUtxos.String():  1,
			fields.SpentUtxos.String():  0,
			fields.RecordUtxos.String(): 1,
		}
		err = client.Put(nil, key, bins)
		require.NoError(t, err)

		// try to spend the UTXO
		spends, err := store.Spend(ctx, spendTx, store.GetBlockHeight()+1)

		var tErr *errors.Error
		require.ErrorAs(t, err, &tErr)
		require.Equal(t, errors.ERR_UTXO_ERROR, tErr.Code())
		require.ErrorIs(t, spends[0].Err, errors.ErrFrozen)

		tSettings := test.CreateBaseTestSettings(t)

		// Call UnFreezeUTXOs
		err = store.UnFreezeUTXOs(ctx, []*utxo.Spend{spend}, tSettings)
		require.NoError(t, err)

		// Verify the UTXO is unfrozen
		rec, err := client.Get(nil, key)
		require.NoError(t, err)
		require.NotNil(t, rec)

		utxos, ok := rec.Bins[fields.Utxos.String()].([]interface{})
		require.True(t, ok)
		require.Len(t, utxos, 1)

		unfrozenUTXO, ok := utxos[0].([]byte)
		require.True(t, ok)
		require.Len(t, unfrozenUTXO, 32)
		assert.Equal(t, utxoHash0[:], unfrozenUTXO)

		// try to spend the UTXO
		_, err = store.Spend(ctx, spendTx, store.GetBlockHeight()+1)
		require.NoError(t, err)
	})

	t.Run("ReAssignUTXO", func(t *testing.T) {
		err := store.SetBlockHeight(101)
		require.NoError(t, err)

		utxoRecTx := utxo2.GetSpendingTx(tx, 0)
		utxoRec := &utxo.Spend{
			TxID:         tx.TxIDChainHash(),
			Vout:         0,
			UTXOHash:     utxoHash0,
			SpendingData: spendpkg.NewSpendingData(utxoRecTx.TxIDChainHash(), 0),
		}

		newUtxoRecTx := utxo2.GetSpendingTx(tx, 0)

		privKey, err := bec.NewPrivateKey()
		require.NoError(t, err)

		newLockingScript, err := bscript.NewP2PKHFromPubKeyBytes(privKey.PubKey().Compressed())
		require.NoError(t, err)

		newOutput := &bt.Output{
			Satoshis:      tx.Outputs[0].Satoshis,
			LockingScript: newLockingScript,
		}

		// extend the tx
		newUtxoRecTx.Inputs[0].PreviousTxSatoshis = newOutput.Satoshis
		newUtxoRecTx.Inputs[0].PreviousTxScript = newOutput.LockingScript

		newUtxoHash, _ := util.UTXOHashFromOutput(tx.TxIDChainHash(), newOutput, 0)

		newUtxoRec := &utxo.Spend{
			TxID:         tx.TxIDChainHash(),
			Vout:         0,
			UTXOHash:     newUtxoHash,
			SpendingData: spendpkg.NewSpendingData(newUtxoRecTx.TxIDChainHash(), 0),
		}

		// Create a key for the UTXO
		keySource := uaerospike.CalculateKeySource(utxoRec.TxID, utxoRec.Vout, store.GetUtxoBatchSize())
		key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), keySource)
		require.NoError(t, err)

		// delete the key
		_, err = client.Delete(nil, key)
		require.NoError(t, err)

		// Insert a mock UTXO record
		bins := aerospike.BinMap{
			fields.Utxos.String():       []interface{}{utxoHash0[:]},
			fields.TotalUtxos.String():  1,
			fields.SpentUtxos.String():  0,
			fields.RecordUtxos.String(): 1,
		}
		err = client.Put(nil, key, bins)
		require.NoError(t, err)

		// check the UTXO is assigned
		rec, err := client.Get(nil, key)
		require.NoError(t, err)
		require.NotNil(t, rec)

		utxos, ok := rec.Bins[fields.Utxos.String()].([]interface{})
		require.True(t, ok)
		require.Len(t, utxos, 1)

		utxoBytes, ok := utxos[0].([]byte)
		require.True(t, ok)
		require.Len(t, utxoBytes, 32)
		assert.Equal(t, utxoHash0[:], utxoBytes)

		tSettings := test.CreateBaseTestSettings(t)

		// Call ReAssignUTXO - should fail, utxo is not frozen
		err = store.ReAssignUTXO(ctx, utxoRec, newUtxoRec, tSettings)
		require.Error(t, err)

		// Call FreezeUTXO
		err = store.FreezeUTXOs(ctx, []*utxo.Spend{utxoRec}, tSettings)
		require.NoError(t, err)

		// Call ReAssignUTXO
		err = store.ReAssignUTXO(ctx, utxoRec, newUtxoRec, tSettings)
		require.NoError(t, err)

		// Verify the UTXO is re-assigned
		rec, err = client.Get(nil, key)
		require.NoError(t, err)
		require.NotNil(t, rec)

		utxos, ok = rec.Bins[fields.Utxos.String()].([]interface{})
		require.True(t, ok)
		require.Len(t, utxos, 1)

		utxoBytes, ok = utxos[0].([]byte)
		require.True(t, ok)
		require.Len(t, utxoBytes, 32)
		assert.NotEqual(t, utxoHash0[:], utxoBytes)
		assert.Equal(t, newUtxoRec.UTXOHash[:], utxoBytes)

		// check the reassignment list
		reassignment, ok := rec.Bins[fields.Reassignments.String()].([]interface{})
		require.True(t, ok)
		require.Len(t, reassignment, 1)

		reassignmentMap, ok := reassignment[0].(map[interface{}]interface{})
		require.True(t, ok)
		require.Len(t, reassignmentMap, 4)

		// check the reassignment record
		assert.Equal(t, utxoHash0[:], reassignmentMap["utxoHash"])
		assert.Equal(t, 0, reassignmentMap["offset"])
		assert.Equal(t, newUtxoRec.UTXOHash[:], reassignmentMap["newUtxoHash"])
		assert.Equal(t, 101, reassignmentMap["blockHeight"])

		utxoSpendableIn, ok := rec.Bins[fields.UtxoSpendableIn.String()].(map[interface{}]interface{})
		require.True(t, ok)

		// check the utxoSpendableIn record
		assert.Equal(t, 1101, utxoSpendableIn[0])

		// check the totalUtxos has been incremented
		recordUtxos, ok := rec.Bins[fields.RecordUtxos.String()].(int)
		require.True(t, ok)
		assert.Equal(t, 2, recordUtxos)

		// try to spend the UTXO with the original hash - should fail
		_, err = store.Spend(ctx, utxoRecTx, store.GetBlockHeight()+1)
		require.Error(t, err)

		// try to spend the UTXO with the new hash - should fail, block height has not been reached
		_, err = store.Spend(ctx, newUtxoRecTx, store.GetBlockHeight()+1)
		require.Error(t, err, "UTXO is not spendable yet")

		err = store.SetBlockHeight(1101)
		require.NoError(t, err)

		// try to spend the UTXO with the new hash
		_, err = store.Spend(ctx, newUtxoRecTx, store.GetBlockHeight()+1)
		require.NoError(t, err)
	})
}
