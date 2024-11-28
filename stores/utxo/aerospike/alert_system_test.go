package aerospike

import (
	"context"
	"testing"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/util/uaerospike"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Create a sample UTXO spend
var txID, _ = chainhash.NewHashFromStr("5e3bc5947f48cec766090aa17f309fd16259de029dcef5d306b514848c9687c7")

func TestAlertSystem(t *testing.T) {
	client, db, _, deferFn := initAerospike(t)
	defer deferFn()

	t.Run("FreezeUTXOs", func(t *testing.T) {
		spend := &utxo.Spend{
			TxID:         txID,
			Vout:         0,
			UTXOHash:     utxoHash0,
			SpendingTxID: txID,
		}

		spends := []*utxo.Spend{spend}

		// Create a key for the UTXO
		keySource := uaerospike.CalculateKeySource(spend.TxID, spend.Vout/uint32(db.utxoBatchSize)) //nolint:gosec
		key, aErr := aerospike.NewKey(db.namespace, db.setName, keySource)
		require.NoError(t, aErr)

		// Insert a mock UTXO record
		bins := aerospike.BinMap{
			"utxos": []interface{}{utxoHash0[:]},
		}
		aErr = client.Put(nil, key, bins)
		require.NoError(t, aErr)

		// Call FreezeUTXO
		err := db.FreezeUTXOs(context.Background(), spends)
		require.NoError(t, err)

		// Verify the UTXO is frozen
		rec, err := client.Get(nil, key)
		require.NoError(t, err)
		require.NotNil(t, rec)

		utxos, ok := rec.Bins["utxos"].([]interface{})
		require.True(t, ok)
		require.Len(t, utxos, 1)

		frozenUTXO, ok := utxos[0].([]byte)
		require.True(t, ok)
		require.Len(t, frozenUTXO, 64)
		assert.Equal(t, utxoHash0[:], frozenUTXO[:32])
		assert.Equal(t, frozenUTXOBytes, frozenUTXO[32:64])

		// try to spend the UTXO
		err = db.Spend(context.Background(), spends, 0)
		require.Error(t, err)
		require.Contains(t, err.Error(), "UTXO is frozen")
	})

	t.Run("UnFreezeUTXOs", func(t *testing.T) {
		var err error

		spend := &utxo.Spend{
			TxID:         txID,
			Vout:         0,
			UTXOHash:     utxoHash0,
			SpendingTxID: txID,
		}

		spends := []*utxo.Spend{spend}

		// Create a key for the UTXO
		keySource := uaerospike.CalculateKeySource(spend.TxID, spend.Vout/uint32(db.utxoBatchSize)) //nolint:gosec
		key, err := aerospike.NewKey(db.namespace, db.setName, keySource)
		require.NoError(t, err)

		utxoBytes := make([]byte, 64)
		copy(utxoBytes[:32], utxoHash0[:])
		copy(utxoBytes[32:64], frozenUTXOBytes)

		// Insert a mock UTXO record
		bins := aerospike.BinMap{
			"utxos":      []interface{}{utxoBytes},
			"nrOfUTXOs":  1,
			"spentUtxos": 0,
		}
		err = client.Put(nil, key, bins)
		require.NoError(t, err)

		// try to spend the UTXO
		err = db.Spend(context.Background(), spends, 0)
		require.Error(t, err)
		require.Contains(t, err.Error(), "UTXO is frozen")

		// Call UnFreezeUTXOs
		err = db.UnFreezeUTXOs(context.Background(), spends)
		require.NoError(t, err)

		// Verify the UTXO is unfrozen
		rec, err := client.Get(nil, key)
		require.NoError(t, err)
		require.NotNil(t, rec)

		utxos, ok := rec.Bins["utxos"].([]interface{})
		require.True(t, ok)
		require.Len(t, utxos, 1)

		unfrozenUTXO, ok := utxos[0].([]byte)
		require.True(t, ok)
		require.Len(t, unfrozenUTXO, 32)
		assert.Equal(t, utxoHash0[:], unfrozenUTXO)

		// try to spend the UTXO
		err = db.Spend(context.Background(), spends, 0)
		require.NoError(t, err)
	})
	t.Run("ReAssignUTXO", func(t *testing.T) {
		err := db.SetBlockHeight(101)
		require.NoError(t, err)

		utxoRec := &utxo.Spend{
			TxID:         txID,
			Vout:         0,
			UTXOHash:     utxoHash0,
			SpendingTxID: txID,
		}

		newUtxoRec := &utxo.Spend{
			TxID:         txID,
			Vout:         0,
			UTXOHash:     utxoHash1,
			SpendingTxID: txID,
		}

		// Create a key for the UTXO
		keySource := uaerospike.CalculateKeySource(utxoRec.TxID, utxoRec.Vout/uint32(db.utxoBatchSize)) //nolint:gosec
		key, err := aerospike.NewKey(db.namespace, db.setName, keySource)
		require.NoError(t, err)

		// delete the key
		_, err = client.Delete(nil, key)
		require.NoError(t, err)

		// Insert a mock UTXO record
		bins := aerospike.BinMap{
			"utxos":      []interface{}{utxoHash0[:]},
			"nrUtxos":    1,
			"spentUtxos": 0,
		}
		err = client.Put(nil, key, bins)
		require.NoError(t, err)

		// check the UTXO is assigned
		rec, err := client.Get(nil, key)
		require.NoError(t, err)
		require.NotNil(t, rec)

		utxos, ok := rec.Bins["utxos"].([]interface{})
		require.True(t, ok)
		require.Len(t, utxos, 1)

		utxoBytes, ok := utxos[0].([]byte)
		require.True(t, ok)
		require.Len(t, utxoBytes, 32)
		assert.Equal(t, utxoHash0[:], utxoBytes)

		// Call ReAssignUTXO - should fail, utxo is not frozen
		err = db.ReAssignUTXO(context.Background(), utxoRec, newUtxoRec)
		require.Error(t, err)

		// Call FreezeUTXO
		err = db.FreezeUTXOs(context.Background(), []*utxo.Spend{utxoRec})
		require.NoError(t, err)

		// Call ReAssignUTXO
		err = db.ReAssignUTXO(context.Background(), utxoRec, newUtxoRec)
		require.NoError(t, err)

		// Verify the UTXO is re-assigned
		rec, err = client.Get(nil, key)
		require.NoError(t, err)
		require.NotNil(t, rec)

		utxos, ok = rec.Bins["utxos"].([]interface{})
		require.True(t, ok)
		require.Len(t, utxos, 1)

		utxoBytes, ok = utxos[0].([]byte)
		require.True(t, ok)
		require.Len(t, utxoBytes, 32)
		assert.NotEqual(t, utxoHash0[:], utxoBytes)
		assert.Equal(t, utxoHash1[:], utxoBytes)

		// check the reassignment list
		reassignment, ok := rec.Bins["reassignments"].([]interface{})
		require.True(t, ok)
		require.Len(t, reassignment, 1)

		reassignmentMap, ok := reassignment[0].(map[interface{}]interface{})
		require.True(t, ok)
		require.Len(t, reassignmentMap, 4)

		// check the reassignment record
		assert.Equal(t, utxoHash0[:], reassignmentMap["utxoHash"])
		assert.Equal(t, 0, reassignmentMap["offset"])
		assert.Equal(t, utxoHash1[:], reassignmentMap["newUtxoHash"])
		assert.Equal(t, 101, reassignmentMap["blockHeight"])

		utxoSpendableIn, ok := rec.Bins["utxoSpendableIn"].(map[interface{}]interface{})
		require.True(t, ok)

		// check the utxoSpendableIn record
		assert.Equal(t, 1101, utxoSpendableIn[0])

		// check the nrUtxos has been incremented
		nrUtxos, ok := rec.Bins["nrUtxos"].(int)
		require.True(t, ok)
		assert.Equal(t, 2, nrUtxos)

		// try to spend the UTXO with the original hash - should fail
		err = db.Spend(context.Background(), []*utxo.Spend{utxoRec}, 0)
		require.Error(t, err)

		// try to spend the UTXO with the new hash - should fail, block height has not been reached
		err = db.Spend(context.Background(), []*utxo.Spend{newUtxoRec}, 0)
		require.Error(t, err, "UTXO is not spendable yet")

		err = db.SetBlockHeight(1101)
		require.NoError(t, err)

		// try to spend the UTXO with the new hash
		err = db.Spend(context.Background(), []*utxo.Spend{newUtxoRec}, 0)
		require.NoError(t, err)
	})
}
