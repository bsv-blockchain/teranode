package aerospike

import (
	"testing"
	"time"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/uaerospike"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStore_SpendMultiRecord(t *testing.T) {
	client, db, ctx, deferFn := initAerospike(t)
	defer deferFn()

	t.Run("Spent tx id", func(t *testing.T) {
		// clean up the externalStore, if needed
		_ = db.externalStore.Del(ctx, tx.TxIDChainHash().CloneBytes(), options.WithFileExtension("tx"))

		// create a tx
		_, err := db.Create(ctx, tx, 101)
		require.NoError(t, err)

		// spend the tx
		err = db.Spend(ctx, []*utxostore.Spend{{TxID: tx.TxIDChainHash(), Vout: 0, UTXOHash: utxoHash0, SpendingTxID: spendingTxID1}}, 102)
		require.NoError(t, err)

		// spend again, should not return an error
		err = db.Spend(ctx, []*utxostore.Spend{{TxID: tx.TxIDChainHash(), Vout: 0, UTXOHash: utxoHash0, SpendingTxID: spendingTxID1}}, 102)
		require.NoError(t, err)

		// try to spend the tx with a different tx, check the spending tx ID
		err = db.Spend(ctx, []*utxostore.Spend{{TxID: tx.TxIDChainHash(), Vout: 0, UTXOHash: utxoHash0, SpendingTxID: spendingTxID2}}, 102)
		require.Error(t, err)

		var uErr errors.Interface
		ok := errors.As(err, &uErr)
		require.True(t, ok)

		assert.Contains(t, uErr.Error(), spendingTxID1.String())
	})

	t.Run("SpendMultiRecord LUA", func(t *testing.T) {
		key, aErr := aerospike.NewKey(db.namespace, db.setName, tx.TxIDChainHash().CloneBytes())
		require.NoError(t, aErr)

		cleanDB(t, client, key, tx)

		db.utxoBatchSize = 1

		// clean up the externalStore, if needed
		_ = db.externalStore.Del(ctx, tx.TxIDChainHash().CloneBytes(), options.WithFileExtension("tx"))

		// create a tx
		_, err := db.Create(ctx, tx, 101)
		require.NoError(t, err)

		// mine the tx
		err = db.SetMinedMulti(ctx, []*chainhash.Hash{tx.TxIDChainHash()}, 101)
		require.NoError(t, err)

		utxoHashes := make([]*chainhash.Hash, len(tx.Outputs))
		for vOut, txOut := range tx.Outputs {
			//nolint:gosec
			utxoHashes[vOut], err = util.UTXOHashFromOutput(tx.TxIDChainHash(), txOut, uint32(vOut))
			require.NoError(t, err)

			//nolint:gosec
			keySource := uaerospike.CalculateKeySource(tx.TxIDChainHash(), uint32(vOut/db.utxoBatchSize))
			key, err := aerospike.NewKey(db.namespace, db.setName, keySource)
			require.NoError(t, err)

			// check we created 5 records in aerospike properly
			resp, err := client.Get(nil, key)
			require.NoError(t, err)

			assert.Equal(t, 1, resp.Bins["nrUtxos"])

			if vOut == 0 {
				assert.Equal(t, true, resp.Bins["external"])
				assert.Equal(t, 5, resp.Bins["nrRecords"])
			} else {
				_, ok := resp.Bins["external"]
				require.False(t, ok)
			}
		}

		// check we created the tx in the external store
		exists, err := db.externalStore.Exists(ctx, tx.TxIDChainHash().CloneBytes(), options.WithFileExtension("tx"))
		require.NoError(t, err)
		require.True(t, exists)

		// check that the TTL is not set on the external store
		ttl, err := db.externalStore.GetTTL(ctx, tx.TxIDChainHash().CloneBytes(), options.WithFileExtension("tx"))
		require.NoError(t, err)
		require.Equal(t, time.Duration(0), ttl)

		keySource := uaerospike.CalculateKeySource(tx.TxIDChainHash(), uint32(0))
		mainRecordKey, err := aerospike.NewKey(db.namespace, db.setName, keySource)
		require.NoError(t, err)

		// spend the utxos, one by one, checking the return values from the lua script
		nrRecords := 5

		for i := 4; i >= 0; i-- {
			//nolint:gosec
			err = db.Spend(ctx, []*utxostore.Spend{{TxID: tx.TxIDChainHash(), Vout: uint32(i), UTXOHash: utxoHashes[i], SpendingTxID: txID}}, 102)
			require.NoError(t, err)

			// give the db time to update the main record
			time.Sleep(100 * time.Millisecond)

			// get nrRecords from main record
			resp, err := client.Get(nil, mainRecordKey)
			require.NoError(t, err)

			nrRecords--
			if nrRecords == 0 {
				// main record check
				assert.Greater(t, resp.Expiration, uint32(0)) // expiration has been set
				assert.Equal(t, 1, resp.Bins["nrRecords"])

				// check the external file ttl has been set
				ttl, err := db.externalStore.GetTTL(ctx, tx.TxIDChainHash().CloneBytes(), options.WithFileExtension("tx"))
				require.NoError(t, err)
				assert.Greater(t, ttl, time.Duration(0))
			} else {
				assert.Equal(t, resp.Expiration, uint32(aerospike.TTLDontExpire)) // expiration has been set
				assert.Equal(t, nrRecords, resp.Bins["nrRecords"])
			}
		}
	})
}

func TestStore_IncrementNrRecords(t *testing.T) {
	client, db, ctx, deferFn := initAerospike(t)
	defer deferFn()

	t.Run("Increment nrRecords", func(t *testing.T) {
		txID := tx.TxIDChainHash()

		key, aErr := aerospike.NewKey(db.namespace, db.setName, txID.CloneBytes())
		require.NoError(t, aErr)

		// Clean up the database
		cleanDB(t, client, key, tx)

		_, err := db.Create(ctx, tx, 101)
		require.NoError(t, err)

		// Increment nrRecords by 1
		res, err := db.incrementNrRecords(txID, 1)
		require.NoError(t, err)
		require.NotNil(t, res)

		// Verify the increment
		resp, err := client.Get(nil, key)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, 2, resp.Bins["nrRecords"])

		// Decrement nrRecords by 1
		res, err = db.incrementNrRecords(txID, -1)
		require.NoError(t, err)
		require.NotNil(t, res)

		// Verify the decrement
		resp, err = client.Get(nil, key)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, 1, resp.Bins["nrRecords"])

		r, ok := res.(string)
		require.True(t, ok)

		ret, err := db.parseLuaReturnValue(r)
		require.NoError(t, err)
		assert.Equal(t, LuaOk, ret.returnValue)
		assert.Equal(t, luaReturnValue(""), ret.signal)
		assert.Nil(t, ret.spendingTxID)
	})

	t.Run("Increment nrRecords - set TTL", func(t *testing.T) {
		txID := tx.TxIDChainHash()

		key, aErr := aerospike.NewKey(db.namespace, db.setName, txID.CloneBytes())
		require.NoError(t, aErr)

		// Clean up the database
		cleanDB(t, client, key, tx)

		_, err := db.Create(ctx, tx, 101)
		require.NoError(t, err)

		// force the values we expect to be set
		err = client.Put(nil, key, aerospike.BinMap{
			"spentUtxos": 5,
			"blockIDs":   []int{101},
			"nrRecords":  2,
		})
		require.NoError(t, err)

		rec, aErr := client.Get(nil, key)
		require.NoError(t, aErr)
		require.NotNil(t, rec)

		// Decrement nrRecords by 1
		res, err := db.incrementNrRecords(txID, -1)
		require.NoError(t, err)
		require.NotNil(t, res)

		r, ok := res.(string)
		require.True(t, ok)

		ret, err := db.parseLuaReturnValue(r)
		require.NoError(t, err)
		assert.Equal(t, LuaOk, ret.returnValue)
		assert.Equal(t, luaReturnValue("TTLSET"), ret.signal)
	})

	t.Run("Increment nrRecords - multi", func(t *testing.T) {
		txID := tx.TxIDChainHash()

		key, aErr := aerospike.NewKey(db.namespace, db.setName, txID.CloneBytes())
		require.NoError(t, aErr)

		// Clean up the database
		cleanDB(t, client, key, tx)

		_, err := db.Create(ctx, tx, 101)
		require.NoError(t, err)

		for i := 0; i < 5; i++ {
			// Decrement nrRecords by 1
			res, err := db.incrementNrRecords(txID, 1)
			require.NoError(t, err)
			require.NotNil(t, res)
		}

		rec, aErr := client.Get(nil, key)
		require.NoError(t, aErr)
		require.NotNil(t, rec)
		assert.Equal(t, 6, rec.Bins["nrRecords"])
	})
}
