//go:build test_all || test_stores || test_utxo || test_aerospike

package aerospike

import (
	"testing"
	"time"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	teranode_aerospike "github.com/bitcoin-sv/teranode/stores/utxo/aerospike"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/uaerospike"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// go test -v -tags test_aerospike ./test/...

func TestStore_SpendMultiRecord(t *testing.T) {
	client, db, ctx, deferFn := initAerospike(t)
	defer deferFn()

	t.Run("Spent tx id", func(t *testing.T) {
		// clean up the externalStore, if needed
		_ = db.GetExternalStore().Del(ctx, tx.TxIDChainHash().CloneBytes(), options.WithFileExtension("tx"))

		// create a tx
		_, err := db.Create(ctx, tx, 101)
		require.NoError(t, err)

		// spend the tx
		_, err = db.Spend(ctx, spendTx)
		require.NoError(t, err)

		// spend again, should not return an error
		_, err = db.Spend(ctx, spendTx)
		require.NoError(t, err)

		// try to spend the tx with a different tx, check the spending tx ID
		spends, err := db.Spend(ctx, spendTx2)
		require.Error(t, err)

		var tErr *errors.Error
		require.ErrorAs(t, err, &tErr)
		require.Equal(t, errors.ERR_TX_INVALID, tErr.Code())
		require.ErrorIs(t, spends[0].Err, errors.ErrSpent)
		require.Equal(t, spendTx.TxIDChainHash().String(), spends[0].ConflictingTxID.String())
	})

	t.Run("SpendMultiRecord LUA", func(t *testing.T) {
		key, aErr := aerospike.NewKey(db.GetNamespace(), db.GetName(), tx.TxIDChainHash().CloneBytes())
		require.NoError(t, aErr)

		cleanDB(t, client, key, tx)

		db.SetUtxoBatchSize(1)

		// clean up the externalStore, if needed
		_ = db.GetExternalStore().Del(ctx, tx.TxIDChainHash().CloneBytes(), options.WithFileExtension("tx"))

		// create a tx
		_, err := db.Create(ctx, tx, 101)
		require.NoError(t, err)

		// mine the tx
		err = db.SetMinedMulti(ctx, []*chainhash.Hash{tx.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: 101, BlockHeight: 101, SubtreeIdx: 101})
		require.NoError(t, err)

		utxoHashes := make([]*chainhash.Hash, len(tx.Outputs))
		for vOut, txOut := range tx.Outputs {
			//nolint:gosec
			utxoHashes[vOut], err = util.UTXOHashFromOutput(tx.TxIDChainHash(), txOut, uint32(vOut))
			require.NoError(t, err)

			//nolint:gosec
			keySource := uaerospike.CalculateKeySource(tx.TxIDChainHash(), uint32(vOut/db.GetUtxoBatchSize()))
			key, err := aerospike.NewKey(db.GetNamespace(), db.GetName(), keySource)
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
		exists, err := db.GetExternalStore().Exists(ctx, tx.TxIDChainHash().CloneBytes(), options.WithFileExtension("tx"))
		require.NoError(t, err)
		require.True(t, exists)

		// check that the TTL is not set on the external store
		ttl, err := db.GetExternalStore().GetTTL(ctx, tx.TxIDChainHash().CloneBytes(), options.WithFileExtension("tx"))
		require.NoError(t, err)
		require.Equal(t, time.Duration(0), ttl)

		keySource := uaerospike.CalculateKeySource(tx.TxIDChainHash(), uint32(0))
		mainRecordKey, err := aerospike.NewKey(db.GetNamespace(), db.GetName(), keySource)
		require.NoError(t, err)

		// spend 1,2,3,4
		_, err = db.Spend(ctx, spendTxRemaining)
		require.NoError(t, err)

		// give the db time to update the main record
		time.Sleep(100 * time.Millisecond)

		// get nrRecords from main record
		resp, err := client.Get(nil, mainRecordKey)
		require.NoError(t, err)

		// assert that the record is not yet marked for TTL
		assert.Equal(t, resp.Expiration, uint32(aerospike.TTLDontExpire)) // expiration has been set
		assert.Equal(t, 1, resp.Bins["nrRecords"])

		// spend 0
		_, err = db.Spend(ctx, spendTx)
		require.NoError(t, err)

		// main record check
		assert.Greater(t, resp.Expiration, uint32(0)) // expiration has been set
		assert.Equal(t, 1, resp.Bins["nrRecords"])

		// check the external file ttl has been set
		ttl, err = db.GetExternalStore().GetTTL(ctx, tx.TxIDChainHash().CloneBytes(), options.WithFileExtension("tx"))
		require.NoError(t, err)
		assert.Greater(t, ttl, time.Duration(0))
	})
}

func TestStore_IncrementNrRecords(t *testing.T) {
	client, db, ctx, deferFn := initAerospike(t)
	defer deferFn()

	t.Run("Increment nrRecords", func(t *testing.T) {
		txID := tx.TxIDChainHash()

		key, aErr := aerospike.NewKey(db.GetNamespace(), db.GetName(), txID.CloneBytes())
		require.NoError(t, aErr)

		// Clean up the database
		cleanDB(t, client, key, tx)

		_, err := db.Create(ctx, tx, 101)
		require.NoError(t, err)

		// Increment nrRecords by 1
		res, err := db.IncrementNrRecords(txID, 1)
		require.NoError(t, err)
		require.NotNil(t, res)

		// Verify the increment
		resp, err := client.Get(nil, key)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, 2, resp.Bins["nrRecords"])

		// Decrement nrRecords by 1
		res, err = db.IncrementNrRecords(txID, -1)
		require.NoError(t, err)
		require.NotNil(t, res)

		// Verify the decrement
		resp, err = client.Get(nil, key)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, 1, resp.Bins["nrRecords"])

		r, ok := res.(string)
		require.True(t, ok)

		ret, err := db.ParseLuaReturnValue(r)
		require.NoError(t, err)
		assert.Equal(t, teranode_aerospike.LuaOk, ret.ReturnValue)
		assert.Equal(t, teranode_aerospike.LuaReturnValue(""), ret.Signal)
		assert.Nil(t, ret.SpendingTxID)
	})

	t.Run("Increment nrRecords - set TTL", func(t *testing.T) {
		txID := tx.TxIDChainHash()

		key, aErr := aerospike.NewKey(db.GetNamespace(), db.GetName(), txID.CloneBytes())
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
		res, err := db.IncrementNrRecords(txID, -1)
		require.NoError(t, err)
		require.NotNil(t, res)

		r, ok := res.(string)
		require.True(t, ok)

		ret, err := db.ParseLuaReturnValue(r)
		require.NoError(t, err)
		assert.Equal(t, teranode_aerospike.LuaOk, ret.ReturnValue)
		assert.Equal(t, teranode_aerospike.LuaReturnValue("TTLSET"), ret.Signal)
	})

	t.Run("Increment nrRecords - multi", func(t *testing.T) {
		txID := tx.TxIDChainHash()

		key, aErr := aerospike.NewKey(db.GetNamespace(), db.GetName(), txID.CloneBytes())
		require.NoError(t, aErr)

		// Clean up the database
		cleanDB(t, client, key, tx)

		_, err := db.Create(ctx, tx, 101)
		require.NoError(t, err)

		for i := 0; i < 5; i++ {
			// Decrement nrRecords by 1
			res, err := db.IncrementNrRecords(txID, 1)
			require.NoError(t, err)
			require.NotNil(t, res)
		}

		rec, aErr := client.Get(nil, key)
		require.NoError(t, aErr)
		require.NotNil(t, rec)
		assert.Equal(t, 6, rec.Bins["nrRecords"])
	})
}

func TestStore_Unspend(t *testing.T) {
	client, db, ctx, deferFn := initAerospike(t)
	defer deferFn()

	t.Run("Successfully unspend a spent tx", func(t *testing.T) {
		// Clean up any existing data
		_ = db.GetExternalStore().Del(ctx, tx.TxIDChainHash().CloneBytes(), options.WithFileExtension("tx"))

		// Create a tx
		_, err := db.Create(ctx, tx, 101)
		require.NoError(t, err)

		// Spend the tx
		spends, err := db.Spend(ctx, spendTx)
		require.NoError(t, err)
		require.Len(t, spends, 1)

		// Unspend the tx
		err = db.Unspend(ctx, spends)
		require.NoError(t, err)

		// Verify we can now spend it again with a different tx
		spends, err = db.Spend(ctx, spendTx2)
		require.NoError(t, err)
		require.Len(t, spends, 1)
	})

	t.Run("Unspend a non-spent tx", func(t *testing.T) {
		txID := tx.TxIDChainHash()

		key, aErr := aerospike.NewKey(db.GetNamespace(), db.GetName(), txID.CloneBytes())
		require.NoError(t, aErr)

		// Clean up the database
		cleanDB(t, client, key, tx)

		// Clean up any existing data
		_ = db.GetExternalStore().Del(ctx, tx.TxIDChainHash().CloneBytes(), options.WithFileExtension("tx"))

		// Create a tx
		_, err := db.Create(ctx, tx, 101)
		require.NoError(t, err)

		utxoHash, err := util.UTXOHashFromOutput(
			tx.TxIDChainHash(),
			tx.Outputs[0],
			0,
		)
		require.NoError(t, err)

		// Try to unspend a tx that hasn't been spent
		err = db.Unspend(ctx, []*utxo.Spend{
			{
				TxID:     tx.TxIDChainHash(),
				Vout:     0,
				UTXOHash: utxoHash,
			},
		})
		require.NoError(t, err)

		// Verify we can still spend it
		spends, err := db.Spend(ctx, spendTx)
		require.NoError(t, err)
		require.Len(t, spends, 1)
	})
}
