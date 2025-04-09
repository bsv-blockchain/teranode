//go:build test_all || test_stores || test_utxo || test_aerospike

package aerospike

import (
	"testing"
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	teranode_aerospike "github.com/bitcoin-sv/teranode/stores/utxo/aerospike"
	"github.com/bitcoin-sv/teranode/stores/utxo/fields"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/bitcoin-sv/teranode/util/uaerospike"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// go test -v -tags test_aerospike ./test/...

func TestStore_SpendMultiRecord(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t, nil)
	tSettings := test.CreateBaseTestSettings()

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)

	t.Cleanup(func() {
		deferFn()
	})

	t.Run("Spent tx id", func(t *testing.T) {
		// clean up the externalStore, if needed
		_ = store.GetExternalStore().Del(ctx, tx.TxIDChainHash().CloneBytes(), options.WithFileExtension("tx"))

		// create a tx
		_, err := store.Create(ctx, tx, 101)
		require.NoError(t, err)

		// spend the tx
		_, err = store.Spend(ctx, spendTx)
		require.NoError(t, err)

		// spend again, should not return an error
		_, err = store.Spend(ctx, spendTx)
		require.NoError(t, err)

		// try to spend the tx with a different tx, check the spending tx ID
		spends, err := store.Spend(ctx, spendTx2)
		require.Error(t, err)

		var tErr *errors.Error
		require.ErrorAs(t, err, &tErr)
		require.Equal(t, errors.ERR_TX_INVALID, tErr.Code())
		require.ErrorIs(t, spends[0].Err, errors.ErrSpent)
		require.Equal(t, spendTx.TxIDChainHash().String(), spends[0].ConflictingTxID.String())
	})

	t.Run("SpendMultiRecord LUA", func(t *testing.T) {
		key, aErr := aerospike.NewKey(store.GetNamespace(), store.GetName(), tx.TxIDChainHash().CloneBytes())
		require.NoError(t, aErr)

		cleanDB(t, client, key, tx)

		store.SetUtxoBatchSize(1)

		// clean up the externalStore, if needed
		_ = store.GetExternalStore().Del(ctx, tx.TxIDChainHash().CloneBytes(), options.WithFileExtension("tx"))

		// create a tx
		_, err := store.Create(ctx, tx, 101)
		require.NoError(t, err)

		resp, err := client.Get(nil, key)
		require.NoError(t, err)

		// Check the totalExtraRecs and spentExtraRecs
		totalExtraRecs, ok := resp.Bins[fields.TotalExtraRecs.String()].(int)
		require.True(t, ok)
		assert.Equal(t, 4, totalExtraRecs) // parent is one, and there are 4 extra records

		_, ok = resp.Bins[fields.SpentExtraRecs.String()].(int)
		assert.False(t, ok)

		// mine the tx
		err = store.SetMinedMulti(ctx, []*chainhash.Hash{tx.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: 101, BlockHeight: 101, SubtreeIdx: 101})
		require.NoError(t, err)

		utxoHashes := make([]*chainhash.Hash, len(tx.Outputs))
		for vOut, txOut := range tx.Outputs {
			//nolint:gosec
			utxoHashes[vOut], err = util.UTXOHashFromOutput(tx.TxIDChainHash(), txOut, uint32(vOut))
			require.NoError(t, err)

			//nolint:gosec
			keySource := uaerospike.CalculateKeySource(tx.TxIDChainHash(), uint32(vOut/store.GetUtxoBatchSize()))
			key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), keySource)
			require.NoError(t, err)

			// check we created 5 records in aerospike properly
			resp, err := client.Get(nil, key)
			require.NoError(t, err)

			// We have a batch limit of 1 utxo per record.  Vout 0 is record 0 (the parent) and will have a totalUtxos of 5.
			// All other records do not have a totalUtxos field.
			if vOut == 0 {
				assert.Equal(t, 5, resp.Bins[fields.TotalUtxos.String()])
			} else {
				_, ok := resp.Bins[fields.TotalUtxos.String()]
				require.False(t, ok)
			}

			assert.Equal(t, 1, resp.Bins[fields.RecordUtxos.String()])

			if vOut == 0 {
				assert.Equal(t, true, resp.Bins[fields.External.String()])
				assert.Equal(t, 4, resp.Bins[fields.TotalExtraRecs.String()])
			} else {
				_, ok := resp.Bins[fields.External.String()]
				require.False(t, ok)
			}
		}

		// check we created the tx in the external store
		exists, err := store.GetExternalStore().Exists(ctx, tx.TxIDChainHash().CloneBytes(), options.WithFileExtension("tx"))
		require.NoError(t, err)
		require.True(t, exists)

		// check that the TTL is not set on the external store
		ttl, err := store.GetExternalStore().GetTTL(ctx, tx.TxIDChainHash().CloneBytes(), options.WithFileExtension("tx"))
		require.NoError(t, err)
		require.Equal(t, time.Duration(0), ttl)

		keySource := uaerospike.CalculateKeySource(tx.TxIDChainHash(), uint32(0))
		mainRecordKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), keySource)
		require.NoError(t, err)

		// spend 1,2,3,4
		_, err = store.Spend(ctx, spendTxRemaining)
		require.NoError(t, err)

		// give the db time to update the main record
		time.Sleep(100 * time.Millisecond)

		// get totalExtraRecs from main record
		resp, err = client.Get(nil, mainRecordKey)
		require.NoError(t, err)

		// assert that the record is not yet marked for TTL
		assert.Equal(t, resp.Expiration, uint32(aerospike.TTLDontExpire)) // expiration has not been set
		assert.Equal(t, 4, resp.Bins[fields.TotalExtraRecs.String()])
		assert.Equal(t, 4, resp.Bins[fields.SpentExtraRecs.String()])

		// spend 0
		_, err = store.Spend(ctx, spendTx)
		require.NoError(t, err)

		// main record check
		assert.Greater(t, resp.Expiration, uint32(0)) // expiration has been set
		assert.Equal(t, 4, resp.Bins[fields.TotalExtraRecs.String()])
		assert.Equal(t, 4, resp.Bins[fields.SpentExtraRecs.String()])

		// check the external file ttl has been set
		ttl, err = store.GetExternalStore().GetTTL(ctx, tx.TxIDChainHash().CloneBytes(), options.WithFileExtension("tx"))
		require.NoError(t, err)
		assert.Greater(t, ttl, time.Duration(0))
	})
}

func TestStore_IncrementSpentRecords(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t, nil)

	tSettings := test.CreateBaseTestSettings()
	tSettings.UtxoStore.UtxoBatchSize = 2

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)

	t.Cleanup(func() {
		deferFn()
	})

	t.Run("Increment spentExtraRecs", func(t *testing.T) {
		txID := tx.TxIDChainHash()

		key, aErr := aerospike.NewKey(store.GetNamespace(), store.GetName(), txID.CloneBytes())
		require.NoError(t, aErr)

		// Clean up the database
		cleanDB(t, client, key, tx)

		_, err := store.Create(ctx, tx, 101)
		require.NoError(t, err)

		// Increment spentExtraRecs by 1
		res, err := store.IncrementSpentRecords(txID, 1)
		require.NoError(t, err)
		require.NotNil(t, res)

		r, ok := res.(string)
		require.True(t, ok)

		ret, err := store.ParseLuaReturnValue(r)
		require.NoError(t, err)
		assert.Equal(t, teranode_aerospike.LuaOk, ret.ReturnValue)
		assert.Equal(t, teranode_aerospike.LuaReturnValue(""), ret.Signal)
		assert.Nil(t, ret.SpendingTxID)

		// Verify the increment
		resp, err := client.Get(nil, key)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, 2, resp.Bins[fields.TotalExtraRecs.String()])
		assert.Equal(t, 1, resp.Bins[fields.SpentExtraRecs.String()])

		// Decrement spentExtraRecs by 1
		res, err = store.IncrementSpentRecords(txID, -1)
		require.NoError(t, err)
		require.NotNil(t, res)

		r, ok = res.(string)
		require.True(t, ok)

		ret, err = store.ParseLuaReturnValue(r)
		require.NoError(t, err)
		assert.Equal(t, teranode_aerospike.LuaOk, ret.ReturnValue)
		assert.Equal(t, teranode_aerospike.LuaReturnValue(""), ret.Signal)
		assert.Nil(t, ret.SpendingTxID)

		// Verify the decrement
		resp, err = client.Get(nil, key)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, 2, resp.Bins[fields.TotalExtraRecs.String()])
		assert.Equal(t, 0, resp.Bins[fields.SpentExtraRecs.String()])
	})

	t.Run("Increment spentExtraRecs - set TTL", func(t *testing.T) {
		txID := tx.TxIDChainHash()

		key, aErr := aerospike.NewKey(store.GetNamespace(), store.GetName(), txID.CloneBytes())
		require.NoError(t, aErr)

		// Clean up the database
		cleanDB(t, client, key, tx)

		_, err := store.Create(ctx, tx, 101)
		require.NoError(t, err)

		// force the values we expect to be set
		err = client.Put(nil, key, aerospike.BinMap{
			fields.SpentUtxos.String():     2,
			fields.BlockIDs.String():       []int{101},
			fields.TotalExtraRecs.String(): 2,
		})
		require.NoError(t, err)

		rec, aErr := client.Get(nil, key)
		require.NoError(t, aErr)
		require.NotNil(t, rec)
		assert.Equal(t, 5, rec.Bins[fields.TotalUtxos.String()])
		assert.Equal(t, 2, rec.Bins[fields.SpentUtxos.String()])
		assert.Equal(t, 2, rec.Bins[fields.TotalExtraRecs.String()])
		assert.Equal(t, []interface{}([]interface{}{101}), rec.Bins[fields.BlockIDs.String()])

		// Increment spentExtraRecs by 1
		res, err := store.IncrementSpentRecords(txID, 1)
		require.NoError(t, err)
		require.NotNil(t, res)

		r, ok := res.(string)
		require.True(t, ok)

		ret, err := store.ParseLuaReturnValue(r)
		require.NoError(t, err)
		assert.Equal(t, teranode_aerospike.LuaOk, ret.ReturnValue)
		assert.Equal(t, teranode_aerospike.LuaReturnValue(""), ret.Signal)

		rec, aErr = client.Get(nil, key)
		require.NoError(t, aErr)
		require.NotNil(t, rec)
		assert.Equal(t, 5, rec.Bins[fields.TotalUtxos.String()])
		assert.Equal(t, 2, rec.Bins[fields.SpentUtxos.String()])
		assert.Equal(t, 2, rec.Bins[fields.TotalExtraRecs.String()])
		assert.Equal(t, 1, rec.Bins[fields.SpentExtraRecs.String()])
		assert.Equal(t, []interface{}([]interface{}{101}), rec.Bins[fields.BlockIDs.String()])

		res, err = store.IncrementSpentRecords(txID, 1)
		require.NoError(t, err)
		require.NotNil(t, res)

		rec, aErr = client.Get(nil, key)
		require.NoError(t, aErr)
		require.NotNil(t, rec)
		assert.Equal(t, 5, rec.Bins[fields.TotalUtxos.String()])
		assert.Equal(t, 2, rec.Bins[fields.SpentUtxos.String()])
		assert.Equal(t, 2, rec.Bins[fields.TotalExtraRecs.String()])
		assert.Equal(t, 2, rec.Bins[fields.SpentExtraRecs.String()])
		assert.Equal(t, []interface{}([]interface{}{101}), rec.Bins[fields.BlockIDs.String()])

		r, ok = res.(string)
		require.True(t, ok)

		ret, err = store.ParseLuaReturnValue(r)
		require.NoError(t, err)
		assert.Equal(t, teranode_aerospike.LuaOk, ret.ReturnValue)
		assert.Equal(t, teranode_aerospike.LuaReturnValue("TTLSET"), ret.Signal)
	})

	t.Run("Increment totalExtraRecs - multi", func(t *testing.T) {
		txID := tx.TxIDChainHash()

		key, aErr := aerospike.NewKey(store.GetNamespace(), store.GetName(), txID.CloneBytes())
		require.NoError(t, aErr)

		// Clean up the database
		cleanDB(t, client, key, tx)

		_, err := store.Create(ctx, tx, 101)
		require.NoError(t, err)

		// We have a master record and 2 extra records
		for i := 0; i < 2; i++ {
			// Increment spentExtraRecs by 1
			res, err := store.IncrementSpentRecords(txID, 1)
			require.NoError(t, err)
			require.NotNil(t, res)
		}

		rec, aErr := client.Get(nil, key)
		require.NoError(t, aErr)
		require.NotNil(t, rec)
		assert.Equal(t, 2, rec.Bins[fields.TotalExtraRecs.String()])
		assert.Equal(t, 2, rec.Bins[fields.SpentExtraRecs.String()])
	})
}

func TestStore_Unspend(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t, nil)
	tSettings := test.CreateBaseTestSettings()

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)

	t.Cleanup(func() {
		deferFn()
	})

	t.Run("Successfully unspend a spent tx", func(t *testing.T) {
		// Clean up any existing data
		_ = store.GetExternalStore().Del(ctx, tx.TxIDChainHash().CloneBytes(), options.WithFileExtension("tx"))

		// Create a tx
		_, err := store.Create(ctx, tx, 101)
		require.NoError(t, err)

		// Spend the tx
		spends, err := store.Spend(ctx, spendTx)
		require.NoError(t, err)
		require.Len(t, spends, 1)

		// Unspend the tx
		err = store.Unspend(ctx, spends)
		require.NoError(t, err)

		// Verify we can now spend it again with a different tx
		spends, err = store.Spend(ctx, spendTx2)
		require.NoError(t, err)
		require.Len(t, spends, 1)
	})

	t.Run("Unspend a non-spent tx", func(t *testing.T) {
		txID := tx.TxIDChainHash()

		key, aErr := aerospike.NewKey(store.GetNamespace(), store.GetName(), txID.CloneBytes())
		require.NoError(t, aErr)

		// Clean up the database
		cleanDB(t, client, key, tx)

		// Clean up any existing data
		_ = store.GetExternalStore().Del(ctx, tx.TxIDChainHash().CloneBytes(), options.WithFileExtension("tx"))

		// Create a tx
		_, err := store.Create(ctx, tx, 101)
		require.NoError(t, err)

		utxoHash, err := util.UTXOHashFromOutput(
			tx.TxIDChainHash(),
			tx.Outputs[0],
			0,
		)
		require.NoError(t, err)

		// Try to unspend a tx that hasn't been spent
		err = store.Unspend(ctx, []*utxo.Spend{
			{
				TxID:     tx.TxIDChainHash(),
				Vout:     0,
				UTXOHash: utxoHash,
			},
		})
		require.NoError(t, err)

		// Verify we can still spend it
		spends, err := store.Spend(ctx, spendTx)
		require.NoError(t, err)
		require.Len(t, spends, 1)
	})
}
