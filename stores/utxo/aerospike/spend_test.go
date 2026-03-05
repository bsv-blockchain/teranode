package aerospike_test

import (
	"testing"
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	teranode_aerospike "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// go test -v -tags test_aerospike ./test/...

func TestStore_SpendMultiRecord(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)

	t.Cleanup(func() {
		deferFn()
	})

	t.Run("Spent tx id", func(t *testing.T) {
		// clean up the externalStore, if needed
		_ = store.GetExternalStore().Del(ctx, tx.TxIDChainHash().CloneBytes(), fileformat.FileTypeTx)

		// create a tx
		_, err := store.Create(ctx, tx, 101)
		require.NoError(t, err)

		// spend the tx
		_, err = store.Spend(ctx, spendTx, store.GetBlockHeight()+1)
		require.NoError(t, err)

		// spend again, should not return an error
		_, err = store.Spend(ctx, spendTx, store.GetBlockHeight()+1)
		require.NoError(t, err)

		// try to spend the tx with a different tx, check the spending tx ID
		spends, err := store.Spend(ctx, spendTx2, store.GetBlockHeight()+1)
		require.Error(t, err)

		var tErr *errors.Error
		require.ErrorAs(t, err, &tErr)
		require.Equal(t, errors.ERR_UTXO_ERROR, tErr.Code())
		require.ErrorIs(t, spends[0].Err, errors.ErrSpent)
		require.Equal(t, spendTx.TxIDChainHash().String(), spends[0].ConflictingTxID.String())
	})

	t.Run("SpendMultiRecord LUA", func(t *testing.T) {
		cleanDB(t, client)

		store.SetUtxoBatchSize(1)

		// clean up the externalStore, if needed
		_ = store.GetExternalStore().Del(ctx, tx.TxIDChainHash().CloneBytes(), fileformat.FileTypeTx)

		// create a tx
		_, err := store.Create(ctx, tx, 101)
		require.NoError(t, err)

		keyTx, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), tx.TxIDChainHash().CloneBytes())
		require.NoError(t, err)

		resp, err := client.Get(nil, keyTx)
		require.NoError(t, err)

		// Check the totalExtraRecs and spentExtraRecs
		totalExtraRecs, ok := resp.Bins[fields.TotalExtraRecs.String()].(int)
		require.True(t, ok)
		assert.Equal(t, 4, totalExtraRecs) // parent is one, and there are 4 extra records

		_, ok = resp.Bins[fields.SpentExtraRecs.String()].(int)
		assert.False(t, ok)

		// mine the tx
		blockIDsMap, err := store.SetMinedMulti(ctx, []*chainhash.Hash{tx.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: 101, BlockHeight: 101, SubtreeIdx: 101, OnLongestChain: true})
		require.NoError(t, err)
		assert.Len(t, blockIDsMap, 1)
		assert.Equal(t, uint32(101), blockIDsMap[*tx.TxIDChainHash()][0])

		utxoHashes := make([]*chainhash.Hash, len(tx.Outputs))
		for vOut, txOut := range tx.Outputs {
			//nolint:gosec
			utxoHashes[vOut], err = util.UTXOHashFromOutput(tx.TxIDChainHash(), txOut, uint32(vOut))
			require.NoError(t, err)

			//nolint:gosec
			keySource := uaerospike.CalculateKeySource(tx.TxIDChainHash(), uint32(vOut), store.GetUtxoBatchSize())
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
		exists, err := store.GetExternalStore().Exists(ctx, tx.TxIDChainHash().CloneBytes(), fileformat.FileTypeTx)
		require.NoError(t, err)
		require.True(t, exists)

		// DAH is now managed centrally by pruner service, not by blob stores
		// External store DAH checks removed

		keySource := uaerospike.CalculateKeySource(tx.TxIDChainHash(), uint32(0), 1)
		mainRecordKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), keySource)
		require.NoError(t, err)

		// spend 1,2,3,4
		_, err = store.Spend(ctx, spendTxRemaining, store.GetBlockHeight()+1)
		require.NoError(t, err)

		// give the db time to update the main record
		// time.Sleep(100 * time.Millisecond)

		// get totalExtraRecs from main record
		resp, err = client.Get(nil, mainRecordKey)
		require.NoError(t, err)

		// assert that the record is not yet marked for DAH
		assert.Nil(t, resp.Bins[fields.DeleteAtHeight.String()])
		assert.Equal(t, 4, resp.Bins[fields.TotalExtraRecs.String()])
		assert.Equal(t, 4, resp.Bins[fields.SpentExtraRecs.String()])

		// spend 0
		_, err = store.Spend(ctx, spendTx, store.GetBlockHeight()+1)
		require.NoError(t, err)

		resp, err = client.Get(nil, mainRecordKey)
		require.NoError(t, err)

		// main record check
		assert.Greater(t, resp.Bins[fields.DeleteAtHeight.String()], 0)
		assert.Equal(t, 4, resp.Bins[fields.TotalExtraRecs.String()])
		assert.Equal(t, 4, resp.Bins[fields.SpentExtraRecs.String()])

		// DAH is now managed centrally by pruner service, not by blob stores
		// External file lifecycle is managed by the pruner service
	})
}

func TestStore_IncrementSpentRecords(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.UtxoBatchSize = 2

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)

	txKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), tx.TxIDChainHash().CloneBytes())
	require.NoError(t, err)

	t.Cleanup(func() {
		deferFn()
	})

	t.Run("Increment spentExtraRecs", func(t *testing.T) {
		cleanDB(t, client)

		txID := tx.TxIDChainHash()

		_, err := store.Create(ctx, tx, 101)
		require.NoError(t, err)

		// Increment spentExtraRecs by 1
		res, err := store.IncrementSpentRecords(txID, 1)
		require.NoError(t, err)
		require.NotNil(t, res)

		ret, err := store.ParseLuaMapResponse(res)
		require.NoError(t, err)
		assert.Equal(t, teranode_aerospike.LuaStatusOK, ret.Status)
		assert.Equal(t, teranode_aerospike.LuaSignal(""), ret.Signal)

		// Verify the increment
		resp, err := client.Get(nil, txKey)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, 2, resp.Bins[fields.TotalExtraRecs.String()])
		assert.Equal(t, 1, resp.Bins[fields.SpentExtraRecs.String()])

		// Decrement spentExtraRecs by 1
		res, err = store.IncrementSpentRecords(txID, -1)
		require.NoError(t, err)
		require.NotNil(t, res)

		ret, err = store.ParseLuaMapResponse(res)
		require.NoError(t, err)
		assert.Equal(t, teranode_aerospike.LuaStatusOK, ret.Status)
		assert.Equal(t, teranode_aerospike.LuaSignal(""), ret.Signal)

		// Verify the decrement
		resp, err = client.Get(nil, txKey)
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, 2, resp.Bins[fields.TotalExtraRecs.String()])
		assert.Equal(t, 0, resp.Bins[fields.SpentExtraRecs.String()])
	})

	t.Run("Increment spentExtraRecs - set DAH", func(t *testing.T) {
		txID := tx.TxIDChainHash()

		key, aErr := aerospike.NewKey(store.GetNamespace(), store.GetName(), txID.CloneBytes())
		require.NoError(t, aErr)

		// Clean up the database
		cleanDB(t, client)

		_, err := store.Create(ctx, tx, 101)
		require.NoError(t, err)

		// force the values we expect to be set
		err = client.Put(nil, key, aerospike.BinMap{
			fields.SpentUtxos.String():     2,
			fields.BlockIDs.String():       []int{101},
			fields.TotalExtraRecs.String(): 2,
			fields.UnminedSince.String():   nil, // Clear unminedSince to simulate transaction on longest chain
		})
		require.NoError(t, err)

		rec, aErr := client.Get(nil, key)
		require.NoError(t, aErr)
		require.NotNil(t, rec)
		assert.Equal(t, 5, rec.Bins[fields.TotalUtxos.String()])
		assert.Equal(t, 2, rec.Bins[fields.SpentUtxos.String()])
		assert.Equal(t, 2, rec.Bins[fields.TotalExtraRecs.String()])
		assert.Equal(t, []interface{}{101}, rec.Bins[fields.BlockIDs.String()])

		// Increment spentExtraRecs by 1
		res, err := store.IncrementSpentRecords(txID, 1)
		require.NoError(t, err)
		require.NotNil(t, res)

		ret, err := store.ParseLuaMapResponse(res)
		require.NoError(t, err)
		assert.Equal(t, teranode_aerospike.LuaStatusOK, ret.Status)
		assert.Equal(t, teranode_aerospike.LuaSignal(""), ret.Signal)

		rec, aErr = client.Get(nil, key)
		require.NoError(t, aErr)
		require.NotNil(t, rec)
		assert.Equal(t, 5, rec.Bins[fields.TotalUtxos.String()])
		assert.Equal(t, 2, rec.Bins[fields.SpentUtxos.String()])
		assert.Equal(t, 2, rec.Bins[fields.TotalExtraRecs.String()])
		assert.Equal(t, 1, rec.Bins[fields.SpentExtraRecs.String()])
		assert.Equal(t, []interface{}{101}, rec.Bins[fields.BlockIDs.String()])

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
		assert.Equal(t, []interface{}{101}, rec.Bins[fields.BlockIDs.String()])

		ret, err = store.ParseLuaMapResponse(res)
		require.NoError(t, err)
		assert.Equal(t, teranode_aerospike.LuaStatusOK, ret.Status)
		assert.Equal(t, teranode_aerospike.LuaSignalDAHSet, ret.Signal)
		assert.Equal(t, 2, ret.ChildCount)
	})

	t.Run("Increment totalExtraRecs - multi", func(t *testing.T) {
		txID := tx.TxIDChainHash()

		key, aErr := aerospike.NewKey(store.GetNamespace(), store.GetName(), txID.CloneBytes())
		require.NoError(t, aErr)

		// Clean up the database
		cleanDB(t, client)

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

func TestStore_IncrementSpentRecords_Timeout(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)

	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)

	t.Cleanup(func() {
		deferFn()
	})

	cleanDB(t, client)

	_, err := store.Create(ctx, tx, 101)
	require.NoError(t, err)

	// Set an extremely short timeout so the batcher can't respond in time
	tSettings.UtxoStore.SpendWaitTimeout = time.Nanosecond

	_, err = store.IncrementSpentRecords(tx.TxIDChainHash(), 1)
	require.Error(t, err)
	assert.True(t, errors.Is(err, errors.ErrServiceUnavailable), "expected service unavailable error, got: %v", err)
}

// TestDriftedCounterDoesNotSetDAH verifies that when spentExtraRecs drifts
// (e.g. due to interrupted rollbacks during DEVICE_OVERLOAD), the Go-side
// sanity check in handleExtraRecords detects that not all children are
// actually spent and clears the DAH that Lua prematurely set.
//
// Flow: create multi-record tx → mine it → inflate spentExtraRecs on master
// to simulate drift → spend only the master's UTXOs (not children) → the
// child ALLSPENT signal triggers handleExtraRecords(+1) → Lua sees
// spentExtraRecs == totalExtraRecs, sets DAH → Go verifies children →
// finds they're NOT all-spent → clears the master DAH.
func TestDriftedCounterDoesNotSetDAH(t *testing.T) {
	batchSize := 2
	numOutputs := 10

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.UtxoBatchSize = batchSize

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(func() {
		deferFn()
	})

	// Build a transaction with many outputs spanning multiple child records
	largeTx := bt.NewTx()
	err := largeTx.From(
		"1111111111111111111111111111111111111111111111111111111111111111",
		0,
		"76a914000000000000000000000000000000000000000088ac",
		uint64(numOutputs*1000+1000),
	)
	require.NoError(t, err)

	for i := 0; i < numOutputs; i++ {
		err = largeTx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000)
		require.NoError(t, err)
	}

	txID := largeTx.TxIDChainHash()

	_, err = store.Create(ctx, largeTx, 1)
	require.NoError(t, err)

	masterKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), txID.CloneBytes())
	require.NoError(t, err)

	// Verify initial state: totalExtraRecs=4, no spentExtraRecs
	rec, err := client.Get(nil, masterKey)
	require.NoError(t, err)
	totalExtraRecs := rec.Bins[fields.TotalExtraRecs.String()].(int)
	expectedExtraRecs := (numOutputs / batchSize) - 1 // 4
	require.Equal(t, expectedExtraRecs, totalExtraRecs)

	// Mine the tx so DAH conditions can be met
	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{txID}, utxo.MinedBlockInfo{
		BlockID: 1, BlockHeight: 1, SubtreeIdx: 1, OnLongestChain: true,
	})
	require.NoError(t, err)

	// Simulate counter drift: inflate spentExtraRecs to totalExtraRecs-1
	// without actually spending any child UTXOs. This is what happens when
	// spend/unspend rollbacks are interrupted by context cancellation.
	err = client.Put(nil, masterKey, aerospike.BinMap{
		fields.SpentExtraRecs.String(): totalExtraRecs - 1,
	})
	require.NoError(t, err)

	// Spend only outputs 0-1 (the master record's UTXOs, batchSize=2).
	// This makes the master's own spentUtxos == recordUtxos.
	// The child at vout 0-1 signals ALLSPENT → handleExtraRecords(+1) fires.
	// Lua increments spentExtraRecs from (totalExtraRecs-1) to totalExtraRecs,
	// allSpent becomes true, Lua sets DAH on master inline.
	// Go then verifies children → most are NOT actually spent → clears DAH.
	spendingTx := bt.NewTx()
	for i := 0; i < batchSize; i++ {
		err = spendingTx.From(
			txID.String(),
			uint32(i),
			largeTx.Outputs[i].LockingScript.String(),
			largeTx.Outputs[i].Satoshis,
		)
		require.NoError(t, err)
	}
	err = spendingTx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", uint64(batchSize*1000-500))
	require.NoError(t, err)

	_, err = store.Spend(ctx, spendingTx, store.GetBlockHeight()+1)
	require.NoError(t, err)

	// The key assertion: despite Lua setting DAH (because the drifted counter
	// said allSpent=true), the Go verification should have cleared it after
	// detecting that children 2-4 still have unspent UTXOs.
	rec, err = client.Get(nil, masterKey)
	require.NoError(t, err)
	assert.Nil(t, rec.Bins[fields.DeleteAtHeight.String()],
		"DAH should NOT be set — Go sanity check should have cleared it after detecting unspent children (counter drift)")
}

func TestStore_Unspend(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)

	t.Cleanup(func() {
		deferFn()
	})

	t.Run("Successfully unspend a spent tx", func(t *testing.T) {
		// Clean up any existing data
		_ = store.GetExternalStore().Del(ctx, tx.TxIDChainHash().CloneBytes(), fileformat.FileTypeTx)

		// Create a tx
		_, err := store.Create(ctx, tx, 101)
		require.NoError(t, err)

		// Spend the tx
		spends, err := store.Spend(ctx, spendTx, store.GetBlockHeight()+1)
		require.NoError(t, err)
		require.Len(t, spends, 1)

		// Unspend the tx
		err = store.Unspend(ctx, spends)
		require.NoError(t, err)

		// Verify we can now spend it again with a different tx
		spends, err = store.Spend(ctx, spendTx2, store.GetBlockHeight()+1)
		require.NoError(t, err)
		require.Len(t, spends, 1)
	})

	t.Run("Unspend a non-spent tx", func(t *testing.T) {
		// Clean up the database
		cleanDB(t, client)

		// Clean up any existing data
		_ = store.GetExternalStore().Del(ctx, tx.TxIDChainHash().CloneBytes(), fileformat.FileTypeTx)

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
		spends, err := store.Spend(ctx, spendTx, store.GetBlockHeight()+1)
		require.NoError(t, err)
		require.Len(t, spends, 1)
	})
}
