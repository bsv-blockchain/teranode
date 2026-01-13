package aerospike_test

import (
	"context"
	"encoding/hex"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	teranode_aerospike "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/stores/utxo/aerospike/pruner"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	utxo2 "github.com/bsv-blockchain/teranode/test/longtest/stores/utxo"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// go test -v -tags test_aerospike ./test/...

func TestAerospike(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)

	t.Cleanup(func() {
		deferFn()
	})

	parentTxHash := tx.Inputs[0].PreviousTxIDChainHash()

	coinbaseTx, err := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff" +
		"19031404002f6d332d617369612fdf5128e62eda1a07e94dbdbdffffffff0500ca9a3b000000001976a914c362d5af234dd4e1f2a1bfbcab9003" +
		"6d38b0aa9f88ac00ca9a3b000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac00ca9a3b000000001976a914c362d5af23" +
		"4dd4e1f2a1bfbcab90036d38b0aa9f88ac00ca9a3b000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac00ca9a3b000000" +
		"001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac00000000")
	require.NoError(t, err)

	coinbaseTXKey, err = aerospike.NewKey(store.GetNamespace(), store.GetName(), coinbaseTx.TxIDChainHash()[:])
	require.NoError(t, err)

	blockID := uint32(123)
	blockID2 := uint32(124)

	spendingTxID1Key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), spendingTxID1[:])
	require.NoError(t, err)

	txKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), tx.TxIDChainHash().CloneBytes())
	require.NoError(t, err)

	txWithOPReturnKey, aErr := aerospike.NewKey(store.GetNamespace(), store.GetName(), txWithOPReturn.TxIDChainHash().CloneBytes())
	require.NoError(t, aErr)

	t.Cleanup(func() {
		policy := util.GetAerospikeWritePolicy(tSettings, 0)
		_, _ = client.Delete(policy, spendingTxID1Key)
	})

	t.Run("aerospike_store", func(t *testing.T) {
		cleanDB(t, client)

		_, err = store.Create(ctx, tx, 0)
		require.NoError(t, err)

		var value *aerospike.Record
		// raw aerospike get
		value, err = client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
		require.NoError(t, err)
		require.Equal(t, uint32(1), value.Generation)
		assert.Equal(t, uint64(215), uint64(value.Bins[fields.Fee.String()].(int)))
		assert.Equal(t, uint64(328), uint64(value.Bins[fields.SizeInBytes.String()].(int)))
		assert.Len(t, value.Bins[fields.Inputs.String()].([]interface{}), 1)
		binParentTxHash := chainhash.Hash(value.Bins[fields.Inputs.String()].([]interface{})[0].([]byte)[0:32])
		assert.Equal(t, parentTxHash[:], binParentTxHash.CloneBytes())
		assert.Equal(t, []interface{}{}, value.Bins[fields.BlockIDs.String()])

		_, err = store.Create(ctx, tx, 0)
		require.Error(t, err)
		assert.True(t, errors.Is(err, errors.ErrTxExists))

		blockIDsMap, err := store.SetMinedMulti(ctx, []*chainhash.Hash{tx.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: blockID, BlockHeight: 101, SubtreeIdx: 3, OnLongestChain: true})
		require.NoError(t, err)
		require.Equal(t, 1, len(blockIDsMap))
		require.Equal(t, 1, len(blockIDsMap[*tx.TxIDChainHash()]))
		require.Equal(t, []uint32{blockID}, blockIDsMap[*tx.TxIDChainHash()])

		value, err = client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
		require.NoError(t, err)
		require.Equal(t, uint32(2), value.Generation)
		assert.Len(t, value.Bins[fields.BlockIDs.String()].([]interface{}), 1)
		assert.Equal(t, []interface{}{int(blockID)}, value.Bins[fields.BlockIDs.String()].([]interface{}))
		assert.Equal(t, []interface{}{101}, value.Bins[fields.BlockHeights.String()].([]interface{}))
		assert.Equal(t, []interface{}{3}, value.Bins[fields.SubtreeIdxs.String()].([]interface{}))

		blockIDsMap, err = store.SetMinedMulti(ctx, []*chainhash.Hash{tx.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: blockID2, BlockHeight: 102, SubtreeIdx: 4, OnLongestChain: true})
		require.NoError(t, err)
		require.Equal(t, 1, len(blockIDsMap))
		require.Equal(t, 2, len(blockIDsMap[*tx.TxIDChainHash()]))
		require.Equal(t, []uint32{blockID, blockID2}, blockIDsMap[*tx.TxIDChainHash()])

		value, err = client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
		require.NoError(t, err)
		require.Equal(t, uint32(3), value.Generation)
		assert.False(t, value.Bins[fields.IsCoinbase.String()].(bool))
		assert.Len(t, value.Bins[fields.BlockIDs.String()].([]interface{}), 2)
		assert.Equal(t, []interface{}{int(blockID), int(blockID2)}, value.Bins[fields.BlockIDs.String()].([]interface{}))
		assert.Equal(t, []interface{}{101, 102}, value.Bins[fields.BlockHeights.String()].([]interface{}))
		assert.Equal(t, []interface{}{3, 4}, value.Bins[fields.SubtreeIdxs.String()].([]interface{}))
	})

	t.Run("aerospike_store_conflicting", func(t *testing.T) {
		cleanDB(t, client)

		_, err = store.Create(ctx, tx, 0, utxo.WithConflicting(true))
		require.NoError(t, err)

		var value *aerospike.Record
		// raw aerospike get
		value, err = client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
		require.NoError(t, err)
		require.Equal(t, uint32(1), value.Generation)
		assert.Equal(t, uint64(215), uint64(value.Bins["fee"].(int)))
		assert.Equal(t, uint64(328), uint64(value.Bins["sizeInBytes"].(int)))
		assert.Len(t, value.Bins["inputs"], 1)
		binParentTxHash := chainhash.Hash(value.Bins["inputs"].([]interface{})[0].([]byte)[0:32])
		assert.Equal(t, parentTxHash[:], binParentTxHash.CloneBytes())
		assert.Equal(t, []interface{}{}, value.Bins["blockIDs"])
		assert.True(t, value.Bins["conflicting"].(bool))

		// check DAH is set
		assert.Greater(t, value.Bins[fields.DeleteAtHeight.String()], 0)
	})

	t.Run("aerospike_store_coinbase", func(t *testing.T) {
		cleanDB(t, client)

		_, err = store.Create(ctx, coinbaseTx, 0)
		require.NoError(t, err)

		var value *aerospike.Record
		// raw aerospike get
		value, err = client.Get(util.GetAerospikeReadPolicy(tSettings), coinbaseTXKey)
		require.NoError(t, err)
		assert.True(t, value.Bins["isCoinbase"].(bool))

		var txMeta *meta.Data
		txMeta, err = store.Get(ctx, coinbaseTx.TxIDChainHash())
		require.NoError(t, err)
		assert.True(t, txMeta.IsCoinbase)
		assert.Equal(t, txMeta.Tx.ExtendedBytes(), coinbaseTx.ExtendedBytes())
	})

	t.Run("aerospike_get", func(t *testing.T) {
		cleanDB(t, client)

		_, err = store.Create(ctx, tx, 0)
		require.NoError(t, err)

		var value *meta.Data
		value, err = store.Get(ctx, tx.TxIDChainHash())
		require.NoError(t, err)
		assert.Equal(t, uint64(215), value.Fee)
		assert.Equal(t, uint64(328), value.SizeInBytes)
		assert.Len(t, value.TxInpoints.ParentTxHashes, 1)
		assert.Equal(t, []chainhash.Hash{*parentTxHash}, value.TxInpoints.ParentTxHashes)
		assert.Len(t, value.BlockIDs, 0)
		assert.NotNil(t, value.BlockIDs)

		blockIDsMap, err := store.SetMinedMulti(ctx, []*chainhash.Hash{tx.TxIDChainHash()}, utxo.MinedBlockInfo{BlockID: blockID2, BlockHeight: 102, SubtreeIdx: 4, OnLongestChain: true})
		require.NoError(t, err)
		require.Equal(t, 1, len(blockIDsMap))
		require.Equal(t, 1, len(blockIDsMap[*tx.TxIDChainHash()]))
		require.Equal(t, []uint32{blockID2}, blockIDsMap[*tx.TxIDChainHash()])

		value, err = store.Get(ctx, tx.TxIDChainHash())
		require.NoError(t, err)
		assert.Equal(t, uint64(215), value.Fee)
		assert.Len(t, value.BlockIDs, 1)
		assert.Equal(t, []uint32{blockID2}, value.BlockIDs)

		assert.Equal(t, []uint32{102}, value.BlockHeights)
		assert.Equal(t, []int{4}, value.SubtreeIdxs)
	})

	t.Run("aerospike_get_spend", func(t *testing.T) {
		cleanDB(t, client)

		txMeta, err := store.Create(ctx, tx, 0)
		require.NoError(t, err)
		assert.NotNil(t, txMeta)

		spendClone := spend.Clone()

		resp, err := store.GetSpend(ctx, spendClone)
		require.NoError(t, err)
		assert.Equal(t, int(utxo.Status_OK), resp.Status)
		assert.Nil(t, resp.SpendingData)

		spendTxClone := spendTx.Clone()

		_ = spendTxClone.Inputs[0].PreviousTxIDAdd(tx.TxIDChainHash())
		_, err = store.Spend(ctx, spendTxClone, 1)
		require.NoError(t, err)

		resp, err = store.GetSpend(ctx, spendClone)
		require.NoError(t, err)
		assert.Equal(t, int(utxo.Status_SPENT), resp.Status)
		require.NotNil(t, resp.SpendingData)
		assert.Equal(t, spendTxClone.TxIDChainHash().String(), resp.SpendingData.TxID.String())
	})

	t.Run("aerospike_get inputs", func(t *testing.T) {
		cleanDB(t, client)

		txMeta, err := store.Create(ctx, tx, 0)
		require.NoError(t, err)
		assert.NotNil(t, txMeta)

		resp, err := store.Get(ctx, tx.TxIDChainHash(), fields.Inputs)
		require.NoError(t, err)

		assert.NotNil(t, resp.Tx)
		assert.Len(t, resp.Tx.Inputs, 1)
	})

	t.Run("aerospike_get utxos", func(t *testing.T) {
		cleanDB(t, client)

		txMeta, err := store.Create(ctx, tx, 0)
		require.NoError(t, err)
		assert.NotNil(t, txMeta)

		resp, err := store.Get(ctx, tx.TxIDChainHash(), fields.Utxos)
		require.NoError(t, err)

		assert.NotNil(t, resp.SpendingDatas)
		assert.Len(t, resp.SpendingDatas, 5)
	})

	t.Run("aerospike_store", func(t *testing.T) {
		cleanDB(t, client)

		var txMeta *meta.Data
		txMeta, err = store.Create(ctx, tx, 0)
		assert.NotNil(t, txMeta)
		require.NoError(t, err)

		var value *aerospike.Record
		value, err = client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
		require.NoError(t, err)

		// assert.Equal(t, value.Bins["tx"], tx.ExtendedBytes())
		assert.Len(t, value.Bins["inputs"], 1)
		assert.Equal(t, value.Bins["inputs"].([]interface{})[0], tx.Inputs[0].ExtendedBytes(false))
		assert.Len(t, value.Bins["outputs"], 5)
		assert.Equal(t, value.Bins["fee"], 215)
		assert.Equal(t, value.Bins["sizeInBytes"], 328)
		assert.Equal(t, uint32(value.Bins["version"].(int)), tx.Version) // nolint:gosec
		utxos, ok := value.Bins["utxos"].([]interface{})
		require.True(t, ok)

		for i := 0; i < len(tx.Outputs); i++ {
			utxoI := utxos[i]
			assert.Len(t, utxoI, 32)
		}

		parentTxHashes, ok := value.Bins["inputs"].([]interface{})
		require.True(t, ok)
		assert.Len(t, parentTxHashes, 1)
		assert.Equal(t, parentTxHashes[0].([]byte)[:32], tx.Inputs[0].PreviousTxIDChainHash().CloneBytes())

		blockIDs, ok := value.Bins["blockIDs"].([]interface{})
		require.True(t, ok)
		assert.Len(t, blockIDs, 0)
		assert.Nil(t, value.Bins[fields.DeleteAtHeight.String()])

		txMeta, err = store.Create(ctx, tx, 0)
		assert.Nil(t, txMeta)
		require.True(t, errors.Is(err, errors.ErrTxExists))

		spendTxClone := spendTx.Clone()

		err = spendTxClone.Inputs[0].PreviousTxIDAdd(tx.TxIDChainHash())
		require.NoError(t, err)

		_, err = store.Spend(ctx, spendTxClone, 1)
		require.NoError(t, err)

		txMeta, err = store.Create(ctx, tx, 0)
		assert.Nil(t, txMeta)
		require.True(t, errors.Is(err, errors.ErrTxExists))
	})

	t.Run("aerospike_spend", func(t *testing.T) {
		cleanDB(t, client)

		var txMeta *meta.Data
		txMeta, err = store.Create(ctx, tx, 0)
		assert.NotNil(t, txMeta)
		require.NoError(t, err)

		err = spendTx.Inputs[0].PreviousTxIDAdd(tx.TxIDChainHash())
		require.NoError(t, err)

		_, err = store.Spend(ctx, spendTx, 1)
		require.NoError(t, err)

		var value *aerospike.Record
		value, err = client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
		require.NoError(t, err)

		utxos, ok := value.Bins["utxos"].([]interface{})
		require.True(t, ok)

		utxoSpendingData := utxos[spends[0].Vout]
		spendingData, ok := utxoSpendingData.([]byte)
		require.True(t, ok)
		require.Equal(t, spendTx.TxIDChainHash()[:], spendingData[32:64])

		var spendingTxHash *chainhash.Hash
		spendingTxHash, err = chainhash.NewHash(spendingData[32:64])
		require.NoError(t, err)

		// try to spend with different txid
		_, err = store.Spend(ctx, spendTx2, 1)
		require.Error(t, err)
		assert.Equal(t, spendTx.TxIDChainHash().String(), spendingTxHash.String())
	})

	t.Run("aerospike_spend_lua", func(t *testing.T) {
		cleanDB(t, client)

		txMeta, err := store.Create(ctx, tx, 0)
		require.NoError(t, err)
		assert.NotNil(t, txMeta)

		rPolicy := util.GetAerospikeReadPolicy(tSettings)

		rec, err := client.Get(rPolicy, txKey, "outputs", "utxos", "spentUtxos")
		require.NoError(t, err)
		require.NotNil(t, rec)

		_, ok := rec.Bins["utxos"].([]interface{})
		require.True(t, ok)

		wPolicy := util.GetAerospikeWritePolicy(tSettings, 0)

		// function spend(rec, offset, utxoHash, spendingData, ignoreConflicting, ignoreLocked, currentBlockHeight, blockHeightRetention)
		ret, aErr := client.Execute(wPolicy, txKey, teranode_aerospike.LuaPackage, "spend",
			aerospike.NewIntegerValue(int(spends[0].Vout)),     // Offset
			aerospike.NewValue(spends[0].UTXOHash[:]),          // UTXO hash
			aerospike.NewValue(spends[0].SpendingData.Bytes()), // SpendingData
			aerospike.NewValue(false),                          // Ignore conflicting
			aerospike.NewValue(false),                          // Ignore locked
			aerospike.NewValue(0),                              // Current height
			aerospike.NewValue(100),                            // BlockHeightRetention
		)
		require.NoError(t, aErr)

		// Parse the map response using the store's parser
		res, err := store.ParseLuaMapResponse(ret)
		require.NoError(t, err)
		assert.Equal(t, teranode_aerospike.LuaStatusOK, res.Status)
		assert.Empty(t, res.BlockIDs)

		_ = spendTx.Inputs[0].PreviousTxIDAdd(tx.TxIDChainHash())

		_, err = store.Spend(ctx, spendTx, 1)
		require.Error(t, err)

		value, err := client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
		require.NoError(t, err)

		utxos, ok := value.Bins["utxos"].([]interface{})
		require.True(t, ok)
		utxoSpendingData, ok := utxos[spends[0].Vout].([]byte)
		require.True(t, ok)
		require.Equal(t, 68, len(utxoSpendingData))
		spendingData, err := spendpkg.NewSpendingDataFromBytes(utxoSpendingData[32:])
		require.NoError(t, err)
		require.Equal(t, spendingTxID1[:], spendingData.TxID[:])
		require.Equal(t, 0, spendingData.Vin)

		// try to spend with different txid
		_, err = store.Spend(ctx, spendTx2, 1)
		require.Error(t, err)
	})

	t.Run("aerospike_spend_all_lua", func(t *testing.T) {
		cleanDB(t, client)
		txMeta, err := store.Create(ctx, tx, 0)
		assert.NotNil(t, txMeta)
		require.NoError(t, err)

		value, err := client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
		require.NoError(t, err)

		totalExtraRecs, ok := value.Bins["totalExtraRecs"].(int)
		require.True(t, ok)
		assert.Equal(t, 0, totalExtraRecs)

		wPolicy := util.GetAerospikeWritePolicy(tSettings, 0)

		for _, s := range spendsAll {
			ret, aErr := client.Execute(wPolicy, txKey, teranode_aerospike.LuaPackage, "spend",
				aerospike.NewIntegerValue(int(s.Vout)),
				aerospike.NewValue(s.UTXOHash[:]),
				aerospike.NewValue(s.SpendingData.Bytes()),
				aerospike.NewValue(false), // Ignore conflicting
				aerospike.NewValue(false), // Ignore locked
				aerospike.NewValue(0),     // Current height
				aerospike.NewValue(100),   // Block retention
			)
			require.NoError(t, aErr)

			// Parse the map response using the store's parser
			res, err := store.ParseLuaMapResponse(ret)
			require.NoError(t, err)
			assert.Equal(t, teranode_aerospike.LuaStatusOK, res.Status)
		}

		value, err = client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
		require.NoError(t, err)

		utxos, ok := value.Bins["utxos"].([]interface{})
		require.True(t, ok)
		utxoSpendingData, ok := utxos[spendsAll[0].Vout].([]byte)
		require.True(t, ok)
		require.Equal(t, 68, len(utxoSpendingData))
		spendingData, err := spendpkg.NewSpendingDataFromBytes(utxoSpendingData[32:])
		require.NoError(t, err)
		require.Equal(t, spendingTxID2[:], spendingData.TxID[:])
		require.Equal(t, 0, spendingData.Vin)
	})

	t.Run("aerospike_lua_errors", func(t *testing.T) {
		cleanDB(t, client)

		txMeta, err := store.Create(ctx, tx, 0)
		assert.NotNil(t, txMeta)
		require.NoError(t, err)

		wPolicy := util.GetAerospikeWritePolicy(tSettings, 0)

		// function spend(rec, offset, utxoHash, spendingTxID, ignoreConflicting, ignoreLocked, currentBlockHeight, blockHeightRetention)
		fakeKey, _ := aerospike.NewKey(aerospikeNamespace, aerospikeSet, []byte{})
		ret, aErr := client.Execute(wPolicy, fakeKey, teranode_aerospike.LuaPackage, "spend",
			aerospike.NewIntegerValue(int(spends[0].Vout)),     // offset
			aerospike.NewValue(spends[0].UTXOHash[:]),          // utxoHash
			aerospike.NewValue(spends[0].SpendingData.Bytes()), // spendingTxID
			aerospike.NewValue(false),                          // Ignore conflicting
			aerospike.NewValue(false),                          // Ignore locked
			aerospike.NewValue(0),                              // Current height
			aerospike.NewValue(100),                            // Block retention
		)
		require.NoError(t, aErr)

		// Parse the map response using the store's parser
		res, err := store.ParseLuaMapResponse(ret)
		require.NoError(t, err)
		assert.Equal(t, teranode_aerospike.LuaStatusError, res.Status)
		assert.Equal(t, "TX not found", res.Message)
	})

	t.Run("aerospike_1_record_spend_1_and_not_expire_no_blockIDs", func(t *testing.T) {
		cleanDB(t, client)

		txMeta, err := store.Create(ctx, tx, 0)
		assert.NotNil(t, txMeta)
		require.NoError(t, err)

		_ = spendTx.Inputs[0].PreviousTxIDAdd(tx.TxIDChainHash())
		_, err = store.Spend(ctx, spendTx, 1)
		require.NoError(t, err)

		value, err := client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
		require.NoError(t, err)

		assert.Nil(t, value.Bins[fields.DeleteAtHeight.String()]) // DAH is 0 because the tx still has UTXOs

		// Now spend all the remaining utxos
		_, err = store.Spend(ctx, spendTxRemaining, 1)
		require.NoError(t, err)

		value, err = client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
		require.NoError(t, err)

		assert.Nil(t, value.Bins[fields.DeleteAtHeight.String()]) // DAH is 0 because the tx has not been in a block yet
	})

	t.Run("aerospike_1_record_spend_1_and_not_expire", func(t *testing.T) {
		cleanDB(t, client)

		txMeta, err := store.Create(ctx, tx, 0, utxo.WithMinedBlockInfo(
			utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 123, SubtreeIdx: 1, OnLongestChain: true},
			utxo.MinedBlockInfo{BlockID: 2, BlockHeight: 124, SubtreeIdx: 2, OnLongestChain: true},
			utxo.MinedBlockInfo{BlockID: 3, BlockHeight: 125, SubtreeIdx: 3, OnLongestChain: true},
		))
		assert.NotNil(t, txMeta)
		require.NoError(t, err)

		_ = spendTx.Inputs[0].PreviousTxIDAdd(tx.TxIDChainHash())
		_, err = store.Spend(ctx, spendTx, 1)
		require.NoError(t, err)

		value, err := client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
		require.NoError(t, err)

		assert.Nil(t, value.Bins[fields.DeleteAtHeight.String()])

		// Now spend all the remaining utxos
		_, err = store.Spend(ctx, spendTxRemaining, 1)
		require.NoError(t, err)

		value, err = client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
		require.NoError(t, err)

		assert.NotNil(t, value.Bins[fields.DeleteAtHeight.String()])
	})

	t.Run("aerospike_spend_all_and_expire", func(t *testing.T) {
		cleanDB(t, client)

		txMeta, err := store.Create(ctx, tx, 0)
		assert.NotNil(t, txMeta)
		require.NoError(t, err)

		_, err = store.Spend(ctx, spendTxAll, 1)
		require.NoError(t, err)

		value, err := client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
		require.NoError(t, err)

		utxos, ok := value.Bins["utxos"].([]interface{})
		require.True(t, ok)

		for i := 0; i < 5; i++ {
			utxoBytes, ok := utxos[spendsAll[i].Vout].([]byte)
			require.True(t, ok)
			require.Equal(t, 68, len(utxoBytes))
			spendingData, err := spendpkg.NewSpendingDataFromBytes(utxoBytes[32:])
			require.NoError(t, err)
			require.Equal(t, spendTxAll.TxIDChainHash()[:], spendingData.TxID[:])
			require.Equal(t, i, spendingData.Vin)
		}

		assert.Nil(t, value.Bins[fields.DeleteAtHeight.String()])

		// Now call SetMinedMulti
		blockIDsMap, err := store.SetMinedMulti(ctx, []*chainhash.Hash{tx.TxIDChainHash()}, utxo.MinedBlockInfo{
			BlockID: 1, BlockHeight: 123, SubtreeIdx: 1, OnLongestChain: true,
		})
		require.NoError(t, err)
		require.Equal(t, 1, len(blockIDsMap))
		require.Equal(t, 1, len(blockIDsMap[*tx.TxIDChainHash()]))
		require.Equal(t, []uint32{1}, blockIDsMap[*tx.TxIDChainHash()])

		value, err = client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
		require.NoError(t, err)

		assert.Equal(t, 11, value.Bins[fields.DeleteAtHeight.String()])

		// try to spend with different txid
		spends, err = store.Spend(ctx, spendTx3, 1)
		require.Error(t, err)

		var tErr *errors.Error
		require.ErrorAs(t, err, &tErr)
		require.Equal(t, errors.ERR_UTXO_ERROR, tErr.Code())
		require.Len(t, spends, 1)
		require.ErrorIs(t, spends[0].Err, errors.ErrSpent)
		require.NotNil(t, spends[0].ConflictingTxID)
		require.Equal(t, spendTxAll.TxIDChainHash().String(), spends[0].ConflictingTxID.String())

		// an error was observed were the utxos map got nilled out when trying to double spend
		// interestingly, this only happened in normal mode, not in batched mode :-S
		// Here we get all the data again and try the double spend again
		utxos, ok = value.Bins["utxos"].([]interface{})
		require.True(t, ok)

		for i := 0; i < 5; i++ {
			utxoBytes, ok := utxos[spendsAll[i].Vout].([]byte)
			require.True(t, ok)
			require.Equal(t, 68, len(utxoBytes))
			spendingData, err := spendpkg.NewSpendingDataFromBytes(utxoBytes[32:])
			require.NoError(t, err)
			require.Equal(t, spendTxAll.TxIDChainHash()[:], spendingData.TxID[:])
			require.Equal(t, i, spendingData.Vin)
		}

		// try to spend with different txid
		spends, err = store.Spend(ctx, spendTx3, 1)
		require.Error(t, err)

		value, err = client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
		require.NoError(t, err)
		assert.Equal(t, tx.Version, uint32(value.Bins["version"].(int))) // nolint:gosec
	})

	t.Run("aerospike_reset", func(t *testing.T) {
		cleanDB(t, client)

		tx2 := tx.Clone()

		txMeta, err := store.Create(ctx, tx2, 0)
		assert.NotNil(t, txMeta)
		require.NoError(t, err)

		_ = store.SetBlockHeight(101)

		utxoHash0, _ := util.UTXOHashFromOutput(tx2.TxIDChainHash(), tx2.Outputs[0], 0)
		spends := []*utxo.Spend{{
			TxID:         tx2.TxIDChainHash(),
			Vout:         0,
			UTXOHash:     utxoHash0,
			SpendingData: spendpkg.NewSpendingData(spendingTxID1, 1),
		}}

		_ = spendTx.Inputs[0].PreviousTxIDAdd(tx.TxIDChainHash())
		_, err = store.Spend(ctx, spendTx, 1)
		require.NoError(t, err)

		value, err := client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
		require.NoError(t, err)

		utxos, ok := value.Bins["utxos"].([]interface{})
		require.True(t, ok)
		utxoBytes, ok := utxos[spends[0].Vout].([]byte)
		require.True(t, ok)
		require.Equal(t, 68, len(utxoBytes))
		spendingData, err := spendpkg.NewSpendingDataFromBytes(utxoBytes[32:])
		require.NoError(t, err)
		require.Equal(t, spendTx.TxIDChainHash()[:], spendingData.TxID[:])
		require.Equal(t, 0, spendingData.Vin)

		// utxoSpendTxID := utxos[spends[0].Vout]
		// require.Equal(t, spendingTxID1.String(), utxoSpendTxID)

		// try to reset the utxo
		err = store.Unspend(ctx, []*utxo.Spend{{
			TxID:         tx2.TxIDChainHash(),
			Vout:         0,
			UTXOHash:     utxoHash0,
			SpendingData: spendpkg.NewSpendingData(spendingTxID2, 2),
		}})
		require.NoError(t, err)

		value, err = client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
		require.NoError(t, err)

		utxos, ok = value.Bins["utxos"].([]interface{})
		require.True(t, ok)
		require.Len(t, utxos, 5)
		require.Len(t, utxos[0].([]byte), 32)
	})

	t.Run("LUAScripts", func(t *testing.T) {
		cleanDB(t, client)

		var txMeta *meta.Data
		txMeta, err = store.Create(ctx, tx, 0)
		require.NoError(t, err)
		assert.NotNil(t, txMeta)

		var resp *aerospike.Record
		resp, err = client.Get(nil, txKey, "utxos")
		require.NoError(t, err)

		utxos, ok := resp.Bins["utxos"].([]interface{})
		require.True(t, ok)
		assert.Len(t, utxos, 5)

		utxoVal, ok := utxos[0].([]byte)
		require.True(t, ok)
		require.Len(t, utxoVal, 32)
		assert.Equal(t, utxoVal, utxoHash0[:])

		_ = spendTx.Inputs[0].PreviousTxIDAdd(tx.TxIDChainHash())
		_, err = store.Spend(ctx, spendTx, 1)
		require.NoError(t, err)

		resp, err = client.Get(nil, txKey, "utxos", "spentUtxos")
		require.NoError(t, err)

		utxos, ok = resp.Bins["utxos"].([]interface{})
		require.True(t, ok)
		require.Len(t, utxos, 5)
		require.Len(t, utxos[0].([]byte), 68)

		spendUtxos, ok := resp.Bins["spentUtxos"].(int)
		require.True(t, ok)
		assert.Equal(t, 1, spendUtxos)

		err = store.Unspend(ctx, spends)
		require.NoError(t, err)

		resp, err = client.Get(nil, txKey, "utxos", "spentUtxos")
		require.NoError(t, err)

		utxos, ok = resp.Bins["utxos"].([]interface{})
		require.True(t, ok)
		require.Len(t, utxos, 5)
		require.Len(t, utxos[0].([]byte), 32)

		spendUtxos, ok = resp.Bins["spentUtxos"].(int)
		require.True(t, ok)
		assert.Equal(t, 0, spendUtxos)
	})

	t.Run("CreateWithBlockIDs", func(t *testing.T) {
		cleanDB(t, client)

		var blockHeight uint32

		txMeta, err := store.Create(ctx, tx, blockHeight, utxo.WithMinedBlockInfo(
			utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 123, SubtreeIdx: 1, OnLongestChain: true},
			utxo.MinedBlockInfo{BlockID: 2, BlockHeight: 124, SubtreeIdx: 2, OnLongestChain: true},
			utxo.MinedBlockInfo{BlockID: 3, BlockHeight: 125, SubtreeIdx: 3, OnLongestChain: true},
		))
		require.NoError(t, err)
		assert.NotNil(t, txMeta)

		resp, err := client.Get(nil, txKey, "blockIDs")
		require.NoError(t, err)

		blockIDs, ok := resp.Bins["blockIDs"].([]interface{})
		require.True(t, ok)
		require.Len(t, blockIDs, 3)

		id1, ok := blockIDs[0].(int)
		require.True(t, ok)
		assert.Equal(t, 1, id1)

		id2, ok := blockIDs[1].(int)
		require.True(t, ok)
		assert.Equal(t, 2, id2)

		id3, ok := blockIDs[2].(int)
		require.True(t, ok)
		assert.Equal(t, 3, id3)
	})

	t.Run("TestStoreOPReturn", func(t *testing.T) {
		cleanDB(t, client)

		txMeta, err := store.Create(ctx, txWithOPReturn, 0)
		require.NoError(t, err)
		assert.NotNil(t, txMeta)

		resp, err := client.Get(nil, txWithOPReturnKey, "utxos")
		require.NoError(t, err)

		utxos, ok := resp.Bins["utxos"].([]interface{})
		require.True(t, ok)
		assert.Len(t, utxos, 2)

		_, ok = utxos[0].([]byte)
		require.False(t, ok)

		utxo1, ok := utxos[1].([]byte)
		require.True(t, ok)
		require.Len(t, utxo1, 32)
	})

	t.Run("FrozenUTXO", func(t *testing.T) {
		cleanDB(t, client)

		txMeta, err := store.Create(ctx, tx, 0)
		require.NoError(t, err)
		assert.NotNil(t, txMeta)

		spendingTx := utxo2.GetSpendingTx(tx, 0, 1)

		err = store.FreezeUTXOs(ctx, []*utxo.Spend{{
			TxID:     tx.TxIDChainHash(),
			Vout:     1,
			UTXOHash: utxoHash1,
		}}, tSettings)
		require.NoError(t, err)

		_, err = store.Spend(ctx, spendingTx, 1)
		require.Error(t, err)

		var tErr *errors.Error
		require.ErrorAs(t, err, &tErr)
		require.Equal(t, errors.ERR_UTXO_ERROR, tErr.Code())
		require.ErrorIs(t, err, errors.ErrFrozen)
	})

	t.Run("aerospike_get_conflicting", func(t *testing.T) {
		cleanDB(t, client)

		txMeta, err := store.Create(ctx, tx, 0)
		assert.NotNil(t, txMeta)
		require.NoError(t, err)

		_, _, err = store.SetConflicting(ctx, []chainhash.Hash{*tx.TxIDChainHash()}, true)
		require.NoError(t, err)

		tx2 := &bt.Tx{}
		require.NoError(t, tx2.From(tx.TxID(), 0, tx.Outputs[0].LockingScript.String(), tx.Outputs[0].Satoshis))
		require.NoError(t, tx2.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))

		var tErr *errors.Error

		txSpends, err := store.Spend(ctx, tx2, 1)
		require.ErrorAs(t, err, &tErr)
		require.Equal(t, errors.ERR_UTXO_ERROR, tErr.Code())

		require.Len(t, txSpends, 1)

		require.ErrorAs(t, txSpends[0].Err, &tErr)
		require.Equal(t, errors.ERR_TX_CONFLICTING, tErr.Code())

		// create the conflicting tx, just like the validator would do, and make sure the parent tx has been updated with the conflicting tx
		txMeta, err = store.Create(ctx, tx2, 1, utxo.WithConflicting(true))
		require.NoError(t, err)

		assert.True(t, txMeta.Conflicting)

		// get the parent tx and make sure it has the conflicting tx flag and tx2 as a conflicting child
		txMeta, err = store.Get(ctx, tx.TxIDChainHash(), fields.Conflicting, fields.ConflictingChildren)
		require.NoError(t, err)

		assert.True(t, txMeta.Conflicting)
		assert.Len(t, txMeta.ConflictingChildren, 1)
		assert.Equal(t, tx2.TxIDChainHash().String(), txMeta.ConflictingChildren[0].String())
	})

	t.Run("aerospike_set_locked", func(t *testing.T) {
		cleanDB(t, client)

		txMeta, err := store.Create(ctx, tx, 0)
		assert.NotNil(t, txMeta)
		require.NoError(t, err)

		err = store.SetLocked(ctx, []chainhash.Hash{*tx.TxIDChainHash()}, true)
		require.NoError(t, err)

		tx2 := &bt.Tx{}
		require.NoError(t, tx2.From(tx.TxID(), 0, tx.Outputs[0].LockingScript.String(), tx.Outputs[0].Satoshis))
		require.NoError(t, tx2.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))

		var tErr *errors.Error

		txSpends, err := store.Spend(ctx, tx2, 1)
		require.ErrorAs(t, err, &tErr)
		require.Equal(t, errors.ERR_UTXO_ERROR, tErr.Code())

		assert.Len(t, txSpends, 1)
		assert.ErrorAs(t, txSpends[0].Err, &tErr)
		require.Equal(t, errors.ERR_TX_LOCKED, tErr.Code())

		err = store.SetLocked(ctx, []chainhash.Hash{*tx.TxIDChainHash()}, false)
		require.NoError(t, err)

		txSpends, err = store.Spend(ctx, tx2, 1)
		require.NoError(t, err)

		assert.Len(t, txSpends, 1)
		assert.Nil(t, txSpends[0].Err)
		assert.Equal(t, tx2.TxIDChainHash().String(), txSpends[0].SpendingData.TxID.String())
	})

	// New test to validate SetMinedMulti over multiple txids in a single call
	t.Run("aerospike_setmined_multi_batch_two", func(t *testing.T) {
		cleanDB(t, client)

		// Create two transactions in the store
		_, err = store.Create(ctx, tx, 0)
		require.NoError(t, err)

		_, err = store.Create(ctx, txWithOPReturn, 0)
		require.NoError(t, err)

		// Call SetMinedMulti for both transactions at once
		blockID1 := uint32(111)
		blockHeight1 := uint32(1001)
		subtreeIdx1 := 1

		blockID2 := uint32(222)
		blockHeight2 := uint32(1002)
		subtreeIdx2 := 2

		ids := []*chainhash.Hash{tx.TxIDChainHash(), txWithOPReturn.TxIDChainHash()}

		blockIDsMap, err := store.SetMinedMulti(ctx, ids, utxo.MinedBlockInfo{BlockID: blockID1, BlockHeight: blockHeight1, SubtreeIdx: subtreeIdx1, OnLongestChain: true})
		require.NoError(t, err)
		require.Equal(t, 2, len(blockIDsMap))
		require.Equal(t, []uint32{blockID1}, blockIDsMap[*tx.TxIDChainHash()])
		require.Equal(t, []uint32{blockID1}, blockIDsMap[*txWithOPReturn.TxIDChainHash()])

		// Verify via direct read of bins
		value, err := client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
		require.NoError(t, err)
		assert.Equal(t, []interface{}{int(blockID1)}, value.Bins[fields.BlockIDs.String()].([]interface{}))

		value2, err := client.Get(util.GetAerospikeReadPolicy(tSettings), txWithOPReturnKey)
		require.NoError(t, err)
		assert.Equal(t, []interface{}{int(blockID1)}, value2.Bins[fields.BlockIDs.String()].([]interface{}))

		// Add another block to both using a single multi call again
		blockIDsMap, err = store.SetMinedMulti(ctx, ids, utxo.MinedBlockInfo{BlockID: blockID2, BlockHeight: blockHeight2, SubtreeIdx: subtreeIdx2, OnLongestChain: true})
		require.NoError(t, err)
		require.Equal(t, 2, len(blockIDsMap))
		require.Equal(t, []uint32{blockID1, blockID2}, blockIDsMap[*tx.TxIDChainHash()])
		require.Equal(t, []uint32{blockID1, blockID2}, blockIDsMap[*txWithOPReturn.TxIDChainHash()])

		value, err = client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
		require.NoError(t, err)
		assert.Equal(t, []interface{}{int(blockID1), int(blockID2)}, value.Bins[fields.BlockIDs.String()].([]interface{}))

		value2, err = client.Get(util.GetAerospikeReadPolicy(tSettings), txWithOPReturnKey)
		require.NoError(t, err)
		assert.Equal(t, []interface{}{int(blockID1), int(blockID2)}, value2.Bins[fields.BlockIDs.String()].([]interface{}))
	})

	// New test to validate IncrementSpentRecordsMulti coalesces increments and updates the counter
	t.Run("aerospike_increment_spent_records_multi", func(t *testing.T) {
		cleanDB(t, client)

		bigTx := createTransactionWithOutputs(tSettings.UtxoStore.UtxoBatchSize + 1) // This will make the tx split into 2 records

		_, err = store.Create(ctx, bigTx, 0)
		require.NoError(t, err)

		bigTxKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), uaerospike.CalculateKeySourceInternal(bigTx.TxIDChainHash(), 0))
		require.NoError(t, err)

		// Get the master record
		rec, err := client.Get(util.GetAerospikeReadPolicy(tSettings), bigTxKey)
		require.NoError(t, err)

		// Check the creating bin is removed
		assert.Nil(t, rec.Bins["creating"])

		// The spentExtraRecords will be nil if not set
		initial := 0
		if v, ok := rec.Bins["spentExtraRecs"].(int); ok {
			initial = v
		}

		totalExtraRecs := rec.Bins[fields.TotalExtraRecs.String()].(int)
		assert.Equal(t, 1, totalExtraRecs)

		childKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), uaerospike.CalculateKeySourceInternal(bigTx.TxIDChainHash(), 1))
		require.NoError(t, err)

		// Get the master record
		rec, err = client.Get(util.GetAerospikeReadPolicy(tSettings), childKey)
		require.NoError(t, err)

		// Check the creating bin is removed
		assert.Nil(t, rec.Bins["creating"])

		// Increment via multi API
		require.NoError(t, store.IncrementSpentRecordsMulti([]*chainhash.Hash{bigTx.TxIDChainHash()}, 1))

		rec2, err := client.Get(util.GetAerospikeReadPolicy(tSettings), bigTxKey)
		require.NoError(t, err)

		v2, ok := rec2.Bins["spentExtraRecs"].(int)
		require.True(t, ok)

		assert.Equal(t, initial+1, v2)
	})

	// New test: SetMinedMulti with one valid and one invalid hash should partially succeed and return an error
	t.Run("aerospike_setmined_multi_partial_failure", func(t *testing.T) {
		cleanDB(t, client)

		_, err = store.Create(ctx, tx, 0)
		require.NoError(t, err)

		valid := tx.TxIDChainHash()
		var invalid chainhash.Hash // zero hash not present in DB
		txids := []*chainhash.Hash{valid, &invalid}

		blockID := uint32(333)
		blockHeight := uint32(2001)
		subtreeIdx := 5

		blockIDsMap, err := store.SetMinedMulti(ctx, txids, utxo.MinedBlockInfo{BlockID: blockID, BlockHeight: blockHeight, SubtreeIdx: subtreeIdx, OnLongestChain: true})
		require.Error(t, err)
		// Valid tx should be present and updated
		require.Contains(t, blockIDsMap, *valid)
		require.Equal(t, []uint32{blockID}, blockIDsMap[*valid])
		// Invalid hash should not be present in successful map
		_, hasInvalid := blockIDsMap[invalid]
		require.False(t, hasInvalid)
	})

	// New test: IncrementSpentRecordsMulti with one invalid key should aggregate errors and still update the valid one
	t.Run("aerospike_increment_spent_records_multi_with_errors", func(t *testing.T) {
		cleanDB(t, client)

		bigTx := createTransactionWithOutputs(tSettings.UtxoStore.UtxoBatchSize + 1) // This will make the tx split into 2 records

		_, err = store.Create(ctx, bigTx, 0)
		require.NoError(t, err)

		bigTxKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), bigTx.TxIDChainHash().CloneBytes())
		require.NoError(t, err)

		rec, err := client.Get(util.GetAerospikeReadPolicy(tSettings), bigTxKey, "spentExtraRecs")
		require.NoError(t, err)
		base := 0
		if v, ok := rec.Bins["spentExtraRecs"].(int); ok {
			base = v
		}

		valid := tx.TxIDChainHash()
		var invalid chainhash.Hash // zero hash, not present
		ids := []*chainhash.Hash{valid, &invalid}

		aggErr := store.IncrementSpentRecordsMulti(ids, 1)
		require.Error(t, aggErr)
		t.Logf("Error: %v", aggErr)

		rec2, err := client.Get(util.GetAerospikeReadPolicy(tSettings), bigTxKey)
		require.NoError(t, err)

		spentExtraRecordsBin := rec2.Bins["spentExtraRecs"]
		if spentExtraRecordsBin == nil {
			t.Logf("spentExtraRecs bin is nil")
		} else {
			v2, ok := spentExtraRecordsBin.(int)
			require.True(t, ok)
			assert.Equal(t, base+1, v2)
		}
	})

	t.Run("set mined with locked", func(t *testing.T) {
		cleanDB(t, client)

		txMeta, err := store.Create(context.Background(), tx, 0, utxo.WithLocked(true))
		assert.NotNil(t, txMeta)
		require.NoError(t, err)

		assert.True(t, txMeta.Locked)

		txMeta, err = store.Get(context.Background(), tx.TxIDChainHash())
		require.NoError(t, err)

		assert.True(t, txMeta.Locked)
	})
}

func TestCoinbase(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(func() {
		deferFn()
	})

	cleanDB(t, client)

	txMeta, err := store.Create(ctx, coinbaseTx, 2)
	require.NoError(t, err)
	assert.NotNil(t, txMeta)
	assert.True(t, txMeta.IsCoinbase)

	var tErr *errors.Error

	spends, err := store.Spend(ctx, spendCoinbaseTx, 1)
	require.ErrorAs(t, err, &tErr)
	require.Equal(t, errors.ERR_UTXO_ERROR, tErr.Code())
	require.ErrorIs(t, spends[0].Err, errors.ErrTxCoinbaseImmature)

	_, err = store.Spend(ctx, spendCoinbaseTx, 3)
	require.NoError(t, err)
}

// func TestBigOPReturn(t *testing.T) {
//	client, aeroErr := uaerospike.NewClient(aerospikeHost, aerospikePort)
//	require.NoError(t, aeroErr)
//
//  aeroURL, err := url.Parse(aerospikeURL)
//	require.NoError(t, err)
//
//	// teranode db client
//	var db *Store
//	db, err = New(ulogger.TestLogger{}, aeroURL)
//	require.NoError(t, err)
//
//	f, err := os.Open("testdata/d51051ebcd649ab7a02de85f130b55c357c514ee5f911da9a8dc3bd2ead750ac.hex")
//	require.NoError(t, err)
//	defer f.Close()
//
//	bigTx := new(bt.Tx)
//
//	_, err = bigTx.ReadFrom(f)
//	require.NoError(t, err)
//
//	key, err := aerospike.NewKey(db.GetNamespace(), db.GetName(), bigTx.TxIDChainHash().CloneBytes())
//	require.NoError(t, err)
//	assert.NotNil(t, key)
//
//	_, err = client.Delete(nil, key)
//	require.NoError(t, err)
//
//	txMeta, err := db.Create(ctx, bigTx, 0)
//	require.NoError(t, err)
//	assert.NotNil(t, txMeta)
//
//	resp, err := client.Get(nil, key, "external", "utxos")
//	require.NoError(t, err)
//	big, ok := resp.Bins["external"].(bool)
//	require.True(t, ok)
//	assert.True(t, big)
//
//	utxos, ok := resp.Bins["utxos"].([]interface{})
//	require.True(t, ok)
//	assert.Len(t, utxos, 2)
//
//	// Get the tx back
//	txMeta, err = db.Get(ctx, bigTx.TxIDChainHash())
//	require.NoError(t, err)
//	txCopy := txMeta.Tx
//	assert.Equal(t, bigTx.TxIDChainHash().String(), txCopy.TxIDChainHash().String())
// }

// func TestMultiUTXORecords(t *testing.T) {
//	// For this test, we will assume that aerospike can never store more than 2 utxos in a single record
//	client, aeroErr := uaerospike.NewClient(aerospikeHost, aerospikePort)
//	require.NoError(t, aeroErr)
//
//	aeroURL, err := url.Parse(aerospikeURL)
//	require.NoError(t, err)
//
//	// teranode db client
//	var db *Store
//	db, err = New(ulogger.TestLogger{}, aeroURL)
//	require.NoError(t, err)
//
//	db.utxoBatchSize = 2 // This is only set for testing purposes
//
//	f, err := os.Open("testdata/8f7724e256343cf21dbb1d8ce8fa5caae79da212c90ca6aef3b415c0a9bc003c.bin")
//	require.NoError(t, err)
//	defer f.Close()
//
//	bigTx := new(bt.Tx)
//
//	_, err = bigTx.ReadFrom(f)
//	require.NoError(t, err)
//
//	key0, err := aerospike.NewKey(db.GetNamespace(), db.GetName(), bigTx.TxIDChainHash().CloneBytes())
//	require.NoError(t, err)
//	assert.NotNil(t, key0)
//
//	_, err = client.Delete(nil, key0)
//	require.NoError(t, err)
//
//	txMeta, err := db.Create(ctx, bigTx, 0)
//	require.NoError(t, err)
//	assert.NotNil(t, txMeta)
//
//	resp, err := client.Get(nil, key0, "utxos", "totalExtraRecs")
//	require.NoError(t, err)
//	utxos, ok := resp.Bins["utxos"].([]interface{})
//	require.True(t, ok)
//	assert.Len(t, utxos, 2)
//	totalExtraRecs, ok := resp.Bins["totalExtraRecs"].(int)
//	require.True(t, ok)
//	assert.Equal(t, 3, totalExtraRecs)
//
//	key1, err := aerospike.NewKey(db.GetNamespace(), db.GetName(), uaerospike.CalculateKeySource(bigTx.TxIDChainHash(), 1))
//	require.NoError(t, err)
//	assert.NotNil(t, key1)
//
//	resp, err = client.Get(nil, key1, "utxos")
//	require.NoError(t, err)
//	utxos, ok = resp.Bins["utxos"].([]interface{})
//	require.True(t, ok)
//	assert.Len(t, utxos, 2)
//
//	key2, err := aerospike.NewKey(db.GetNamespace(), db.GetName(), uaerospike.CalculateKeySource(bigTx.TxIDChainHash(), 2))
//	require.NoError(t, err)
//	assert.NotNil(t, key2)
//
//	resp, err = client.Get(nil, key2, "utxos")
//	require.NoError(t, err)
//	utxos, ok = resp.Bins["utxos"].([]interface{})
//	require.True(t, ok)
//	assert.Len(t, utxos, 1)
//
//	// Now spend the first utxo
//	utxoHash, err := util.UTXOHashFromOutput(bigTx.TxIDChainHash(), bigTx.Outputs[0], 0)
//	require.NoError(t, err)
//
//	spend0 := &utxo.Spend{
//		TxID:         bigTx.TxIDChainHash(),
//		Vout:         0,
//		UTXOHash:     utxoHash,
//		SpendingTxID: spendingTxID1,
//	}
//
//	err = db.Spend(ctx, []*utxo.Spend{spend0}, 0)
//	require.NoError(t, err)
//
//	resp, err = client.Get(nil, key0, "utxos", "totalExtraRecs")
//	require.NoError(t, err)
//	utxos, ok = resp.Bins["utxos"].([]interface{})
//	require.True(t, ok)
//	assert.Len(t, utxos, 2)
//	utxo0 := utxos[0].([]byte)
//	assert.Len(t, utxo0, 64)
//	totalExtraRecs, ok = resp.Bins["totalExtraRecs"].(int)
//	require.True(t, ok)
//	assert.Equal(t, 3, totalExtraRecs)
//
//	// Spend the 5th utxo
//	utxoHash4, err := util.UTXOHashFromOutput(bigTx.TxIDChainHash(), bigTx.Outputs[4], 4)
//	require.NoError(t, err)
//
//	spend4 := &utxo.Spend{
//		TxID:         bigTx.TxIDChainHash(),
//		Vout:         4,
//		UTXOHash:     utxoHash4,
//		SpendingTxID: spendingTxID1,
//	}
//	err = db.Spend(ctx, []*utxo.Spend{spend4}, 0)
//	require.NoError(t, err)
//
//	resp, err = client.Get(nil, key2, "utxos")
//	require.NoError(t, err)
//	utxos, ok = resp.Bins["utxos"].([]interface{})
//	require.True(t, ok)
//	assert.Len(t, utxos, 1)
//	utxo4 := utxos[0].([]byte)
//	assert.Len(t, utxo4, 64)
//
//	resp, err = client.Get(nil, key0, "totalExtraRecs")
//	require.NoError(t, err)
//	totalExtraRecs, ok = resp.Bins["totalExtraRecs"].(int)
//	require.True(t, ok)
//	assert.Equal(t, 2, totalExtraRecs)
// }

func TestIncrementSpentRecords(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)

	t.Cleanup(func() {
		deferFn()
	})

	key, aErr := aerospike.NewKey(store.GetNamespace(), store.GetName(), tx.TxIDChainHash().CloneBytes())
	require.NoError(t, aErr)
	assert.NotNil(t, key)

	_, aErr = client.Delete(nil, key)
	require.NoError(t, aErr)

	txMeta, err := store.Create(ctx, tx, 0)
	require.NoError(t, err)
	assert.NotNil(t, txMeta)

	resp, err := client.Get(nil, key, "totalExtraRecs")
	require.NoError(t, err)

	totalExtraRecs, ok := resp.Bins["totalExtraRecs"].(int)
	require.True(t, ok)
	assert.Equal(t, 0, totalExtraRecs)

	res, err := store.IncrementSpentRecords(tx.TxIDChainHash(), 1)
	require.NoError(t, err) // IncrementSpentRecords doesn't return error directly

	// Parse the response to check for error
	parsedRes, err := store.ParseLuaMapResponse(res)
	require.NoError(t, err)
	assert.Equal(t, teranode_aerospike.LuaStatusError, parsedRes.Status)
	assert.Contains(t, parsedRes.Message, "spentExtraRecs cannot be greater than totalExtraRecs")
}

func TestStoreDecorate(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)

	t.Cleanup(func() {
		deferFn()
	})

	t.Run("aerospike_BatchDecorate", func(t *testing.T) {
		cleanDB(t, client)

		txMeta, err := store.Create(ctx, tx, 0)
		require.NoError(t, err)
		assert.NotNil(t, txMeta)

		txID := tx.TxIDChainHash().String()
		_ = txID

		assert.NotNil(t, txMeta)
		require.NoError(t, err)

		items := []*utxo.UnresolvedMetaData{
			{
				Hash: *tx.TxIDChainHash(),
				Idx:  0,
			},
			{
				Hash: *tx.TxIDChainHash(),
				Idx:  1,
			},
			{
				Hash: *tx.TxIDChainHash(),
				Idx:  2,
			},
			{
				Hash: *tx.TxIDChainHash(),
				Idx:  3,
			},
			{
				Hash: *tx.TxIDChainHash(),
				Idx:  4,
			},
		}
		err = store.BatchDecorate(ctx, items)
		require.NoError(t, err)

		// check field values
		for _, item := range items {
			assert.Equal(t, uint64(215), item.Data.Fee)
			assert.Equal(t, uint64(328), item.Data.SizeInBytes)
		}
	})

	t.Run("aerospike_PreviousOutputsDecorate", func(t *testing.T) {
		cleanDB(t, client)

		txMeta, err := store.Create(ctx, tx, 0)
		require.NoError(t, err)
		assert.NotNil(t, txMeta)

		txID := tx.TxIDChainHash().String()
		_ = txID

		assert.NotNil(t, txMeta)
		require.NoError(t, err)

		childTx := &bt.Tx{}
		childTx.Inputs = make([]*bt.Input, 0)

		input := &bt.Input{
			PreviousTxOutIndex: 0,
		}
		_ = input.PreviousTxIDAdd(tx.TxIDChainHash())
		childTx.Inputs = append(childTx.Inputs, input)

		input = &bt.Input{
			PreviousTxOutIndex: 4,
		}
		_ = input.PreviousTxIDAdd(tx.TxIDChainHash())
		childTx.Inputs = append(childTx.Inputs, input)

		input = &bt.Input{
			PreviousTxOutIndex: 3,
		}
		_ = input.PreviousTxIDAdd(tx.TxIDChainHash())
		childTx.Inputs = append(childTx.Inputs, input)

		input = &bt.Input{
			PreviousTxOutIndex: 2,
		}
		_ = input.PreviousTxIDAdd(tx.TxIDChainHash())
		childTx.Inputs = append(childTx.Inputs, input)

		input = &bt.Input{
			PreviousTxOutIndex: 1,
		}
		_ = input.PreviousTxIDAdd(tx.TxIDChainHash())
		childTx.Inputs = append(childTx.Inputs, input)

		err = store.PreviousOutputsDecorate(ctx, childTx)
		require.NoError(t, err)

		// check field values for vout 0 - item 0
		assert.Len(t, *childTx.Inputs[0].PreviousTxScript, 25)
		assert.Equal(t, uint64(5_000_000), childTx.Inputs[0].PreviousTxSatoshis)

		// check field values for vout 4 - item 1
		assert.Len(t, *childTx.Inputs[1].PreviousTxScript, 25)
		assert.Equal(t, uint64(2_817_689), childTx.Inputs[1].PreviousTxSatoshis)

		// check field values for vout 3 - item 2
		assert.Len(t, *childTx.Inputs[2].PreviousTxScript, 25)
		assert.Equal(t, uint64(20_000), childTx.Inputs[2].PreviousTxSatoshis)

		// check field values for vout 2 - item 3
		assert.Len(t, *childTx.Inputs[3].PreviousTxScript, 25)
		assert.Equal(t, uint64(20_000), childTx.Inputs[3].PreviousTxSatoshis)

		// check field values for vout 1 - item 4
		assert.Len(t, *childTx.Inputs[4].PreviousTxScript, 25)
		assert.Equal(t, uint64(2_000_000), childTx.Inputs[4].PreviousTxSatoshis)
	})
}

// func TestLargeUTXO(t *testing.T) {
//	// For this test, we will assume that aerospike can never store more than 2 utxos in a single record
//	client, aeroErr := uaerospike.NewClient(aerospikeHost, aerospikePort)
//	require.NoError(t, aeroErr)
//
//	aeroURL, err := url.Parse(fmt.Sprintf(aerospikeURLFormat, aerospikeHost, aerospikePort, aerospikeNamespace, aerospikeSet, aerospikeExpiration))
//	require.NoError(t, err)
//
//	// teranode db client
//	var db *Store
//	db, err = New(ulogger.TestLogger{}, aeroURL)
//	require.NoError(t, err)
//
//	fParent, err := os.Open("testdata/ac4849b3b03e44d5fcba8becfc642a8670049b59436d6c7ab89a4d3873d9a3ef.bin")
//	require.NoError(t, err)
//	defer fParent.Close()
//
//	parentTx := new(bt.Tx)
//	_, err = parentTx.ReadFrom(fParent)
//	require.NoError(t, err)
//	require.Equal(t, "ac4849b3b03e44d5fcba8becfc642a8670049b59436d6c7ab89a4d3873d9a3ef", parentTx.TxIDChainHash().String())
//
//	fChild, err := os.Open("testdata/1bd4f08ffbeefbb67d82a340dd35259a97c5626368f8a6efa056571b293fae52.bin")
//	require.NoError(t, err)
//	defer fChild.Close()
//
//	childTx := new(bt.Tx)
//	_, err = childTx.ReadFrom(fChild)
//	require.NoError(t, err)
//	require.Equal(t, "1bd4f08ffbeefbb67d82a340dd35259a97c5626368f8a6efa056571b293fae52", childTx.TxIDChainHash().String())
//
//	keyParent, err := aerospike.NewKey(db.GetNamespace(), db.GetName(), parentTx.TxIDChainHash().CloneBytes())
//	require.NoError(t, err)
//	assert.NotNil(t, keyParent)
//
//	_, err = client.Delete(nil, keyParent)
//	require.NoError(t, err)
//
//	parentMeta, err := db.Create(ctx, parentTx, 0)
//	require.NoError(t, err)
//	assert.NotNil(t, parentMeta)
//
//	keyChild, err := aerospike.NewKey(db.GetNamespace(), db.GetName(), childTx.TxIDChainHash().CloneBytes())
//	require.NoError(t, err)
//	assert.NotNil(t, keyChild)
//
//	_, err = client.Delete(nil, keyChild)
//	require.NoError(t, err)
//
//	childMeta, err := db.Create(ctx, childTx, 0)
//	require.NoError(t, err)
//	assert.NotNil(t, childMeta)
//
//	previousTxID, err := chainhash.NewHashFromStr("ac4849b3b03e44d5fcba8becfc642a8670049b59436d6c7ab89a4d3873d9a3ef")
//	require.NoError(t, err)
//
//	previousOutput := &meta.PreviousOutput{
//		PreviousTxID: *previousTxID,
//		Vout:         31243,
//	}
//
//	assert.Nil(t, previousOutput.LockingScript)
//
//	err = db.PreviousOutputsDecorate(ctx, []*meta.PreviousOutput{previousOutput})
//	require.NoError(t, err)
//	assert.NotNil(t, previousOutput.LockingScript)
//	// t.Log(previousOutput)
// }

func TestSmokeTests(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	_, store, ctx, deferFn := initAerospike(t, tSettings, logger)

	t.Cleanup(func() {
		deferFn()
	})

	t.Run("aerospike_store", func(t *testing.T) {
		err := store.Delete(ctx, tests.TXHash)
		require.NoError(t, err)

		tests.Store(t, store)
	})

	t.Run("aerospike_spend", func(t *testing.T) {
		err := store.Delete(ctx, tests.TXHash)
		require.NoError(t, err)

		tests.Spend(t, store)
	})

	t.Run("aerospike_reset", func(t *testing.T) {
		err := store.Delete(ctx, tests.TXHash)
		require.NoError(t, err)

		tests.Restore(t, store)
	})

	t.Run("aerospike_freeze", func(t *testing.T) {
		err := store.Delete(ctx, tests.TXHash)
		require.NoError(t, err)

		tests.Freeze(t, store)
	})

	t.Run("aerospike_reassign", func(t *testing.T) {
		err := store.Delete(ctx, tests.TXHash)
		require.NoError(t, err)

		tests.ReAssign(t, store)
	})

	t.Run("set mined", func(t *testing.T) {
		err := store.Delete(ctx, tests.TXHash)
		require.NoError(t, err)

		tests.SetMined(t, store)
	})

	t.Run("aerospike_conflicting", func(t *testing.T) {
		err := store.Delete(ctx, tests.TXHash)
		require.NoError(t, err)

		tests.Conflicting(t, store)
	})
}

func TestCreateZeroSat(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)

	t.Cleanup(func() {
		deferFn()
	})

	// Read hex file from os
	txHex, err := os.ReadFile("testdata/a3041ffd31b46f9bb6e4cd78c89287b405fb911e893a5dead18372400832abc6.hex")
	require.NoError(t, err)

	tx, err := bt.NewTxFromString(string(txHex))
	require.NoError(t, err)

	// Create the tx
	_, err = store.Create(ctx, tx, 0)
	require.NoError(t, err)

	key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), tx.TxIDChainHash().CloneBytes())
	require.NoError(t, err)

	// Check the tx was created with 2 utxos and 0 spent utxos and no expiration
	response, err := client.Get(nil, key)
	require.NoError(t, err)

	assert.NotNil(t, response)
	assert.Equal(t, 2, response.Bins["totalUtxos"])
	assert.Equal(t, 2, response.Bins["recordUtxos"])
	assert.Equal(t, 0, response.Bins["spentUtxos"])
	assert.Nil(t, response.Bins[fields.DeleteAtHeight.String()])

	spendingTx1 := utxo2.GetSpendingTx(tx, 1)

	_, err = store.Spend(ctx, spendingTx1, 1)
	require.NoError(t, err)

	// Check the tx was updated to 1 spent utxo...
	response, err = client.Get(nil, key)
	require.NoError(t, err)

	assert.NotNil(t, response)
	assert.Equal(t, 2, response.Bins["totalUtxos"])
	assert.Equal(t, 1, response.Bins["spentUtxos"])
	assert.Nil(t, response.Bins[fields.DeleteAtHeight.String()])

	// Now setMined and check the DAH is not set
	blockIDsMap, err := store.SetMinedMulti(ctx, []*chainhash.Hash{tx.TxIDChainHash()}, utxo.MinedBlockInfo{
		BlockID: 1, BlockHeight: 123, SubtreeIdx: 1, OnLongestChain: true,
	})
	require.NoError(t, err)
	require.Len(t, blockIDsMap, 1)
	require.Len(t, blockIDsMap[*tx.TxIDChainHash()], 1)
	require.Equal(t, []uint32{1}, blockIDsMap[*tx.TxIDChainHash()])

	response, err = client.Get(nil, key)
	require.NoError(t, err)

	assert.NotNil(t, response)
	assert.Equal(t, 2, response.Bins["totalUtxos"])
	assert.Equal(t, 1, response.Bins["spentUtxos"])
	assert.Nil(t, response.Bins[fields.DeleteAtHeight.String()])

	// Spend the output 0
	spendingTx0 := utxo2.GetSpendingTx(tx, 0)
	_, err = store.Spend(ctx, spendingTx0, 1)
	require.NoError(t, err)

	// Check the tx was updated to 2 spent utxos...
	response, err = client.Get(nil, key)
	require.NoError(t, err)

	assert.NotNil(t, response)
	assert.Equal(t, 2, response.Bins["totalUtxos"])
	assert.Equal(t, 2, response.Bins["spentUtxos"])
	assert.Equal(t, 11, response.Bins[fields.DeleteAtHeight.String()])
}

func TestAerospikeWithBatchSize(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.UtxoBatchSize = 2 // Maximum batch of 2 per record

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)

	t.Cleanup(func() {
		deferFn()
	})

	txKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), tx.TxIDChainHash().CloneBytes())
	require.NoError(t, err)

	t.Run("aerospike_spend_all_lua_with_extra_records", func(t *testing.T) {
		cleanDB(t, client)

		txMeta, err := store.Create(ctx, tx, 0) // Tx has 5 utxos
		assert.NotNil(t, txMeta)
		require.NoError(t, err)

		value, err := client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
		require.NoError(t, err)

		assert.NotNil(t, value)
		assert.Equal(t, 5, value.Bins[fields.TotalUtxos.String()])
		assert.Equal(t, 2, value.Bins[fields.RecordUtxos.String()])
		assert.Equal(t, 0, value.Bins[fields.SpentUtxos.String()])

		totalExtraRecs, ok := value.Bins[fields.TotalExtraRecs.String()].(int)
		require.True(t, ok)
		assert.Equal(t, 2, totalExtraRecs) // We should have 1 master record and 2 extra records: 2 utxos in master, 2 in extra 1 and 1 in extra 2

		external, ok := value.Bins[fields.External.String()].(bool)
		require.True(t, ok)
		assert.True(t, external)

		// Check the creating bin is removed
		value, err = client.Get(util.GetAerospikeReadPolicy(tSettings), txKey, fields.Creating.String())
		require.NoError(t, err)
		assert.Nil(t, value.Bins[fields.Creating.String()])

		spendsAll, err := store.Spend(ctx, spendTxAll, 1)
		require.NoError(t, err)
		assert.Equal(t, 5, len(spendsAll))

		// value, err = client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
		// require.NoError(t, err)

		// utxos, ok := value.Bins["utxos"].([]interface{})
		// require.True(t, ok)
		// assert.Len(t, utxos, 2)

		// totalExtraRecs, ok = value.Bins["totalExtraRecs"].(int)
		// require.True(t, ok)
		// assert.Equal(t, 2, totalExtraRecs)

		// spentExtraRecs, ok := value.Bins["spentExtraRecs"].(int)
		// assert.True(t, ok)
		// assert.Equal(t, 2, spentExtraRecs)

		time.Sleep(3 * time.Second)

		value, err = client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
		require.NoError(t, err)

		utxos, ok := value.Bins[fields.Utxos.String()].([]interface{})
		require.True(t, ok)
		assert.Len(t, utxos, 2)

		totalExtraRecs, ok = value.Bins[fields.TotalExtraRecs.String()].(int)
		require.True(t, ok)
		assert.Equal(t, 2, totalExtraRecs)

		spentExtraRecs, ok := value.Bins[fields.SpentExtraRecs.String()].(int)
		require.True(t, ok)
		assert.Equal(t, 2, spentExtraRecs)
	})

	t.Run("aerospike_increment_spent_records", func(t *testing.T) {
		cleanDB(t, client)

		txMeta, err := store.Create(ctx, tx, 0)
		require.NoError(t, err)
		assert.NotNil(t, txMeta)

		resp, err := client.Get(nil, txKey, fields.TotalExtraRecs.String())
		require.NoError(t, err)

		totalExtraRecs, ok := resp.Bins[fields.TotalExtraRecs.String()].(int)
		require.True(t, ok)
		assert.Equal(t, 2, totalExtraRecs)

		_, err = store.IncrementSpentRecords(tx.TxIDChainHash(), 1)
		require.NoError(t, err)

		resp, err = client.Get(nil, txKey)
		require.NoError(t, err)

		totalExtraRecs, ok = resp.Bins[fields.TotalExtraRecs.String()].(int)
		require.True(t, ok)
		assert.Equal(t, 2, totalExtraRecs)

		spentExtraRecs, ok := resp.Bins[fields.SpentExtraRecs.String()].(int)
		require.True(t, ok)
		assert.Equal(t, 1, spentExtraRecs)

		_, err = store.IncrementSpentRecords(tx.TxIDChainHash(), -1)
		require.NoError(t, err)

		resp, err = client.Get(nil, txKey)
		require.NoError(t, err)

		totalExtraRecs, ok = resp.Bins[fields.TotalExtraRecs.String()].(int)
		require.True(t, ok)
		assert.Equal(t, 2, totalExtraRecs)

		spentExtraRecs, ok = resp.Bins[fields.SpentExtraRecs.String()].(int)
		require.True(t, ok)
		assert.Equal(t, 0, spentExtraRecs)
	})

	t.Run("aerospike_setTTLBatch", func(t *testing.T) {
		cleanDB(t, client)

		txMeta, err := store.Create(ctx, tx, 0)
		require.NoError(t, err)
		assert.NotNil(t, txMeta)

		resp, err := client.Get(nil, txKey, fields.TotalExtraRecs.String())
		require.NoError(t, err)

		totalExtraRecs, ok := resp.Bins[fields.TotalExtraRecs.String()].(int)
		require.True(t, ok)
		assert.Equal(t, 2, totalExtraRecs)

		err = store.SetDAHForChildRecords(tx.TxIDChainHash(), 2, 1)
		require.NoError(t, err)

		for i := uint32(1); i <= 2; i++ {
			childKey := uaerospike.CalculateKeySourceInternal(tx.TxIDChainHash(), i)

			aKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), childKey)
			require.NoError(t, err)
			assert.NotNil(t, aKey)

			rec, err := client.Get(nil, aKey)
			require.NoError(t, err)
			assert.Equal(t, 1, rec.Bins[fields.DeleteAtHeight.String()])
		}

		err = store.SetDAHForChildRecords(tx.TxIDChainHash(), 2, 0)
		require.NoError(t, err)

		for i := uint32(1); i <= 2; i++ {
			childKey := uaerospike.CalculateKeySourceInternal(tx.TxIDChainHash(), i)

			aKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), childKey)
			require.NoError(t, err)
			assert.NotNil(t, aKey)

			rec, err := client.Get(nil, aKey)
			require.NoError(t, err)
			assert.Nil(t, rec.Bins[fields.DeleteAtHeight.String()])
		}

	})

	t.Run("aerospike_getAllChildRecords", func(t *testing.T) {
		// Create a random transaction with 5 outputs
		tx, err := utxo2.CreateTransaction(5)
		require.NoError(t, err)

		txMeta, err := store.Create(ctx, tx, 0)
		require.NoError(t, err)
		assert.NotNil(t, txMeta)

		keySource0 := uaerospike.CalculateKeySourceInternal(tx.TxIDChainHash(), 0)
		keySource1 := uaerospike.CalculateKeySourceInternal(tx.TxIDChainHash(), 1)

		key0, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), keySource0)
		require.NoError(t, err)

		key1, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), keySource1)
		require.NoError(t, err)

		record0, err := client.Get(nil, key0)
		require.NoError(t, err)
		assert.Equal(t, 5, record0.Bins[fields.TotalUtxos.String()].(int))
		assert.Equal(t, 2, record0.Bins[fields.TotalExtraRecs.String()].(int))

		spendingTx1 := utxo2.GetSpendingTx(tx, 1)
		spendingTx3 := utxo2.GetSpendingTx(tx, 3)

		spends, err = store.Spend(ctx, spendingTx1, 1)
		require.NoError(t, err)
		assert.Len(t, spends, 1)
		assert.NoError(t, spends[0].Err)

		record0, err = client.Get(nil, key0)
		require.NoError(t, err)
		assert.Equal(t, 5, record0.Bins[fields.TotalUtxos.String()].(int))
		assert.Equal(t, 2, record0.Bins[fields.TotalExtraRecs.String()].(int))

		// logger.SetMuted(true)
		spends, err = store.Spend(ctx, spendingTx3, 1)
		// logger.SetMuted(false)
		require.NoError(t, err)
		assert.Len(t, spends, 1)
		assert.NoError(t, spends[0].Err)

		record1, err := client.Get(nil, key1)
		require.NoError(t, err)
		assert.Equal(t, 2, record1.Bins[fields.RecordUtxos.String()].(int))
		assert.Equal(t, 1, record1.Bins[fields.SpentUtxos.String()].(int))

		resp, err := store.Get(ctx, tx.TxIDChainHash(), fields.Utxos)
		require.NoError(t, err)

		assert.Nil(t, resp.SpendingDatas[0])
		assert.NotNil(t, resp.SpendingDatas[1])
		assert.Nil(t, resp.SpendingDatas[2])
		assert.NotNil(t, resp.SpendingDatas[3])
		assert.Nil(t, resp.SpendingDatas[4])
	})
}

func TestSpendSimple(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)

	t.Cleanup(func() {
		deferFn()
	})

	// Create the tx
	_, err := store.Create(ctx, tx, 0)
	require.NoError(t, err)

	key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), tx.TxIDChainHash().CloneBytes())
	require.NoError(t, err)

	// Check the tx was created with 2 utxos and 0 spent utxos and no expiration
	response, err := client.Get(nil, key)
	require.NoError(t, err)

	assert.NotNil(t, response)
	assert.Equal(t, 5, response.Bins[fields.TotalUtxos.String()])
	assert.Equal(t, 0, response.Bins[fields.SpentUtxos.String()])
	assert.Nil(t, response.Bins[fields.DeleteAtHeight.String()])

	spendingTx1 := utxo2.GetSpendingTx(tx, 1)

	_, err = store.Spend(ctx, spendingTx1, 1)
	require.NoError(t, err)

	// Check the tx was updated to 1 spent utxo...
	response, err = client.Get(nil, key)
	require.NoError(t, err)

	assert.NotNil(t, response)
	assert.Equal(t, 5, response.Bins[fields.TotalUtxos.String()])
	assert.Equal(t, 1, response.Bins[fields.SpentUtxos.String()])
	assert.Nil(t, response.Bins[fields.DeleteAtHeight.String()])
}

func TestRespendExpiredChild(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)

	t.Cleanup(func() {
		deferFn()
	})

	// Create the tx
	_, err := store.Create(ctx, tx, 0)
	require.NoError(t, err)

	// creating spend tx
	spendingTx1 := utxo2.GetSpendingTx(tx, 1)

	spends, err := store.Spend(ctx, spendingTx1, 1)
	require.NoError(t, err)
	assert.Len(t, spends, 1)

	// spending again should be OK
	_, err = store.Spend(ctx, spendingTx1, 1)
	require.NoError(t, err)
	_, err = store.Spend(ctx, spendingTx1, 1)
	require.NoError(t, err)

	// mark the output 1 as spend and child deleted
	key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), tx.TxIDChainHash().CloneBytes())
	require.NoError(t, err)

	// mark the child as deleted
	bin := aerospike.NewBin(fields.DeletedChildren.String(), map[string]bool{
		spendingTx1.TxIDChainHash().String(): true,
	})
	err = client.PutBins(nil, key, bin)
	require.NoError(t, err)

	// spending again should NOT be OK now
	spend, err := store.Spend(ctx, spendingTx1, 1)
	require.Error(t, err)
	assert.Error(t, spend[0].Err)
	assert.ErrorIs(t, spend[0].Err, errors.ErrUtxoError)
}

func TestStore_AerospikeTwoPhaseCommit(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)

	t.Cleanup(func() {
		deferFn()
	})

	// Create the tx
	_, err := store.Create(ctx, tx, 0, utxo.WithLocked(true))
	require.NoError(t, err)

	key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), tx.TxIDChainHash().CloneBytes())
	require.NoError(t, err)

	// Check the tx was created with 2 utxos and 0 spent utxos and no expiration
	response, err := client.Get(nil, key)
	require.NoError(t, err)

	assert.NotNil(t, response)
	assert.True(t, response.Bins[fields.Locked.String()].(bool))

	// Now try to spend it
	spendingTx1 := utxo2.GetSpendingTx(tx, 1)

	spends, err := store.Spend(ctx, spendingTx1, 1)
	require.Error(t, err)
	assert.Len(t, spends, 1)
	assert.ErrorIs(t, err, errors.ErrTxLocked)
}

func TestStore_AerospikeSplitTx(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)

	t.Cleanup(func() {
		deferFn()
	})

	// Create the tx
	txBytes, err := os.ReadFile("testdata/79a2d893527750d622eff3ef728c304c6e0e75516244ade05a5d9d99804d4b2a.bin")
	require.NoError(t, err)

	tx, err := bt.NewTxFromBytes(txBytes)
	require.NoError(t, err)

	// Create the tx
	_, err = store.Create(ctx, tx, 0, utxo.WithLocked(true))
	require.NoError(t, err)

	key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), tx.TxIDChainHash().CloneBytes())
	require.NoError(t, err)

	res, err := client.Get(nil, key)
	require.NoError(t, err)

	assert.NotNil(t, res)
	assert.True(t, res.Bins[fields.Locked.String()].(bool), "Record should be true")

	extraRecords := res.Bins[fields.TotalExtraRecs.String()].(int)
	assert.Equal(t, 3, extraRecords)

	for i := 1; i <= extraRecords; i++ {
		keySource := uaerospike.CalculateKeySourceInternal(tx.TxIDChainHash(), uint32(i))

		key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), keySource)
		require.NoError(t, err)

		res, err := client.Get(nil, key)
		require.NoError(t, err)

		assert.NotNil(t, res)
		assert.True(t, res.Bins[fields.Locked.String()].(bool), "Record %d should be true", i)
	}

	err = store.SetLocked(ctx, []chainhash.Hash{*tx.TxIDChainHash()}, false)
	require.NoError(t, err)

	for i := 0; i <= extraRecords; i++ {
		keySource := uaerospike.CalculateKeySourceInternal(tx.TxIDChainHash(), uint32(i))

		key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), keySource)
		require.NoError(t, err)

		res, err := client.Get(nil, key)
		require.NoError(t, err)

		assert.NotNil(t, res)
		assert.False(t, res.Bins[fields.Locked.String()].(bool), "Record %d should be false", i)
	}
}

func TestRespendSameUTXO(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)

	t.Cleanup(func() {
		deferFn()
	})

	privKey, _ := bec.PrivateKeyFromBytes([]byte("ALWAYS_THE_SAME"))

	parentTx := transactions.Create(t,
		transactions.WithCoinbaseData(100, "/Test miner/"),
		transactions.WithP2PKHOutputs(1, 100000, privKey.PubKey()),
	)

	t.Logf("Parent tx: %s", parentTx.TxIDChainHash().String())

	// Create the parent tx
	_, err := store.Create(ctx, parentTx, 0)
	require.NoError(t, err)

	parentKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), parentTx.TxIDChainHash().CloneBytes())
	require.NoError(t, err)

	parentResp, err := client.Get(nil, parentKey, fields.Utxos.String())
	require.NoError(t, err)
	utxos := parentResp.Bins[fields.Utxos.String()].([]interface{})
	require.Equal(t, 1, len(utxos))
	h := hex.EncodeToString(utxos[0].([]byte))
	require.Equal(t, "323ad5e6f4e332dfcc0995d4ab11491498da4935b3b5478d696df0b62e14a554", h)

	// Create the tx
	childTx1 := transactions.Create(t,
		transactions.WithInput(parentTx, 0, privKey),
		transactions.WithP2PKHOutputs(1, 100, privKey.PubKey()),
	)

	t.Logf("Child tx1: %s", childTx1.TxIDChainHash().String())

	// Now let's create an alternative conflicting transaction
	childTx2 := transactions.Create(t,
		transactions.WithInput(parentTx, 0, privKey),
		transactions.WithP2PKHOutputs(1, 101, privKey.PubKey()),
	)

	t.Logf("Child tx2: %s", childTx2.TxIDChainHash().String())

	spendFn := func(tx *bt.Tx, expectError bool) {
		spends, err := store.Spend(ctx, tx, 1)

		require.Len(t, spends, 1)
		if expectError {
			assert.Error(t, err)
			assert.Error(t, spends[0].Err)
		} else {
			assert.NoError(t, err)
			assert.NoError(t, spends[0].Err)
		}

		// parentResp, err := client.Get(nil, parentKey, fields.Utxos.String())
		// require.NoError(t, err)
		// t.Logf("Parent utxos: %x", parentResp.Bins[fields.Utxos.String()])
		// t.Logf("Spend %s: %v", tx.TxIDChainHash().String(), spends[0])
	}

	// Explicitly spend childTx1 first
	spendFn(childTx1, false)

	// parentResp, err = client.Get(nil, parentKey, fields.Utxos.String())
	// require.NoError(t, err)
	// utxo := fmt.Sprintf("%x", parentResp.Bins[fields.Utxos.String()])
	// require.Len(t, utxo, 136) // 32 + 32 + 4 bytes are hex
	// require.Equal(t, "323ad5e6f4e332dfcc0995d4ab11491498da4935b3b5478d696df0b62e14a554", utxo[0:64])
	// require.Equal(t, "87251f9f8ecb3d5a80a71e0b2d409b1f9c63a13e7b6eb35843b32c7094d6fd75", utxo[64:128])
	// require.Equal(t, "00000000", utxo[128:])

	wg := sync.WaitGroup{}
	wg.Add(20)

	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()

			spendFn(childTx1, false)
		}()
	}

	for i := 0; i < 10; i++ {
		go func() {
			defer wg.Done()

			spendFn(childTx2, true)
		}()
	}

	wg.Wait()
}

func TestDeleteByBin(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, _, _, deferFn := initAerospike(t, tSettings, logger)

	t.Cleanup(func() {
		deferFn()
	})

	putBins(t, client, aerospike.NewWritePolicy(0, 0), "key1", aerospike.BinMap{"ID": 1})
	putBins(t, client, aerospike.NewWritePolicy(0, 0), "key2", aerospike.BinMap{"ID": 2})
	putBins(t, client, aerospike.NewWritePolicy(0, aerospike.TTLDontExpire), "key3", aerospike.BinMap{"ID": 3})

	statement := aerospike.NewStatement("test", "test")

	recordSet, err := client.Query(nil, statement)
	require.NoError(t, err)

	count := 0

	for result := range recordSet.Results() {
		if result != nil {
			count++
		}
	}

	assert.Equal(t, 3, count)

	filterExpression := aerospike.ExpLessEq(
		aerospike.ExpIntBin("ID"),
		aerospike.ExpIntVal(2),
	)

	queryPolicy := aerospike.NewQueryPolicy()
	queryPolicy.FilterExpression = filterExpression

	recordSet, err = client.Query(queryPolicy, statement)
	require.NoError(t, err)

	count = 0

	for result := range recordSet.Results() {
		if result != nil {
			count++
		}
	}

	assert.Equal(t, 2, count)

	writePolicy := aerospike.NewWritePolicy(0, 0)
	// Note: writePolicy.FilterExpression is not needed here because queryPolicy.FilterExpression
	// already filters which records are selected for deletion. The writePolicy only controls
	// how the write operation is applied to the already-filtered record set.

	task, err := client.QueryExecute(queryPolicy, writePolicy, statement, aerospike.DeleteOp())
	require.NoError(t, err)

	// Wait for background delete job to complete on all nodes.
	// task.IsDone() returns true when the job has finished executing.
	require.Eventually(t, func() bool {
		done, err := task.IsDone()
		if err != nil {
			t.Logf("Error checking task status: %v", err)
			return false
		}
		return done
	}, 500*time.Millisecond, 50*time.Millisecond, "Background delete job never completed")

	// IMPORTANT: task.IsDone() returning true does NOT mean deletions are visible to queries.
	// Background job completion only means write operations finished, but changes may not yet
	// be visible to the query engine due to Aerospike's eventual consistency model.
	// We must poll until the expected state appears in query results.
	require.Eventually(t, func() bool {
		recordSet, err := client.Query(nil, statement)
		if err != nil {
			return false
		}

		count := 0
		for result := range recordSet.Results() {
			if result != nil {
				count++
			}
		}

		return count == 1
	}, 500*time.Millisecond, 100*time.Millisecond, "Background deletes completed but not yet visible to queries")
}

func putBins(t *testing.T, client *uaerospike.Client, wp *aerospike.WritePolicy, akey string, bins aerospike.BinMap) {
	key, err := aerospike.NewKey("test", "test", akey)
	require.NoError(t, err)

	err = client.Put(wp, key, bins)
	require.NoError(t, err)
}

type mockIndexWaiter struct{}

// WaitForIndexReady is a mock implementation of the IndexWaiter interface.
func (m *mockIndexWaiter) WaitForIndexReady(_ context.Context, _ string) error {
	return nil
}

func TestAerospikeCleanupService(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)

	t.Cleanup(func() {
		deferFn()
	})

	// start the cleanup service
	cleanupService, err := pruner.NewService(tSettings, pruner.Options{
		Ctx:           ctx,
		Logger:        logger,
		ExternalStore: memory.New(),
		Client:        client,
		Namespace:     store.GetNamespace(),
		Set:           store.GetName(),
		IndexWaiter:   &mockIndexWaiter{},
	})
	require.NoError(t, err)

	// Start the cleanup service
	cleanupService.Start(ctx)
}

func TestDeletedChildren(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)

	t.Cleanup(func() {
		deferFn()
	})

	privKey, _ := bec.PrivateKeyFromBytes([]byte("ALWAYS_THE_SAME"))

	// Create the coinbase tx
	coinbaseTx := transactions.Create(t,
		transactions.WithCoinbaseData(100, "/Test miner/"),
		transactions.WithP2PKHOutputs(1, 5_000_000_000, privKey.PubKey()),
	)

	_, err := store.Create(ctx, coinbaseTx, 0, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{
		BlockID:        1,
		BlockHeight:    1,
		OnLongestChain: true,
	}))
	require.NoError(t, err)

	t.Logf("Coinbase tx: %s", coinbaseTx.TxIDChainHash().String())

	// Create the parentTx with 500 outputs
	parentTxOptions := []transactions.TxOption{
		transactions.WithInput(coinbaseTx, 0, privKey),
	}

	for i := 0; i < 500; i++ {
		parentTxOptions = append(parentTxOptions, transactions.WithP2PKHOutputs(1, 10_000_000, privKey.PubKey()))
	}

	parentTx := transactions.Create(t, parentTxOptions...)

	_, err = store.Create(ctx, parentTx, 0, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{
		BlockID:        1,
		BlockHeight:    1,
		OnLongestChain: true,
	}))
	require.NoError(t, err)

	t.Logf("Parent tx: %s", parentTx.TxIDChainHash().String())

	// Create the childTx with 100 outputs
	childTxOptions := []transactions.TxOption{
		transactions.WithInput(parentTx, 0, privKey),
	}

	for i := 0; i < 100; i++ {
		childTxOptions = append(childTxOptions, transactions.WithP2PKHOutputs(1, 100_000, privKey.PubKey()))
	}

	childTx := transactions.Create(t, childTxOptions...)

	_, err = store.Spend(ctx, childTx, 1)
	require.NoError(t, err)

	_, err = store.Create(ctx, childTx, 0, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{
		BlockID:        1,
		BlockHeight:    1,
		OnLongestChain: true,
	}))

	require.NoError(t, err)

	t.Logf("Child tx: %s", childTx.TxIDChainHash().String())

	// Now we spend all the outputs in the childTx so it can be deleted...
	for i := 0; i < len(childTx.Outputs); i++ {
		spendTx := transactions.Create(t,
			transactions.WithInput(childTx, uint32(i), privKey),
			transactions.WithP2PKHOutputs(1, 100, privKey.PubKey()),
		)

		_, err = store.Spend(ctx, spendTx, 1)
		require.NoError(t, err)
	}

	parentKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), parentTx.TxIDChainHash().CloneBytes())
	require.NoError(t, err)

	childKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), childTx.TxIDChainHash().CloneBytes())
	require.NoError(t, err)

	childResp, err := client.Get(nil, childKey, fields.DeleteAtHeight.String())
	require.NoError(t, err)

	assert.Equal(t, 11, childResp.Bins[fields.DeleteAtHeight.String()])

	opts := pruner.Options{
		Ctx:           ctx,
		Logger:        logger,
		Client:        client,
		ExternalStore: memory.New(),
		Namespace:     store.GetNamespace(),
		Set:           store.GetName(),
		IndexWaiter:   &mockIndexWaiter{},
	}

	cleanupService, err := pruner.NewService(tSettings, opts)
	require.NoError(t, err)

	err = cleanupService.ProcessSingleRecord(childTx.TxIDChainHash(), childTx.Inputs)
	require.NoError(t, err)

	parentResp, err := client.Get(nil, parentKey, fields.DeletedChildren.String())
	require.NoError(t, err)

	deletedChildrenMap := parentResp.Bins[fields.DeletedChildren.String()].(map[interface{}]interface{})
	assert.Len(t, deletedChildrenMap, 1)
}
