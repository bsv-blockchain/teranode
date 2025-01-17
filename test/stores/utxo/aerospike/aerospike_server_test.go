//go:build test_all || test_stores || test_utxo || test_aerospike

package aerospike

import (
	"context"
	"math"
	"os"
	"testing"
	"time"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	teranode_aerospike "github.com/bitcoin-sv/teranode/stores/utxo/aerospike"
	"github.com/bitcoin-sv/teranode/stores/utxo/meta"
	"github.com/bitcoin-sv/teranode/stores/utxo/tests"
	utxo2 "github.com/bitcoin-sv/teranode/test/stores/utxo"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// go test -v -tags test_aerospike ./test/...

func TestAerospike(t *testing.T) {
	client, db, _, deferFn := initAerospike(t)
	defer deferFn()

	tSettings := settings.NewSettings()

	parentTxHash := tx.Inputs[0].PreviousTxIDChainHash()

	coinbaseTx, err := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff19031404002f6d332d617369612fdf5128e62eda1a07e94dbdbdffffffff0500ca9a3b000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac00ca9a3b000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac00ca9a3b000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac00ca9a3b000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac00ca9a3b000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac00000000")
	require.NoError(t, err)

	coinbaseKey, err = aerospike.NewKey(db.GetNamespace(), db.GetName(), coinbaseTx.TxIDChainHash()[:])
	require.NoError(t, err)

	blockID := uint32(123)
	blockID2 := uint32(124)

	var key *aerospike.Key
	key, err = aerospike.NewKey(db.GetNamespace(), db.GetName(), spendingTxID1[:])
	require.NoError(t, err)

	txKey, err := aerospike.NewKey(db.GetNamespace(), db.GetName(), tx.TxIDChainHash().CloneBytes())
	require.NoError(t, err)

	opReturnKey, aErr := aerospike.NewKey(db.GetNamespace(), db.GetName(), txWithOPReturn.TxIDChainHash().CloneBytes())
	require.NoError(t, aErr)

	t.Cleanup(func() {
		policy := util.GetAerospikeWritePolicy(tSettings, 0, aerospike.TTLDontExpire)
		_, _ = client.Delete(policy, key)
	})

	t.Run("aerospikestore", func(t *testing.T) {
		ctx := context.Background()

		cleanDB(t, client, key, tx)

		_, err = db.Create(context.Background(), tx, 0)
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

		_, err = db.Create(context.Background(), tx, 0)
		require.Error(t, err)
		assert.True(t, errors.Is(err, errors.ErrTxExists))

		err = db.SetMinedMulti(ctx, []*chainhash.Hash{tx.TxIDChainHash()}, blockID)
		require.NoError(t, err)

		value, err = client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
		require.NoError(t, err)
		require.Equal(t, uint32(2), value.Generation)
		assert.Len(t, value.Bins["blockIDs"].([]interface{}), 1)
		assert.Equal(t, []interface{}{int(blockID)}, value.Bins["blockIDs"].([]interface{}))

		err = db.SetMinedMulti(ctx, []*chainhash.Hash{tx.TxIDChainHash()}, blockID2)
		require.NoError(t, err)

		value, err = client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
		require.NoError(t, err)
		require.Equal(t, uint32(3), value.Generation)
		assert.Len(t, value.Bins["blockIDs"].([]interface{}), 2)
		assert.Equal(t, []interface{}{int(blockID), int(blockID2)}, value.Bins["blockIDs"].([]interface{}))
		assert.False(t, value.Bins["isCoinbase"].(bool))
	})

	t.Run("aerospike store conflicting", func(t *testing.T) {
		cleanDB(t, client, key, tx)

		_, err = db.Create(context.Background(), tx, 0, utxo.WithConflicting(true))
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

		// check ttl is set
		require.NotEqual(t, uint32(0), value.Expiration)
	})

	t.Run("aerospike store coinbase", func(t *testing.T) {
		ctx := context.Background()

		cleanDB(t, client, key)

		_, err = db.Create(ctx, coinbaseTx, 0)
		require.NoError(t, err)

		var value *aerospike.Record
		// raw aerospike get
		value, err = client.Get(util.GetAerospikeReadPolicy(tSettings), coinbaseKey)
		require.NoError(t, err)
		assert.True(t, value.Bins["isCoinbase"].(bool))

		var txMeta *meta.Data
		txMeta, err = db.Get(ctx, coinbaseTx.TxIDChainHash())
		require.NoError(t, err)
		assert.True(t, txMeta.IsCoinbase)
		assert.Equal(t, txMeta.Tx.ExtendedBytes(), coinbaseTx.ExtendedBytes())
	})

	t.Run("aerospike get", func(t *testing.T) {
		ctx := context.Background()

		cleanDB(t, client, key, tx)

		_, err = db.Create(ctx, tx, 0)
		require.NoError(t, err)

		var value *meta.Data
		value, err = db.Get(ctx, tx.TxIDChainHash())
		require.NoError(t, err)
		assert.Equal(t, uint64(215), value.Fee)
		assert.Equal(t, uint64(328), value.SizeInBytes)
		assert.Len(t, value.ParentTxHashes, 1)
		assert.Equal(t, []chainhash.Hash{*parentTxHash}, value.ParentTxHashes)
		assert.Len(t, value.BlockIDs, 0)
		assert.Nil(t, value.BlockIDs)

		err = db.SetMinedMulti(ctx, []*chainhash.Hash{tx.TxIDChainHash()}, blockID2)
		require.NoError(t, err)

		value, err = db.Get(context.Background(), tx.TxIDChainHash())
		require.NoError(t, err)
		assert.Equal(t, uint64(215), value.Fee)
		assert.Len(t, value.BlockIDs, 1)
		assert.Equal(t, []uint32{blockID2}, value.BlockIDs)
	})

	t.Run("aerospike get", func(t *testing.T) {
		cleanDB(t, client, key, tx)
		txMeta, err := db.Create(context.Background(), tx, 0)
		assert.NotNil(t, txMeta)
		require.NoError(t, err)

		resp, err := db.GetSpend(context.Background(), spend)
		require.NoError(t, err)
		assert.Equal(t, int(utxo.Status_OK), resp.Status)
		assert.Nil(t, resp.SpendingTxID)

		_ = spendTx.Inputs[0].PreviousTxIDAdd(tx.TxIDChainHash())
		_, err = db.Spend(context.Background(), spendTx)
		require.NoError(t, err)

		resp, err = db.GetSpend(context.Background(), spend)
		require.NoError(t, err)
		assert.Equal(t, int(utxo.Status_SPENT), resp.Status)
		assert.Equal(t, spendTx.TxIDChainHash().String(), resp.SpendingTxID.String())
	})

	t.Run("aerospike store", func(t *testing.T) {
		cleanDB(t, client, key, tx)
		txMeta, err := db.Create(context.Background(), tx, 0)
		assert.NotNil(t, txMeta)
		require.NoError(t, err)

		value, err := client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
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
		require.Equal(t, value.Expiration, uint32(math.MaxUint32))

		txMeta, err = db.Create(context.Background(), tx, 0)
		assert.Nil(t, txMeta)
		require.True(t, errors.Is(err, errors.ErrTxExists))

		_ = spendTx.Inputs[0].PreviousTxIDAdd(tx.TxIDChainHash())
		_, err = db.Spend(context.Background(), spendTx)
		require.NoError(t, err)

		txMeta, err = db.Create(context.Background(), tx, 0)
		assert.Nil(t, txMeta)
		require.True(t, errors.Is(err, errors.ErrTxExists))
	})

	t.Run("aerospike spend", func(t *testing.T) {
		cleanDB(t, client, key, tx)
		txMeta, err := db.Create(context.Background(), tx, 0)
		assert.NotNil(t, txMeta)
		require.NoError(t, err)

		_ = spendTx.Inputs[0].PreviousTxIDAdd(tx.TxIDChainHash())
		_, err = db.Spend(context.Background(), spendTx)
		require.NoError(t, err)

		value, err := client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
		require.NoError(t, err)

		utxos, ok := value.Bins["utxos"].([]interface{})
		require.True(t, ok)

		utxoSpendTxID := utxos[spends[0].Vout]
		spendingTxID, ok := utxoSpendTxID.([]byte)
		require.True(t, ok)
		require.Equal(t, spendTx.TxIDChainHash()[:], spendingTxID[32:])

		spendingTxHash, err := chainhash.NewHash(spendingTxID[32:])
		require.NoError(t, err)

		// try to spend with different txid
		_, err = db.Spend(context.Background(), spendTx2)
		assert.Error(t, err)
		assert.Equal(t, spendTx.TxIDChainHash().String(), spendingTxHash.String())
	})

	t.Run("aerospike spend lua", func(t *testing.T) {
		cleanDB(t, client, key, tx)
		txMeta, err := db.Create(context.Background(), tx, 0)
		require.NoError(t, err)
		assert.NotNil(t, txMeta)

		rPolicy := util.GetAerospikeReadPolicy(tSettings)
		rec, err := client.Get(rPolicy, txKey, "outputs", "utxos", "spentUtxos")
		require.NoError(t, err)
		require.NotNil(t, rec)

		_, ok := rec.Bins["utxos"].([]interface{})
		require.True(t, ok)

		wPolicy := util.GetAerospikeWritePolicy(tSettings, 0, aerospike.TTLDontExpire)

		// spend_v1(rec, utxoHash, spendingTxID, currentBlockHeight, currentUnixTime, ttl)
		ret, aErr := client.Execute(wPolicy, txKey, teranode_aerospike.LuaPackage, "spend",
			aerospike.NewIntegerValue(int(spends[0].Vout)),
			aerospike.NewValue(spends[0].UTXOHash[:]),
			aerospike.NewValue(spends[0].SpendingTxID[:]),
			aerospike.NewValue(32), // ttl
		)
		require.NoError(t, aErr)
		assert.Equal(t, teranode_aerospike.LuaOk, teranode_aerospike.LuaReturnValue(ret.(string)))

		_ = spendTx.Inputs[0].PreviousTxIDAdd(tx.TxIDChainHash())
		_, err = db.Spend(context.Background(), spendTx)
		require.NoError(t, err)

		value, err := client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
		require.NoError(t, err)

		utxos, ok := value.Bins["utxos"].([]interface{})
		require.True(t, ok)
		utxoBytes, ok := utxos[spends[0].Vout].([]byte)
		require.True(t, ok)
		require.Equal(t, 64, len(utxoBytes))
		require.Equal(t, spendTx.TxIDChainHash()[:], utxoBytes[32:])

		// try to spend with different txid
		_, err = db.Spend(context.Background(), spendTx2)
		require.Error(t, err)
	})

	t.Run("aerospike spend all lua", func(t *testing.T) {
		cleanDB(t, client, key, tx)
		txMeta, err := db.Create(context.Background(), tx, 0)
		assert.NotNil(t, txMeta)
		require.NoError(t, err)

		wPolicy := util.GetAerospikeWritePolicy(tSettings, 0, aerospike.TTLDontExpire)

		for _, s := range spendsAll {
			ret, aErr := client.Execute(wPolicy, txKey, teranode_aerospike.LuaPackage, "spend",
				aerospike.NewIntegerValue(int(s.Vout)),
				aerospike.NewValue(s.UTXOHash[:]),
				aerospike.NewValue(s.SpendingTxID[:]),
				aerospike.NewValue(32), // ttl
			)
			require.NoError(t, aErr)
			assert.Equal(t, teranode_aerospike.LuaOk, teranode_aerospike.LuaReturnValue(ret.(string)))
		}

		value, err := client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
		require.NoError(t, err)

		utxos, ok := value.Bins["utxos"].([]interface{})
		require.True(t, ok)
		utxoBytes, ok := utxos[spendsAll[0].Vout].([]byte)
		require.True(t, ok)
		require.Equal(t, 64, len(utxoBytes))
		require.Equal(t, spendingTxID2[:], utxoBytes[32:])
	})

	t.Run("aerospike lua errors", func(t *testing.T) {
		cleanDB(t, client, key, tx)
		txMeta, err := db.Create(context.Background(), tx, 0)
		assert.NotNil(t, txMeta)
		require.NoError(t, err)

		wPolicy := util.GetAerospikeWritePolicy(tSettings, 0, aerospike.TTLDontExpire)

		// spend_v1(rec, utxoHash, spendingTxID, currentBlockHeight, currentUnixTime, ttl)
		fakeKey, _ := aerospike.NewKey(aerospikeNamespace, aerospikeSet, []byte{})
		ret, aErr := client.Execute(wPolicy, fakeKey, teranode_aerospike.LuaPackage, "spend",
			aerospike.NewIntegerValue(int(spends[0].Vout)),
			aerospike.NewValue(spends[0].UTXOHash[:]),
			aerospike.NewValue(spends[0].SpendingTxID[:]),
			aerospike.NewValue(32), // ttl
		)
		require.NoError(t, aErr)
		assert.Equal(t, "ERROR:TX not found", ret)
	})

	t.Run("aerospike 1 record spend 1 and not expire no blockIDs", func(t *testing.T) {
		cleanDB(t, client, key, tx)
		txMeta, err := db.Create(context.Background(), tx, 0)
		assert.NotNil(t, txMeta)
		require.NoError(t, err)

		_ = spendTx.Inputs[0].PreviousTxIDAdd(tx.TxIDChainHash())
		_, err = db.Spend(context.Background(), spendTx)
		require.NoError(t, err)

		value, err := client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
		require.NoError(t, err)

		require.Equal(t, uint32(0xffffffff), value.Expiration) // Expiration is -1 because the tx still has UTXOs

		// Now spend all the remaining utxos
		_, err = db.Spend(context.Background(), spendTxRemaining)
		require.NoError(t, err)

		value, err = client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
		require.NoError(t, err)

		require.Equal(t, uint32(0xffffffff), value.Expiration) // Expiration is -1 because the tx has not been in a block yet
	})

	t.Run("aerospike 1 record spend 1 and not expire", func(t *testing.T) {
		cleanDB(t, client, key, tx)
		txMeta, err := db.Create(context.Background(), tx, 0, utxo.WithBlockIDs(1, 2, 3)) // Important that blockIDs are set
		assert.NotNil(t, txMeta)
		require.NoError(t, err)

		_ = spendTx.Inputs[0].PreviousTxIDAdd(tx.TxIDChainHash())
		_, err = db.Spend(context.Background(), spendTx)
		require.NoError(t, err)

		value, err := client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
		require.NoError(t, err)

		require.Equal(t, uint32(0xffffffff), value.Expiration) // Expiration is -1 because the tx still has UTXOs

		// Now spend all the remaining utxos
		_, err = db.Spend(context.Background(), spendTxRemaining)
		require.NoError(t, err)

		value, err = client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
		require.NoError(t, err)

		require.Equal(t, aerospikeExpiration, value.Expiration) // Now TTL should be set to aerospikeExpiration as all UTXOs are spent
	})

	t.Run("aerospike spend all and expire", func(t *testing.T) {
		cleanDB(t, client, key, tx)
		txMeta, err := db.Create(context.Background(), tx, 0)
		assert.NotNil(t, txMeta)
		require.NoError(t, err)

		_, err = db.Spend(context.Background(), spendTxAll)
		require.NoError(t, err)

		value, err := client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
		require.NoError(t, err)

		utxos, ok := value.Bins["utxos"].([]interface{})
		require.True(t, ok)

		for i := 0; i < 5; i++ {
			utxoBytes, ok := utxos[spendsAll[i].Vout].([]byte)
			require.True(t, ok)
			require.Equal(t, 64, len(utxoBytes))
			require.Equal(t, spendTxAll.TxIDChainHash()[:], utxoBytes[32:])
		}

		require.Equal(t, uint32(0xffffffff), value.Expiration) // Expiration is -1 because the tx has not yet been mined

		// Now call SetMinedMulti
		err = db.SetMinedMulti(context.Background(), []*chainhash.Hash{tx.TxIDChainHash()}, 1)
		require.NoError(t, err)

		value, err = client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
		require.NoError(t, err)

		require.Equal(t, aerospikeExpiration, value.Expiration) // Now TTL should be set to aerospikeExpiration

		// try to spend with different txid
		spends, err := db.Spend(context.Background(), spendTx3)
		require.Error(t, err)

		var tErr *errors.Error
		require.ErrorAs(t, err, &tErr)
		require.Equal(t, errors.ERR_TX_INVALID, tErr.Code())
		require.ErrorIs(t, spends[0].Err, errors.ErrSpent)
		require.Equal(t, spendTxAll.TxIDChainHash().String(), spends[0].ConflictingTxID.String())

		// an error was observed were the utxos map got nilled out when trying to double spend
		// interestingly, this only happened in normal mode, not in batched mode :-S
		// Here we get all the data again and try the double spend again
		utxos, ok = value.Bins["utxos"].([]interface{})
		require.True(t, ok)

		for i := 0; i < 5; i++ {
			utxoBytes, ok := utxos[spendsAll[i].Vout].([]byte)
			require.True(t, ok)
			require.Equal(t, 64, len(utxoBytes))
			require.Equal(t, spendTxAll.TxIDChainHash()[:], utxoBytes[32:])
		}

		// try to spend with different txid
		_, err = db.Spend(context.Background(), spendTx3)
		require.Error(t, err)
		// require.ErrorIs(t, err, utxo.ErrTypeSpent)

		value, err = client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
		require.NoError(t, err)
		assert.Equal(t, tx.Version, uint32(value.Bins["version"].(int))) // nolint:gosec
	})

	t.Run("aerospike reset", func(t *testing.T) {
		tx2 := tx.Clone()
		key, _ := aerospike.NewKey(aerospikeNamespace, aerospikeSet, tx2.TxIDChainHash().CloneBytes())
		cleanDB(t, client, key, tx2)

		txMeta, err := db.Create(context.Background(), tx2, 0)
		assert.NotNil(t, txMeta)
		require.NoError(t, err)

		_ = db.SetBlockHeight(101)

		utxoHash0, _ := util.UTXOHashFromOutput(tx2.TxIDChainHash(), tx2.Outputs[0], 0)
		spends := []*utxo.Spend{{
			TxID:         tx2.TxIDChainHash(),
			Vout:         0,
			UTXOHash:     utxoHash0,
			SpendingTxID: spendingTxID1,
		}}

		_ = spendTx.Inputs[0].PreviousTxIDAdd(tx.TxIDChainHash())
		_, err = db.Spend(context.Background(), spendTx)
		require.NoError(t, err)

		value, err := client.Get(util.GetAerospikeReadPolicy(tSettings), key)
		require.NoError(t, err)

		utxos, ok := value.Bins["utxos"].([]interface{})
		require.True(t, ok)
		utxoBytes, ok := utxos[spends[0].Vout].([]byte)
		require.True(t, ok)
		require.Equal(t, 64, len(utxoBytes))
		require.Equal(t, spendTx.TxIDChainHash()[:], utxoBytes[32:])

		// utxoSpendTxID := utxos[spends[0].Vout]
		// require.Equal(t, spendingTxID1.String(), utxoSpendTxID)

		// try to reset the utxo
		err = db.UnSpend(context.Background(), []*utxo.Spend{{
			TxID:         tx2.TxIDChainHash(),
			Vout:         0,
			UTXOHash:     utxoHash0,
			SpendingTxID: spendingTxID2,
		}})
		require.NoError(t, err)

		value, err = client.Get(util.GetAerospikeReadPolicy(tSettings), key)
		require.NoError(t, err)

		utxos, ok = value.Bins["utxos"].([]interface{})
		require.True(t, ok)
		require.Len(t, utxos, 5)
		require.Len(t, utxos[0].([]byte), 32)
	})

	t.Run("LUAScripts", func(t *testing.T) {
		cleanDB(t, client, key, tx)

		txMeta, err := db.Create(context.Background(), tx, 0)
		require.NoError(t, err)
		assert.NotNil(t, txMeta)

		resp, err := client.Get(nil, txKey, "utxos")
		require.NoError(t, err)

		utxos, ok := resp.Bins["utxos"].([]interface{})
		require.True(t, ok)
		assert.Len(t, utxos, 5)

		utxo, ok := utxos[0].([]byte)
		require.True(t, ok)
		require.Len(t, utxo, 32)
		assert.Equal(t, utxo, utxoHash0[:])

		_ = spendTx.Inputs[0].PreviousTxIDAdd(tx.TxIDChainHash())
		_, err = db.Spend(context.Background(), spendTx)
		require.NoError(t, err)

		resp, err = client.Get(nil, txKey, "utxos", "spentUtxos")
		require.NoError(t, err)

		utxos, ok = resp.Bins["utxos"].([]interface{})
		require.True(t, ok)
		require.Len(t, utxos, 5)
		require.Len(t, utxos[0].([]byte), 64)

		spendUtxos, ok := resp.Bins["spentUtxos"].(int)
		require.True(t, ok)
		assert.Equal(t, 1, spendUtxos)

		err = db.UnSpend(context.Background(), spends)
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
		cleanDB(t, client, key, tx)

		var blockHeight uint32

		txMeta, err := db.Create(context.Background(), tx, blockHeight, utxo.WithBlockIDs(1, 2, 3))
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
		cleanDB(t, client, key, tx)

		txMeta, err := db.Create(context.Background(), txWithOPReturn, 0)
		require.NoError(t, err)
		assert.NotNil(t, txMeta)

		resp, err := client.Get(nil, opReturnKey, "utxos")
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

	t.Run("FrozenTX", func(t *testing.T) {
		cleanDB(t, client, key, tx)

		txMeta, err := db.Create(context.Background(), tx, 0)
		require.NoError(t, err)
		assert.NotNil(t, txMeta)

		frozenBin := aerospike.NewBin("frozen", aerospike.NewIntegerValue(1))

		// Write the bin to the record
		err = client.PutBins(nil, txKey, frozenBin)
		require.NoError(t, err)

		_ = spendTx.Inputs[0].PreviousTxIDAdd(tx.TxIDChainHash())
		spends, err := db.Spend(context.Background(), spendTx)

		var tErr *errors.Error
		require.ErrorAs(t, err, &tErr)
		require.Equal(t, errors.ERR_TX_INVALID, tErr.Code())
		require.ErrorIs(t, spends[0].Err, errors.ErrFrozen)
	})

	t.Run("FrozenUTXO", func(t *testing.T) {
		cleanDB(t, client, key, tx)

		txMeta, err := db.Create(context.Background(), tx, 0)
		require.NoError(t, err)
		assert.NotNil(t, txMeta)

		frozenMarker, err := chainhash.NewHashFromStr("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF")
		require.NoError(t, err)

		spendTxFrozen := utxo2.GetSpendingTx(tx, 0)
		spendTxFrozen.SetTxHash(frozenMarker) // hard code the hash to be the frozen marker

		_, err = db.Spend(context.Background(), spendTxFrozen)
		require.NoError(t, err)

		_ = spendTx.Inputs[0].PreviousTxIDAdd(tx.TxIDChainHash())
		spends, err := db.Spend(context.Background(), spendTx)

		var tErr *errors.Error
		require.ErrorAs(t, err, &tErr)
		require.Equal(t, errors.ERR_TX_INVALID, tErr.Code())
		require.ErrorIs(t, spends[0].Err, errors.ErrFrozen)
	})
}

func TestCoinbase(t *testing.T) {
	client, db, _, deferFn := initAerospike(t)
	defer deferFn()

	key, aErr := aerospike.NewKey(db.GetNamespace(), db.GetName(), coinbaseTx.TxIDChainHash().CloneBytes())
	require.NoError(t, aErr)
	assert.NotNil(t, key)

	_, aErr = client.Delete(nil, key)
	require.NoError(t, aErr)

	txMeta, err := db.Create(context.Background(), coinbaseTx, 0)
	require.NoError(t, err)
	assert.NotNil(t, txMeta)
	assert.True(t, txMeta.IsCoinbase)

	var tErr *errors.Error

	spends, err := db.Spend(context.Background(), spendCoinbaseTx)
	require.ErrorAs(t, err, &tErr)
	require.Equal(t, errors.ERR_TX_INVALID, tErr.Code())
	require.ErrorIs(t, spends[0].Err, errors.ErrTxCoinbaseImmature)

	err = db.SetBlockHeight(5000)
	require.NoError(t, err)

	_, err = db.Spend(context.Background(), spendCoinbaseTx)
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
//	txMeta, err := db.Create(context.Background(), bigTx, 0)
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
//	txMeta, err = db.Get(context.Background(), bigTx.TxIDChainHash())
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
//	txMeta, err := db.Create(context.Background(), bigTx, 0)
//	require.NoError(t, err)
//	assert.NotNil(t, txMeta)
//
//	resp, err := client.Get(nil, key0, "utxos", "nrRecords")
//	require.NoError(t, err)
//	utxos, ok := resp.Bins["utxos"].([]interface{})
//	require.True(t, ok)
//	assert.Len(t, utxos, 2)
//	nrRecords, ok := resp.Bins["nrRecords"].(int)
//	require.True(t, ok)
//	assert.Equal(t, 3, nrRecords)
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
//	err = db.Spend(context.Background(), []*utxo.Spend{spend0}, 0)
//	require.NoError(t, err)
//
//	resp, err = client.Get(nil, key0, "utxos", "nrRecords")
//	require.NoError(t, err)
//	utxos, ok = resp.Bins["utxos"].([]interface{})
//	require.True(t, ok)
//	assert.Len(t, utxos, 2)
//	utxo0 := utxos[0].([]byte)
//	assert.Len(t, utxo0, 64)
//	nrRecords, ok = resp.Bins["nrRecords"].(int)
//	require.True(t, ok)
//	assert.Equal(t, 3, nrRecords)
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
//	err = db.Spend(context.Background(), []*utxo.Spend{spend4}, 0)
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
//	resp, err = client.Get(nil, key0, "nrRecords")
//	require.NoError(t, err)
//	nrRecords, ok = resp.Bins["nrRecords"].(int)
//	require.True(t, ok)
//	assert.Equal(t, 2, nrRecords)
// }

func TestIncrementNrRecords(t *testing.T) {
	client, db, _, deferFn := initAerospike(t)
	defer deferFn()

	key, aErr := aerospike.NewKey(db.GetNamespace(), db.GetName(), tx.TxIDChainHash().CloneBytes())
	require.NoError(t, aErr)
	assert.NotNil(t, key)

	_, aErr = client.Delete(nil, key)
	require.NoError(t, aErr)

	txMeta, err := db.Create(context.Background(), tx, 0)
	require.NoError(t, err)
	assert.NotNil(t, txMeta)

	resp, err := client.Get(nil, key, "nrRecords")
	require.NoError(t, err)

	nrRecords, ok := resp.Bins["nrRecords"].(int)
	require.True(t, ok)
	assert.Equal(t, 1, nrRecords)

	_, err = db.IncrementNrRecords(tx.TxIDChainHash(), 1)
	require.NoError(t, err)

	resp, err = client.Get(nil, key, "nrRecords")
	require.NoError(t, err)

	nrRecords, ok = resp.Bins["nrRecords"].(int)
	require.True(t, ok)
	assert.Equal(t, 2, nrRecords)

	_, err = db.IncrementNrRecords(tx.TxIDChainHash(), -1)
	require.NoError(t, err)

	resp, err = client.Get(nil, key, "nrRecords")
	require.NoError(t, err)

	nrRecords, ok = resp.Bins["nrRecords"].(int)
	require.True(t, ok)
	assert.Equal(t, 1, nrRecords)
}

func TestStoreDecorate(t *testing.T) {
	client, db, _, deferFn := initAerospike(t)
	defer deferFn()

	key, aErr := aerospike.NewKey(db.GetNamespace(), db.GetName(), spendingTxID1[:])
	require.NoError(t, aErr)

	t.Run("aerospike BatchDecorate", func(t *testing.T) {
		cleanDB(t, client, key, tx)
		txMeta, err := db.Create(context.Background(), tx, 0)

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
		err = db.BatchDecorate(context.Background(), items)
		require.NoError(t, err)

		// check field values
		for _, item := range items {
			assert.Equal(t, uint64(215), item.Data.Fee)
			assert.Equal(t, uint64(328), item.Data.SizeInBytes)
		}
	})

	t.Run("aerospike PreviousOutputsDecorate", func(t *testing.T) {
		cleanDB(t, client, key, tx)
		txMeta, err := db.Create(context.Background(), tx, 0)

		txID := tx.TxIDChainHash().String()
		_ = txID

		assert.NotNil(t, txMeta)
		require.NoError(t, err)

		items := []*meta.PreviousOutput{
			{
				PreviousTxID: *tx.TxIDChainHash(),
				Vout:         0,
				Idx:          0,
			},
			{
				PreviousTxID: *tx.TxIDChainHash(),
				Vout:         4,
				Idx:          1,
			},
			{
				PreviousTxID: *tx.TxIDChainHash(),
				Vout:         3,
				Idx:          2,
			},
			{
				PreviousTxID: *tx.TxIDChainHash(),
				Vout:         2,
				Idx:          3,
			},
			{
				PreviousTxID: *tx.TxIDChainHash(),
				Vout:         1,
				Idx:          4,
			},
		}
		err = db.PreviousOutputsDecorate(context.Background(), items)
		require.NoError(t, err)

		// check field values for vout 0 - item 0
		assert.Len(t, items[0].LockingScript, 25)
		assert.Equal(t, uint64(5_000_000), items[0].Satoshis)

		// check field values for vout 4 - item 1
		assert.Len(t, items[1].LockingScript, 25)
		assert.Equal(t, uint64(2_817_689), items[1].Satoshis)

		// check field values for vout 3 - item 2
		assert.Len(t, items[2].LockingScript, 25)
		assert.Equal(t, uint64(20_000), items[2].Satoshis)

		// check field values for vout 2 - item 3
		assert.Len(t, items[3].LockingScript, 25)
		assert.Equal(t, uint64(20_000), items[3].Satoshis)

		// check field values for vout 1 - item 4
		assert.Len(t, items[4].LockingScript, 25)
		assert.Equal(t, uint64(2_000_000), items[4].Satoshis)
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
//	parentMeta, err := db.Create(context.Background(), parentTx, 0)
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
//	childMeta, err := db.Create(context.Background(), childTx, 0)
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
//	err = db.PreviousOutputsDecorate(context.Background(), []*meta.PreviousOutput{previousOutput})
//	require.NoError(t, err)
//	assert.NotNil(t, previousOutput.LockingScript)
//	// t.Log(previousOutput)
// }

func TestSmokeTests(t *testing.T) {
	_, db, ctx, deferFn := initAerospike(t)
	defer deferFn()

	t.Run("aerospike store", func(t *testing.T) {
		err := db.Delete(ctx, tests.TXHash)
		require.NoError(t, err)

		tests.Store(t, db)
	})

	t.Run("aerospike spend", func(t *testing.T) {
		err := db.Delete(ctx, tests.TXHash)
		require.NoError(t, err)

		tests.Spend(t, db)
	})

	t.Run("aerospike reset", func(t *testing.T) {
		err := db.Delete(ctx, tests.TXHash)
		require.NoError(t, err)

		tests.Restore(t, db)
	})

	t.Run("aerospike freeze", func(t *testing.T) {
		err := db.Delete(ctx, tests.TXHash)
		require.NoError(t, err)

		tests.Freeze(t, db)
	})

	t.Run("aerospike reassign", func(t *testing.T) {
		err := db.Delete(ctx, tests.TXHash)
		require.NoError(t, err)

		tests.ReAssign(t, db)
	})

	t.Run("aerospike conflicting", func(t *testing.T) {
		err := db.Delete(ctx, tests.TXHash)
		require.NoError(t, err)

		tests.Conflicting(t, db)
	})
}

func TestCreateZeroSat(t *testing.T) {
	client, s, ctx, deferFn := initAerospike(t)
	defer deferFn()

	// Read hex file from os
	txHex, err := os.ReadFile("testdata/a3041ffd31b46f9bb6e4cd78c89287b405fb911e893a5dead18372400832abc6.hex")
	require.NoError(t, err)

	tx, err := bt.NewTxFromString(string(txHex))
	require.NoError(t, err)

	// Create the tx
	_, err = s.Create(ctx, tx, 0)
	require.NoError(t, err)

	key, err := aerospike.NewKey(s.GetNamespace(), s.GetName(), tx.TxIDChainHash().CloneBytes())
	require.NoError(t, err)

	// Check the tx was created with 2 utxos and 0 spent utxos and no expiration
	response, err := client.Get(nil, key)
	require.NoError(t, err)

	assert.NotNil(t, response)
	assert.Equal(t, 2, response.Bins["nrUtxos"])
	assert.Equal(t, 0, response.Bins["spentUtxos"])
	assert.Equal(t, uint32(aerospike.TTLDontExpire), response.Expiration)

	spendingTx1 := utxo2.GetSpendingTx(tx, 1)

	_, err = s.Spend(ctx, spendingTx1)
	require.NoError(t, err)

	// Check the tx was updated to 1 spent utxo...
	response, err = client.Get(nil, key)
	require.NoError(t, err)

	assert.NotNil(t, response)
	assert.Equal(t, 2, response.Bins["nrUtxos"])
	assert.Equal(t, 1, response.Bins["spentUtxos"])
	assert.Equal(t, uint32(aerospike.TTLDontExpire), response.Expiration)

	// No set mined to see the TTL is set
	err = s.SetMinedMulti(ctx, []*chainhash.Hash{tx.TxIDChainHash()}, 1)
	require.NoError(t, err)

	response, err = client.Get(nil, key)
	require.NoError(t, err)

	assert.NotNil(t, response)
	assert.Equal(t, 2, response.Bins["nrUtxos"])
	assert.Equal(t, 1, response.Bins["spentUtxos"])
	assert.Equal(t, uint32(aerospike.TTLDontExpire), response.Expiration)

	// Spend the output 0
	spendingTx0 := utxo2.GetSpendingTx(tx, 0)
	_, err = s.Spend(ctx, spendingTx0)
	require.NoError(t, err)

	// Check the tx was updated to 2 spent utxos...
	response, err = client.Get(nil, key)
	require.NoError(t, err)

	assert.NotNil(t, response)
	assert.Equal(t, 2, response.Bins["nrUtxos"])
	assert.Equal(t, 2, response.Bins["spentUtxos"])
	assert.Equal(t, aerospikeExpiration, response.Expiration)

	time.Sleep(2 * time.Duration(aerospikeExpiration) * time.Second)

	response, err = client.Get(nil, key)
	require.Error(t, err)
	require.ErrorIs(t, err, aerospike.ErrKeyNotFound)

	if testing.Verbose() && response != nil {
		// Normally response is nil at this point so the test will
		// not be overly chatty.
		printResponse(response)
	}
}
