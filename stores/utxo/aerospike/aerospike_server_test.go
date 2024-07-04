//go:build aerospike

package aerospike

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"

	aero "github.com/aerospike/aerospike-client-go/v7"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	aerospikeHost       = "localhost" // "localhost"
	aerospikePort       = 3000        // 3800
	aerospikeNamespace  = "test"      // test
	aerospikeSet        = "utxo-test" // utxo-test
	aerospikeExpiration = uint32(30)
)

var (
	coinbaseKey *aero.Key
	tx, _       = bt.NewTxFromString("010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")
	hash, _     = chainhash.NewHashFromStr("5e3bc5947f48cec766090aa17f309fd16259de029dcef5d306b514848c9687c7")
	hash2, _    = chainhash.NewHashFromStr("663bc5947f48cec766090aa17f309fd16259de029dcef5d306b514848c9687c8")

	utxoHash0, _ = util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[0], 0)
	utxoHash1, _ = util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[1], 1)
	utxoHash2, _ = util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[2], 2)
	utxoHash3, _ = util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[3], 3)
	utxoHash4, _ = util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[4], 4)

	spend = &utxo.Spend{
		TxID:         tx.TxIDChainHash(),
		Vout:         0,
		UTXOHash:     utxoHash0,
		SpendingTxID: hash,
	}
	spends  = []*utxo.Spend{spend}
	spends2 = []*utxo.Spend{{
		TxID:         tx.TxIDChainHash(),
		Vout:         0,
		UTXOHash:     utxoHash0,
		SpendingTxID: hash2,
	}}
	spends3 = []*utxo.Spend{{
		TxID:         tx.TxIDChainHash(),
		Vout:         0,
		UTXOHash:     utxoHash0,
		SpendingTxID: utxoHash3,
	}}
	spendsAll = []*utxo.Spend{{
		TxID:         tx.TxIDChainHash(),
		Vout:         0,
		UTXOHash:     utxoHash0,
		SpendingTxID: hash2,
	}, {
		TxID:         tx.TxIDChainHash(),
		Vout:         1,
		UTXOHash:     utxoHash1,
		SpendingTxID: hash2,
	}, {
		TxID:         tx.TxIDChainHash(),
		Vout:         2,
		UTXOHash:     utxoHash2,
		SpendingTxID: hash2,
	}, {
		TxID:         tx.TxIDChainHash(),
		Vout:         3,
		UTXOHash:     utxoHash3,
		SpendingTxID: hash2,
	}, {
		TxID:         tx.TxIDChainHash(),
		Vout:         4,
		UTXOHash:     utxoHash4,
		SpendingTxID: hash2,
	}}
)

func TestAerospike(t *testing.T) {
	gocore.Config().Set("utxostore_batchingEnabled", "false")
	internalTest(t)
}

func TestAerospikeBatching(t *testing.T) {
	gocore.Config().Set("utxostore_batchingEnabled", "true")
	internalTest(t)
}

func internalTest(t *testing.T) {
	// raw client to be able to do gets and cleanup
	client, aeroErr := aero.NewClient(aerospikeHost, aerospikePort)
	require.NoError(t, aeroErr)

	// TODO use the container in tests
	// client := setupAerospike(t)

	aeroURL, err := url.Parse(fmt.Sprintf("aerospike://%s:%d/%s?set=%s&expiration=%d", aerospikeHost, aerospikePort, aerospikeNamespace, aerospikeSet, aerospikeExpiration))
	require.NoError(t, err)

	// ubsv db client
	var db *Store
	db, err = New(ulogger.TestLogger{}, aeroURL)
	require.NoError(t, err)

	parentTxHash := tx.Inputs[0].PreviousTxIDChainHash()

	coinbaseTx, err := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff19031404002f6d332d617369612fdf5128e62eda1a07e94dbdbdffffffff0500ca9a3b000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac00ca9a3b000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac00ca9a3b000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac00ca9a3b000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac00ca9a3b000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac00000000")
	require.NoError(t, err)

	coinbaseKey, err = aero.NewKey(db.namespace, db.setName, coinbaseTx.TxIDChainHash()[:])
	require.NoError(t, err)

	blockID := uint32(123)
	blockID2 := uint32(124)

	var key *aero.Key
	key, err = aero.NewKey(db.namespace, db.setName, hash[:])
	require.NoError(t, err)

	txKey, err := aero.NewKey(db.namespace, db.setName, tx.TxIDChainHash().CloneBytes())
	require.NoError(t, err)

	t.Cleanup(func() {
		policy := util.GetAerospikeWritePolicy(0, 0)
		_, err = client.Delete(policy, key)
		require.NoError(t, err)
	})
	blockHeight := uint32(0)

	t.Run("aerospike store", func(t *testing.T) {
		cleanDB(t, client, key, tx)

		_, err = db.Create(context.Background(), tx)
		require.NoError(t, err)

		var value *aero.Record
		// raw aerospike get
		value, err = client.Get(util.GetAerospikeReadPolicy(), txKey)
		require.NoError(t, err)
		require.Equal(t, uint32(1), value.Generation)
		assert.Equal(t, uint64(215), uint64(value.Bins["fee"].(int)))
		assert.Equal(t, uint64(328), uint64(value.Bins["sizeInBytes"].(int)))
		assert.Len(t, value.Bins["inputs"], 1)
		binParentTxHash := chainhash.Hash(value.Bins["inputs"].([]interface{})[0].([]byte)[0:32])
		assert.Equal(t, parentTxHash[:], binParentTxHash.CloneBytes())
		assert.Equal(t, []interface{}{}, value.Bins["blockIDs"])

		_, err = db.Create(context.Background(), tx)
		require.Error(t, err)
		assert.True(t, errors.Is(err, errors.ErrTxAlreadyExists))

		err = db.SetMined(context.Background(), tx.TxIDChainHash(), blockID)
		require.NoError(t, err)

		value, err = client.Get(util.GetAerospikeReadPolicy(), txKey)
		require.NoError(t, err)
		require.Equal(t, uint32(2), value.Generation)
		assert.Len(t, value.Bins["blockIDs"].([]interface{}), 1)
		assert.Equal(t, []interface{}{int(blockID)}, value.Bins["blockIDs"].([]interface{}))

		err = db.SetMined(context.Background(), tx.TxIDChainHash(), blockID2)
		require.NoError(t, err)

		value, err = client.Get(util.GetAerospikeReadPolicy(), txKey)
		require.NoError(t, err)
		require.Equal(t, uint32(3), value.Generation)
		assert.Len(t, value.Bins["blockIDs"].([]interface{}), 2)
		assert.Equal(t, []interface{}{int(blockID), int(blockID2)}, value.Bins["blockIDs"].([]interface{}))
		assert.False(t, value.Bins["isCoinbase"].(bool))
	})

	t.Run("aerospike store coinbase", func(t *testing.T) {
		cleanDB(t, client, key)
		_, err = db.Create(context.Background(), coinbaseTx)
		require.NoError(t, err)

		var value *aero.Record
		// raw aerospike get
		value, err = client.Get(util.GetAerospikeReadPolicy(), coinbaseKey)
		require.NoError(t, err)
		assert.True(t, value.Bins["isCoinbase"].(bool))

		var txMeta *meta.Data
		txMeta, err = db.Get(context.Background(), coinbaseTx.TxIDChainHash())
		require.NoError(t, err)
		assert.True(t, txMeta.IsCoinbase)
		assert.Equal(t, txMeta.Tx.ExtendedBytes(), coinbaseTx.ExtendedBytes())
	})

	t.Run("aerospike get", func(t *testing.T) {
		cleanDB(t, client, key, tx)

		_, err = db.Create(context.Background(), tx)
		require.NoError(t, err)

		var value *meta.Data
		value, err = db.Get(context.Background(), tx.TxIDChainHash())
		require.NoError(t, err)
		assert.Equal(t, uint64(215), value.Fee)
		assert.Equal(t, uint64(328), value.SizeInBytes)
		assert.Len(t, value.ParentTxHashes, 1)
		assert.Equal(t, []chainhash.Hash{*parentTxHash}, value.ParentTxHashes)
		assert.Len(t, value.BlockIDs, 0)
		assert.Nil(t, value.BlockIDs)

		err = db.SetMined(context.Background(), tx.TxIDChainHash(), blockID2)
		require.NoError(t, err)

		value, err = db.Get(context.Background(), tx.TxIDChainHash())
		require.NoError(t, err)
		assert.Equal(t, uint64(215), value.Fee)
		assert.Len(t, value.BlockIDs, 1)
		assert.Equal(t, []uint32{blockID2}, value.BlockIDs)
	})

	t.Run("aerospike get", func(t *testing.T) {
		cleanDB(t, client, key, tx)
		txMeta, err := db.Create(context.Background(), tx)
		assert.NotNil(t, txMeta)
		require.NoError(t, err)

		resp, err := db.GetSpend(context.Background(), spend)
		require.NoError(t, err)
		assert.Equal(t, int(utxo.Status_OK), resp.Status)
		assert.Equal(t, uint32(0), resp.LockTime)
		assert.Nil(t, resp.SpendingTxID)

		err = db.Spend(context.Background(), spends, blockHeight)
		require.NoError(t, err)

		resp, err = db.GetSpend(context.Background(), spend)
		require.NoError(t, err)
		assert.Equal(t, int(utxo.Status_SPENT), resp.Status)
		assert.Equal(t, uint32(0), resp.LockTime)
		assert.Equal(t, hash, resp.SpendingTxID)
	})

	t.Run("aerospike get with locktime", func(t *testing.T) {
		tx2 := tx.Clone()
		tx2.LockTime = 123
		cleanDB(t, client, key, tx2)

		txMeta, err := db.Create(context.Background(), tx2)
		assert.NotNil(t, txMeta)
		require.NoError(t, err)

		spend := &utxo.Spend{
			TxID:     tx2.TxIDChainHash(),
			Vout:     0,
			UTXOHash: hash,
		}
		resp, err := db.GetSpend(context.Background(), spend)
		require.NoError(t, err)
		assert.Equal(t, int(utxo.Status_LOCKED), resp.Status)
		assert.Equal(t, uint32(123), resp.LockTime)
	})

	t.Run("aerospike store", func(t *testing.T) {
		cleanDB(t, client, key, tx)
		txMeta, err := db.Create(context.Background(), tx)
		assert.NotNil(t, txMeta)
		require.NoError(t, err)

		value, err := client.Get(util.GetAerospikeReadPolicy(), txKey)
		require.NoError(t, err)
		//assert.Equal(t, value.Bins["tx"], tx.ExtendedBytes())
		assert.Len(t, value.Bins["inputs"], 1)
		assert.Equal(t, value.Bins["inputs"].([]interface{})[0], tx.Inputs[0].ExtendedBytes(false))
		assert.Len(t, value.Bins["outputs"], 5)
		assert.Equal(t, value.Bins["fee"], 215)
		assert.Equal(t, value.Bins["sizeInBytes"], 328)
		assert.Equal(t, uint32(value.Bins["version"].(int)), tx.Version)
		assert.Equal(t, value.Bins["locktime"], 0)
		utxos, ok := value.Bins["utxos"].(map[interface{}]interface{})
		require.True(t, ok)
		for i := 0; i < len(tx.Outputs); i++ {
			utxoHash, _ := util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[i], uint32(i))
			utxo, ok := utxos[utxoHash.String()]
			require.True(t, ok)
			assert.Equal(t, "", utxo)
		}
		parentTxHashes, ok := value.Bins["inputs"].([]interface{})
		require.True(t, ok)
		assert.Len(t, parentTxHashes, 1)
		assert.Equal(t, parentTxHashes[0].([]byte)[:32], tx.Inputs[0].PreviousTxIDChainHash().CloneBytes())
		blockIDs, ok := value.Bins["blockIDs"].([]interface{})
		require.True(t, ok)
		assert.Len(t, blockIDs, 0)
		require.Equal(t, value.Expiration, uint32(math.MaxUint32))

		txMeta, err = db.Create(context.Background(), tx)
		assert.Nil(t, txMeta)
		require.True(t, errors.Is(err, errors.ErrTxAlreadyExists))

		err = db.Spend(context.Background(), spends, blockHeight)
		require.NoError(t, err)

		txMeta, err = db.Create(context.Background(), tx)
		assert.Nil(t, txMeta)
		require.True(t, errors.Is(err, errors.ErrTxAlreadyExists))

	})

	t.Run("aerospike spend", func(t *testing.T) {
		cleanDB(t, client, key, tx)
		txMeta, err := db.Create(context.Background(), tx)
		assert.NotNil(t, txMeta)
		require.NoError(t, err)

		err = db.Spend(context.Background(), spends, blockHeight)
		require.NoError(t, err)

		value, err := client.Get(util.GetAerospikeReadPolicy(), txKey)
		require.NoError(t, err)
		utxoMap, ok := value.Bins["utxos"].(map[interface{}]interface{})
		require.True(t, ok)
		utxoSpendTxID, ok := utxoMap[spends[0].UTXOHash.String()]
		require.True(t, ok)
		require.Equal(t, hash.String(), utxoSpendTxID)

		// try to spend with different txid
		err = db.Spend(context.Background(), spends2, blockHeight)
		require.ErrorIs(t, err, errors.NewUtxoSpentErr(*tx.TxIDChainHash(), *utxo.ErrTypeSpent.SpendingTxID, time.Now(), err))

		// get the doc to check expiry etc.
		value, err = client.Get(util.GetAerospikeReadPolicy(), txKey)
		require.NoError(t, err)
		_, ok = value.Bins["lastSpend"].(int)
		require.False(t, ok)
		require.Equal(t, value.Expiration, uint32(math.MaxUint32))
	})

	t.Run("aerospike spend lua", func(t *testing.T) {
		cleanDB(t, client, key, tx)
		txMeta, err := db.Create(context.Background(), tx)
		assert.NotNil(t, txMeta)
		require.NoError(t, err)

		wPolicy := util.GetAerospikeWritePolicy(0, math.MaxUint32)

		// spend_v1(rec, utxoHash, spendingTxID, currentBlockHeight, currentUnixTime, ttl)
		ret, aErr := client.Execute(wPolicy, txKey, luaSpendFunction, "spend",
			aero.NewValue(spends[0].UTXOHash.String()),
			aero.NewValue(spends[0].SpendingTxID.String()),
			aero.NewValue(100),
			aero.NewValue(time.Now().Unix()),
			aero.NewValue(32), // ttl
		)
		require.NoError(t, aErr)
		assert.Equal(t, "OK", ret)

		err = db.Spend(context.Background(), spends, blockHeight)
		require.NoError(t, err)

		value, err := client.Get(util.GetAerospikeReadPolicy(), txKey)
		require.NoError(t, err)
		utxoMap, ok := value.Bins["utxos"].(map[interface{}]interface{})
		require.True(t, ok)
		utxoSpendTxID, ok := utxoMap[spends[0].UTXOHash.String()]
		require.True(t, ok)
		require.Equal(t, hash.String(), utxoSpendTxID)

		// try to spend with different txid
		err = db.Spend(context.Background(), spends2, blockHeight)
		require.ErrorIs(t, err, utxo.ErrTypeSpent)

		// get the doc to check expiry etc.
		value, err = client.Get(util.GetAerospikeReadPolicy(), txKey)
		require.NoError(t, err)
		_, ok = value.Bins["lastSpend"].(int)
		require.False(t, ok)
		require.Equal(t, value.Expiration, uint32(math.MaxUint32))
	})

	t.Run("aerospike spend all lua", func(t *testing.T) {
		cleanDB(t, client, key, tx)
		txMeta, err := db.Create(context.Background(), tx)
		assert.NotNil(t, txMeta)
		require.NoError(t, err)

		wPolicy := util.GetAerospikeWritePolicy(0, math.MaxUint32)

		for _, s := range spendsAll {
			ret, aErr := client.Execute(wPolicy, txKey, luaSpendFunction, "spend",
				aero.NewValue(s.UTXOHash.String()),
				aero.NewValue(s.SpendingTxID.String()),
				aero.NewValue(100),
				aero.NewValue(time.Now().Unix()),
				aero.NewValue(32), // ttl
			)
			require.NoError(t, aErr)
			assert.Equal(t, "OK", ret)
		}

		value, err := client.Get(util.GetAerospikeReadPolicy(), txKey)
		require.NoError(t, err)
		utxoMap, ok := value.Bins["utxos"].(map[interface{}]interface{})
		require.True(t, ok)
		utxoSpendTxID, ok := utxoMap[spendsAll[0].UTXOHash.String()]
		require.True(t, ok)
		require.Equal(t, hash2.String(), utxoSpendTxID)
	})

	t.Run("aerospike lua errors", func(t *testing.T) {
		cleanDB(t, client, key, tx)
		txMeta, err := db.Create(context.Background(), tx)
		assert.NotNil(t, txMeta)
		require.NoError(t, err)

		wPolicy := util.GetAerospikeWritePolicy(0, math.MaxUint32)

		// spend_v1(rec, utxoHash, spendingTxID, currentBlockHeight, currentUnixTime, ttl)
		fakeKey, _ := aero.NewKey(aerospikeNamespace, aerospikeSet, []byte{})
		ret, aErr := client.Execute(wPolicy, fakeKey, luaSpendFunction, "spend",
			aero.NewValue(spends[0].UTXOHash.String()),
			aero.NewValue(spends[0].SpendingTxID.String()),
			aero.NewValue(100),
			aero.NewValue(time.Now().Unix()),
			aero.NewValue(32), // ttl
		)
		require.NoError(t, aErr)
		assert.Equal(t, "ERROR:TX not found", ret)
	})

	t.Run("aerospike spend all and expire", func(t *testing.T) {
		cleanDB(t, client, key, tx)
		txMeta, err := db.Create(context.Background(), tx)
		assert.NotNil(t, txMeta)
		require.NoError(t, err)

		err = db.Spend(context.Background(), spendsAll, blockHeight)
		require.NoError(t, err)

		value, err := client.Get(util.GetAerospikeReadPolicy(), txKey)
		require.NoError(t, err)
		utxoMap, ok := value.Bins["utxos"].(map[interface{}]interface{})
		require.True(t, ok)
		for i := 0; i < 5; i++ {
			utxoSpendTxID, ok := utxoMap[spendsAll[i].UTXOHash.String()]
			require.True(t, ok)
			require.Equal(t, hash2.String(), utxoSpendTxID)
		}
		require.Equal(t, value.Expiration, aerospikeExpiration)

		// try to spend with different txid
		err = db.Spend(context.Background(), spends3, blockHeight)
		require.Error(t, err)
		require.ErrorIs(t, err, utxo.ErrTypeSpent)

		// get the doc to check expiry etc.
		value, err = client.Get(util.GetAerospikeReadPolicy(), txKey)
		require.NoError(t, err)
		lastSpend, ok := value.Bins["lastSpend"].(int)
		require.True(t, ok)
		require.Greater(t, lastSpend, 0)
		require.Greater(t, value.Expiration, uint32(0))
		require.Less(t, value.Expiration, uint32(math.MaxUint32))

		// an error was observed were the utxos map got nilled out when trying to double spend
		// interestingly, this only happened in normal mode, not in batched mode :-S
		// Here we get all the data again and try the double spend again
		utxoMap, ok = value.Bins["utxos"].(map[interface{}]interface{})
		require.True(t, ok)
		for i := 0; i < 5; i++ {
			utxoSpendTxID, ok := utxoMap[spendsAll[i].UTXOHash.String()]
			require.True(t, ok)
			require.Equal(t, hash2.String(), utxoSpendTxID)
		}

		// try to spend with different txid
		err = db.Spend(context.Background(), spends3, blockHeight)
		require.Error(t, err)
		require.ErrorIs(t, err, utxo.ErrTypeSpent)

		value, err = client.Get(util.GetAerospikeReadPolicy(), txKey)
		require.NoError(t, err)
		assert.Equal(t, tx.Version, uint32(value.Bins["version"].(int)))
	})

	t.Run("aerospike reset", func(t *testing.T) {
		tx2 := tx.Clone()
		tx2.LockTime = 100
		key, _ := aero.NewKey(aerospikeNamespace, aerospikeSet, tx2.TxIDChainHash().CloneBytes())
		cleanDB(t, client, key, tx2)

		txMeta, err := db.Create(context.Background(), tx2)
		assert.NotNil(t, txMeta)
		require.NoError(t, err)

		_ = db.SetBlockHeight(101)

		utxoHash0, _ := util.UTXOHashFromOutput(tx2.TxIDChainHash(), tx2.Outputs[0], 0)
		spends := []*utxo.Spend{{
			TxID:         tx2.TxIDChainHash(),
			Vout:         0,
			UTXOHash:     utxoHash0,
			SpendingTxID: hash,
		}}
		err = db.Spend(context.Background(), spends, blockHeight)
		require.NoError(t, err)

		value, err := client.Get(util.GetAerospikeReadPolicy(), key)
		require.NoError(t, err)
		utxoMap, ok := value.Bins["utxos"].(map[interface{}]interface{})
		require.True(t, ok)
		utxoSpendTxID, ok := utxoMap[spends[0].UTXOHash.String()]
		require.True(t, ok)
		require.Equal(t, hash.String(), utxoSpendTxID)

		// try to reset the utxo
		err = db.UnSpend(context.Background(), []*utxo.Spend{{
			TxID:         tx2.TxIDChainHash(),
			Vout:         0,
			UTXOHash:     utxoHash0,
			SpendingTxID: hash2,
		}})
		require.NoError(t, err)

		value, err = client.Get(util.GetAerospikeReadPolicy(), key)
		require.NoError(t, err)
		utxoMap, ok = value.Bins["utxos"].(map[interface{}]interface{})
		require.True(t, ok)
		utxoSpendTxID, ok = utxoMap[spends[0].UTXOHash.String()]
		require.True(t, ok)
		require.Equal(t, "", utxoSpendTxID)
	})

	t.Run("aerospike block locktime", func(t *testing.T) {
		tx2 := tx.Clone()
		tx2.LockTime = 1000
		key, _ := aero.NewKey(aerospikeNamespace, aerospikeSet, tx2.TxIDChainHash().CloneBytes())
		cleanDB(t, client, key, tx, tx2)

		txMeta, err := db.Create(context.Background(), tx2)
		assert.NotNil(t, txMeta)
		require.NoError(t, err)

		utxoHash0, _ := util.UTXOHashFromOutput(tx2.TxIDChainHash(), tx2.Outputs[0], 0)
		spends := []*utxo.Spend{{
			TxID:         tx2.TxIDChainHash(),
			Vout:         0,
			UTXOHash:     utxoHash0,
			SpendingTxID: hash,
		}}
		err = db.Spend(context.Background(), spends, blockHeight)
		require.ErrorIs(t, err, utxo.ErrTypeLockTime)

		value, err := client.Get(util.GetAerospikeReadPolicy(), key)
		require.NoError(t, err)
		utxoMap, ok := value.Bins["utxos"].(map[interface{}]interface{})
		require.True(t, ok)
		utxoSpendTxID, ok := utxoMap[spends[0].UTXOHash.String()]
		require.True(t, ok)
		require.Equal(t, "", utxoSpendTxID)

		_ = db.SetBlockHeight(1001)

		err = db.Spend(context.Background(), spends, blockHeight)
		require.NoError(t, err)

		value, err = client.Get(util.GetAerospikeReadPolicy(), key)
		require.NoError(t, err)
		utxoMap, ok = value.Bins["utxos"].(map[interface{}]interface{})
		require.True(t, ok)
		utxoSpendTxID, ok = utxoMap[spends[0].UTXOHash.String()]
		require.True(t, ok)
		require.Equal(t, hash.String(), utxoSpendTxID)
	})

	t.Run("aerospike unix time locktime", func(t *testing.T) {
		tx2 := tx.Clone()
		tx2.LockTime = uint32(time.Now().UTC().Unix() + 2)
		key, _ := aero.NewKey(aerospikeNamespace, aerospikeSet, tx2.TxIDChainHash().CloneBytes())
		cleanDB(t, client, key, tx2)

		txMeta, err := db.Create(context.Background(), tx2) // valid in 2 seconds
		assert.NotNil(t, txMeta)
		require.NoError(t, err)

		value, err := client.Get(util.GetAerospikeReadPolicy(), key)
		require.NoError(t, err)
		assert.Equal(t, tx2.Version, uint32(value.Bins["version"].(int)))

		utxoHash0, _ := util.UTXOHashFromOutput(tx2.TxIDChainHash(), tx2.Outputs[0], 0)
		spends := []*utxo.Spend{{
			TxID:         tx2.TxIDChainHash(),
			Vout:         0,
			UTXOHash:     utxoHash0,
			SpendingTxID: hash,
		}}
		err = db.Spend(context.Background(), spends, blockHeight)
		require.ErrorIs(t, err, utxo.ErrTypeLockTime)

		value, err = client.Get(util.GetAerospikeReadPolicy(), key)
		require.NoError(t, err)
		utxoMap, ok := value.Bins["utxos"].(map[interface{}]interface{})
		require.True(t, ok)
		utxoSpendTxID, ok := utxoMap[spends[0].UTXOHash.String()]
		require.True(t, ok)
		require.Equal(t, "", utxoSpendTxID)

		time.Sleep(2 * time.Second)

		err = db.Spend(context.Background(), spends, blockHeight)
		require.NoError(t, err)

		value, err = client.Get(util.GetAerospikeReadPolicy(), key)
		require.NoError(t, err)
		utxoMap, ok = value.Bins["utxos"].(map[interface{}]interface{})
		require.True(t, ok)
		utxoSpendTxID, ok = utxoMap[spends[0].UTXOHash.String()]
		require.True(t, ok)
		require.Equal(t, hash.String(), utxoSpendTxID)
	})
}

func cleanDB(t *testing.T, client *aero.Client, key *aero.Key, txs ...*bt.Tx) {
	policy := util.GetAerospikeWritePolicy(0, 0)

	_, err := client.Delete(policy, key)
	require.NoError(t, err)

	_, err = client.Delete(policy, coinbaseKey)
	require.NoError(t, err)

	if len(txs) > 0 {
		for _, tx := range txs {
			key, _ = aero.NewKey(aerospikeNamespace, aerospikeSet, tx.TxIDChainHash()[:])
			_, err = client.Delete(policy, key)
			require.NoError(t, err)
		}
	}
}
