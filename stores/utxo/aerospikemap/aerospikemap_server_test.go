//go:build aerospike

package aerospikemap

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/ulogger"

	aero "github.com/aerospike/aerospike-client-go/v7"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	aerospikeHost      = "localhost" // "localhost"
	aerospikePort      = 3000        // 3800
	aerospikeNamespace = "test"      // test
)

var (
	tx, _        = bt.NewTxFromString("010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")
	utxoHash0, _ = util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[0], 0)
	spend        = &utxostore.Spend{
		TxID: tx.TxIDChainHash(),
		Vout: 0,
		Hash: utxoHash0,
	}
	hash, _  = chainhash.NewHashFromStr("5e3bc5947f48cec766090aa17f309fd16259de029dcef5d306b514848c9687c7")
	hash2, _ = chainhash.NewHashFromStr("663bc5947f48cec766090aa17f309fd16259de029dcef5d306b514848c9687c8")
	spends   = []*utxostore.Spend{{
		TxID:         tx.TxIDChainHash(),
		Vout:         0,
		Hash:         utxoHash0,
		SpendingTxID: hash,
	}}
	spends2 = []*utxostore.Spend{{
		TxID:         tx.TxIDChainHash(),
		Vout:         0,
		Hash:         utxoHash0,
		SpendingTxID: hash2,
	}}
	key, _ = aero.NewKey(aerospikeNamespace, "utxo", tx.TxIDChainHash().CloneBytes())
)

func TestAerospike(t *testing.T) {
	// raw client to be able to do gets and cleanup
	client, aeroErr := aero.NewClient(aerospikeHost, aerospikePort)
	require.NoError(t, aeroErr)

	aeroURL, err := url.Parse(fmt.Sprintf("aerospike://%s:%d/%s", aerospikeHost, aerospikePort, aerospikeNamespace))
	require.NoError(t, err)

	// ubsv db client
	var db utxostore.Interface
	db, err = New(ulogger.TestLogger{}, aeroURL)
	require.NoError(t, err)

	t.Cleanup(func() {
		policy := util.GetAerospikeWritePolicy(0, 0)
		_, err = client.Delete(policy, key)
		require.NoError(t, err)
	})

	var resp *utxostore.Response
	var value *aero.Record
	t.Run("aerospike get", func(t *testing.T) {
		cleanDB(t, client, tx)
		err = db.Store(context.Background(), tx)
		require.NoError(t, err)

		resp, err = db.Get(context.Background(), spend)
		require.NoError(t, err)
		assert.Equal(t, int(utxostore.Status_OK), resp.Status)
		assert.Equal(t, uint32(0), resp.LockTime)

		err = db.Spend(context.Background(), spends)
		require.NoError(t, err)

		resp, err = db.Get(context.Background(), spend)
		require.NoError(t, err)
		assert.Equal(t, int(utxostore.Status_SPENT), resp.Status)
		assert.Equal(t, uint32(0), resp.LockTime)
		assert.Equal(t, hash, resp.SpendingTxID)
	})

	t.Run("aerospike get with locktime", func(t *testing.T) {
		tx2 := tx.Clone()
		tx2.LockTime = 123
		cleanDB(t, client, tx2)

		err = db.Store(context.Background(), tx2)
		require.NoError(t, err)

		spend := &utxostore.Spend{
			TxID: tx2.TxIDChainHash(),
			Vout: 0,
			Hash: hash,
		}
		resp, err = db.Get(context.Background(), spend)
		require.NoError(t, err)
		assert.Equal(t, int(utxostore.Status_LOCKED), resp.Status)
		assert.Equal(t, uint32(123), resp.LockTime)
	})

	t.Run("aerospike store", func(t *testing.T) {
		cleanDB(t, client, tx)
		err = db.Store(context.Background(), tx)
		require.NoError(t, err)

		value, err = client.Get(util.GetAerospikeReadPolicy(), key)
		require.NoError(t, err)

		err = db.Store(context.Background(), tx)
		require.Error(t, err, utxostore.ErrAlreadyExists)

		err = db.Spend(context.Background(), spends)
		require.NoError(t, err)

		err = db.Store(context.Background(), tx)
		require.Error(t, err, utxostore.ErrAlreadyExists)
	})

	t.Run("aerospike spend", func(t *testing.T) {
		cleanDB(t, client, tx)
		err = db.Store(context.Background(), tx)
		require.NoError(t, err)

		err = db.Spend(context.Background(), spends)
		require.NoError(t, err)

		value, err = client.Get(util.GetAerospikeReadPolicy(), key)
		require.NoError(t, err)
		utxoMap, ok := value.Bins["utxos"].(map[interface{}]interface{})
		require.True(t, ok)
		utxoSpendTxID, ok := utxoMap[spends[0].Hash.String()]
		require.True(t, ok)
		require.Equal(t, hash[:], utxoSpendTxID)

		// try to spend with different txid
		err = db.Spend(context.Background(), spends2)
		require.ErrorIs(t, err, utxostore.ErrTypeSpent)
	})

	t.Run("aerospike reset", func(t *testing.T) {
		tx2 := tx.Clone()
		tx2.LockTime = 100
		key, _ := aero.NewKey(aerospikeNamespace, "utxo", tx2.TxIDChainHash().CloneBytes())
		cleanDB(t, client, tx2)

		err = db.Store(context.Background(), tx2)
		require.NoError(t, err)

		_ = db.SetBlockHeight(101)

		utxoHash0, _ := util.UTXOHashFromOutput(tx2.TxIDChainHash(), tx2.Outputs[0], 0)
		spends := []*utxostore.Spend{{
			TxID:         tx2.TxIDChainHash(),
			Vout:         0,
			Hash:         utxoHash0,
			SpendingTxID: hash,
		}}
		err = db.Spend(context.Background(), spends)
		require.NoError(t, err)

		value, err = client.Get(util.GetAerospikeReadPolicy(), key)
		require.NoError(t, err)
		utxoMap, ok := value.Bins["utxos"].(map[interface{}]interface{})
		require.True(t, ok)
		utxoSpendTxID, ok := utxoMap[spends[0].Hash.String()]
		require.True(t, ok)
		require.Equal(t, hash[:], utxoSpendTxID)

		// try to reset the utxo
		err = db.UnSpend(context.Background(), []*utxostore.Spend{{
			TxID: tx2.TxIDChainHash(),
			Vout: 0,
			Hash: utxoHash0,
		}})
		require.NoError(t, err)

		value, err = client.Get(util.GetAerospikeReadPolicy(), key)
		require.NoError(t, err)
		utxoMap, ok = value.Bins["utxos"].(map[interface{}]interface{})
		require.True(t, ok)
		utxoSpendTxID, ok = utxoMap[spends[0].Hash.String()]
		require.True(t, ok)
		require.Nil(t, utxoSpendTxID)
	})

	t.Run("aerospike block locktime", func(t *testing.T) {
		tx2 := tx.Clone()
		tx2.LockTime = 1000
		key, _ := aero.NewKey(aerospikeNamespace, "utxo", tx2.TxIDChainHash().CloneBytes())
		cleanDB(t, client, tx2)

		err = db.Store(context.Background(), tx2)
		require.NoError(t, err)

		utxoHash0, _ := util.UTXOHashFromOutput(tx2.TxIDChainHash(), tx2.Outputs[0], 0)
		spends := []*utxostore.Spend{{
			TxID:         tx2.TxIDChainHash(),
			Vout:         0,
			Hash:         utxoHash0,
			SpendingTxID: hash,
		}}
		err = db.Spend(context.Background(), spends)
		require.ErrorIs(t, err, utxostore.ErrTypeLockTime)

		value, err = client.Get(util.GetAerospikeReadPolicy(), key)
		require.NoError(t, err)
		utxoMap, ok := value.Bins["utxos"].(map[interface{}]interface{})
		require.True(t, ok)
		utxoSpendTxID, ok := utxoMap[spends[0].Hash.String()]
		require.True(t, ok)
		require.Nil(t, utxoSpendTxID)

		_ = db.SetBlockHeight(1001)

		err = db.Spend(context.Background(), spends)
		require.NoError(t, err)

		value, err = client.Get(util.GetAerospikeReadPolicy(), key)
		require.NoError(t, err)
		utxoMap, ok = value.Bins["utxos"].(map[interface{}]interface{})
		require.True(t, ok)
		utxoSpendTxID, ok = utxoMap[spends[0].Hash.String()]
		require.True(t, ok)
		require.Equal(t, hash[:], utxoSpendTxID)
	})

	t.Run("aerospike unix time locktime", func(t *testing.T) {
		tx2 := tx.Clone()
		tx2.LockTime = uint32(time.Now().UTC().Unix() + 2)
		key, _ := aero.NewKey(aerospikeNamespace, "utxo", tx2.TxIDChainHash().CloneBytes())
		cleanDB(t, client, tx2)

		err = db.Store(context.Background(), tx2) // valid in 1 second
		require.NoError(t, err)

		value, err = client.Get(util.GetAerospikeReadPolicy(), key)

		utxoHash0, _ := util.UTXOHashFromOutput(tx2.TxIDChainHash(), tx2.Outputs[0], 0)
		spends := []*utxostore.Spend{{
			TxID:         tx2.TxIDChainHash(),
			Vout:         0,
			Hash:         utxoHash0,
			SpendingTxID: hash,
		}}
		err = db.Spend(context.Background(), spends)
		require.ErrorIs(t, err, utxostore.ErrTypeLockTime)

		value, err = client.Get(util.GetAerospikeReadPolicy(), key)
		require.NoError(t, err)
		utxoMap, ok := value.Bins["utxos"].(map[interface{}]interface{})
		require.True(t, ok)
		utxoSpendTxID, ok := utxoMap[spends[0].Hash.String()]
		require.True(t, ok)
		require.Nil(t, utxoSpendTxID)

		time.Sleep(2 * time.Second)

		err = db.Spend(context.Background(), spends)
		require.NoError(t, err)

		value, err = client.Get(util.GetAerospikeReadPolicy(), key)
		require.NoError(t, err)
		utxoMap, ok = value.Bins["utxos"].(map[interface{}]interface{})
		require.True(t, ok)
		utxoSpendTxID, ok = utxoMap[spends[0].Hash.String()]
		require.True(t, ok)
		require.Equal(t, hash[:], utxoSpendTxID)
	})
}

func cleanDB(t *testing.T, client *aero.Client, tx *bt.Tx) {
	key, _ = aero.NewKey(aerospikeNamespace, "utxo", tx.TxIDChainHash().CloneBytes())
	policy := util.GetAerospikeWritePolicy(0, 0)
	_, err := client.Delete(policy, key)
	require.NoError(t, err)
}
