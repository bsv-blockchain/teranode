//go:build aerospike

package aerospike

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/ulogger"
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
	tx, _        = bt.NewTxFromString("010000000152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")
	txLocked, _  = bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff17030100002f6d312d65752f1ce2e178c565fcb45ad83e75ffffffff0500ca9a3b000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac00ca9a3b000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac00ca9a3b000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac00ca9a3b000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac00ca9a3b000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88ac00000000")
	utxoHash0, _ = util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[0], 0)
	spend        = &utxo.Spend{
		TxID: tx.TxIDChainHash(),
		Vout: 0,
		Hash: utxoHash0,
	}
	hash, _  = chainhash.NewHashFromStr("5e3bc5947f48cec766090aa17f309fd16259de029dcef5d306b514848c9687c7")
	hash2, _ = chainhash.NewHashFromStr("663bc5947f48cec766090aa17f309fd16259de029dcef5d306b514848c9687c8")
	spends   = []*utxo.Spend{{
		TxID:         tx.TxIDChainHash(),
		Vout:         0,
		Hash:         utxoHash0,
		SpendingTxID: hash,
	}}
	spends2 = []*utxo.Spend{{
		TxID:         tx.TxIDChainHash(),
		Vout:         0,
		Hash:         utxoHash0,
		SpendingTxID: hash2,
	}}
	key, _ = aerospike.NewKey(aerospikeNamespace, "utxo", utxoHash0.CloneBytes())
	ctx    = context.Background()
)

func TestAerospike(t *testing.T) {
	aeroURL, err := url.Parse(fmt.Sprintf("aerospike://%s:%d/%s", aerospikeHost, aerospikePort, aerospikeNamespace))
	require.NoError(t, err)

	// ubsv db client
	var db *Store
	db, err = New(ulogger.TestLogger{}, aeroURL) // SAO - call this before aerospike.NewClient() as we want to SetLevel of the aerospike logger to DEBUG before any other aerospike calls
	require.NoError(t, err)

	// raw client to be able to do gets and cleanup
	client, aeroErr := aerospike.NewClient(aerospikeHost, aerospikePort)
	require.NoError(t, aeroErr)

	t.Cleanup(func() {
		policy := util.GetAerospikeWritePolicy(0, 0)
		_, err = client.Delete(policy, key)
		require.NoError(t, err)
	})

	var resp *utxo.Response
	var value *aerospike.Record
	t.Run("aerospike get", func(t *testing.T) {
		cleanDB(t, client, tx)
		err = db.Store(ctx, tx)
		require.NoError(t, err)

		resp, err = db.Get(ctx, spend)
		require.NoError(t, err)
		assert.Equal(t, int(utxo.Status_OK), resp.Status)
		assert.Equal(t, uint32(0), resp.LockTime)

		err = db.Spend(ctx, spends)
		require.NoError(t, err)

		resp, err = db.Get(ctx, spend)
		require.NoError(t, err)
		assert.Equal(t, int(utxo.Status_SPENT), resp.Status)
		assert.Equal(t, uint32(0), resp.LockTime)
		assert.Equal(t, hash, resp.SpendingTxID)
	})

	t.Run("aerospike get with locktime", func(t *testing.T) {
		tx2 := tx.Clone()
		tx2.LockTime = 123
		cleanDB(t, client, tx2)

		err = db.Store(ctx, tx2)
		require.NoError(t, err)

		utxoHash0, _ := util.UTXOHashFromOutput(tx2.TxIDChainHash(), tx2.Outputs[0], 0)
		spend := &utxo.Spend{
			TxID: tx2.TxIDChainHash(),
			Vout: 0,
			Hash: utxoHash0,
		}
		resp, err = db.Get(ctx, spend)
		require.NoError(t, err)
		assert.Equal(t, int(utxo.Status_LOCKED), resp.Status)
		assert.Equal(t, uint32(123), resp.LockTime)
	})

	t.Run("aerospike store multi", func(t *testing.T) {
		txM := tx.Clone()
		_ = txM.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1)
		_ = txM.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1)
		_ = txM.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1)
		_ = txM.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1)
		_ = txM.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1)

		cleanDB(t, client, txM)
		err = db.Store(ctx, txM)
		require.NoError(t, err)
	})

	t.Run("aerospike spend", func(t *testing.T) {
		cleanDB(t, client, tx)
		err = db.Store(ctx, tx)
		require.NoError(t, err)

		err = db.Spend(ctx, spends)
		require.NoError(t, err)

		resp, err = db.Get(ctx, spend)
		require.NoError(t, err)
		require.Equal(t, int(utxo.Status_SPENT), resp.Status)
		require.Equal(t, hash, resp.SpendingTxID)

		value, err = client.Get(util.GetAerospikeReadPolicy(), key)
		require.NoError(t, err)
		require.Equal(t, hash[:], value.Bins["txid"].([]byte))
		require.Equal(t, uint32(2), value.Generation)

		// try to spend with different txid
		err = db.Spend(ctx, spends2)
		require.ErrorIs(t, err, utxo.ErrTypeSpent)
	})

	t.Run("aerospike spend with expiry", func(t *testing.T) {
		cleanDB(t, client, tx)
		err = db.Store(ctx, tx)
		require.NoError(t, err) // first store should work

		db.expiration = 1

		err = db.Spend(ctx, spends)
		require.NoError(t, err) // first spend should work

		resp, err = db.Get(ctx, spend)
		require.NoError(t, err)
		require.Equal(t, int(utxo.Status_SPENT), resp.Status)
		require.Equal(t, hash, resp.SpendingTxID)

		value, err = client.Get(util.GetAerospikeReadPolicy(), key)
		require.NoError(t, err)
		require.Equal(t, hash[:], value.Bins["txid"].([]byte))
		require.Equal(t, uint32(2), value.Generation)

		// try to spend with different txid
		err = db.Spend(ctx, spends2)
		require.ErrorIs(t, err, utxo.ErrTypeSpent)

		time.Sleep(2 * time.Second)

		policy := util.GetAerospikeReadPolicy()
		_, err = client.Get(policy, key)
		require.ErrorIs(t, err, aerospike.ErrKeyNotFound)

		resp, err = db.Get(ctx, spend)
		require.ErrorIs(t, err, utxo.ErrNotFound)
	})

	t.Run("aerospike reset", func(t *testing.T) {
		tx2 := tx.Clone()
		tx2.LockTime = 100
		cleanDB(t, client, tx2)

		err = db.Store(ctx, tx2)
		require.NoError(t, err)

		_ = db.SetBlockHeight(101)

		utxoHash0, _ := util.UTXOHashFromOutput(tx2.TxIDChainHash(), tx2.Outputs[0], 0)
		spends := []*utxo.Spend{{
			TxID:         tx2.TxIDChainHash(),
			Vout:         0,
			Hash:         utxoHash0,
			SpendingTxID: hash,
		}}
		err = db.Spend(ctx, spends)
		require.NoError(t, err)

		// try to reset the utxo
		err = db.UnSpend(ctx, spends)
		require.NoError(t, err)

		key, _ := aerospike.NewKey(aerospikeNamespace, "utxo", utxoHash0.CloneBytes())

		value, err = client.Get(util.GetAerospikeReadPolicy(), key)
		require.NoError(t, err)
		require.Nil(t, value.Bins["txid"])
		require.Equal(t, 100, value.Bins["locktime"])
		require.Equal(t, uint32(1), value.Generation)
	})

	t.Run("aerospike block locktime", func(t *testing.T) {
		tx2 := tx.Clone()
		tx2.LockTime = 1000
		cleanDB(t, client, tx2)

		err = db.Store(ctx, tx2)
		require.NoError(t, err)

		utxoHash0, _ := util.UTXOHashFromOutput(tx2.TxIDChainHash(), tx2.Outputs[0], 0)
		spends := []*utxo.Spend{{
			TxID:         tx2.TxIDChainHash(),
			Vout:         0,
			Hash:         utxoHash0,
			SpendingTxID: hash,
		}}
		key, _ := aerospike.NewKey(aerospikeNamespace, "utxo", utxoHash0.CloneBytes())

		value, err = client.Get(util.GetAerospikeReadPolicy(), key)
		require.NoError(t, err)
		require.Nil(t, value.Bins["txid"])
		require.Equal(t, uint32(1), value.Generation)

		err = db.Spend(ctx, spends)
		require.ErrorIs(t, err, utxo.ErrTypeLockTime)

		value, err = client.Get(util.GetAerospikeReadPolicy(), key)
		require.NoError(t, err)
		require.Nil(t, value.Bins["txid"])
		require.Equal(t, uint32(1), value.Generation)

		_ = db.SetBlockHeight(1001)

		err = db.Spend(ctx, spends)
		require.NoError(t, err)

		value, err = client.Get(util.GetAerospikeReadPolicy(), key)
		require.NoError(t, err)
		require.Equal(t, hash[:], value.Bins["txid"].([]byte))
		require.Equal(t, uint32(2), value.Generation)
	})

	t.Run("aerospike unix time locktime", func(t *testing.T) {
		tx2 := tx.Clone()
		tx2.LockTime = uint32(time.Now().UTC().Unix() + 2)
		cleanDB(t, client, tx2)

		err = db.Store(ctx, tx2) // valid in 1 second
		require.NoError(t, err)

		utxoHash0, _ := util.UTXOHashFromOutput(tx2.TxIDChainHash(), tx2.Outputs[0], 0)
		spends := []*utxo.Spend{{
			TxID:         tx2.TxIDChainHash(),
			Vout:         0,
			Hash:         utxoHash0,
			SpendingTxID: hash,
		}}
		key, _ := aerospike.NewKey(aerospikeNamespace, "utxo", utxoHash0.CloneBytes())

		value, err = client.Get(util.GetAerospikeReadPolicy(), key)
		require.NoError(t, err)
		require.Nil(t, value.Bins["txid"])
		require.Equal(t, uint32(1), value.Generation)

		err = db.Spend(ctx, spends)
		require.ErrorIs(t, err, utxo.ErrTypeLockTime)

		value, err = client.Get(util.GetAerospikeReadPolicy(), key)
		require.NoError(t, err)
		require.Nil(t, value.Bins["txid"])
		require.Equal(t, uint32(1), value.Generation)

		time.Sleep(2 * time.Second)

		err = db.Spend(ctx, spends)
		require.NoError(t, err)

		value, err = client.Get(util.GetAerospikeReadPolicy(), key)
		require.NoError(t, err)
		require.Equal(t, hash[:], value.Bins["txid"].([]byte))
		require.Equal(t, uint32(2), value.Generation)
	})

	t.Run("aerospike store no error", func(t *testing.T) {
		cleanDB(t, client, tx)
		err = db.Store(ctx, tx)
		require.NoError(t, err)
	})
}

func cleanDB(t *testing.T, client *aerospike.Client, tx *bt.Tx) {
	utxoHashes, err := utxo.GetUtxoHashes(tx)
	require.NoError(t, err)

	for _, utxoHash := range utxoHashes {
		key, _ := aerospike.NewKey(aerospikeNamespace, "utxo", utxoHash.CloneBytes())
		policy := util.GetAerospikeWritePolicy(0, 0)
		_, err = client.Delete(policy, key)
		require.NoError(t, err)
	}
}
