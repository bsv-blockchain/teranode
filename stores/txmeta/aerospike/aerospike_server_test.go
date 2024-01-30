//go:build aerospike

package aerospike

import (
	"context"
	"net/url"
	"testing"
	"time"

	aero "github.com/aerospike/aerospike-client-go/v6"
	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	key  *aero.Key
	key2 *aero.Key
	key3 *aero.Key
	key4 *aero.Key
)

func TestAerospike(t *testing.T) {
	aeroURL, err := url.Parse("aerospike://localhost:3000/test")
	require.NoError(t, err)

	// ubsv db client
	var db *Store
	db, err = New(ulogger.TestLogger{}, aeroURL) // SAO - call this before aero.NewClient() as we want to SetLevel of the aerospike logger to DEBUG before any other aerospike calls
	require.NoError(t, err)

	// raw client to be able to do gets and cleanup
	client, aeroErr := aero.NewClient("localhost", 3000)
	require.NoError(t, aeroErr)

	parentTx := bt.NewTx()
	parentTx.LockTime = 1
	parentTxHash := parentTx.TxIDChainHash()
	require.NoError(t, err)

	tx := bt.NewTx()
	tx.Inputs = []*bt.Input{
		{
			PreviousTxSatoshis: 201,
			PreviousTxScript:   &bscript.Script{},
			UnlockingScript:    &bscript.Script{},
			PreviousTxOutIndex: 0,
			SequenceNumber:     0,
		},
	}
	_ = tx.Inputs[0].PreviousTxIDAdd(parentTxHash)

	tx.AddOutput(&bt.Output{
		Satoshis:      100,
		LockingScript: &bscript.Script{},
	})
	tx.LockTime = 0
	hash := tx.TxIDChainHash()
	require.NoError(t, err)

	tx2 := tx.Clone()
	tx2.Inputs[0].PreviousTxSatoshis = 202
	tx2.LockTime = 2

	tx3 := tx.Clone()
	tx3.Inputs[0].PreviousTxSatoshis = 203
	tx3.LockTime = 3

	hash2 := tx2.TxIDChainHash()
	hash3 := tx3.TxIDChainHash()

	var blockHash *chainhash.Hash
	blockHash, err = chainhash.NewHashFromStr("5e3bc5947f48cec766090aa17f309fd16259de029dcef5d306b514848c9687c8")
	require.NoError(t, err)

	key, err = aero.NewKey("test", "txmeta", hash[:])
	require.NoError(t, err)

	key2, err = aero.NewKey("test", "txmeta", hash2[:])
	require.NoError(t, err)

	key3, err = aero.NewKey("test", "txmeta", hash3[:])
	require.NoError(t, err)

	key4, err = aero.NewKey("test", "txmeta", blockHash[:])
	require.NoError(t, err)

	t.Cleanup(func() {
		policy := util.GetAerospikeWritePolicy(0, 0)
		_, err = client.Delete(policy, key)
		require.NoError(t, err)
	})

	t.Run("aerospike store", func(t *testing.T) {
		cleanDB(t, client)
		_, err = db.Create(context.Background(), tx)
		require.NoError(t, err)

		var value *aero.Record
		// raw aerospike get
		value, err = client.Get(util.GetAerospikeReadPolicy(), key)
		require.NoError(t, err)
		require.Equal(t, uint32(1), value.Generation)
		assert.Equal(t, uint64(101), uint64(value.Bins["fee"].(int)))
		assert.Equal(t, uint64(60), uint64(value.Bins["sizeInBytes"].(int)))
		assert.Len(t, value.Bins["parentTxHashes"].([]byte), 32)
		assert.Equal(t, parentTxHash[:], value.Bins["parentTxHashes"])
		assert.Nil(t, value.Bins["blockIDs"])

		_, err = db.Create(context.Background(), tx)
		// not allowed
		require.Error(t, err)

		err = db.SetMined(context.Background(), hash, 1)
		require.NoError(t, err)

		value, err = client.Get(util.GetAerospikeReadPolicy(), key)
		require.NoError(t, err)
		require.Equal(t, uint32(2), value.Generation)
		assert.Len(t, value.Bins["blockIDs"].([]interface{}), 1)
		assert.Equal(t, 1, value.Bins["blockIDs"].([]interface{})[0])

		err = db.SetMined(context.Background(), hash, 2)
		require.NoError(t, err)

		value, err = client.Get(util.GetAerospikeReadPolicy(), key)
		require.NoError(t, err)
		require.Equal(t, uint32(3), value.Generation)
		assert.Len(t, value.Bins["blockIDs"].([]interface{}), 2)
		assert.Equal(t, 2, value.Bins["blockIDs"].([]interface{})[1])
	})

	t.Run("aerospike get", func(t *testing.T) {
		cleanDB(t, client)
		_, err = db.Create(context.Background(), tx)
		require.NoError(t, err)

		var value *txmeta.Data
		value, err = db.Get(context.Background(), hash)
		require.NoError(t, err)
		assert.Equal(t, uint64(101), value.Fee)
		assert.Equal(t, uint64(60), value.SizeInBytes)
		assert.Len(t, value.ParentTxHashes, 1)
		assert.Equal(t, []chainhash.Hash{*parentTxHash}, value.ParentTxHashes)
		assert.Len(t, value.BlockIDs, 0)
		assert.Nil(t, value.BlockIDs)

		err = db.SetMined(context.Background(), hash, 2)
		require.NoError(t, err)

		value, err = db.Get(context.Background(), hash)
		require.NoError(t, err)
		assert.Equal(t, uint64(101), value.Fee)
		assert.Len(t, value.BlockIDs, 1)
		assert.Equal(t, []uint32{2}, value.BlockIDs)
	})

	t.Run("aerospike get - expired", func(t *testing.T) {
		cleanDB(t, client)

		db.expiration = 1

		_, err = db.Create(context.Background(), tx)
		require.NoError(t, err)

		var value *txmeta.Data
		value, err = db.Get(context.Background(), hash)
		require.NoError(t, err)
		assert.Equal(t, uint64(101), value.Fee)
		assert.Equal(t, uint64(60), value.SizeInBytes)
		assert.Len(t, value.ParentTxHashes, 1)
		assert.Equal(t, []chainhash.Hash{*parentTxHash}, value.ParentTxHashes)
		assert.Len(t, value.BlockIDs, 0)
		assert.Nil(t, value.BlockIDs)

		err = db.SetMined(context.Background(), hash, 2)
		require.NoError(t, err)

		value, err = db.Get(context.Background(), hash)
		require.NoError(t, err)
		assert.Equal(t, uint64(101), value.Fee)
		assert.Len(t, value.BlockIDs, 1)
		assert.Equal(t, []uint32{2}, value.BlockIDs)

		time.Sleep(2 * time.Second)

		_, err = db.Get(context.Background(), hash)
		require.ErrorIs(t, err, txmeta.NewErrTxmetaNotFound(hash))
	})

	t.Run("aerospike set mined multi", func(t *testing.T) {
		cleanDB(t, client)
		_, err = db.Create(context.Background(), tx)
		require.NoError(t, err)

		_, err = db.Create(context.Background(), tx2)
		require.NoError(t, err)

		_, err = db.Create(context.Background(), tx3)
		require.NoError(t, err)

		err = db.SetMinedMulti(context.Background(), []*chainhash.Hash{hash, hash2, hash3, blockHash}, 2)
		require.NoError(t, err)

		value, err := db.Get(context.Background(), hash)
		require.NoError(t, err)
		assert.Equal(t, uint64(101), value.Fee)
		assert.Len(t, value.BlockIDs, 1)
		assert.Equal(t, []uint32{2}, value.BlockIDs)

		value, err = db.Get(context.Background(), hash2)
		require.NoError(t, err)
		assert.Equal(t, uint64(102), value.Fee)
		assert.Len(t, value.BlockIDs, 1)
		assert.Equal(t, []uint32{2}, value.BlockIDs)

		value, err = db.Get(context.Background(), hash3)
		require.NoError(t, err)
		assert.Equal(t, uint64(103), value.Fee)
		assert.Len(t, value.BlockIDs, 1)
		assert.Equal(t, []uint32{2}, value.BlockIDs)

		value, err = db.Get(context.Background(), blockHash)
		require.ErrorIs(t, err, txmeta.NewErrTxmetaNotFound(blockHash))
	})

}

func cleanDB(t *testing.T, client *aero.Client) {
	policy := util.GetAerospikeWritePolicy(0, 0)
	_, err := client.Delete(policy, key)
	require.NoError(t, err)
	_, err = client.Delete(policy, key2)
	require.NoError(t, err)
	_, err = client.Delete(policy, key3)
	require.NoError(t, err)
	_, err = client.Delete(policy, key4)
	require.NoError(t, err)
}
