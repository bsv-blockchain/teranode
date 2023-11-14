//go:build aerospike

package aerospike

import (
	"context"
	"net/url"
	"testing"
	"time"

	aero "github.com/aerospike/aerospike-client-go/v6"
	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAerospike(t *testing.T) {
	// raw client to be able to do gets and cleanup
	client, aeroErr := aero.NewClient("localhost", 3800)
	require.NoError(t, aeroErr)

	aeroURL, err := url.Parse("aerospike://localhost:3800/test")
	require.NoError(t, err)

	// ubsv db client
	var db *Store
	db, err = New(aeroURL)
	require.NoError(t, err)

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

	var blockHash *chainhash.Hash
	blockHash, err = chainhash.NewHashFromStr("5e3bc5947f48cec766090aa17f309fd16259de029dcef5d306b514848c9687c8")
	require.NoError(t, err)

	var blockHash2 *chainhash.Hash
	blockHash2, err = chainhash.NewHashFromStr("5e3bc5947f48cec766090aa17f309fd16259de029dcef5d306b514848c9687c9")
	require.NoError(t, err)

	var key *aero.Key
	key, err = aero.NewKey("test", "txmeta", hash[:])
	require.NoError(t, err)

	t.Cleanup(func() {
		policy := util.GetAerospikeWritePolicy(0, 0)
		_, err = client.Delete(policy, key)
		require.NoError(t, err)
	})

	t.Run("aerospike store", func(t *testing.T) {
		cleanDB(t, client, key)
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
		assert.LessOrEqual(t, int(time.Now().Unix()), value.Bins["firstSeen"].(int))
		assert.Nil(t, value.Bins["blockHashes"])

		_, err = db.Create(context.Background(), tx)
		// not allowed
		require.Error(t, err)

		err = db.SetMined(context.Background(), hash, blockHash)
		require.NoError(t, err)

		value, err = client.Get(util.GetAerospikeReadPolicy(), key)
		require.NoError(t, err)
		require.Equal(t, uint32(2), value.Generation)
		assert.Len(t, value.Bins["blockHashes"].([]byte), 32)
		assert.Equal(t, blockHash[:], value.Bins["blockHashes"].([]byte))

		err = db.SetMined(context.Background(), hash, blockHash2)
		require.NoError(t, err)

		value, err = client.Get(util.GetAerospikeReadPolicy(), key)
		require.NoError(t, err)
		require.Equal(t, uint32(3), value.Generation)
		assert.Len(t, value.Bins["blockHashes"].([]byte), 2*32)
		assert.Equal(t, append(blockHash[:], blockHash2[:]...), value.Bins["blockHashes"].([]byte))
	})

	t.Run("aerospike get", func(t *testing.T) {
		cleanDB(t, client, key)
		_, err = db.Create(context.Background(), tx)
		require.NoError(t, err)

		var value *txmeta.Data
		value, err = db.Get(context.Background(), hash)
		require.NoError(t, err)
		assert.Equal(t, uint64(101), value.Fee)
		assert.Equal(t, uint64(60), value.SizeInBytes)
		assert.Len(t, value.ParentTxHashes, 1)
		assert.Equal(t, []*chainhash.Hash{parentTxHash}, value.ParentTxHashes)
		assert.Len(t, value.BlockHashes, 0)
		assert.Nil(t, value.BlockHashes)

		err = db.SetMined(context.Background(), hash, blockHash2)
		require.NoError(t, err)

		value, err = db.Get(context.Background(), hash)
		require.NoError(t, err)
		assert.Equal(t, uint64(101), value.Fee)
		assert.Len(t, value.BlockHashes, 1)
		assert.Equal(t, []*chainhash.Hash{blockHash2}, value.BlockHashes)
	})

	t.Run("aerospike get - expired", func(t *testing.T) {
		cleanDB(t, client, key)

		db.expiration = 1

		_, err = db.Create(context.Background(), tx)
		require.NoError(t, err)

		var value *txmeta.Data
		value, err = db.Get(context.Background(), hash)
		require.NoError(t, err)
		assert.Equal(t, uint64(101), value.Fee)
		assert.Equal(t, uint64(60), value.SizeInBytes)
		assert.Len(t, value.ParentTxHashes, 1)
		assert.Equal(t, []*chainhash.Hash{parentTxHash}, value.ParentTxHashes)
		assert.Len(t, value.BlockHashes, 0)
		assert.Nil(t, value.BlockHashes)

		err = db.SetMined(context.Background(), hash, blockHash2)
		require.NoError(t, err)

		value, err = db.Get(context.Background(), hash)
		require.NoError(t, err)
		assert.Equal(t, uint64(101), value.Fee)
		assert.Len(t, value.BlockHashes, 1)
		assert.Equal(t, []*chainhash.Hash{blockHash2}, value.BlockHashes)

		time.Sleep(2 * time.Second)

		_, err = db.Get(context.Background(), hash)
		require.ErrorIs(t, err, txmeta.ErrNotFound)
	})
}

func cleanDB(t *testing.T, client *aero.Client, key *aero.Key) {
	policy := util.GetAerospikeWritePolicy(0, 0)
	_, err := client.Delete(policy, key)
	require.NoError(t, err)
}
