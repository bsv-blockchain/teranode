//go:build aerospike

package aerospike

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/TAAL-GmbH/ubsv/stores/txmeta"
	"github.com/TAAL-GmbH/ubsv/util"
	aero "github.com/aerospike/aerospike-client-go/v6"
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

	var hash *chainhash.Hash
	hash, err = chainhash.NewHashFromStr("5e3bc5947f48cec766090aa17f309fd16259de029dcef5d306b514848c9687c7")
	require.NoError(t, err)

	var parentTxHash *chainhash.Hash
	parentTxHash, err = chainhash.NewHashFromStr("3e3bc5947f48cec766090aa17f309fd16259de029dcef5d306b514848c9687c7")
	require.NoError(t, err)

	var utxoHash *chainhash.Hash
	utxoHash, err = chainhash.NewHashFromStr("3e3bc5947f48cec766090aa17f309fd16259de029dcef5d306b514848c9687c8")
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
		err = db.Create(context.Background(), hash, 101, []*chainhash.Hash{parentTxHash}, []*chainhash.Hash{utxoHash})
		require.NoError(t, err)

		var value *aero.Record
		// raw aerospike get
		value, err = client.Get(util.GetAerospikeReadPolicy(), key)
		require.NoError(t, err)
		require.Equal(t, uint32(1), value.Generation)
		assert.Equal(t, uint64(101), uint64(value.Bins["fee"].(int)))
		assert.Len(t, value.Bins["utxoHashes"].([]interface{}), 1)
		assert.Equal(t, []interface{}{utxoHash[:]}, value.Bins["utxoHashes"])
		assert.Len(t, value.Bins["parentTxHashes"].([]interface{}), 1)
		assert.Equal(t, []interface{}{parentTxHash[:]}, value.Bins["parentTxHashes"])
		assert.LessOrEqual(t, int(time.Now().Unix()), value.Bins["firstSeen"].(int))
		assert.Nil(t, value.Bins["blockHashes"])

		err = db.Create(context.Background(), hash, 102, []*chainhash.Hash{parentTxHash}, []*chainhash.Hash{utxoHash})
		// not allowed
		require.Error(t, err)

		err = db.SetMined(context.Background(), hash, blockHash)
		require.NoError(t, err)

		value, err = client.Get(util.GetAerospikeReadPolicy(), key)
		require.NoError(t, err)
		require.Equal(t, uint32(2), value.Generation)
		assert.Len(t, value.Bins["blockHashes"].([]interface{}), 1)
		assert.Equal(t, []interface{}{blockHash[:]}, value.Bins["blockHashes"].([]interface{}))

		err = db.SetMined(context.Background(), hash, blockHash2)
		require.NoError(t, err)

		value, err = client.Get(util.GetAerospikeReadPolicy(), key)
		require.NoError(t, err)
		require.Equal(t, uint32(3), value.Generation)
		assert.Len(t, value.Bins["blockHashes"].([]interface{}), 2)
		assert.Equal(t, []interface{}{blockHash[:], blockHash2[:]}, value.Bins["blockHashes"].([]interface{}))
	})

	t.Run("aerospike get", func(t *testing.T) {
		cleanDB(t, client, key)
		err = db.Create(context.Background(), hash, 103, []*chainhash.Hash{parentTxHash}, []*chainhash.Hash{utxoHash})
		require.NoError(t, err)

		var value *txmeta.Data
		value, err = db.Get(context.Background(), hash)
		require.NoError(t, err)
		assert.Equal(t, uint64(103), value.Fee)
		assert.Len(t, value.UtxoHashes, 1)
		assert.Equal(t, []*chainhash.Hash{utxoHash}, value.UtxoHashes)
		assert.Len(t, value.ParentTxHashes, 1)
		assert.Equal(t, []*chainhash.Hash{parentTxHash}, value.ParentTxHashes)
		assert.Len(t, value.BlockHashes, 0)
		assert.Nil(t, value.BlockHashes)

		err = db.SetMined(context.Background(), hash, blockHash2)
		require.NoError(t, err)

		value, err = db.Get(context.Background(), hash)
		require.NoError(t, err)
		assert.Equal(t, uint64(103), value.Fee)
		assert.Len(t, value.BlockHashes, 1)
		assert.Equal(t, []*chainhash.Hash{blockHash2}, value.BlockHashes)
	})
}

func cleanDB(t *testing.T, client *aero.Client, key *aero.Key) {
	policy := util.GetAerospikeWritePolicy(0, 0)
	_, err := client.Delete(policy, key)
	require.NoError(t, err)
}
