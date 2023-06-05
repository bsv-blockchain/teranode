//go:build aerospike

package aerospike

import (
	"context"
	"net/url"
	"testing"

	"github.com/TAAL-GmbH/ubsv/services/utxo/utxostore_api"
	utxostore "github.com/TAAL-GmbH/ubsv/stores/utxo"
	aero "github.com/aerospike/aerospike-client-go/v6"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
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

	var hash2 *chainhash.Hash
	hash2, err = chainhash.NewHashFromStr("5e3bc5947f48cec766090aa17f309fd16259de029dcef5d306b514848c9687c8")
	require.NoError(t, err)

	var key *aero.Key
	key, err = aero.NewKey("test", "utxo", hash[:])
	require.NoError(t, err)

	t.Cleanup(func() {
		policy := aero.NewWritePolicy(0, 0)
		_, err = client.Delete(policy, key)
		require.NoError(t, err)
	})

	var resp *utxostore.UTXOResponse
	var value *aero.Record
	t.Run("aerospike store", func(t *testing.T) {
		cleanDB(t, client, key)
		resp, err = db.Store(context.Background(), hash)
		require.NoError(t, err)
		require.Equal(t, int(utxostore_api.Status_OK), resp.Status)

		value, err = client.Get(aero.NewPolicy(), key)
		require.NoError(t, err)
		require.Equal(t, uint32(1), value.Generation)

		resp, err = db.Store(context.Background(), hash)
		require.NoError(t, err)
		require.Equal(t, int(utxostore_api.Status_OK), resp.Status)

		resp, err = db.Spend(context.Background(), hash, hash)
		require.NoError(t, err)
		require.Equal(t, int(utxostore_api.Status_OK), resp.Status)

		resp, err = db.Store(context.Background(), hash)
		require.NoError(t, err)
		require.Equal(t, int(utxostore_api.Status_SPENT), resp.Status)
	})

	t.Run("aerospike spend", func(t *testing.T) {
		cleanDB(t, client, key)
		resp, err = db.Store(context.Background(), hash)
		require.NoError(t, err)
		require.Equal(t, int(utxostore_api.Status_OK), resp.Status)

		resp, err = db.Spend(context.Background(), hash, hash)
		require.NoError(t, err)

		value, err = client.Get(aero.NewPolicy(), key)
		require.NoError(t, err)
		require.Equal(t, hash[:], value.Bins["txid"].([]byte))
		require.Equal(t, uint32(2), value.Generation)

		// try to spend with different txid
		resp, err = db.Spend(context.Background(), hash, hash2)
		require.NoError(t, err)
		require.Equal(t, int(utxostore_api.Status_SPENT), resp.Status)
	})

	t.Run("aerospike reset", func(t *testing.T) {
		cleanDB(t, client, key)
		resp, err = db.Store(context.Background(), hash)
		require.NoError(t, err)
		require.Equal(t, int(utxostore_api.Status_OK), resp.Status)

		resp, err = db.Spend(context.Background(), hash, hash)
		require.NoError(t, err)

		// try to reset the utxo
		resp, err = db.Reset(context.Background(), hash)
		require.NoError(t, err)

		value, err = client.Get(aero.NewPolicy(), key)
		require.NoError(t, err)
		require.Equal(t, []byte{}, value.Bins["txid"].([]byte))
		require.Equal(t, uint32(1), value.Generation)
	})
}

func cleanDB(t *testing.T, client *aero.Client, key *aero.Key) {
	policy := aero.NewWritePolicy(0, 0)
	_, err := client.Delete(policy, key)
	require.NoError(t, err)
}
