//go:build aerospike

package aerospike

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"
	"unsafe"

	aero "github.com/aerospike/aerospike-client-go/v6"
	"github.com/bitcoin-sv/ubsv/services/utxo/utxostore_api"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	aerospikeHost      = "aero.ubsv-store0.eu-north-1.ubsv.dev" // "localhost"
	aerospikePort      = 3000                                   // 3800
	aerospikeNamespace = "utxostore"                            // test
)

func main() {
	// your byte slice that you know is 32 bytes
	message := []byte("This is a 32-byte message!!")

	// check if the length of message is 32 bytes
	if len(message) != 32 {
		panic("message is not 32 bytes long")
	}

	// convert to [32]byte without allocation
	var array [32]byte
	*(*[32]byte)(unsafe.Pointer(&array)) = *(*[32]byte)(unsafe.Pointer(&message[0]))

	fmt.Println(array) // This will print the [32]byte array
}

func TestAerospike(t *testing.T) {
	// raw client to be able to do gets and cleanup
	client, aeroErr := aero.NewClient(aerospikeHost, aerospikePort)
	require.NoError(t, aeroErr)

	aeroURL, err := url.Parse(fmt.Sprintf("aerospike://%s:%d/%s", aerospikeHost, aerospikePort, aerospikeNamespace))
	require.NoError(t, err)

	// ubsv db client
	var db *Store
	db, err = New(aeroURL)
	require.NoError(t, err)

	var hash *chainhash.Hash
	hash, err = chainhash.NewHashFromStr("5e3bc5947f48cec766090aa17f309fd16259de029dcef5d306b514848c9687c7")
	require.NoError(t, err)

	var hash2 *chainhash.Hash
	hash2, err = chainhash.NewHashFromStr("663bc5947f48cec766090aa17f309fd16259de029dcef5d306b514848c9687c8")
	require.NoError(t, err)

	var key *aero.Key
	key, err = aero.NewKey(aerospikeNamespace, "utxo", hash[:])
	require.NoError(t, err)

	t.Cleanup(func() {
		policy := util.GetAerospikeWritePolicy(0, 0)
		_, err = client.Delete(policy, key)
		require.NoError(t, err)
	})

	var resp *utxostore.UTXOResponse
	var value *aero.Record
	t.Run("aerospike get", func(t *testing.T) {
		cleanDB(t, client, key)
		resp, err = db.Store(context.Background(), hash, 0)
		require.NoError(t, err)
		assert.Equal(t, int(utxostore_api.Status_OK), resp.Status)

		resp, err = db.Get(context.Background(), hash)
		require.NoError(t, err)
		assert.Equal(t, int(utxostore_api.Status_OK), resp.Status)
		assert.Equal(t, uint32(0), resp.LockTime)

		resp, err = db.Spend(context.Background(), hash, hash)
		require.NoError(t, err)
		assert.Equal(t, int(utxostore_api.Status_OK), resp.Status)

		resp, err = db.Get(context.Background(), hash)
		require.NoError(t, err)
		assert.Equal(t, int(utxostore_api.Status_SPENT), resp.Status)
		assert.Equal(t, uint32(0), resp.LockTime)
		assert.Equal(t, hash, resp.SpendingTxID)
	})

	t.Run("aerospike get with locktime", func(t *testing.T) {
		cleanDB(t, client, key)
		resp, err = db.Store(context.Background(), hash, 123)
		require.NoError(t, err)
		assert.Equal(t, int(utxostore_api.Status_OK), resp.Status)

		resp, err = db.Get(context.Background(), hash)
		require.NoError(t, err)
		assert.Equal(t, int(utxostore_api.Status_LOCK_TIME), resp.Status)
		assert.Equal(t, uint32(123), resp.LockTime)
	})

	t.Run("aerospike store", func(t *testing.T) {
		cleanDB(t, client, key)
		resp, err = db.Store(context.Background(), hash, 0)
		require.NoError(t, err)
		require.Equal(t, int(utxostore_api.Status_OK), resp.Status)

		value, err = client.Get(util.GetAerospikeReadPolicy(), key)
		require.NoError(t, err)
		require.Equal(t, uint32(1), value.Generation)

		resp, err = db.Store(context.Background(), hash, 0)
		require.NoError(t, err)
		require.Equal(t, int(utxostore_api.Status_OK), resp.Status)

		resp, err = db.Spend(context.Background(), hash, hash)
		require.NoError(t, err)
		require.Equal(t, int(utxostore_api.Status_OK), resp.Status)

		resp, err = db.Store(context.Background(), hash, 0)
		require.NoError(t, err)
		require.Equal(t, int(utxostore_api.Status_SPENT), resp.Status)
	})

	t.Run("aerospike spend", func(t *testing.T) {
		cleanDB(t, client, key)
		resp, err = db.Store(context.Background(), hash, 0)
		require.NoError(t, err)
		require.Equal(t, int(utxostore_api.Status_OK), resp.Status)

		resp, err = db.Spend(context.Background(), hash, hash)
		require.NoError(t, err)

		value, err = client.Get(util.GetAerospikeReadPolicy(), key)
		require.NoError(t, err)
		require.Equal(t, hash[:], value.Bins["txid"].([]byte))
		// generation will be 3, because we did a touch for the TTL
		require.Equal(t, uint32(3), value.Generation)

		// try to spend with different txid
		resp, err = db.Spend(context.Background(), hash, hash2)
		require.NoError(t, err)
		require.Equal(t, int(utxostore_api.Status_SPENT), resp.Status)
	})

	t.Run("aerospike reset", func(t *testing.T) {
		cleanDB(t, client, key)
		resp, err = db.Store(context.Background(), hash, 100)
		require.NoError(t, err)
		require.Equal(t, int(utxostore_api.Status_OK), resp.Status)

		_ = db.SetBlockHeight(101)

		resp, err = db.Spend(context.Background(), hash, hash)
		require.NoError(t, err)

		// try to reset the utxo
		resp, err = db.Reset(context.Background(), hash)
		require.NoError(t, err)

		value, err = client.Get(util.GetAerospikeReadPolicy(), key)
		require.NoError(t, err)
		require.Nil(t, value.Bins["txid"])
		require.Equal(t, 100, value.Bins["locktime"])
		require.Equal(t, uint32(1), value.Generation)
	})

	t.Run("aerospike block locktime", func(t *testing.T) {
		cleanDB(t, client, key)
		resp, err = db.Store(context.Background(), hash, 1000)
		require.NoError(t, err)
		require.Equal(t, int(utxostore_api.Status_OK), resp.Status)

		value, err = client.Get(util.GetAerospikeReadPolicy(), key)
		require.NoError(t, err)
		require.Nil(t, value.Bins["txid"])
		require.Equal(t, uint32(1), value.Generation)

		resp, err = db.Spend(context.Background(), hash, hash)
		require.NoError(t, err)
		require.Equal(t, int(utxostore_api.Status_LOCK_TIME), resp.Status)

		value, err = client.Get(util.GetAerospikeReadPolicy(), key)
		require.NoError(t, err)
		require.Nil(t, value.Bins["txid"])
		require.Equal(t, uint32(1), value.Generation)

		_ = db.SetBlockHeight(1001)

		resp, err = db.Spend(context.Background(), hash, hash)
		require.NoError(t, err)
		require.Equal(t, int(utxostore_api.Status_OK), resp.Status)

		value, err = client.Get(util.GetAerospikeReadPolicy(), key)
		require.NoError(t, err)
		require.Equal(t, hash[:], value.Bins["txid"].([]byte))
		require.Equal(t, uint32(3), value.Generation)
	})

	t.Run("aerospike unix time locktime", func(t *testing.T) {
		cleanDB(t, client, key)
		resp, err = db.Store(context.Background(), hash, uint32(time.Now().Unix()+1)) // valid in 1 second
		require.NoError(t, err)
		require.Equal(t, int(utxostore_api.Status_OK), resp.Status)

		value, err = client.Get(util.GetAerospikeReadPolicy(), key)

		resp, err = db.Spend(context.Background(), hash, hash)
		require.NoError(t, err)
		require.Equal(t, int(utxostore_api.Status_LOCK_TIME), resp.Status)

		value, err = client.Get(util.GetAerospikeReadPolicy(), key)
		require.NoError(t, err)
		require.Nil(t, value.Bins["txid"])
		require.Equal(t, uint32(1), value.Generation)

		time.Sleep(1 * time.Second)

		resp, err = db.Spend(context.Background(), hash, hash)
		require.NoError(t, err)
		require.Equal(t, int(utxostore_api.Status_OK), resp.Status)

		value, err = client.Get(util.GetAerospikeReadPolicy(), key)
		require.NoError(t, err)
		require.Equal(t, hash[:], value.Bins["txid"].([]byte))
		require.Equal(t, uint32(3), value.Generation)

	})
}

func cleanDB(t *testing.T, client *aero.Client, key *aero.Key) {
	policy := util.GetAerospikeWritePolicy(0, 0)
	_, err := client.Delete(policy, key)
	require.NoError(t, err)
}
