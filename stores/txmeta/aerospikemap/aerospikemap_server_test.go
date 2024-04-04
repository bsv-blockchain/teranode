// //go:build aerospike

package aerospikemap

import (
	"context"
	"fmt"
	aero "github.com/aerospike/aerospike-client-go/v7"
	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/stores/utxo/aerospikemap"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/url"
	"testing"
)

const (
	aerospikeHost      = "localhost" // "localhost"
	aerospikePort      = 3000        // 3800
	aerospikeNamespace = "test"      // test
)

func TestAerospike(t *testing.T) {
	gocore.Config().Set("utxostore_spendBatcherEnabled", "false")
	gocore.Config().Set("txmeta_store_storeBatcherEnabled", "false")
	gocore.Config().Set("txmeta_store_getBatcherEnabled", "false")
	internalTest(t)
}

func TestAerospikeBatching(t *testing.T) {
	gocore.Config().Set("utxostore_spendBatcherEnabled", "true")
	gocore.Config().Set("txmeta_store_storeBatcherEnabled", "true")
	gocore.Config().Set("txmeta_store_getBatcherEnabled", "true")
	internalTest(t)
}

func internalTest(t *testing.T) {
	// raw client to be able to do gets and cleanup
	client, aeroErr := aero.NewClient(aerospikeHost, aerospikePort)
	require.NoError(t, aeroErr)

	aeroURL, err := url.Parse(fmt.Sprintf("aerospike://%s:%d/%s", aerospikeHost, aerospikePort, aerospikeNamespace))
	require.NoError(t, err)

	// ubsv db client
	var db *Store
	db, err = New(ulogger.TestLogger{}, aeroURL)
	require.NoError(t, err)

	//var utxoDb utxostore.Interface
	utxoDb, err := aerospikemap.New(ulogger.TestLogger{}, aeroURL)
	require.NoError(t, err)
	_ = utxoDb

	tx, _ := bt.NewTxFromString("010000000000000000ef0152a9231baa4e4b05dc30c8fbb7787bab5f460d4d33b039c39dd8cc006f3363e4020000006b483045022100ce3605307dd1633d3c14de4a0cf0df1439f392994e561b648897c4e540baa9ad02207af74878a7575a95c9599e9cdc7e6d73308608ee59abcd90af3ea1a5c0cca41541210275f8390df62d1e951920b623b8ef9c2a67c4d2574d408e422fb334dd1f3ee5b6ffffffff706b9600000000001976a914a32f7eaae3afd5f73a2d6009b93f91aa11d16eef88ac05404b4c00000000001976a914aabb8c2f08567e2d29e3a64f1f833eee85aaf74d88ac80841e00000000001976a914a4aff400bef2fa074169453e703c611c6b9df51588ac204e0000000000001976a9144669d92d46393c38594b2f07587f01b3e5289f6088ac204e0000000000001976a914a461497034343a91683e86b568c8945fb73aca0288ac99fe2a00000000001976a914de7850e419719258077abd37d4fcccdb0a659b9388ac00000000")
	hash := tx.TxIDChainHash()
	parentTxHash := tx.Inputs[0].PreviousTxIDChainHash()

	blockID := uint32(123)
	blockID2 := uint32(124)

	var key *aero.Key
	key, err = aero.NewKey("test", db.setName, hash[:])
	require.NoError(t, err)

	t.Cleanup(func() {
		policy := util.GetAerospikeWritePolicy(0, 0)
		_, err = client.Delete(policy, key)
		require.NoError(t, err)
	})

	t.Run("aerospike store", func(t *testing.T) {
		cleanDB(t, client, key)

		// txs are stored when txmeta is created, not in utxo
		err = utxoDb.Store(context.Background(), tx)
		require.NoError(t, err)

		_, err = db.Create(context.Background(), tx)
		require.NoError(t, err)

		var value *aero.Record
		// raw aerospike get
		value, err = client.Get(util.GetAerospikeReadPolicy(), key)
		require.NoError(t, err)
		require.Equal(t, uint32(1), value.Generation)
		assert.Equal(t, uint64(215), uint64(value.Bins["fee"].(int)))
		assert.Equal(t, uint64(328), uint64(value.Bins["sizeInBytes"].(int)))
		assert.Len(t, value.Bins["parentTxHashes"].([]byte), 32)
		binParentTxHash := chainhash.Hash(value.Bins["parentTxHashes"].([]byte))
		assert.Equal(t, parentTxHash[:], binParentTxHash.CloneBytes())
		assert.Equal(t, []interface{}{}, value.Bins["blockIDs"])

		err = utxoDb.Store(context.Background(), tx)
		// no-op, so should be nil
		require.NoError(t, err)

		_, err = db.Create(context.Background(), tx)
		require.Error(t, err)
		assert.ErrorIs(t, err, txmeta.NewErrTxmetaAlreadyExists(hash))

		err = db.SetMined(context.Background(), hash, blockID)
		require.NoError(t, err)

		value, err = client.Get(util.GetAerospikeReadPolicy(), key)
		require.NoError(t, err)
		require.Equal(t, uint32(2), value.Generation)
		assert.Len(t, value.Bins["blockIDs"].([]interface{}), 1)
		assert.Equal(t, []interface{}{int(blockID)}, value.Bins["blockIDs"].([]interface{}))

		err = db.SetMined(context.Background(), hash, blockID2)
		require.NoError(t, err)

		value, err = client.Get(util.GetAerospikeReadPolicy(), key)
		require.NoError(t, err)
		require.Equal(t, uint32(3), value.Generation)
		assert.Len(t, value.Bins["blockIDs"].([]interface{}), 2)
		assert.Equal(t, []interface{}{int(blockID), int(blockID2)}, value.Bins["blockIDs"].([]interface{}))
	})

	t.Run("aerospike get", func(t *testing.T) {
		cleanDB(t, client, key)

		// txs are stored when utxos are created, not in tx meta
		err = utxoDb.Store(context.Background(), tx)
		require.NoError(t, err)

		_, err = db.Create(context.Background(), tx)
		require.NoError(t, err)

		var value *txmeta.Data
		value, err = db.Get(context.Background(), hash)
		require.NoError(t, err)
		assert.Equal(t, uint64(215), value.Fee)
		assert.Equal(t, uint64(328), value.SizeInBytes)
		assert.Len(t, value.ParentTxHashes, 1)
		assert.Equal(t, []chainhash.Hash{*parentTxHash}, value.ParentTxHashes)
		assert.Len(t, value.BlockIDs, 0)
		assert.Nil(t, value.BlockIDs)

		err = db.SetMined(context.Background(), hash, blockID2)
		require.NoError(t, err)

		value, err = db.Get(context.Background(), hash)
		require.NoError(t, err)
		assert.Equal(t, uint64(215), value.Fee)
		assert.Len(t, value.BlockIDs, 1)
		assert.Equal(t, []uint32{blockID2}, value.BlockIDs)
	})
}

func cleanDB(t *testing.T, client *aero.Client, key *aero.Key) {
	policy := util.GetAerospikeWritePolicy(0, 0)
	_, err := client.Delete(policy, key)
	require.NoError(t, err)
}
