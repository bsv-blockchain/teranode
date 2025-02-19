//go:build test_all || test_stores || test_utxo || test_aerospike

package aerospike

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/aerospike/aerospike-client-go/v7"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	teranode_aerospike "github.com/bitcoin-sv/teranode/stores/utxo/aerospike"
	"github.com/bitcoin-sv/teranode/util"
	batcher "github.com/bitcoin-sv/teranode/util/batcher_temp"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/libsv/go-bt/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// go test -v -tags test_aerospike ./test/...

func TestStore_GetBinsToStore(t *testing.T) {
	s := teranode_aerospike.Store{}
	s.SetUtxoBatchSize(100)

	t.Run("TestStore_GetBinsToStore empty", func(t *testing.T) {
		tx := &bt.Tx{}
		bins, hasUtxos, err := s.GetBinsToStore(tx, 0, nil, nil, nil, false, tx.TxIDChainHash(), false, false)
		require.Error(t, err)
		require.Nil(t, bins)
		require.False(t, hasUtxos)
	})

	t.Run("TestStore_GetBinsToStore", func(t *testing.T) {
		teranode_aerospike.InitPrometheusMetrics()

		// read hex file from os
		txHex, err := os.ReadFile("testdata/fbebcc148e40cb6c05e57c6ad63abd49d5e18b013c82f704601bc4ba567dfb90.hex")
		require.NoError(t, err)

		tx, err := bt.NewTxFromString(string(txHex))
		require.NoError(t, err)

		bins, hasUtxos, err := s.GetBinsToStore(tx, 0, nil, nil, nil, false, tx.TxIDChainHash(), false, false)
		require.NoError(t, err)
		require.NotNil(t, bins)
		require.True(t, hasUtxos)

		// check the bins
		require.Equal(t, 1, len(bins))

		utxos, _ := utxo.GetUtxoHashes(tx)

		var blockIDs []uint32
		var blockHeights []uint32
		var subtreeIdxs []int

		expectedBinValues := map[string]aerospike.Value{
			"version":      aerospike.NewIntegerValue(int(tx.Version)),
			"locktime":     aerospike.NewIntegerValue(int(tx.LockTime)),
			"fee":          aerospike.NewIntegerValue(187),
			"sizeInBytes":  aerospike.NewIntegerValue(tx.Size()),
			"extendedSize": aerospike.NewIntegerValue(len(tx.ExtendedBytes())),
			"spentUtxos":   aerospike.NewIntegerValue(0),
			"nrUtxos":      aerospike.NewIntegerValue(2),
			"nrRecords":    aerospike.NewIntegerValue(1),
			"isCoinbase":   aerospike.BoolValue(false),
			"utxos": aerospike.NewListValue([]interface{}{
				aerospike.BytesValue(utxos[0].CloneBytes()),
				aerospike.BytesValue(utxos[1].CloneBytes()),
			}),
			"inputs": aerospike.NewListValue([]interface{}{
				tx.Inputs[0].ExtendedBytes(false),
				tx.Inputs[1].ExtendedBytes(false),
			}),
			"outputs": aerospike.NewListValue([]interface{}{
				tx.Outputs[0].Bytes(),
				tx.Outputs[1].Bytes(),
			}),
			"blockIDs":     aerospike.NewValue(blockIDs),
			"blockHeights": aerospike.NewValue(blockHeights),
			"subtreeIdxs":  aerospike.NewValue(subtreeIdxs),
		}

		// check the bin values
		for _, v := range bins[0] {
			if _, ok := expectedBinValues[v.Name]; ok {
				assert.Equal(t, expectedBinValues[v.Name], v.Value)
			} else {
				t.Errorf("unexpected bin name: %s", v.Name)
			}
		}
	})

	// to run this HUGE tx test, download the tx and put it in the testdata folder
	t.Run("TestStore_GetBinsToStore very large", func(t *testing.T) {
		t.Skip("Skipping test with missing tx.")

		// read hex file from os
		txHex, err := os.ReadFile("testdata/337e211af7bcf90470ead4f92910b2990b635dcab8414bf5849f3b1e25800b0c_extended.hex")
		require.NoError(t, err)

		tx, err := bt.NewTxFromString(string(txHex))
		require.NoError(t, err)

		// external should be set by the aerospike create function for huge txs
		external := len(tx.ExtendedBytes()) > teranode_aerospike.MaxTxSizeInStoreInBytes

		bins, hasUtxos, err := s.GetBinsToStore(tx, 0, nil, nil, nil, external, tx.TxIDChainHash(), false, false)
		require.NoError(t, err)
		require.NotNil(t, bins)
		require.True(t, hasUtxos)
	})
}

func TestStore_StoreTransactionExternally(t *testing.T) {
	ctx := context.Background()
	client, db, _, deferFn := initAerospike(t)

	defer deferFn()

	t.Run("TestStore_StoreTransactionExternally", func(t *testing.T) {
		s := setupStore(t, client)

		tSettings := test.CreateBaseTestSettings()
		s.SetSettings(tSettings)

		teranode_aerospike.InitPrometheusMetrics()

		tx := readTransaction(t, "testdata/fbebcc148e40cb6c05e57c6ad63abd49d5e18b013c82f704601bc4ba567dfb90.hex")
		bItem, binsToStore, hasUtxos := prepareBatchStoreItem(t, s, tx, 0, []uint32{}, []uint32{}, []int{})
		require.True(t, hasUtxos)

		go s.StoreTransactionExternally(ctx, bItem, binsToStore, hasUtxos)

		err := bItem.RecvDone()
		require.NoError(t, err)

		key, err := aerospike.NewKey(db.GetNamespace(), db.GetName(), bItem.GetTxHash().CloneBytes())
		require.NoError(t, err)

		value, err := client.Get(util.GetAerospikeReadPolicy(tSettings), key)
		require.NoError(t, err)

		assert.Equal(t, true, value.Bins["external"])
		assert.Nil(t, value.Bins["inputs"])
		assert.Nil(t, value.Bins["outputs"])

		exists, err := s.GetExternalStore().Exists(ctx, bItem.GetTxHash().CloneBytes(), options.WithFileExtension("tx"))
		require.NoError(t, err)
		assert.True(t, exists)

		// check that the file does not have a TTL
		ttl, err := s.GetExternalStore().GetTTL(ctx, bItem.GetTxHash().CloneBytes(), options.WithFileExtension("tx"))
		require.NoError(t, err)
		assert.Equal(t, time.Duration(0), ttl)
	})

	t.Run("TestStore_StoreTransactionExternally - no utxos", func(t *testing.T) {
		s := setupStore(t, client)

		teranode_aerospike.InitPrometheusMetrics()

		tSettings := test.CreateBaseTestSettings()
		s.SetSettings(tSettings)

		tx := readTransaction(t, "testdata/fbebcc148e40cb6c05e57c6ad63abd49d5e18b013c82f704601bc4ba567dfb90.hex")
		tx.Outputs = []*bt.Output{}
		_ = tx.AddOpReturnOutput([]byte("test"))
		bItem, binsToStore, hasUtxos := prepareBatchStoreItem(t, s, tx, 0, []uint32{}, []uint32{}, []int{})
		require.False(t, hasUtxos)

		go s.StoreTransactionExternally(ctx, bItem, binsToStore, hasUtxos)

		err := bItem.RecvDone()
		require.NoError(t, err)

		key, err := aerospike.NewKey(db.GetNamespace(), db.GetName(), bItem.GetTxHash().CloneBytes())
		require.NoError(t, err)

		value, err := client.Get(util.GetAerospikeReadPolicy(tSettings), key)
		require.NoError(t, err)

		assert.Equal(t, true, value.Bins["external"])
		assert.Nil(t, value.Bins["inputs"])
		assert.Nil(t, value.Bins["outputs"])

		exists, err := s.GetExternalStore().Exists(ctx, bItem.GetTxHash().CloneBytes(), options.WithFileExtension("tx"))
		require.NoError(t, err)
		assert.True(t, exists)

		// check that the file has a TTL
		ttl, err := s.GetExternalStore().GetTTL(ctx, bItem.GetTxHash().CloneBytes(), options.WithFileExtension("tx"))
		require.NoError(t, err)
		assert.NotEqual(t, time.Duration(0), ttl)
	})
}

func TestStore_StorePartialTransactionExternally(t *testing.T) {
	ctx := context.Background()
	client, db, _, deferFn := initAerospike(t)

	defer deferFn()

	t.Run("TestStore_StorePartialTransactionExternally", func(t *testing.T) {
		s := setupStore(t, client)

		tSettings := test.CreateBaseTestSettings()
		s.SetSettings(tSettings)

		teranode_aerospike.InitPrometheusMetrics()

		tx := readTransaction(t, "testdata/fbebcc148e40cb6c05e57c6ad63abd49d5e18b013c82f704601bc4ba567dfb90.hex")
		bItem, binsToStore, hasUtxos := prepareBatchStoreItem(t, s, tx, 0, []uint32{}, []uint32{}, []int{})
		require.True(t, hasUtxos)

		go s.StorePartialTransactionExternally(ctx, bItem, binsToStore, hasUtxos)

		err := bItem.RecvDone()
		require.NoError(t, err)

		key, err := aerospike.NewKey(db.GetNamespace(), db.GetName(), bItem.GetTxHash().CloneBytes())
		require.NoError(t, err)

		value, err := client.Get(util.GetAerospikeReadPolicy(tSettings), key)
		require.NoError(t, err)

		assert.Equal(t, true, value.Bins["external"])
		assert.Nil(t, value.Bins["inputs"])
		assert.Nil(t, value.Bins["outputs"])

		exists, err := s.GetExternalStore().Exists(ctx, bItem.GetTxHash().CloneBytes(), options.WithFileExtension("outputs"))
		require.NoError(t, err)
		assert.True(t, exists)
	})
}

func BenchmarkStore_Create(b *testing.B) {
	teranode_aerospike.InitPrometheusMetrics()

	// read hex file from os
	txHex, err := os.ReadFile("testdata/fbebcc148e40cb6c05e57c6ad63abd49d5e18b013c82f704601bc4ba567dfb90.hex")
	require.NoError(b, err)

	tx, err := bt.NewTxFromString(string(txHex))
	require.NoError(b, err)

	s := &teranode_aerospike.Store{}
	s.SetUtxoBatchSize(100)

	sendStoreBatch := func(batch []*teranode_aerospike.BatchStoreItem) {
		// do nothing
		for _, item := range batch {
			item.SendDone(nil)
		}
	}
	s.SetStoreBatcher(batcher.New[teranode_aerospike.BatchStoreItem](100, 1, sendStoreBatch, true))

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err = s.Create(context.Background(), tx, 0)
		require.NoError(b, err)
	}
}
