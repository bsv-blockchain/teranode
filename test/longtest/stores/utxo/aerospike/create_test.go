package aerospike

import (
	"context"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/bitcoin-sv/teranode/daemon"
	"github.com/bitcoin-sv/teranode/pkg/fileformat"
	"github.com/bitcoin-sv/teranode/pkg/go-batcher"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	teranode_aerospike "github.com/bitcoin-sv/teranode/stores/utxo/aerospike"
	"github.com/bitcoin-sv/teranode/stores/utxo/fields"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// go test -v -tags test_aerospike ./test/...

func TestStore_GetBinsToStore(t *testing.T) {
	s := teranode_aerospike.Store{}
	s.SetUtxoBatchSize(100)
	s.SetSettings(test.CreateBaseTestSettings())

	t.Run("TestStore_GetBinsToStore empty", func(t *testing.T) {
		tx := &bt.Tx{}
		bins, hasUtxos, err := s.GetBinsToStore(tx, 0, nil, nil, nil, false, tx.TxIDChainHash(), false, false, false)
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

		bins, hasUtxos, err := s.GetBinsToStore(tx, 0, nil, nil, nil, false, tx.TxIDChainHash(), false, false, false)
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
			fields.Version.String():        aerospike.NewIntegerValue(int(tx.Version)),
			fields.LockTime.String():       aerospike.NewIntegerValue(int(tx.LockTime)),
			fields.Fee.String():            aerospike.NewIntegerValue(187),
			fields.SizeInBytes.String():    aerospike.NewIntegerValue(tx.Size()),
			fields.ExtendedSize.String():   aerospike.NewIntegerValue(len(tx.ExtendedBytes())),
			fields.SpentUtxos.String():     aerospike.NewIntegerValue(0),
			fields.TotalUtxos.String():     aerospike.NewIntegerValue(2),
			fields.RecordUtxos.String():    aerospike.NewIntegerValue(2),
			fields.TotalExtraRecs.String(): aerospike.NewIntegerValue(0),
			fields.IsCoinbase.String():     aerospike.BoolValue(false),
			fields.Utxos.String(): aerospike.NewListValue([]interface{}{
				aerospike.BytesValue(utxos[0].CloneBytes()),
				aerospike.BytesValue(utxos[1].CloneBytes()),
			}),
			fields.Inputs.String(): aerospike.NewListValue([]interface{}{
				tx.Inputs[0].ExtendedBytes(false),
				tx.Inputs[1].ExtendedBytes(false),
			}),
			fields.Outputs.String(): aerospike.NewListValue([]interface{}{
				tx.Outputs[0].Bytes(),
				tx.Outputs[1].Bytes(),
			}),
			fields.BlockIDs.String():     aerospike.NewValue(blockIDs),
			fields.BlockHeights.String(): aerospike.NewValue(blockHeights),
			fields.SubtreeIdxs.String():  aerospike.NewValue(subtreeIdxs),
			fields.Conflicting.String():  aerospike.BoolValue(false),
			fields.Unspendable.String():  aerospike.BoolValue(false),
		}

		// check the bin values
		for _, v := range bins[0] {
			if _, ok := expectedBinValues[v.Name]; ok {
				assert.Equal(t, expectedBinValues[v.Name], v.Value, "expected %v, got %v, for bin name: %s", expectedBinValues[v.Name], v.Value, v.Name)
			} else {
				t.Errorf("unexpected bin name: %s", v.Name)
			}
		}
	})

	t.Run("TestStore_GetBinsToStore very large", func(t *testing.T) {
		t.Skip("Skipping test with missing tx.")

		// read hex file from os
		txHex, err := os.ReadFile("testdata/337e211af7bcf90470ead4f92910b2990b635dcab8414bf5849f3b1e25800b0c_extended.hex")
		require.NoError(t, err)

		tx, err := bt.NewTxFromString(string(txHex))
		require.NoError(t, err)

		// external should be set by the aerospike create function for huge txs
		external := len(tx.ExtendedBytes()) > teranode_aerospike.MaxTxSizeInStoreInBytes

		bins, hasUtxos, err := s.GetBinsToStore(tx, 0, nil, nil, nil, external, tx.TxIDChainHash(), false, false, false)
		require.NoError(t, err)
		require.NotNil(t, bins)
		require.True(t, hasUtxos)
	})

	t.Run("coinbase tx with conflicting and unspendable", func(t *testing.T) {
		teranode_aerospike.InitPrometheusMetrics()

		// read hex file from os
		txHex, err := os.ReadFile("testdata/fbebcc148e40cb6c05e57c6ad63abd49d5e18b013c82f704601bc4ba567dfb90.hex")
		require.NoError(t, err)

		tx, err := bt.NewTxFromString(string(txHex))
		require.NoError(t, err)

		// external should be set by the aerospike create function for huge txs
		external := len(tx.ExtendedBytes()) > teranode_aerospike.MaxTxSizeInStoreInBytes

		bins, hasUtxos, err := s.GetBinsToStore(tx, 0, nil, nil, nil, external, tx.TxIDChainHash(), true, true, true)
		require.NoError(t, err)
		require.NotNil(t, bins)
		require.True(t, hasUtxos)

		// check the bins
		require.Equal(t, 1, len(bins))
		require.Equal(t, 19, len(bins[0]))

		hasCoinbase := false
		hasConflicting := false
		hasUnspendable := false

		for _, bin := range bins[0] {
			if bin.Name == fields.IsCoinbase.String() {
				hasCoinbase = true
				assert.Equal(t, aerospike.BoolValue(true), bin.Value)
			}
			if bin.Name == fields.Conflicting.String() {
				hasConflicting = true
				assert.Equal(t, aerospike.BoolValue(true), bin.Value)
			}
			if bin.Name == fields.Unspendable.String() {
				hasUnspendable = true
				assert.Equal(t, aerospike.BoolValue(true), bin.Value)
			}
		}

		assert.True(t, hasCoinbase)
		assert.True(t, hasConflicting)
		assert.True(t, hasUnspendable)
	})
}

func TestStore_StoreTransactionExternally(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings()

	client, db, ctx, deferFn := initAerospike(t, tSettings, logger)

	t.Cleanup(func() {
		deferFn()
	})

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

		assert.Equal(t, true, value.Bins[fields.External.String()])
		assert.Nil(t, value.Bins[fields.Inputs.String()])
		assert.Nil(t, value.Bins[fields.Outputs.String()])

		exists, err := s.GetExternalStore().Exists(ctx, bItem.GetTxHash().CloneBytes(), fileformat.FileTypeTx)
		require.NoError(t, err)
		assert.True(t, exists)

		// check that the file does not have a DAH
		dah, err := s.GetExternalStore().GetDAH(ctx, bItem.GetTxHash().CloneBytes(), fileformat.FileTypeTx)
		require.NoError(t, err)
		assert.Equal(t, uint32(0), dah)
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

		assert.Equal(t, true, value.Bins[fields.External.String()])
		assert.Nil(t, value.Bins[fields.Inputs.String()])
		assert.Nil(t, value.Bins[fields.Outputs.String()])

		exists, err := s.GetExternalStore().Exists(ctx, bItem.GetTxHash().CloneBytes(), fileformat.FileTypeTx)
		require.NoError(t, err)
		assert.True(t, exists)

		// check that the file has a DAH
		dah, err := s.GetExternalStore().GetDAH(ctx, bItem.GetTxHash().CloneBytes(), fileformat.FileTypeTx)
		require.NoError(t, err)
		assert.NotEqual(t, uint32(0), dah)
	})
}

func TestStore_StorePartialTransactionExternally(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings()

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)

	t.Cleanup(func() {
		deferFn()
	})

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

		key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), bItem.GetTxHash().CloneBytes())
		require.NoError(t, err)

		value, err := client.Get(util.GetAerospikeReadPolicy(tSettings), key)
		require.NoError(t, err)

		assert.Equal(t, true, value.Bins[fields.External.String()])
		assert.Nil(t, value.Bins[fields.Inputs.String()])
		assert.Nil(t, value.Bins[fields.Outputs.String()])

		exists, err := s.GetExternalStore().Exists(ctx, bItem.GetTxHash().CloneBytes(), fileformat.FileTypeOutputs)
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
	s.SetStoreBatcher(batcher.New(100, 1, sendStoreBatch, true))

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err = s.Create(context.Background(), tx, 0)
		require.NoError(b, err)
	}
}

func TestStore_TwoPhaseCommit(t *testing.T) {
	var td *daemon.TestDaemon
	var err error

	// Retry up to 3 times with random delays to reduce port conflicts
	for attempt := 0; attempt < 3; attempt++ {
		// Add random delay to reduce chance of simultaneous port allocation
		if attempt > 0 {
			delay := time.Duration(100+rand.Intn(500)) * time.Millisecond
			t.Logf("Retrying test setup after delay of %v (attempt %d)", delay, attempt+1)
			time.Sleep(delay)
		}

		// Try to create the test daemon
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Logf("Recovered from panic in TestStore_TwoPhaseCommit setup (attempt %d): %v", attempt+1, r)
				}
			}()

			td = daemon.NewTestDaemon(t, daemon.TestOptions{
				EnableRPC:       true,
				SettingsContext: "dev.system.test",
			})
		}()

		// If successful, break out of retry loop
		if td != nil {
			break
		}
	}

	// If all attempts failed, skip the test
	if td == nil {
		t.Skip("Failed to create test daemon after 3 attempts, likely due to port conflicts")
		return
	}

	defer td.Stop(t)

	// set run state
	err = td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// Generate initial blocks
	_, err = td.CallRPC(td.Ctx, "generate", []interface{}{11})
	require.NoError(t, err)

	block11, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 11)
	require.NoError(t, err)

	// Wait for block assembly to get to block 11
	td.WaitForBlockHeight(t, block11, 10*time.Second)

	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, uint32(1), block1.Height)

	tx := td.CreateTransaction(t, block1.CoinbaseTx)

	txMeta, err := td.UtxoStore.Create(td.Ctx, tx, 0)
	require.NoError(t, err)

	// err = td.PropagationClient.ProcessTransaction(td.Ctx, tx)
	// require.NoError(t, err)

	// data, err := td.UtxoStore.Get(td.Ctx, tx.TxIDChainHash())
	// require.NoError(t, err)

	t.Logf("%v", txMeta)
}
