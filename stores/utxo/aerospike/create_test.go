package aerospike_test

import (
	"context"
	"crypto/rand"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-batcher/v2"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	teranodeaerospike "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	testutil "github.com/bsv-blockchain/teranode/test"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// go test -v -tags test_aerospike ./test/...

func TestStore_GetBinsToStore(t *testing.T) {
	s := teranodeaerospike.Store{}
	s.SetUtxoBatchSize(100)
	s.SetSettings(test.CreateBaseTestSettings(t))

	t.Run("TestStore_GetBinsToStore empty", func(t *testing.T) {
		tx := &bt.Tx{}
		bins, err := s.GetBinsToStore(tx, 0, nil, nil, nil, false, tx.TxIDChainHash(), false, false, false, nil)
		require.Error(t, err)
		require.Nil(t, bins)
	})

	t.Run("TestStore_GetBinsToStore", func(t *testing.T) {
		teranodeaerospike.InitPrometheusMetrics()

		// read a hex file from os
		txHex, err := os.ReadFile("testdata/fbebcc148e40cb6c05e57c6ad63abd49d5e18b013c82f704601bc4ba567dfb90.hex")
		require.NoError(t, err)

		tx, err := bt.NewTxFromString(string(txHex))
		require.NoError(t, err)

		bins, err := s.GetBinsToStore(tx, 0, nil, nil, nil, false, tx.TxIDChainHash(), false, false, false, nil)
		require.NoError(t, err)
		require.NotNil(t, bins)

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
			fields.Locked.String():       aerospike.BoolValue(false),
			fields.TxID.String():         aerospike.BytesValue(tx.TxIDChainHash().CloneBytes()),
		}

		// check the bin values
		for _, v := range bins[0] {
			if _, ok := expectedBinValues[v.Name]; ok {
				assert.Equal(t, expectedBinValues[v.Name], v.Value, "expected %v, got %v, for bin name: %s", expectedBinValues[v.Name], v.Value, v.Name)

				continue
			}

			if v.Name == fields.CreatedAt.String() {
				assert.GreaterOrEqual(t, v.Value, aerospike.NewIntegerValue(0))

				continue
			}

			if v.Name == fields.UnminedSince.String() {
				assert.Equal(t, v.Value, aerospike.NewIntegerValue(0))

				continue
			}

			t.Errorf("unexpected bin name: %s", v.Name)
		}
	})

	t.Run("TestStore_GetBinsToStore very large", func(t *testing.T) {
		t.Skip("Skipping test with missing tx.")

		// read a hex file from os
		txHex, err := os.ReadFile("testdata/337e211af7bcf90470ead4f92910b2990b635dcab8414bf5849f3b1e25800b0c_extended.hex")
		require.NoError(t, err)

		tx, err := bt.NewTxFromString(string(txHex))
		require.NoError(t, err)

		// external should be set by the aerospike create function for huge txs
		external := len(tx.ExtendedBytes()) > teranodeaerospike.MaxTxSizeInStoreInBytes

		bins, err := s.GetBinsToStore(tx, 0, nil, nil, nil, external, tx.TxIDChainHash(), false, false, false, nil)
		require.NoError(t, err)
		require.NotNil(t, bins)
	})

	t.Run("coinbase tx with conflicting and locked", func(t *testing.T) {
		teranodeaerospike.InitPrometheusMetrics()

		// read a hex file from os
		txHex, err := os.ReadFile("testdata/fbebcc148e40cb6c05e57c6ad63abd49d5e18b013c82f704601bc4ba567dfb90.hex")
		require.NoError(t, err)

		tx, err := bt.NewTxFromString(string(txHex))
		require.NoError(t, err)

		// external should be set by the aerospike create function for huge txs
		external := len(tx.ExtendedBytes()) > teranodeaerospike.MaxTxSizeInStoreInBytes

		bins, err := s.GetBinsToStore(tx, 0, nil, nil, nil, external, tx.TxIDChainHash(), true, true, true, nil)
		require.NoError(t, err)
		require.NotNil(t, bins)

		// check the bins
		require.Equal(t, 1, len(bins))
		require.Equal(t, 22, len(bins[0]))

		hasCoinbase := false
		hasConflicting := false
		hasLocked := false

		for _, bin := range bins[0] {
			if bin.Name == fields.IsCoinbase.String() {
				hasCoinbase = true
				assert.Equal(t, aerospike.BoolValue(true), bin.Value)
			}
			if bin.Name == fields.Conflicting.String() {
				hasConflicting = true
				assert.Equal(t, aerospike.BoolValue(true), bin.Value)
			}
			if bin.Name == fields.Locked.String() {
				hasLocked = true
				assert.Equal(t, aerospike.BoolValue(true), bin.Value)
			}
		}

		assert.True(t, hasCoinbase)
		assert.True(t, hasConflicting)
		assert.True(t, hasLocked)
	})
}

// TestStore_GetBinsToStore_UnspendableTransactionExpires verifies that a mined
// transaction with no spendable outputs (e.g. an OP_RETURN-only data-carrier
// transaction) is assigned a deleteAtHeight at creation time so the pruner can
// expire it after the retention window.
//
// Such transactions never transition to "all spent" via the spend path (there
// is nothing spendable to spend), and when they are created already-mined during
// block validation they also bypass setMined - the other place that assigns a
// deleteAtHeight. Without this, the record is never eligible for pruning and is
// retained in the UTXO store forever. We only want truly spendable outputs to be
// retained indefinitely.
func TestStore_GetBinsToStore_UnspendableTransactionExpires(t *testing.T) {
	teranodeaerospike.InitPrometheusMetrics()

	tSettings := test.CreateBaseTestSettings(t)

	retention := tSettings.GetUtxoStoreBlockHeightRetention()
	require.Greater(t, retention, uint32(0), "test requires a non-zero block height retention")

	s := teranodeaerospike.Store{}
	s.SetUtxoBatchSize(100)
	s.SetSettings(tSettings)

	const minedHeight = uint32(1000)

	findBin := func(t *testing.T, bins [][]*aerospike.Bin, name string) (aerospike.Value, bool) {
		t.Helper()
		require.NotEmpty(t, bins)
		for _, b := range bins[0] {
			if b.Name == name {
				return b.Value, true
			}
		}

		return nil, false
	}

	// opReturnOnlyTx builds a transaction whose only output is an OP_RETURN data
	// output, i.e. a transaction with zero spendable outputs (recordUtxos == 0).
	opReturnOnlyTx := func(t *testing.T) *bt.Tx {
		t.Helper()
		txHex, err := os.ReadFile("testdata/fbebcc148e40cb6c05e57c6ad63abd49d5e18b013c82f704601bc4ba567dfb90.hex")
		require.NoError(t, err)
		tx, err := bt.NewTxFromString(string(txHex))
		require.NoError(t, err)

		tx.Outputs = []*bt.Output{}
		require.NoError(t, tx.AddOpReturnOutput([]byte("teranode op_return data carrier")))
		require.False(t, utxo.ShouldStoreOutputAsUTXO(tx.Outputs[0], minedHeight, tSettings.ChainCfgParams.GenesisActivationHeight),
			"sanity: the op_return output must be unspendable")

		return tx
	}

	t.Run("mined unspendable tx is given a deleteAtHeight", func(t *testing.T) {
		tx := opReturnOnlyTx(t)

		bins, err := s.GetBinsToStore(tx, minedHeight,
			[]uint32{1}, []uint32{minedHeight}, []int{0},
			false, tx.TxIDChainHash(), false, false, false, nil)
		require.NoError(t, err)

		dah, ok := findBin(t, bins, fields.DeleteAtHeight.String())
		require.True(t, ok, "a mined tx with no spendable outputs must have a deleteAtHeight so the pruner can expire it")
		assert.Equal(t, aerospike.NewIntegerValue(int(minedHeight+retention)), dah)

		_, hasUnmined := findBin(t, bins, fields.UnminedSince.String())
		assert.False(t, hasUnmined, "a mined tx must not carry an unminedSince marker")
	})

	t.Run("unmined unspendable tx is not expired", func(t *testing.T) {
		tx := opReturnOnlyTx(t)

		bins, err := s.GetBinsToStore(tx, minedHeight,
			nil, nil, nil,
			false, tx.TxIDChainHash(), false, false, false, nil)
		require.NoError(t, err)

		_, ok := findBin(t, bins, fields.DeleteAtHeight.String())
		assert.False(t, ok, "an unmined tx must not be given a deleteAtHeight - it is not in a block yet")

		_, hasUnmined := findBin(t, bins, fields.UnminedSince.String())
		assert.True(t, hasUnmined, "an unmined tx must carry an unminedSince marker")
	})

	t.Run("mined spendable tx is not expired at creation", func(t *testing.T) {
		txHex, err := os.ReadFile("testdata/fbebcc148e40cb6c05e57c6ad63abd49d5e18b013c82f704601bc4ba567dfb90.hex")
		require.NoError(t, err)
		tx, err := bt.NewTxFromString(string(txHex))
		require.NoError(t, err)

		bins, err := s.GetBinsToStore(tx, minedHeight,
			[]uint32{1}, []uint32{minedHeight}, []int{0},
			false, tx.TxIDChainHash(), false, false, false, nil)
		require.NoError(t, err)

		_, ok := findBin(t, bins, fields.DeleteAtHeight.String())
		assert.False(t, ok, "a tx with spendable outputs must only get a deleteAtHeight once those outputs are spent, not at creation")
	})
}

// TestCreateMinedUnspendableGetsDAH is the end-to-end counterpart to
// TestStore_GetBinsToStore_UnspendableTransactionExpires: it drives the real
// Create path with a transaction that is created already-mined (as happens
// during block validation / catchup) and whose only output is an OP_RETURN, and
// asserts the persisted record carries a deleteAtHeight so the pruner can expire
// it. Before the fix this record had no deleteAtHeight and was retained forever.
func TestCreateMinedUnspendableGetsDAH(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	retention := tSettings.GetUtxoStoreBlockHeightRetention()
	require.Greater(t, retention, uint32(0), "test requires a non-zero block height retention")

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)

	teranodeaerospike.InitPrometheusMetrics()

	// Build an OP_RETURN-only transaction: real inputs, a single unspendable
	// (zero spendable outputs) OP_RETURN output.
	txHex, err := os.ReadFile("testdata/fbebcc148e40cb6c05e57c6ad63abd49d5e18b013c82f704601bc4ba567dfb90.hex")
	require.NoError(t, err)
	tx, err := bt.NewTxFromString(string(txHex))
	require.NoError(t, err)
	tx.Outputs = []*bt.Output{}
	require.NoError(t, tx.AddOpReturnOutput([]byte("teranode op_return data carrier")))

	const minedHeight = uint32(1000)

	// Create the transaction already-mined, mirroring the block-validation path.
	_, err = store.Create(ctx, tx, minedHeight, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{
		BlockID: 1, BlockHeight: minedHeight, SubtreeIdx: 0, OnLongestChain: true,
	}))
	require.NoError(t, err)

	key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), tx.TxIDChainHash().CloneBytes())
	require.NoError(t, err)

	response, err := client.Get(nil, key)
	require.NoError(t, err)
	require.NotNil(t, response)

	assert.Equal(t, 0, response.Bins[fields.RecordUtxos.String()], "tx must have no spendable outputs")
	assert.Equal(t, int(minedHeight+retention), response.Bins[fields.DeleteAtHeight.String()],
		"a mined unspendable tx must be assigned deleteAtHeight = minedHeight + retention so the pruner can expire it")
}

func TestStore_StoreTransactionExternally(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, db, ctx, deferFn := initAerospike(t, tSettings, logger)

	t.Cleanup(func() {
		deferFn()
	})

	t.Run("TestStore_StoreTransactionExternally", func(t *testing.T) {
		s := setupStore(t, client)

		tSettings := test.CreateBaseTestSettings(t)
		s.SetSettings(tSettings)

		teranodeaerospike.InitPrometheusMetrics()

		tx := readTransaction(t, "testdata/fbebcc148e40cb6c05e57c6ad63abd49d5e18b013c82f704601bc4ba567dfb90.hex")
		bItem, binsToStore := prepareBatchStoreItem(t, s, tx, 0, []uint32{}, []uint32{}, []int{})

		// bItem.RecvDone() returns as soon as StoreTransactionExternally signals
		// done, but the goroutine still has deferred cleanup to run (releaseLock
		// issues an Aerospike Delete). Wait for the goroutine itself so that
		// work finishes before the outer t.Cleanup closes the client — otherwise
		// Client.Close races with the in-flight Delete on the partition map.
		storeDone := make(chan struct{})
		go func() {
			defer close(storeDone)
			s.StoreTransactionExternally(ctx, bItem, binsToStore)
		}()

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

		// DAH is now managed centrally by pruner service, not by blob stores

		<-storeDone
	})

	t.Run("TestStore_StoreTransactionExternally - no utxos", func(t *testing.T) {
		s := setupStore(t, client)

		teranodeaerospike.InitPrometheusMetrics()

		tSettings := test.CreateBaseTestSettings(t)
		s.SetSettings(tSettings)

		tx := readTransaction(t, "testdata/fbebcc148e40cb6c05e57c6ad63abd49d5e18b013c82f704601bc4ba567dfb90.hex")
		tx.Outputs = []*bt.Output{}
		_ = tx.AddOpReturnOutput([]byte("test"))
		bItem, binsToStore := prepareBatchStoreItem(t, s, tx, 0, []uint32{}, []uint32{}, []int{})

		storeDone := make(chan struct{})
		go func() {
			defer close(storeDone)
			s.StoreTransactionExternally(ctx, bItem, binsToStore)
		}()

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

		// DAH is now managed centrally by pruner service, not by blob stores

		<-storeDone
	})
}

func TestStore_StorePartialTransactionExternally(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)

	t.Cleanup(func() {
		deferFn()
	})

	t.Run("TestStore_StorePartialTransactionExternally", func(t *testing.T) {
		s := setupStore(t, client)

		tSettings := test.CreateBaseTestSettings(t)
		s.SetSettings(tSettings)

		teranodeaerospike.InitPrometheusMetrics()

		tx := readTransaction(t, "testdata/fbebcc148e40cb6c05e57c6ad63abd49d5e18b013c82f704601bc4ba567dfb90.hex")
		bItem, binsToStore := prepareBatchStoreItem(t, s, tx, 0, []uint32{}, []uint32{}, []int{})

		storeDone := make(chan struct{})
		go func() {
			defer close(storeDone)
			s.StorePartialTransactionExternally(ctx, bItem, binsToStore)
		}()

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

		<-storeDone
	})
}

func BenchmarkStore_Create(b *testing.B) {
	teranodeaerospike.InitPrometheusMetrics()

	// read a hex file from os
	txHex, err := os.ReadFile("testdata/fbebcc148e40cb6c05e57c6ad63abd49d5e18b013c82f704601bc4ba567dfb90.hex")
	require.NoError(b, err)

	tx, err := bt.NewTxFromString(string(txHex))
	require.NoError(b, err)

	s := &teranodeaerospike.Store{}
	s.SetUtxoBatchSize(100)

	sendStoreBatch := func(batch []*teranodeaerospike.BatchStoreItem) {
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
		// Add random delay to reduce the chance of simultaneous port allocation
		if attempt > 0 {
			cryptoRand := make([]byte, 2)
			_, err := rand.Read(cryptoRand)
			if err != nil {
				t.Fatalf("failed to generate random delay: %v", err)
			}
			delay := time.Duration(100+int(cryptoRand[0])%500) * time.Millisecond
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
				EnableRPC: true,
				SettingsOverrideFunc: testutil.ComposeSettings(
					testutil.SystemTestSettings(),
					func(s *settings.Settings) {
						s.Validator.UseLocalValidator = true
						s.TracingEnabled = true
						s.TracingSampleRate = 1.0
					},
				),
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

	// Generate 11 blocks directly via BlockAssemblyClient (avoids RPC catchup issues)
	err = td.BlockAssemblyClient.GenerateBlocks(td.Ctx, &blockassembly_api.GenerateBlocksRequest{Count: 11})
	require.NoError(t, err)

	// Fetch block 1 for the coinbase transaction
	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, uint32(1), block1.Height)

	tx := td.CreateTransaction(t, block1.CoinbaseTx)

	txMeta, _, err := td.UtxoStore.SpendAndCreate(td.Ctx, tx, 0, utxo.WithCreateOnly())
	require.NoError(t, err)

	// err = td.PropagationClient.ProcessTransaction(td.Ctx, tx)
	// require.NoError(t, err)

	// data, err := td.UtxoStore.Get(td.Ctx, tx.TxIDChainHash())
	// require.NoError(t, err)

	t.Logf("%v", txMeta)
}

// TestSpendAndCreateKeepsSpendsWhenCreateInDoubt runs a create whose write
// lands on the server while the client reports it in doubt, as a timeout after
// sending does. SpendAndCreate must keep the parent coin spent: the
// transaction is stored, and unspending its input would let a second
// transaction spend the coin while the first stands, which a repeat never
// heals because a repeat drops a transaction whose record exists.
func TestSpendAndCreateKeepsSpendsWhenCreateInDoubt(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)

	_, _, err := store.SpendAndCreate(ctx, tests.Tx, 1000, utxo.WithCreateOnly())
	require.NoError(t, err)

	in := tests.Tx.Outputs[0]

	child := bt.NewTx()
	require.NoError(t, child.From(tests.Tx.TxIDChainHash().String(), 0, in.LockingScript.String(), in.Satoshis))
	child.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x00, 0x48, 0x30, 0x45})
	require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", in.Satoshis-1_000))

	// Run every batch for real, then report a batch that writes a record as
	// in doubt. The spends go through as UDF calls and are left alone.
	store.SetBatchOperateFn(func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
		realErr := client.BatchOperate(policy, records)

		for _, rec := range records {
			if _, isWrite := rec.(*aerospike.BatchWrite); isWrite {
				return &aerospike.AerospikeError{ResultCode: types.TIMEOUT, InDoubt: true}
			}
		}

		return realErr
	})
	t.Cleanup(func() { store.SetBatchOperateFn(nil) })

	_, spends, err := store.SpendAndCreate(ctx, child, store.GetBlockHeight()+1)
	require.Error(t, err)
	require.True(t, utxo.IsCreateOutcomeUnknown(err), "an in-doubt create must leave the outcome unknown: %v", err)
	require.Len(t, spends, 1, "the spends stay in effect and are returned")

	store.SetBatchOperateFn(nil)

	_, err = store.Get(ctx, child.TxIDChainHash())
	require.NoError(t, err, "the write landed, so the transaction is stored")

	utxoHash, err := util.UTXOHashFromOutput(tests.Tx.TxIDChainHash(), in, 0)
	require.NoError(t, err)

	resp, err := store.GetSpend(ctx, &utxo.Spend{TxID: tests.Tx.TxIDChainHash(), Vout: 0, UTXOHash: utxoHash})
	require.NoError(t, err)
	require.NotNil(t, resp.SpendingData, "the parent coin must stay spent by the stored transaction")
	require.Equal(t, child.TxIDChainHash().String(), resp.SpendingData.TxID.String())
}

// TestCreateMinedReportsCreatingNotCleared covers a paginated transaction
// created already mined, as quick validation creates every transaction of a
// block below the checkpoint, whose clear of the creating bin fails. Nothing
// repeats a create that returned success, and setMined does not run for a
// transaction created mined unless it is found existing, so a success there
// could leave the records with the creating bin and every output on them
// refused with CREATING. The create must return a retryable error and keep the
// spends; a repeat must finish the clear.
func TestCreateMinedReportsCreatingNotCleared(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	const (
		blockHeight = 604315
		blockID     = 11
		// The parent spends the zero hash, so the store takes it for a
		// coinbase; spend it well past maturity.
		spendHeight = blockHeight + 1_000
	)

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	// Batch size 4 and 10 outputs: the master holds vouts 0-3, extra record 1
	// holds 4-7, extra record 2 holds 8-9.
	tSettings.UtxoStore.UtxoBatchSize = 4

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)

	// failClears fails the next n clears of the creating bin. A clear is the
	// only UPDATE_ONLY batch write a create sends; it fails before anything
	// is sent, so the records keep the bin. The external write runs in its own
	// goroutine beside the store's other batches, so the count is atomic.
	failClears := func(n int32) {
		var left atomic.Int32

		left.Store(n)

		store.SetBatchOperateFn(func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
			if len(records) > 0 {
				if write, ok := records[0].(*aerospike.BatchWrite); ok && write.Policy != nil &&
					write.Policy.RecordExistsAction == aerospike.UPDATE_ONLY && left.Add(-1) >= 0 {
					return &aerospike.AerospikeError{ResultCode: types.SERVER_NOT_AVAILABLE}
				}
			}

			return client.BatchOperate(policy, records)
		})
		t.Cleanup(func() { store.SetBatchOperateFn(nil) })
	}

	hasCreatingBin := func(t *testing.T, tx *bt.Tx, recordIdx uint32) bool {
		t.Helper()

		key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), uaerospike.CalculateKeySourceInternal(tx.TxIDChainHash(), recordIdx))
		require.NoError(t, err)

		rec, err := client.Get(nil, key)
		require.NoError(t, err)
		require.NotNil(t, rec, "record %d of %s must exist", recordIdx, tx.TxIDChainHash())

		_, ok := rec.Bins[fields.Creating.String()]

		return ok
	}

	spendVout := func(parent *bt.Tx, vout uint32) error {
		out := parent.Outputs[vout]

		child := bt.NewTx()
		require.NoError(t, child.From(parent.TxIDChainHash().String(), vout, out.LockingScript.String(), out.Satoshis))
		child.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x00, 0x48, 0x30, 0x45})
		require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", out.Satoshis-100))

		_, err := store.Spend(ctx, child, spendHeight)

		return err
	}

	mined := utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: blockID, BlockHeight: blockHeight})

	requireCreatingNotCleared := func(t *testing.T, err error) {
		t.Helper()

		require.Error(t, err)
		require.NotErrorIs(t, err, errors.ErrTxExists, "a failed clear must not read as a finished create")
		require.True(t, errors.IsRetryableError(err), "the caller must repeat the create: %v", err)
		require.True(t, utxo.IsCreateOutcomeUnknown(err), "the records are stored, so the spends must stay: %v", err)
	}

	requireAllCleared := func(t *testing.T, parent *bt.Tx) {
		t.Helper()

		for idx := uint32(0); idx <= 2; idx++ {
			require.Falsef(t, hasCreatingBin(t, parent, idx), "record %d must lose the creating bin", idx)
		}

		// One output on each record: master, extra 1, extra 2.
		for _, vout := range []uint32{0, 5, 9} {
			require.NoErrorf(t, spendVout(parent, vout), "vout %d must be spendable", vout)
		}
	}

	t.Run("repeat finishes the clear", func(t *testing.T) {
		cleanDB(t, client)

		parent := createTransactionWithOutputs(10)

		failClears(1)

		_, err := store.Create(ctx, parent, blockHeight, mined)
		requireCreatingNotCleared(t, err)
		require.ErrorContains(t, err, "failed to unlock child records", "the error carries the clear's own failure")

		require.True(t, hasCreatingBin(t, parent, 2), "the failed clear leaves the bin on the extra record")
		require.ErrorIs(t, spendVout(parent, 9), errors.ErrTxCreating, "an output on the extra record is refused until the clear is done")

		_, err = store.Create(ctx, parent, blockHeight, mined)
		require.ErrorIs(t, err, errors.ErrTxExists, "the repeat finds the records and finishes the clear")

		requireAllCleared(t, parent)
	})

	t.Run("repeat whose clear fails reports it again", func(t *testing.T) {
		cleanDB(t, client)

		parent := createTransactionWithOutputs(10)

		failClears(2)

		_, err := store.Create(ctx, parent, blockHeight, mined)
		requireCreatingNotCleared(t, err)

		// The repeat finds the master record; its failed clear must not come
		// back as ErrTxExists, which the caller reads as done.
		_, err = store.Create(ctx, parent, blockHeight, mined)
		requireCreatingNotCleared(t, err)

		_, err = store.Create(ctx, parent, blockHeight, mined)
		require.ErrorIs(t, err, errors.ErrTxExists)

		requireAllCleared(t, parent)
	})

	t.Run("create with no mined info still succeeds", func(t *testing.T) {
		cleanDB(t, client)

		parent := createTransactionWithOutputs(10)

		// A validator create keeps its recovery paths: the transaction is seen
		// again in a subtree, or setMined runs when its block is mined.
		failClears(1)

		_, err := store.Create(ctx, parent, blockHeight)
		require.NoError(t, err)
		require.True(t, hasCreatingBin(t, parent, 2))
	})

	t.Run("repeat with no mined info whose clear fails still reports exists", func(t *testing.T) {
		cleanDB(t, client)

		parent := createTransactionWithOutputs(10)

		failClears(2)

		_, err := store.Create(ctx, parent, blockHeight)
		require.NoError(t, err)

		// The repeat path keeps the same gate as the first write: only a
		// mined create turns a failed clear into an error.
		_, err = store.Create(ctx, parent, blockHeight)
		require.ErrorIs(t, err, errors.ErrTxExists)
		require.True(t, hasCreatingBin(t, parent, 2))
	})

	t.Run("held lock is not reported as exists to a mined create", func(t *testing.T) {
		cleanDB(t, client)

		parent := createTransactionWithOutputs(10)

		// A lock record left in place, as when an earlier attempt's release
		// failed: it lives out its TTL, and a repeat inside that window never
		// reaches the clear.
		lockKey, keyErr := aerospike.NewKey(store.GetNamespace(), store.GetName(),
			uaerospike.CalculateKeySourceInternal(parent.TxIDChainHash(), teranodeaerospike.LockRecordIndex))
		require.NoError(t, keyErr)
		require.NoError(t, client.PutBins(nil, lockKey, aerospike.NewBin("lock_token", "left-behind")))

		_, err := store.Create(ctx, parent, blockHeight, mined)
		requireCreatingNotCleared(t, err)
		require.ErrorContains(t, err, "transaction creation in progress or already exists", "the error carries the lock's own answer")

		// Control: a validator create keeps today's answer.
		_, err = store.Create(ctx, parent, blockHeight)
		require.ErrorIs(t, err, errors.ErrTxExists)

		deleted, delErr := client.Delete(nil, lockKey)
		require.Nil(t, delErr)
		require.True(t, deleted)

		_, err = store.Create(ctx, parent, blockHeight, mined)
		require.NoError(t, err, "once the lock is gone the repeat writes and clears the records")

		requireAllCleared(t, parent)
	})

	t.Run("lock write that fails for another reason is not outcome-unknown", func(t *testing.T) {
		cleanDB(t, client)

		parent := createTransactionWithOutputs(10)

		// The lock write fails before this call writes any record, so its error
		// is not marked outcome-unknown and the caller may unspend. Only a held
		// lock turns into the outcome-unknown error. (If an earlier attempt had
		// already stored the records, that unspend would leave them with live
		// inputs until a repeat re-spends them; that is unchanged from main.)
		var lockKey *aerospike.Key

		store.SetLockPutFn(func(policy *aerospike.WritePolicy, key *aerospike.Key, bins ...*aerospike.Bin) aerospike.Error {
			if policy == nil || key == nil || len(bins) == 0 {
				return &aerospike.AerospikeError{ResultCode: types.PARAMETER_ERROR}
			}

			lockKey = key

			return &aerospike.AerospikeError{ResultCode: types.TIMEOUT}
		})
		t.Cleanup(func() { store.SetLockPutFn(nil) })

		_, err := store.Create(ctx, parent, blockHeight, mined)
		require.Error(t, err)
		require.NotErrorIs(t, err, errors.ErrTxExists)
		require.False(t, utxo.IsCreateOutcomeUnknown(err), "nothing was written, so the outcome is known: %v", err)
		require.ErrorContains(t, err, "failed to acquire lock")
		require.NotNil(t, lockKey, "the lock write went through the hook")

		lockRec, getErr := client.Get(nil, lockKey)
		require.Nil(t, lockRec, "the failed lock write leaves no lock record")
		require.NotNil(t, getErr)

		store.SetLockPutFn(nil)

		_, err = store.Get(ctx, parent.TxIDChainHash())
		require.ErrorIs(t, err, errors.ErrTxNotFound, "the failed lock write stores nothing")
	})
}
