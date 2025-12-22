package aerospike_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"log"
	"net/url"
	"os"
	"runtime/pprof"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/netsync"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/file"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/blockchain"
	teranode_aerospike "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/ordishs/go-bitcoin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// go test -v -tags test_aerospike ./test/...

func TestStore_GetTxFromExternalStore(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, _, ctx, deferFn := initAerospike(t, tSettings, logger)

	t.Cleanup(func() {
		deferFn()
	})

	t.Run("TestStore_GetTxFromExternalStore", func(t *testing.T) {
		s := &teranode_aerospike.Store{}
		s.SetExternalStore(memory.New())
		s.SetClient(client)
		s.SetNamespace(aerospikeNamespace)
		s.SetName(aerospikeSet)
		s.SetExternalTxCache(util.NewExpiringConcurrentCache[chainhash.Hash, *bt.Tx](1 * time.Minute))

		// read a sample transaction from testdata and store it in the external store
		f, err := os.ReadFile("testdata/fbebcc148e40cb6c05e57c6ad63abd49d5e18b013c82f704601bc4ba567dfb90.hex")
		require.NoError(t, err)

		txFromFile, err := bt.NewTxFromString(string(f))
		require.NoError(t, err)

		txHash := txFromFile.TxIDChainHash()
		txBytes := txFromFile.Bytes()

		err = s.GetExternalStore().Set(ctx, txHash.CloneBytes(), fileformat.FileTypeTx, txBytes)
		require.NoError(t, err)

		// Test fetching the transaction from the external store
		fetchedTx, err := s.GetTxFromExternalStore(ctx, *txHash)
		require.NoError(t, err)
		require.NotNil(t, fetchedTx)
		require.Equal(t, txFromFile.Version, fetchedTx.Version)
		require.Equal(t, txFromFile.LockTime, fetchedTx.LockTime)
		require.Equal(t, len(txFromFile.Inputs), len(fetchedTx.Inputs))
		require.Equal(t, len(txFromFile.Outputs), len(fetchedTx.Outputs))
		require.Equal(t, txFromFile.Outputs[0].Satoshis, fetchedTx.Outputs[0].Satoshis)
		require.Equal(t, txFromFile.Outputs[0].LockingScript, fetchedTx.Outputs[0].LockingScript)
	})

	t.Run("TestStore_GetTxFromExternalStore concurrent", func(t *testing.T) {
		s := &teranode_aerospike.Store{}
		s.SetExternalStore(memory.New())
		s.SetClient(client)
		s.SetNamespace(aerospikeNamespace)
		s.SetName(aerospikeSet)
		s.SetExternalTxCache(util.NewExpiringConcurrentCache[chainhash.Hash, *bt.Tx](1 * time.Minute))

		// read a sample transaction from testdata and store it in the external store
		f, err := os.ReadFile("testdata/fbebcc148e40cb6c05e57c6ad63abd49d5e18b013c82f704601bc4ba567dfb90.hex")
		require.NoError(t, err)

		txFromFile, err := bt.NewTxFromString(string(f))
		require.NoError(t, err)

		txHash := txFromFile.TxIDChainHash()
		txBytes := txFromFile.Bytes()

		err = s.GetExternalStore().Set(ctx, txHash.CloneBytes(), fileformat.FileTypeTx, txBytes)
		require.NoError(t, err)

		// Test fetching the transaction from the external store concurrently
		g := errgroup.Group{}
		for i := 0; i < 100; i++ {
			g.Go(func() error {
				fetchedTx, err := s.GetOutpointsFromExternalStore(ctx, *txHash)
				if err != nil {
					return err
				}

				require.NotNil(t, fetchedTx)

				return nil
			})
		}

		err = g.Wait()
		require.NoError(t, err)

		// check how often the external store was accessed
		memStore, ok := s.GetExternalStore().(*memory.Memory)
		require.True(t, ok)
		assert.Equal(t, memStore.Counters["set"], 1)
		assert.Equal(t, memStore.Counters["get"], 1)
	})
}

// update with real data to access the bitcoin node
var (
	rpcHost  = "localhost"
	rpcPort  = 8332
	username = "bitcoin"
	password = "bitcoin"
)

// TestGetExternalFromLargeTx simulates how the legacy service would process a large transaction in a block
func TestGetExternalFromLargeTx(t *testing.T) {
	// comment this to run test manually
	t.Skip("Skipping test as it needs a lot of external data to be present in the store")

	// get the block we need 367886 - 0000000000000000096aa43cd0d602b704bfa23f620141eea4006f179d40ce08
	blockHeight := uint32(367886)
	blockHex := "0000000000000000096aa43cd0d602b704bfa23f620141eea4006f179d40ce08"

	runTestGetExternalFromLargeBlock(t, blockHex, blockHeight)
}

// TestGetExternalFromLargeBlock simulates how the legacy service would process a large block
func TestGetExternalFromLargeBlock(t *testing.T) {
	// comment this to run test manually
	t.Skip("Skipping test as it needs a lot of external data to be present in the store")

	// get the block we need 700908 - 00000000000000000fb76af158b8d10896eb719625f45255e3ec11e8cdacb2e7
	blockHeight := uint32(700908)
	blockHex := "00000000000000000fb76af158b8d10896eb719625f45255e3ec11e8cdacb2e7"

	runTestGetExternalFromLargeBlock(t, blockHex, blockHeight)
}

func runTestGetExternalFromLargeBlock(t *testing.T, blockHex string, blockHeight uint32) {
	// ctx := context.Background()

	logger := ulogger.NewErrorTestLogger(t)

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.ChainCfgParams = &chaincfg.MainNetParams
	tSettings.UtxoStore.GetBatcherSize = 8192
	tSettings.UtxoStore.SpendBatcherSize = 8192
	tSettings.BlockAssembly.Disabled = true

	_, store, ctx, deferFn := initAerospike(t, tSettings, logger)

	t.Cleanup(func() {
		deferFn()
	})

	store.SetSettings(tSettings)

	txStoreURL, _ := url.Parse("file://./data/txstore")
	txStore, err := file.New(ulogger.TestLogger{}, txStoreURL)
	if err != nil {
		t.Fatal(err)
	}

	externalStoreURL, _ := url.Parse("file://./data/externalStore")
	externalStore, err := file.New(ulogger.TestLogger{}, externalStoreURL)
	if err != nil {
		t.Fatal(err)
	}

	store.SetExternalStore(externalStore)
	store.SetExternalTxCache(util.NewExpiringConcurrentCache[chainhash.Hash, *bt.Tx](1 * time.Minute))

	_ = store.SetBlockHeight(blockHeight)
	_ = store.SetMedianBlockTime(121233)

	b, err := bitcoin.New(rpcHost, rpcPort, username, password, false)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Getting block %s", blockHex)
	block, err := b.GetBlock(blockHex)
	if err != nil {
		t.Fatal(err)
	}

	parentTxs := make(map[string]struct{})

	txMap := txmap.NewSyncedMap[chainhash.Hash, *netsync.TxMapWrapper]()

	t.Logf("Getting %d transactions from block %s", len(block.Tx), blockHex)
	for idx, txID := range block.Tx {
		t.Logf("Processing %s, %d of %d\r", txID, idx, len(block.Tx))
		if idx == 0 {
			// skip the coinbase
			continue
		}

		tx, err = fetchTransaction(ctx, txStore, b, txID)
		if err != nil {
			t.Fatal(err)
		}

		txMap.Set(*tx.TxIDChainHash(), &netsync.TxMapWrapper{
			Tx: tx,
		})

		if err = ProcessTx(ctx, txStore, b, store, tx, blockHeight, &parentTxs); err != nil {
			t.Fatal(err)
		}
	}

	t.Logf("Extending %d transactions from block %s", len(block.Tx), blockHex)
	g, gCtx := errgroup.WithContext(ctx) // we don't want the tracing to be linked to these calls

	validationClient, err := validator.New(ctx, ulogger.TestLogger{}, store.GetSettings(), store, nil, nil, nil, nil)
	require.NoError(t, err)

	mockBlockchain := &blockchain.MockStore{}
	mockBlockchain.BestBlock = &model.Block{
		Height: blockHeight,
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      1233232,
			Bits:           model.NBit{},
			Nonce:          12333,
		},
	}

	blockchainClient, err := blockchain2.NewLocalClient(ulogger.TestLogger{}, tSettings, mockBlockchain, nil, store)
	require.NoError(t, err)

	sm, err := netsync.New(ctx,
		ulogger.TestLogger{},
		store.GetSettings(),
		blockchainClient,
		validationClient,
		store,
		nil,
		nil,
		nil,
		nil,
		&netsync.Config{
			PeerNotifier:            nil,
			ChainParams:             store.GetSettings().ChainCfgParams,
			DisableCheckpoints:      false,
			MaxPeers:                1,
			MinSyncPeerNetworkSpeed: 1000,
		},
	)
	require.NoError(t, err)

	// we have now cached all transactions (for the next step) and inserted them into the aerospike store
	// extend the transactions in parallel and check for any errors (the real test)
	for idx, txID := range block.Tx {
		if idx == 0 {
			continue
		}

		tx, err := fetchTransaction(ctx, txStore, b, txID)
		if err != nil {
			t.Fatal(err)
		}

		g.Go(func() error {
			if err := sm.ExtendTransaction(gCtx, tx, txMap); err != nil {
				return errors.NewTxError("failed to extend transaction", err)
			}

			return nil
		})
	}

	if err = g.Wait(); err != nil {
		t.Fatal(err)
	}

	fCPU, _ := os.Create("cpu.prof")

	defer fCPU.Close()

	_ = pprof.StartCPUProfile(fCPU)
	defer pprof.StopCPUProfile()

	blockHash, err := chainhash.NewHashFromStr(blockHex)
	if err != nil {
		t.Fatal(err)
	}

	if err = sm.PreValidateTransactions(ctx, txMap, *blockHash, uint32(block.Height)); err != nil {
		t.Fatal(err)
	}

	fMem, _ := os.Create("mem.prof")
	defer fMem.Close()

	_ = pprof.WriteHeapProfile(fMem)
}

func ProcessTx(ctx context.Context, txStore blob.Store, b *bitcoin.Bitcoind, s *teranode_aerospike.Store, tx *bt.Tx,
	blockHeight uint32, parentTxs *map[string]struct{}) (err error) {

	g, gCtx := errgroup.WithContext(ctx)
	util.SafeSetLimit(g, 32)

	parentTxsMu := sync.Mutex{}

	for _, input := range tx.Inputs {
		parentTxsMu.Lock()
		_, ok := (*parentTxs)[input.PreviousTxIDChainHash().String()]
		if !ok {
			// get the parent tx and store in the map
			parentTxID := input.PreviousTxIDChainHash().String()

			(*parentTxs)[parentTxID] = struct{}{}

			g.Go(func() error {
				parentTx, err := fetchTransaction(gCtx, txStore, b, parentTxID)
				if err != nil {
					return err
				}

				// calculate a fake fee to be able to store the parent tx in the store without issues
				neededFee := uint64(1)
				for _, output := range parentTx.Outputs {
					neededFee += output.Satoshis
				}

				// store the parent tx in the aerospike store
				// this will store the transactions externally if applicable
				// set fake fees and input script
				for _, input := range parentTx.Inputs {
					input.PreviousTxSatoshis = neededFee // set the needed fee to the child fee
					input.PreviousTxScript = bscript.NewFromBytes([]byte{0x00})
				}

				if _, err = s.Create(gCtx, parentTx, blockHeight); err != nil {
					log.Fatalf("Failed to store parent tx %s: %s", parentTx.TxIDChainHash().String(), err)
					return err
				}

				return nil
			})
		}
		parentTxsMu.Unlock()
	}

	return g.Wait()
}

func fetchTransaction(ctx context.Context, txStore blob.Store, b *bitcoin.Bitcoind, txIDHex string) (*bt.Tx, error) {
	// try the blob store
	txHash, _ := chainhash.NewHashFromStr(txIDHex)
	txBytes, _ := txStore.Get(ctx, txHash[:], fileformat.FileTypeTx)
	if txBytes != nil {
		return bt.NewTxFromBytes(txBytes)
	}

	rawTx, err := b.GetRawTransaction(txIDHex)
	if err != nil {
		return nil, err
	}

	tx, err := bt.NewTxFromString(rawTx.Hex)
	if err != nil {
		return nil, err
	}

	// store the tx in the blob store
	err = txStore.Set(ctx, txHash[:], fileformat.FileTypeTx, tx.Bytes())
	if err != nil {
		return nil, err
	}

	return tx, nil
}

func TestParseInputReferencesOnly(t *testing.T) {
	t.Run("parses single input correctly", func(t *testing.T) {
		tx := createTestTxWithInputs(t, 1, 100)
		txBytes := tx.ExtendedBytes() // External transactions use Extended Format

		inputs, err := teranode_aerospike.ParseInputReferencesOnly(bytes.NewReader(txBytes))

		require.NoError(t, err)
		require.Len(t, inputs, 1)
		require.NotNil(t, inputs[0].PreviousTxIDChainHash())
		require.Equal(t, uint32(0), inputs[0].PreviousTxOutIndex)
		require.Nil(t, inputs[0].UnlockingScript)
	})

	t.Run("parses multiple inputs", func(t *testing.T) {
		tx := createTestTxWithInputs(t, 10, 50)
		txBytes := tx.ExtendedBytes() // External transactions use Extended Format

		inputs, err := teranode_aerospike.ParseInputReferencesOnly(bytes.NewReader(txBytes))

		require.NoError(t, err)
		require.Len(t, inputs, 10)

		for i, input := range inputs {
			require.NotNil(t, input.PreviousTxIDChainHash())
			require.Equal(t, uint32(i), input.PreviousTxOutIndex)
		}
	})

	t.Run("skips large scripts without allocation", func(t *testing.T) {
		tx := createTestTxWithInputs(t, 2, 1024*100)
		txBytes := tx.ExtendedBytes() // External transactions use Extended Format

		inputs, err := teranode_aerospike.ParseInputReferencesOnly(bytes.NewReader(txBytes))

		require.NoError(t, err)
		require.Len(t, inputs, 2)
	})

	t.Run("handles zero inputs", func(t *testing.T) {
		tx := &bt.Tx{
			Version: 1,
			Inputs:  []*bt.Input{},
			Outputs: []*bt.Output{{Satoshis: 100, LockingScript: &bscript.Script{0x76}}},
		}
		txBytes := tx.ExtendedBytes() // External transactions use Extended Format

		inputs, err := teranode_aerospike.ParseInputReferencesOnly(bytes.NewReader(txBytes))

		require.NoError(t, err)
		require.Len(t, inputs, 0)
	})

	t.Run("error on truncated prevTxID", func(t *testing.T) {
		tx := createTestTxWithInputs(t, 1, 10)
		txBytes := tx.ExtendedBytes() // External transactions use Extended Format

		// Truncate in middle of first input's prevTxID
		truncated := txBytes[:20] // Adjusted for Extended Format marker

		_, err := teranode_aerospike.ParseInputReferencesOnly(bytes.NewReader(truncated))

		require.Error(t, err)
		require.Contains(t, err.Error(), "input")
	})

	t.Run("error on truncated input count", func(t *testing.T) {
		// Just version, no input count
		buf := bytes.NewBuffer(nil)
		err := binary.Write(buf, binary.LittleEndian, uint32(1))
		require.NoError(t, err)

		_, err = teranode_aerospike.ParseInputReferencesOnly(buf)
		require.Error(t, err)
	})

	t.Run("does not read outputs", func(t *testing.T) {
		tx := createTestTxWithInputs(t, 1, 10)

		// Add massive output
		largeScript := make(bscript.Script, 1024*1024*5)
		tx.Outputs = []*bt.Output{{Satoshis: 100, LockingScript: &largeScript}}

		txBytes := tx.ExtendedBytes() // External transactions use Extended Format

		// Should complete without reading the 5MB output
		inputs, err := teranode_aerospike.ParseInputReferencesOnly(bytes.NewReader(txBytes))

		require.NoError(t, err)
		require.Len(t, inputs, 1)
	})
}

func createTestTxWithInputs(t *testing.T, numInputs int, scriptSize int) *bt.Tx {
	tx := &bt.Tx{
		Version: 1,
		Inputs:  make([]*bt.Input, numInputs),
		Outputs: []*bt.Output{{Satoshis: 1000, LockingScript: &bscript.Script{0x76, 0xa9}}},
	}

	for i := 0; i < numInputs; i++ {
		hashBytes := make([]byte, 32)
		binary.BigEndian.PutUint32(hashBytes[28:], uint32(i+1))
		prevHash, err := chainhash.NewHash(hashBytes)
		require.NoError(t, err)

		script := make(bscript.Script, scriptSize)
		for j := range script {
			script[j] = byte(j % 256)
		}

		tx.Inputs[i] = &bt.Input{
			PreviousTxOutIndex: uint32(i),
			UnlockingScript:    &script,
			SequenceNumber:     0xffffffff,
		}
		err = tx.Inputs[i].PreviousTxIDAdd(prevHash)
		require.NoError(t, err)
	}

	return tx
}

// TestParseInputReferencesOnlyWithExtendedFormat tests that ParseInputReferencesOnly correctly
// parses external transactions using the ACTUAL production code path from create.go:869 and get.go:1432.
func TestParseInputReferencesOnlyWithExtendedFormat(t *testing.T) {
	ctx := context.Background()

	// Create a transaction with multiple inputs and Extended Format metadata
	tx := bt.NewTx()

	// Add 3 inputs with Extended Format fields populated
	for i := 0; i < 3; i++ {
		prevTxIDBytes := make([]byte, 32)
		for j := range prevTxIDBytes {
			prevTxIDBytes[j] = byte(i*10 + j)
		}
		prevTxID, err := chainhash.NewHash(prevTxIDBytes)
		require.NoError(t, err)

		unlockingScript, err := bscript.NewFromASM("OP_1 OP_2")
		require.NoError(t, err)

		previousTxScript, err := bscript.NewFromASM("OP_DUP OP_HASH160 OP_3 OP_EQUALVERIFY OP_CHECKSIG")
		require.NoError(t, err)

		input := &bt.Input{
			PreviousTxOutIndex: uint32(i),
			UnlockingScript:    unlockingScript,
			SequenceNumber:     0xffffffff,
			PreviousTxSatoshis: uint64(1000 * (i + 1)), // Extended Format field
			PreviousTxScript:   previousTxScript,       // Extended Format field
		}
		err = input.PreviousTxIDAdd(prevTxID)
		require.NoError(t, err)

		tx.Inputs = append(tx.Inputs, input)
	}

	// Add a dummy output so the transaction is complete
	script, err := bscript.NewFromASM("OP_FALSE OP_RETURN")
	require.NoError(t, err)
	tx.Outputs = append(tx.Outputs, &bt.Output{
		LockingScript: script,
		Satoshis:      0,
	})

	// Create external blob store
	tempDir := t.TempDir()
	u, err := url.Parse("file://" + tempDir)
	require.NoError(t, err)

	externalStore, err := file.New(ulogger.TestLogger{}, u)
	require.NoError(t, err)

	txHash := *tx.TxIDChainHash()

	// Store using EXACT production code from create.go:869
	// This is how external transactions are written in production
	extendedBytes := tx.ExtendedBytes()
	err = externalStore.Set(ctx, txHash[:], fileformat.FileTypeTx, extendedBytes, options.WithDeleteAt(0))
	require.NoError(t, err)

	// Create a minimal Store with external store configured
	store := &teranode_aerospike.Store{}
	store.SetExternalStore(externalStore)
	store.SetLogger(ulogger.TestLogger{})

	// Use the EXACT production code path from get.go:1426 (GetTxInpointsFromExternalStore)
	// This is how external transactions are read when loading unmined transactions
	txInpoints, err := store.GetTxInpointsFromExternalStore(ctx, txHash)
	require.NoError(t, err, "GetTxInpointsFromExternalStore should successfully parse Extended Format")

	// Verify the TxInpoints has the correct parent tx hashes
	parentHashes := txInpoints.GetParentTxHashes()
	require.Equal(t, 3, len(parentHashes), "Should have 3 unique parent tx hashes")

	// Verify each parent tx hash matches the original inputs
	for i := 0; i < 3; i++ {
		expectedHash := tx.Inputs[i].PreviousTxID()
		found := false
		for _, hash := range parentHashes {
			if bytes.Equal(expectedHash, hash[:]) {
				found = true
				break
			}
		}
		require.True(t, found, "Input %d prevTxID should be in parent hashes", i)
	}
}

// TestParseInputReferencesOnlyRejectsNonExtendedFormat verifies that ParseInputReferencesOnly
// returns an error when the transaction is not in extended format.
func TestParseInputReferencesOnlyRejectsNonExtendedFormat(t *testing.T) {
	// Create a simple transaction
	tx := bt.NewTx()

	// Add a single input
	prevTxIDBytes := make([]byte, 32)
	for j := range prevTxIDBytes {
		prevTxIDBytes[j] = byte(j)
	}
	prevTxID, err := chainhash.NewHash(prevTxIDBytes)
	require.NoError(t, err)

	unlockingScript, err := bscript.NewFromASM("OP_1 OP_2")
	require.NoError(t, err)

	input := &bt.Input{
		PreviousTxOutIndex: 0,
		UnlockingScript:    unlockingScript,
		SequenceNumber:     0xffffffff,
	}
	err = input.PreviousTxIDAdd(prevTxID)
	require.NoError(t, err)

	tx.Inputs = append(tx.Inputs, input)

	// Add a dummy output
	script, err := bscript.NewFromASM("OP_FALSE OP_RETURN")
	require.NoError(t, err)
	tx.Outputs = append(tx.Outputs, &bt.Output{
		LockingScript: script,
		Satoshis:      0,
	})

	// Use standard Bytes() instead of ExtendedBytes() - this is NOT extended format
	txBytes := tx.Bytes()

	// Attempt to parse - should fail because it's not in extended format
	reader := bytes.NewReader(txBytes)
	inputs, err := teranode_aerospike.ParseInputReferencesOnly(reader)

	// Verify that we get an error about not being extended format
	require.Error(t, err, "ParseInputReferencesOnly should reject non-extended format transactions")
	require.Nil(t, inputs, "Should return nil inputs on error")
	require.Contains(t, err.Error(), "transaction is not in extended format", "Error message should indicate the transaction is not in extended format")
}
