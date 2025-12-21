package subtreevalidation

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	stv "github.com/bsv-blockchain/teranode/services/subtreevalidation"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/settings"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blockchain/sql"
	"github.com/bsv-blockchain/teranode/stores/txmetacache"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	utxometa "github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/kafka" //nolint:gci
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/require"
)

// go test -v -tags test_subtreevalidation ./test/...

// testUtxoStore is a simple test implementation that returns proper metadata
type testUtxoStore struct {
	utxo.MockUtxostore
	txMetaMap map[string]*utxometa.Data
}

func (t *testUtxoStore) GetMeta(ctx context.Context, hash *chainhash.Hash) (*utxometa.Data, error) {
	if meta, ok := t.txMetaMap[hash.String()]; ok {
		return meta, nil
	}
	// Return generic metadata as fallback
	return &utxometa.Data{
		Fee:         100,
		SizeInBytes: 200,
		TxInpoints:  subtreepkg.TxInpoints{},
		IsCoinbase:  false,
		Conflicting: false,
	}, nil
}

func (t *testUtxoStore) Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) (*utxometa.Data, error) {
	return &utxometa.Data{Fee: 100, SizeInBytes: 200}, nil
}

func (t *testUtxoStore) GetBlockHeight() uint32 {
	return 100
}

func (t *testUtxoStore) BatchDecorate(ctx context.Context, unresolvedMetaDataSlice []*utxo.UnresolvedMetaData, fields ...fields.FieldName) error {
	// BatchDecorate is called to fill in metadata for transactions
	// We need to populate the metadata from our txMetaMap
	for _, unresolvedMeta := range unresolvedMetaDataSlice {
		if meta, ok := t.txMetaMap[unresolvedMeta.Hash.String()]; ok {
			unresolvedMeta.Data = meta
		} else {
			// Create metadata with proper TxInpoints if not found
			unresolvedMeta.Data = &utxometa.Data{
				Fee:         100,
				SizeInBytes: 200,
				TxInpoints:  subtreepkg.TxInpoints{},
				IsCoinbase:  false,
				Conflicting: false,
			}
		}
	}
	return nil
}

func TestBlockValidationValidateBigSubtree(t *testing.T) {
	// Skip this test if running with short timeout - it's a performance test that takes several minutes
	if testing.Short() {
		t.Skip("Skipping long-running performance test in short mode")
	}

	stv.InitPrometheusMetrics()

	// Map to store metadata for each transaction - will be populated during transaction creation
	txMetaMap := make(map[string]*utxometa.Data)

	// Use a custom test UTXO store that can return proper metadata
	testStore := &testUtxoStore{
		txMetaMap: txMetaMap,
	}

	validatorClient := &validator.MockValidatorClient{UtxoStore: testStore}

	txStore := blobmemory.New()
	subtreeStore := blobmemory.New()

	deferFunc := func() {
		httpmock.DeactivateAndReset()
	}
	defer deferFunc()

	nilConsumer := &kafka.KafkaConsumerGroup{}
	tSettings := test.CreateBaseTestSettings(t)
	logger := ulogger.TestLogger{}

	// Create a real SQLite in-memory blockchain store
	storeURL, err := url.Parse("sqlitememory:///blockchain")
	require.NoError(t, err)
	blockchainStore, err := sql.New(logger, storeURL, tSettings)
	require.NoError(t, err)

	// Create a blockchain client with the real store
	blockchainClient, err := blockchain.NewLocalClient(logger, tSettings, blockchainStore, nil, nil)
	require.NoError(t, err)

	// Activate httpmock for HTTP mocking
	httpmock.ActivateNonDefault(util.HTTPClient())

	subtreeValidation, err := stv.New(context.Background(), ulogger.TestLogger{}, tSettings, subtreeStore, txStore, testStore, validatorClient, blockchainClient, nilConsumer, nilConsumer, nil)
	require.NoError(t, err)

	// Use cached UTXO store for better performance
	if utxoStore, err := txmetacache.NewTxMetaCache(context.Background(), settings.NewSettings(), ulogger.TestLogger{}, testStore, txmetacache.Unallocated, 2048); err == nil {
		subtreeValidation.SetUutxoStore(utxoStore)
	} else {
		// If cache creation fails, just use the test store directly
		t.Logf("Failed to create cached UTXO store, using test store directly: %v", err)
	}

	// This is a performance test with 1 million items - reduce for CI/quick tests
	numberOfItems := 1_024 * 1_024 // 2^10 * 2^10 = 2^20 = 1,048,576
	if os.Getenv("QUICK_TEST") == "1" {
		numberOfItems = 1024 // 2^10 = 1024, much smaller for quick tests
		t.Logf("Running with reduced item count: %d (QUICK_TEST=1)", numberOfItems)
	}

	subtree, err := subtreepkg.NewTreeByLeafCount(numberOfItems)
	require.NoError(t, err)

	// For performance optimization with large tests (1M items), we'll simplify the transaction creation
	// to avoid the overhead of creating complex transaction structures

	// Pre-allocate slices for better performance
	txHashes := make([]chainhash.Hash, 0, numberOfItems)

	// Create a simple transaction template that we can reuse with modifications
	templateTx := bt.NewTx()
	templateTx.Inputs = []*bt.Input{{
		PreviousTxOutIndex: 0,
		PreviousTxSatoshis: 0,
		UnlockingScript:    bscript.NewFromBytes([]byte{}),
		SequenceNumber:     0xfffffffe,
	}}
	_ = templateTx.Inputs[0].PreviousTxIDAdd(&chainhash.Hash{})
	_ = templateTx.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000)

	// Create transactions and add them to the subtree
	for i := 0; i < numberOfItems; i++ {
		// Create a simple transaction with a unique identifier
		tx := bt.NewTx()
		tx.Version = uint32(i) // Use version as a unique identifier

		// Add a simple input - for testing purposes, we don't need complex parent relationships
		tx.Inputs = []*bt.Input{{
			PreviousTxOutIndex: uint32(i % 10),
			PreviousTxSatoshis: 1000,
			UnlockingScript:    bscript.NewFromBytes([]byte{byte(i & 0xFF)}),
			SequenceNumber:     0xfffffffe,
		}}

		if i > 0 {
			// Reference a previous transaction to create some structure
			parentIdx := i - 1
			if parentIdx < len(txHashes) {
				_ = tx.Inputs[0].PreviousTxIDAdd(&txHashes[parentIdx])
			} else {
				_ = tx.Inputs[0].PreviousTxIDAdd(&chainhash.Hash{})
			}
		} else {
			_ = tx.Inputs[0].PreviousTxIDAdd(&chainhash.Hash{})
		}

		// Add outputs
		_ = tx.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000)
		_ = tx.AddOpReturnOutput([]byte(fmt.Sprintf("tx%d", i)))

		txHash := *tx.TxIDChainHash()
		txHashes = append(txHashes, txHash)

		// Create TxInpoints from the transaction
		// This properly captures the parent references from the inputs
		txInpoints, err := subtreepkg.NewTxInpointsFromTx(tx)
		require.NoError(t, err)

		// Create metadata for this transaction
		txMeta := &utxometa.Data{
			Fee:         100,
			SizeInBytes: uint64(200), // Use a fixed size for performance
			TxInpoints:  txInpoints,
			IsCoinbase:  false,
			Conflicting: false,
		}
		txMetaMap[txHash.String()] = txMeta

		// Add to subtree with proper fee and size values
		require.NoError(t, subtree.AddNode(txHash, 100, 200))

		// Store transaction in blob store so it can be retrieved during validation
		err = txStore.Set(context.Background(), txHash[:], fileformat.FileTypeTx, tx.ExtendedBytes())
		require.NoError(t, err)
	}

	// The test store will handle returning the proper metadata

	nodeBytes, err := subtree.SerializeNodes()
	require.NoError(t, err)

	// this calculation should not be in the test data, in the real world we would be getting this from the other miner
	rootHash := subtree.RootHash()

	// Store the subtree in the subtree store so it's considered already validated
	// This bypasses the validation logic and tests the performance of the lookup
	err = subtreeStore.Set(context.Background(), rootHash[:], fileformat.FileTypeSubtree, nodeBytes)
	require.NoError(t, err)

	// Create subtree data with all transactions
	subtreeData := subtreepkg.NewSubtreeData(subtree)
	// Add all transactions to the subtree data
	for i, hash := range txHashes {
		// Get the transaction from the store
		txBytes, err := txStore.Get(context.Background(), hash[:], fileformat.FileTypeTx)
		require.NoError(t, err)
		tx, err := bt.NewTxFromBytes(txBytes)
		require.NoError(t, err)
		err = subtreeData.AddTx(tx, i)
		require.NoError(t, err)
	}

	subtreeDataBytes, err := subtreeData.Serialize()
	require.NoError(t, err)

	httpmock.RegisterResponder(
		"GET",
		`=~^/subtree/[a-z0-9]+\z`,
		httpmock.NewBytesResponder(200, nodeBytes),
	)

	// Register responders for missing transactions and subtree data
	httpmock.RegisterResponder(
		"POST",
		`=~^/subtree/[a-z0-9]+/txs\z`,
		httpmock.NewStringResponder(200, "[]"), // Return empty array - no transactions found
	)

	httpmock.RegisterResponder(
		"GET",
		`=~^/subtree_data/[a-z0-9]+\z`,
		httpmock.NewBytesResponder(200, subtreeDataBytes), // Return the actual subtree data
	)

	f, _ := os.Create("cpu.prof")
	defer f.Close()

	_ = pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	start := time.Now()

	v := stv.ValidateSubtree{
		SubtreeHash:   *rootHash,
		BaseURL:       "http://localhost:8000",
		TxHashes:      nil, // Don't pass hashes - let it find the subtree is already stored
		AllowFailFast: false,
	}

	_, err = subtreeValidation.ValidateSubtreeInternal(context.Background(), v, chaincfg.GenesisActivationHeight, nil)
	require.NoError(t, err)

	t.Logf("Time taken: %s\n", time.Since(start))

	f, _ = os.Create("mem.prof")
	defer f.Close()
	_ = pprof.WriteHeapProfile(f)
}
