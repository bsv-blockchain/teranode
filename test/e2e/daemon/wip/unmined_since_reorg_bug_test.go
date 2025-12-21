package smoke

import (
	"context"
	"encoding/hex"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/test"
	"github.com/bsv-blockchain/teranode/test/utils/aerospike"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestUnminedSinceReorgScenarios tests unmined_since field handling during various reorg scenarios
func TestUnminedSinceReorgScenarios(t *testing.T) {
	testCases := []struct {
		name           string
		storeType      string // "sql" or "aerospike"
		scenario       string // "side-to-main"
		expectedBefore uint32 // expected unmined_since before reorg
		expectedAfter  uint32 // expected unmined_since after reorg (0 = NULL)
		onChainBefore  bool   // should be on current chain before reorg
		onChainAfter   bool   // should be on current chain after reorg
	}{
		{
			name:           "SQL: Side to Main",
			storeType:      "sql",
			scenario:       "side-to-main",
			expectedBefore: 1010, // Set when tx in mempool/side chain (varies by coinbase maturity)
			expectedAfter:  0,    // Should be NULL on main chain
			onChainBefore:  false,
			onChainAfter:   true,
		},
		{
			name:           "Aerospike: Side to Main",
			storeType:      "aerospike",
			scenario:       "side-to-main",
			expectedBefore: 1010, // Set when tx in mempool/side chain
			expectedAfter:  0,    // Should be NULL on main chain
			onChainBefore:  false,
			onChainAfter:   true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			SharedTestLock.Lock()
			defer SharedTestLock.Unlock()

			runReorgScenario(t, tc.storeType, tc.scenario, tc.expectedBefore, tc.expectedAfter, tc.onChainBefore, tc.onChainAfter)
		})
	}
}

func runReorgScenario(t *testing.T, storeType, scenario string, expectedBefore, expectedAfter uint32, onChainBefore, onChainAfter bool) {
	const testCoinbaseMaturity = 5

	// Setup Aerospike container if needed
	var aerospikeCleanup func() error
	var aerospikeURL *url.URL

	if storeType == "aerospike" {
		utxoStoreURLStr, teardown, err := aerospike.InitAerospikeContainer()
		require.NoError(t, err, "Failed to setup Aerospike container")
		aerospikeCleanup = teardown
		aerospikeURL, err = url.Parse(utxoStoreURLStr)
		require.NoError(t, err, "Failed to parse Aerospike URL")
		t.Cleanup(func() {
			_ = aerospikeCleanup()
		})
	}

	// Setup test daemon with appropriate store
	testOpts := daemon.TestOptions{
		EnableRPC:       true,
		EnableValidator: true,
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
			func(s *settings.Settings) {
				if s.ChainCfgParams != nil {
					chainParams := *s.ChainCfgParams
					chainParams.CoinbaseMaturity = testCoinbaseMaturity
					s.ChainCfgParams = &chainParams
				}
				s.UtxoStore.UnminedTxRetention = 5

				if storeType == "aerospike" && aerospikeURL != nil {
					s.UtxoStore.UtxoStore = aerospikeURL
				}
			},
		),
		FSMState: blockchain.FSMStateRUNNING,
	}

	td := daemon.NewTestDaemon(t, testOpts)
	defer td.Stop(t, true)

	ctx := context.Background()

	t.Logf("Testing scenario: %s with %s store", scenario, storeType)

	// Step 1: Mine to height > 1000 (required for large reorg path)
	t.Log("Step 1: Mining to height > 1000...")
	var forkPointHeight uint32
	_, err := td.CallRPC(ctx, "generate", []any{1005})
	require.NoError(t, err)

	currentHeight, _, err := td.BlockchainClient.GetBestHeightAndTime(ctx)
	require.NoError(t, err)
	require.Greater(t, currentHeight, uint32(1000), "Height must be > 1000")

	coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, ctx)
	require.NotNil(t, coinbaseTx)

	forkPointHeight, _, err = td.BlockchainClient.GetBestHeightAndTime(ctx)
	require.NoError(t, err)
	t.Logf("Fork point height: %d", forkPointHeight)

	forkPointBlock, err := td.BlockchainClient.GetBlockByHeight(ctx, forkPointHeight)
	require.NoError(t, err)

	// Create test transaction
	testTx := td.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(1, 5000000),
	)
	testTxHash := testTx.TxIDChainHash()
	t.Logf("Created test transaction: %s", testTxHash.String())

	// Send to mempool and wait for it to be processed
	testTxBytes := hex.EncodeToString(testTx.ExtendedBytes())
	_, err = td.CallRPC(ctx, "sendrawtransaction", []any{testTxBytes})
	require.NoError(t, err)
	td.WaitForBlockAssemblyToProcessTx(t, testTxHash.String())

	// Test Side→Main scenario
	testSideToMain(t, td, ctx, testTx, testTxHash, forkPointBlock, forkPointHeight, testCoinbaseMaturity)

	// Test Main→Side scenario using the same daemon (reuses the blockchain state)
	// This is more efficient than creating a new daemon
	testMainToSideAfterSideToMain(t, td, ctx, forkPointHeight, testCoinbaseMaturity)
}

// testSideToMain tests: Transaction mined on side chain → side chain becomes main chain
func testSideToMain(t *testing.T, td *daemon.TestDaemon, ctx context.Context, testTx *bt.Tx, testTxHash *chainhash.Hash,
	forkPointBlock *model.Block, forkPointHeight uint32, testCoinbaseMaturity int) {

	t.Log("=== Testing Side → Main Scenario ===")

	// Mine blocks on main chain WITHOUT test tx
	t.Logf("Mining %d blocks on main chain (without test tx)...", testCoinbaseMaturity+1)
	for i := 0; i < testCoinbaseMaturity+1; i++ {
		_, mainBlock := createTestBlockWithCorrectSubsidy(t, td, forkPointBlock, uint32(10000+i), nil)
		require.NoError(t, td.BlockValidationClient.ProcessBlock(ctx, mainBlock, mainBlock.Height, "", "legacy"))
		forkPointBlock = mainBlock
	}

	mainChainHeight, _, err := td.BlockchainClient.GetBestHeightAndTime(ctx)
	require.NoError(t, err)
	t.Logf("Main chain at height: %d", mainChainHeight)

	// Reset fork point for side chain
	forkPointBlock, err = td.BlockchainClient.GetBlockByHeight(ctx, forkPointHeight)
	require.NoError(t, err)

	// Mine blocks on SIDE chain WITH test tx
	t.Logf("Mining %d blocks on SIDE chain (first contains test tx)...", testCoinbaseMaturity+2)
	td.WaitForBlockAssemblyToProcessTx(t, testTxHash.String())

	_, sideBlock1 := createTestBlockWithCorrectSubsidy(t, td, forkPointBlock, uint32(20000), []*bt.Tx{testTx})
	require.NoError(t, td.BlockValidationClient.ProcessBlock(ctx, sideBlock1, sideBlock1.Height, "", "legacy"))

	// Wait for mined_set background job to complete
	td.WaitForBlockBeingMined(t, sideBlock1)

	// Verify BEFORE reorg
	meta, err := td.UtxoStore.Get(ctx, testTxHash, fields.UnminedSince, fields.BlockIDs)
	require.NoError(t, err)
	require.NotEmpty(t, meta.BlockIDs, "Should have block_id from side chain")

	onChain, err := td.BlockchainClient.CheckBlockIsInCurrentChain(ctx, meta.BlockIDs)
	require.NoError(t, err)
	assert.False(t, onChain, "Transaction should NOT be on main chain before reorg (on side chain)")
	assert.NotEqual(t, uint32(0), meta.UnminedSince, "Transaction should have unmined_since set on side chain")
	t.Logf("BEFORE reorg - unmined_since: %d, onChain: %t", meta.UnminedSince, onChain)

	// Mine remaining side chain blocks to make it longest
	prevBlock := sideBlock1
	for i := 1; i < testCoinbaseMaturity+2; i++ {
		_, sideBlock := createTestBlockWithCorrectSubsidy(t, td, prevBlock, uint32(20000+i), nil)
		require.NoError(t, td.BlockValidationClient.ProcessBlock(ctx, sideBlock, sideBlock.Height, "", "legacy"))
		prevBlock = sideBlock
	}

	// Wait for reorg to complete - verify we're at expected height with expected block
	td.WaitForBlockHeight(t, prevBlock, 30*time.Second)

	// Verify AFTER reorg
	meta, err = td.UtxoStore.Get(ctx, testTxHash, fields.UnminedSince, fields.BlockIDs)
	require.NoError(t, err)

	onChain, err = td.BlockchainClient.CheckBlockIsInCurrentChain(ctx, meta.BlockIDs)
	require.NoError(t, err)

	t.Logf("AFTER reorg - unmined_since: %d, onChain: %t, blockIDs: %v", meta.UnminedSince, onChain, meta.BlockIDs)

	assert.True(t, onChain, "Transaction SHOULD be on main chain after reorg")
	assert.Equal(t, uint32(0), meta.UnminedSince,
		"After side→main reorg, unmined_since should be NULL(0) - Fix #1 ensures this is cleared")
	t.Log("✅ Side→Main test passed")
}

// testMainToSideAfterSideToMain tests: Transaction on main chain gets orphaned
// This reuses the daemon from testSideToMain to avoid re-mining 1005 blocks
func testMainToSideAfterSideToMain(t *testing.T, td *daemon.TestDaemon, ctx context.Context, forkPointHeight uint32, testCoinbaseMaturity int) {
	t.Log("=== Testing Main → Side Scenario ===")

	// Create a new transaction
	currentHeight, _, err := td.BlockchainClient.GetBestHeightAndTime(ctx)
	require.NoError(t, err)

	// Get a spendable coinbase (we should have many from previous mining)
	spendableBlock, err := td.BlockchainClient.GetBlockByHeight(ctx, currentHeight-100)
	require.NoError(t, err)

	testTx2 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(spendableBlock.CoinbaseTx, 0),
		transactions.WithP2PKHOutputs(1, 4000000),
	)
	testTx2Hash := testTx2.TxIDChainHash()
	t.Logf("Created test transaction for Main→Side: %s", testTx2Hash.String())

	// Send to mempool and wait for processing
	testTx2Bytes := hex.EncodeToString(testTx2.ExtendedBytes())
	_, err = td.CallRPC(ctx, "sendrawtransaction", []any{testTx2Bytes})
	require.NoError(t, err)
	td.WaitForBlockAssemblyToProcessTx(t, testTx2Hash.String())

	// Mine block with testTx2 on main chain
	mainBlock := td.MineAndWait(t, 1)

	// Wait for mined_set to complete
	td.WaitForBlockBeingMined(t, mainBlock)

	// Verify BEFORE reorg - tx on main chain
	meta, err := td.UtxoStore.Get(ctx, testTx2Hash, fields.UnminedSince, fields.BlockIDs)
	require.NoError(t, err)
	require.NotEmpty(t, meta.BlockIDs, "Should have block_id from main chain")

	onChain, err := td.BlockchainClient.CheckBlockIsInCurrentChain(ctx, meta.BlockIDs)
	require.NoError(t, err)
	assert.True(t, onChain, "Transaction SHOULD be on main chain before reorg")
	assert.Equal(t, uint32(0), meta.UnminedSince, "unmined_since should be NULL on main chain")
	t.Logf("BEFORE reorg - unmined_since: %d, onChain: %t", meta.UnminedSince, onChain)

	// Get parent of the block we just mined (to create fork)
	parentBlock, err := td.BlockchainClient.GetBlockByHeight(ctx, mainBlock.Height-1)
	require.NoError(t, err)

	// Create a longer side chain WITHOUT testTx2 (orphaning the main chain block)
	t.Logf("Mining %d blocks on side chain to orphan main...", testCoinbaseMaturity+1)
	prevBlock := parentBlock
	for i := 0; i < testCoinbaseMaturity+1; i++ {
		_, sideBlock := createTestBlockWithCorrectSubsidy(t, td, prevBlock, uint32(30000+i), nil)
		require.NoError(t, td.BlockValidationClient.ProcessBlock(ctx, sideBlock, sideBlock.Height, "", "legacy"))
		prevBlock = sideBlock
		t.Logf("Side chain block %d/%d mined at height %d", i+1, testCoinbaseMaturity+1, sideBlock.Height)
	}

	// Wait for reorg to complete - verify we're at expected height
	td.WaitForBlockHeight(t, prevBlock, 30*time.Second)

	// Verify AFTER reorg - tx now on orphaned side chain
	meta, err = td.UtxoStore.Get(ctx, testTx2Hash, fields.UnminedSince, fields.BlockIDs)
	require.NoError(t, err)

	onChain, err = td.BlockchainClient.CheckBlockIsInCurrentChain(ctx, meta.BlockIDs)
	require.NoError(t, err)
	t.Logf("AFTER reorg - unmined_since: %d, onChain: %t, blockIDs: %v", meta.UnminedSince, onChain, meta.BlockIDs)

	assert.False(t, onChain, "Transaction should NOT be on main chain after reorg (orphaned)")
	assert.NotEqual(t, uint32(0), meta.UnminedSince,
		"After main→side reorg, unmined_since should be set - Fix #3 ensures orphaned txs are marked unmined")
	t.Log("✅ Main→Side test passed")
}

// createTestBlockWithCorrectSubsidy creates a test block with the correct block subsidy for the height
func createTestBlockWithCorrectSubsidy(t *testing.T, td *daemon.TestDaemon, previousBlock *model.Block, nonce uint32, txs []*bt.Tx) (*subtree.Subtree, *model.Block) {
	address, err := bscript.NewAddressFromString("1MUMxUTXcPQ1kAqB7MtJWneeAwVW4cHzzp")
	require.NoError(t, err)

	height := previousBlock.Height + 1
	blockSubsidy := util.GetBlockSubsidyForHeight(height, td.Settings.ChainCfgParams)

	totalFees := uint64(0)
	if txs != nil {
		for range txs {
			totalFees += 1000
		}
	}

	coinbaseValue := blockSubsidy + totalFees
	coinbaseTx, err := model.CreateCoinbase(height, coinbaseValue, "test", []string{address.AddressString})
	require.NoError(t, err)

	var (
		merkleRoot *chainhash.Hash
		st         *subtree.Subtree
		subtrees   []*chainhash.Hash
	)

	transactionCount := uint64(len(txs) + 1)
	sizeInBytes := uint64(coinbaseTx.Size())
	for _, tx := range txs {
		sizeInBytes += uint64(tx.Size())
	}

	if len(txs) == 0 {
		merkleRoot = coinbaseTx.TxIDChainHash()
		subtrees = []*chainhash.Hash{}
	} else {
		st, err = subtree.NewIncompleteTreeByLeafCount(len(txs) + 1)
		require.NoError(t, err)

		subtreeData := subtree.NewSubtreeData(st)
		subtreeMeta := subtree.NewSubtreeMeta(st)

		err = st.AddCoinbaseNode()
		require.NoError(t, err)

		for i, tx := range txs {
			err = st.AddNode(*tx.TxIDChainHash(), uint64(i), uint64(i))
			require.NoError(t, err)

			err = subtreeData.AddTx(tx, i+1)
			require.NoError(t, err)

			err = subtreeMeta.SetTxInpointsFromTx(tx)
			require.NoError(t, err)
		}

		rootHash := st.RootHash()

		subtreeBytes, err := st.Serialize()
		require.NoError(t, err)
		err = td.SubtreeStore.Set(td.Ctx, rootHash[:], fileformat.FileTypeSubtreeToCheck, subtreeBytes,
			options.WithDeleteAt(100), options.WithAllowOverwrite(true))
		require.NoError(t, err)

		subtreeDataBytes, err := subtreeData.Serialize()
		require.NoError(t, err)
		err = td.SubtreeStore.Set(td.Ctx, rootHash[:], fileformat.FileTypeSubtreeData, subtreeDataBytes,
			options.WithDeleteAt(100), options.WithAllowOverwrite(true))
		require.NoError(t, err)

		subtreeMetaBytes, err := subtreeMeta.Serialize()
		require.NoError(t, err)
		err = td.SubtreeStore.Set(td.Ctx, rootHash[:], fileformat.FileTypeSubtreeMeta, subtreeMetaBytes,
			options.WithDeleteAt(100), options.WithAllowOverwrite(true))
		require.NoError(t, err)

		merkleRoot, err = st.RootHashWithReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, uint64(coinbaseTx.Size()))
		require.NoError(t, err)

		subtrees = []*chainhash.Hash{st.RootHash()}
	}

	block := &model.Block{
		Subtrees:         subtrees,
		CoinbaseTx:       coinbaseTx,
		TransactionCount: transactionCount,
		SizeInBytes:      sizeInBytes,
		Header: &model.BlockHeader{
			HashPrevBlock:  previousBlock.Header.Hash(),
			HashMerkleRoot: merkleRoot,
			Version:        536870912,
			Timestamp:      uint32(time.Now().Unix()),
			Bits:           previousBlock.Header.Bits,
			Nonce:          nonce,
		},
		Height: height,
	}

	// Mine the block (find valid nonce)
	for {
		ok, _, _ := block.Header.HasMetTargetDifficulty()
		if ok {
			break
		}
		block.Header.Nonce++
	}

	return st, block
}
