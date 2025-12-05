package smoke

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/test"
	helper "github.com/bsv-blockchain/teranode/test/utils"
	"github.com/stretchr/testify/require"
)

// Reasonable timeout for block synchronization
var blockWait = 30 * time.Second

func TestMoveUp(t *testing.T) {
	t.Skip("Skipping until p2p client is refactored")
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()
	node2 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC: true,
		EnableP2P: true,
		// EnableFullLogging: true,
		FSMState: blockchain.FSMStateRUNNING,
	})
	defer node2.Stop(t, true)

	_, err := node2.CallRPC(node2.Ctx, "generate", []any{1})
	require.NoError(t, err)

	node1 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC: true,
		EnableP2P: true,
		// EnableFullLogging: true,
		FSMState: blockchain.FSMStateRUNNING,
	})
	defer node1.Stop(t, true)

	// connect node2 to node1 via p2p
	node2.ConnectToPeer(t, node1)

	// wait for node1 to catchup to block 1
	err = helper.WaitForNodeBlockHeight(t.Context(), node1.BlockchainClient, 1, blockWait)
	require.NoError(t, err)

	// generate 1 block on node1
	_, err = node1.CallRPC(node1.Ctx, "generate", []any{1})
	require.NoError(t, err)

	// verify block height on node1
	err = helper.WaitForNodeBlockHeight(t.Context(), node1.BlockchainClient, 2, blockWait)
	require.NoError(t, err)

	// verify block height on node2
	err = helper.WaitForNodeBlockHeight(t.Context(), node2.BlockchainClient, 2, blockWait)
	require.NoError(t, err)
}

func TestMoveDownMoveUpWhenNewBlockIsGenerated(t *testing.T) {
	t.Skip("Skipping until p2p client is refactored")
	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()
	node2 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableP2P:       true,
		EnableValidator: true,
		// EnableFullLogging: true,
		SettingsOverrideFunc: func(s *settings.Settings) {
			s.BlockValidation.SecretMiningThreshold = 9999
			// Create a copy to avoid race conditions
			if s.ChainCfgParams != nil {
				chainParams := *s.ChainCfgParams
				chainParams.CoinbaseMaturity = 4
				s.ChainCfgParams = &chainParams
			}
		},
		FSMState: blockchain.FSMStateRUNNING,
	})
	defer node2.Stop(t, true)

	// mine 3 blocks on node2
	_, err := node2.CallRPC(node2.Ctx, "generate", []any{3})
	require.NoError(t, err)

	// verify blockheight on node2
	err = helper.WaitForNodeBlockHeight(t.Context(), node2.BlockchainClient, 3, blockWait)
	require.NoError(t, err)

	// mine 2 blocks on node1
	node1 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableP2P:       true,
		EnableValidator: true,
		// EnableFullLogging: true,
		SettingsOverrideFunc: func(s *settings.Settings) {
			s.BlockValidation.SecretMiningThreshold = 9999
			// Create a copy to avoid race conditions
			if s.ChainCfgParams != nil {
				chainParams := *s.ChainCfgParams
				chainParams.CoinbaseMaturity = 4
				s.ChainCfgParams = &chainParams
			}
		},
	})
	defer node1.Stop(t, true)

	_, err = node1.CallRPC(node1.Ctx, "generate", []any{2})
	require.NoError(t, err)

	// verify blockheight on node1
	err = helper.WaitForNodeBlockHeight(t.Context(), node1.BlockchainClient, 2, blockWait)
	require.NoError(t, err)

	// connect both nodes, node 2 (which is at height 3) and node 1 (height 2)
	// this will sync node 1 to height 3
	node2.ConnectToPeer(t, node1)

	// Wait for node1 to sync to height 3 before generating the next block
	err = helper.WaitForNodeBlockHeight(t.Context(), node1.BlockchainClient, 3, blockWait)
	require.NoError(t, err)

	_, err = node2.CallRPC(node2.Ctx, "generate", []any{1})
	require.NoError(t, err)

	// verify blockheight on node2
	err = helper.WaitForNodeBlockHeight(t.Context(), node2.BlockchainClient, 4, blockWait)
	require.NoError(t, err)

	// verify blockheight on node1
	err = helper.WaitForNodeBlockHeight(t.Context(), node1.BlockchainClient, 4, blockWait)

	require.NoError(t, err)
}

func TestMoveDownMoveUpWhenNoNewBlockIsGenerated(t *testing.T) {
	t.Skip("Skipping until p2p client is refactored")

	SharedTestLock.Lock()
	defer SharedTestLock.Unlock()
	node2 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableP2P:       true,
		EnableValidator: true,
		SettingsOverrideFunc: func(s *settings.Settings) {
			s.BlockValidation.SecretMiningThreshold = 9999
			// Create a copy to avoid race conditions
			if s.ChainCfgParams != nil {
				chainParams := *s.ChainCfgParams
				chainParams.CoinbaseMaturity = 4
				s.ChainCfgParams = &chainParams
			}
		},
		FSMState: blockchain.FSMStateRUNNING,
	})
	defer node2.Stop(t, true)

	// mine 3 blocks on node2
	_, err := node2.CallRPC(node2.Ctx, "generate", []any{3})
	require.NoError(t, err)

	// mine 2 blocks on node1
	node1 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableP2P:       true,
		EnableValidator: true,
		// EnableFullLogging: true,
		SettingsOverrideFunc: func(s *settings.Settings) {
			s.BlockValidation.SecretMiningThreshold = 9999
			// Create a copy to avoid race conditions
			if s.ChainCfgParams != nil {
				chainParams := *s.ChainCfgParams
				chainParams.CoinbaseMaturity = 4
				s.ChainCfgParams = &chainParams
			}
		},
	})
	defer node1.Stop(t, true)

	_, err = node1.CallRPC(node1.Ctx, "generate", []any{2})
	require.NoError(t, err)

	// connect nodes
	//	node 2 (which is at height 3) and node 1 (height 2)
	// this will sync node 1 to height 3
	node2.ConnectToPeer(t, node1)

	// verify blockheight on node2
	err = helper.WaitForNodeBlockHeight(t.Context(), node2.BlockchainClient, 3, blockWait)
	require.NoError(t, err)

	// verify blockheight on node1
	err = helper.WaitForNodeBlockHeight(t.Context(), node1.BlockchainClient, 3, blockWait)
	require.NoError(t, err)
}

func TestTDRestart(t *testing.T) {
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableP2P:       false,
		EnableValidator: true,
		UTXOStoreType:   "aerospike",
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
		),
	})

	// err := td.BlockchainClient.Run(td.Ctx, "test")
	// require.NoError(t, err)

	_, err := td.CallRPC(td.Ctx, "generate", []any{1})
	require.NoError(t, err)

	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)

	td.Stop(t)

	td.ResetServiceManagerContext(t)

	td = daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:         true,
		EnableP2P:         false,
		EnableValidator:   true,
		SkipRemoveDataDir: true, // we are re-starting so don't delete data dir
		UTXOStoreType:     "aerospike",
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
		),
	})

	td.WaitForBlockHeight(t, block1, blockWait, true)

	td.Stop(t)
}

// Test Reset
// 1. Start node2 and node3
// 2. Generate 100 blocks on node2
// 3. Start node1
// 6. Verify blockheight on node2

func checkSubtrees(t *testing.T, td *daemon.TestDaemon, expectedTxCount int) {
	// Get a mining candidate to verify transactions are in subtrees
	candidate, err := td.BlockAssemblyClient.GetMiningCandidate(td.Ctx, true)
	require.NoError(t, err)
	require.NotEmpty(t, candidate.SubtreeHashes, "Expected at least one subtree in mining candidate")

	t.Logf("Number of subtrees in candidate: %d", len(candidate.SubtreeHashes))

	// Mine a block
	_, err = td.CallRPC(td.Ctx, "generate", []interface{}{1})
	require.NoError(t, err)

	for i, subtreeBytes := range candidate.SubtreeHashes {
		subtreeHash := chainhash.Hash(subtreeBytes)

		// // Get the subtree bytes from the store
		subtreeReader, err := td.SubtreeStore.GetIoReader(td.Ctx, subtreeHash[:], fileformat.FileTypeSubtree)
		require.NoError(t, err, "Failed to get subtree data from store")

		// Parse the subtree
		subtree, err := subtree.NewSubtreeFromReader(subtreeReader)
		_ = subtreeReader.Close() // Ensure we close the reader
		require.NoError(t, err, "Failed to parse subtree from bytes")

		t.Logf("Subtree %d - Root hash: %s", i+1, subtree.RootHash())
		t.Logf("Subtree %d - Size: %d", i+1, subtree.Size())

		// Get transactions from this subtree
		subtreeTxs, err := helper.GetSubtreeTxHashes(td.Ctx, td.Logger, &subtreeHash, td.AssetURL, td.Settings)
		require.NoError(t, err)

		t.Logf("Found %d transactions in subtree %d", len(subtreeTxs), i+1)

		require.Equal(t, len(subtreeTxs), subtree.Size())
	}
}

func TestDynamicSubtreeSize(t *testing.T) {
	t.Skip("Skipping dynamic subtree size test")
	// Initialize test daemon with required services
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableP2P:       false,
		EnableValidator: true,
	})

	defer td.Stop(t)

	// Start the blockchain
	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// Generate initial blocks
	initialBlocks := 150
	_, err = td.CallRPC(td.Ctx, "generate", []interface{}{initialBlocks})
	require.NoError(t, err)

	// Configuration for the test
	iterations := 20
	baseOutputCount := 100 // Start with 100 outputs
	// outputMultiplier := 2  // Double the outputs each iteration

	t.Logf("Starting test with %d iterations", iterations)
	t.Logf("Initial merkle items per subtree: %d", td.Settings.BlockAssembly.InitialMerkleItemsPerSubtree)

	// Get the initial block to create transactions from

	//nolint:gosec
	for i := 0; i < iterations; i++ {
		blockToSpend, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, uint32(i+1))
		require.NoError(t, err)

		outputCount := baseOutputCount * (i + 1)
		// outputCount := baseOutputCount
		t.Logf("=== Iteration %d/%d with %d outputs ===", i+1, iterations, outputCount)

		// Create parent transaction with multiple outputs
		parentTx, err := td.CreateParentTransactionWithNOutputs(t, blockToSpend.CoinbaseTx, outputCount)
		require.NoError(t, err)
		t.Logf("Created parent transaction with %d outputs: %s", outputCount, parentTx.TxID())

		// Wait a bit for the parent transaction to be processed
		time.Sleep(2 * time.Second)

		// // Create and send child transactions concurrently
		t.Logf("Sending %d transactions concurrently...", outputCount)
		_, _, err = td.CreateAndSendTxsConcurrently(t, parentTx)
		require.NoError(t, err)
		// require.Equal(t, outputCount, len(transactions),
		// "Expected to create exactly %d transactions", outputCount)

		// // Wait for transactions to be processed
		time.Sleep(5 * time.Second)

		// // Check subtrees
		t.Logf("Checking subtrees for iteration %d", i+1)
		checkSubtrees(t, td, outputCount)

		// // Mine a block to ensure all transactions are processed
		_, err = td.CallRPC(td.Ctx, "generate", []interface{}{1})
		require.NoError(t, err)

		// // Wait between iterations to allow for subtree size adjustments
		time.Sleep(2 * time.Second)
	}
}

func TestInvalidateBlock(t *testing.T) {
	node1 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:     true,
		UTXOStoreType: "aerospike",
		SettingsOverrideFunc: test.ComposeSettings(
			test.SystemTestSettings(),
		),
	})
	defer node1.Stop(t, true)

	_, err := node1.CallRPC(node1.Ctx, "generate", []any{3})
	require.NoError(t, err)

	node1BestBlockHeader, node1BestBlockHeaderMeta, err := node1.BlockchainClient.GetBestBlockHeader(t.Context())
	require.NoError(t, err)
	require.Equal(t, uint32(3), node1BestBlockHeaderMeta.Height)

	// Invalidate best block 3
	_, err = node1.BlockchainClient.InvalidateBlock(t.Context(), node1BestBlockHeader.Hash())
	require.NoError(t, err)

	// Best block should be 2
	node1BestBlockHeaderNew, node1BestBlockHeaderMetaNew, err := node1.BlockchainClient.GetBestBlockHeader(t.Context())
	require.NoError(t, err)
	require.NotEqual(t, node1BestBlockHeader.Hash(), node1BestBlockHeaderNew.Hash())
	require.Equal(t, node1BestBlockHeaderMetaNew.Height, uint32(2))
}
