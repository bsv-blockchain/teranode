package smoke

import (
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/daemon"
	"github.com/bitcoin-sv/teranode/pkg/fileformat"
	"github.com/bitcoin-sv/teranode/settings"
	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/require"
)

var (
	// Reasonable timeout for block synchronization
	blockWait = 30 * time.Second
)

func TestMoveUp(t *testing.T) {
	node2 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC: true,
		EnableP2P: true,
		// EnableFullLogging: true,
		SettingsContext: "docker.host.teranode2.daemon",
	})
	defer node2.Stop(t)

	_, err := node2.CallRPC(node2.Ctx, "generate", []any{1})
	require.NoError(t, err)

	node1 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC: true,
		EnableP2P: true,
		// EnableFullLogging: true,
		SettingsContext: "docker.host.teranode1.daemon",
	})
	defer node1.Stop(t)

	// connect node2 to node1 via p2p
	node2.ConnectToPeer(t, node1)

	// Wait for node1 to have at least one peer (node2)
	err = helper.WaitForNodePeerCount(t.Context(), node1, 1, blockWait)
	require.NoError(t, err)

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
	node2 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableP2P:       true,
		EnableValidator: true,
		// EnableFullLogging: true,
		SettingsContext: "docker.host.teranode2.daemon",
		SettingsOverrideFunc: func(s *settings.Settings) {
			s.BlockValidation.SecretMiningThreshold = 9999
		},
	})
	defer node2.Stop(t)

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
		SettingsContext: "docker.host.teranode1.daemon",
		SettingsOverrideFunc: func(s *settings.Settings) {
			s.BlockValidation.SecretMiningThreshold = 9999
		},
	})
	defer node1.Stop(t)

	_, err = node1.CallRPC(node1.Ctx, "generate", []any{2})
	require.NoError(t, err)

	// verify blockheight on node1
	err = helper.WaitForNodeBlockHeight(t.Context(), node1.BlockchainClient, 2, blockWait)
	require.NoError(t, err)

	// connect both nodes, node 2 (which is at height 3) and node 1 (height 2)
	// this will sync node 1 to height 3
	node2.ConnectToPeer(t, node1)

	// Wait for node2 to have at least one peer (node1)
	err = helper.WaitForNodePeerCount(t.Context(), node2, 1, blockWait)
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
	node2 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableP2P:       true,
		EnableValidator: true,
		SettingsContext: "docker.host.teranode2.daemon",
		SettingsOverrideFunc: func(s *settings.Settings) {
			s.BlockValidation.SecretMiningThreshold = 9999
		},
	})
	defer node2.Stop(t)

	// mine 3 blocks on node2
	_, err := node2.CallRPC(node2.Ctx, "generate", []any{3})
	require.NoError(t, err)

	// mine 2 blocks on node1
	node1 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableP2P:       true,
		EnableValidator: true,
		// EnableFullLogging: true,
		SettingsContext: "docker.host.teranode1.daemon",
		SettingsOverrideFunc: func(s *settings.Settings) {
			s.BlockValidation.SecretMiningThreshold = 9999
		},
	})
	defer node1.Stop(t)

	_, err = node1.CallRPC(node1.Ctx, "generate", []any{2})
	require.NoError(t, err)

	// connect nodes
	//	node 2 (which is at height 3) and node 1 (height 2)
	// this will sync node 1 to height 3
	node2.ConnectToPeer(t, node1)

	// Wait for node2 to have at least one peer (node1)
	err = helper.WaitForNodePeerCount(t.Context(), node2, 1, blockWait)
	require.NoError(t, err)

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
		SettingsContext: "docker.host.teranode1.daemon",
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
		SettingsContext:   "docker.host.teranode1.daemon",
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
		SettingsContext: "docker.host.teranode1.daemon",
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

func TestInvalidBlock(t *testing.T) {
	node1 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		SettingsContext: "docker.host.teranode1.daemon",
	})
	defer node1.Stop(t)

	_, err := node1.CallRPC(node1.Ctx, "generate", []any{3})
	require.NoError(t, err)

	node1BestBlockHeader, node1BestBlockHeaderMeta, err := node1.BlockchainClient.GetBestBlockHeader(t.Context())
	require.NoError(t, err)
	require.Equal(t, uint32(3), node1BestBlockHeaderMeta.Height)

	// Invalidate best block 3
	err = node1.BlockchainClient.InvalidateBlock(t.Context(), node1BestBlockHeader.Hash())
	require.NoError(t, err)

	// Best block should be 2
	node1BestBlockHeaderNew, node1BestBlockHeaderMetaNew, err := node1.BlockchainClient.GetBestBlockHeader(t.Context())
	require.NoError(t, err)
	require.NotEqual(t, node1BestBlockHeader.Hash(), node1BestBlockHeaderNew.Hash())
	require.Equal(t, node1BestBlockHeaderMetaNew.Height, uint32(2))
}

func TestBlockValidationCatchup(t *testing.T) {
	// Start node2
	node2 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableP2P:       true,
		EnableValidator: true,
		SettingsContext: "docker.host.teranode2.daemon",
		SettingsOverrideFunc: func(s *settings.Settings) {
			s.Asset.HTTPPort = 28090
			s.GlobalBlockHeightRetention = 99999
			s.BlockValidation.SecretMiningThreshold = 99999
		},
	})
	defer node2.Stop(t)

	// Start node1 and generate 100 blocks
	node1 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableP2P:       true, // Start without P2P to allow separate chain
		EnableValidator: true,
		SettingsContext: "docker.host.teranode1.daemon",
		SettingsOverrideFunc: func(s *settings.Settings) {
			s.Asset.HTTPPort = 18090
			s.GlobalBlockHeightRetention = 99999
			s.BlockValidation.SecretMiningThreshold = 99999
		},
		// EnableFullLogging: true,
	})
	defer node1.Stop(t)

	// connect node2 to node1 via p2p
	node2.ConnectToPeer(t, node1)

	_, err := node1.CallRPC(node1.Ctx, "generate", []any{100})
	require.NoError(t, err)

	// Wait for node1 to have at least one peer (node2)
	err = helper.WaitForNodePeerCount(t.Context(), node1, 1, blockWait)
	require.NoError(t, err)

	// Print peer info for debugging
	_, err = helper.GetAndPrintPeerInfo(t.Context(), node1)
	require.NoError(t, err)

	_, err = node1.CallRPC(node1.Ctx, "generate", []any{100})
	require.NoError(t, err)
	// 0 -> 1 ... 100 (node1 main chain)

	// Verify node2 has synced to node1's chain
	err = helper.WaitForNodeBlockHeight(t.Context(), node2.BlockchainClient, 100, blockWait)
	require.NoError(t, err)

	// disconnect node2 from node1
	node2.DisconnectFromPeer(t, node1)

	const extraBlocks = 1000

	_, err = node2.CallRPC(node2.Ctx, "generate", []any{extraBlocks})
	require.NoError(t, err)
	//                / 101a -> ... -> 1100a (node2 fork)
	// 0 -> 1 ... 100
	//                \ 100b (node1 stays)

	// generate 1 more block on node1
	_, err = node1.CallRPC(node1.Ctx, "generate", []any{1})
	require.NoError(t, err)
	//                / 101a -> ... -> 1100a
	// 0 -> 1 ... 100
	//                \ 100b -> ... -> 101b (node1 longer chain)

	// Now reconnect node2 to trigger reorg
	node2.ConnectToPeer(t, node1)

	// _, err = node2.CallRPC("generate", []any{1})

	// Wait for node2 to have at least one peer (node1)
	err = helper.WaitForNodePeerCount(t.Context(), node2, 1, blockWait)
	require.NoError(t, err)

	// Print peer info for debugging
	_, err = helper.GetAndPrintPeerInfo(t.Context(), node2)
	require.NoError(t, err)

	// _, err = node2.CallRPC(node2.Ctx, "generate", []any{1})
	// require.NoError(t, err)

	//                / 201a -> ... -> 301a
	// 0 -> 1 ... 100
	//                \ 100b -> ... -> 101b (* reorg to longer chain)

	// At this stage, call BA reset
	// err = node1.BlockAssemblyClient.ResetBlockAssembly(node1.Ctx)
	// require.NoError(t, err)

	// Verify node1 has synced to node2's chain
	err = helper.WaitForNodeBlockHeight(t.Context(), node1.BlockchainClient, extraBlocks+100, blockWait)
	require.NoError(t, err)
}
