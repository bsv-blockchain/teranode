//go:build test_docker_daemon || debug

package smoke

import (
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/daemon"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/test/testcontainers"
	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

var (
	// DEBUG DEBUG DEBUG
	blockWait = 30 * time.Second
)

func TestMoveUp(t *testing.T) {
	tc, err := testcontainers.NewTestContainer(t, testcontainers.TestContainersConfig{
		ComposeFile: "../../docker-compose-host.yml",
	})
	require.NoError(t, err)

	node2 := tc.GetNodeClients(t, "docker.host.teranode2")
	_, err = node2.CallRPC(t, "generate", []interface{}{101})
	require.NoError(t, err)

	// tc.StopNode(t, "teranode-1")

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:        true,
		EnableP2P:        true,
		EnableValidator:  true,
		KillTeranode:     true,
		SettingsOverride: settings.NewSettings("docker.host.teranode1"),
	})

	t.Cleanup(func() {
		td.Stop()
	})

	// set run state
	err = td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	block101, err := node2.BlockchainClient.GetBlockByHeight(tc.Ctx, 101)
	require.NoError(t, err)

	td.WaitForBlockHeight(t, block101, blockWait, true)

	// generate 1 block on node1
	_, err = td.CallRPC("generate", []interface{}{1})
	require.NoError(t, err)

	// verify blockheight on node1
	_, err = td.BlockchainClient.GetBlockByHeight(td.Ctx, 102)
	require.NoError(t, err)

	time.Sleep(10 * time.Second)
	_, err = node2.BlockchainClient.GetBlockByHeight(tc.Ctx, 102)
	require.NoError(t, err)

	// assert.Equal(t, block102Node1.Header.Hash(), block102Node2.Header.Hash())
}

func TestMoveDownMoveUp(t *testing.T) {
	// t.Skip("Test is disabled")
	tc, err := testcontainers.NewTestContainer(t, testcontainers.TestContainersConfig{
		ComposeFile: "../docker-compose-host.yml",
	})
	// Add cleanup for test container at the start
	t.Cleanup(func() {
		if tc != nil {
			tc.Compose.Down(tc.Ctx)
		}
	})
	require.NoError(t, err)

	tc.StopNode(t, "teranode-1")

	node2 := tc.GetNodeClients(t, "docker.host.teranode2")
	_, err = node2.CallRPC(t, "generate", []interface{}{300})
	require.NoError(t, err)

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:        true,
		EnableP2P:        false,
		EnableValidator:  true,
		SettingsOverride: settings.NewSettings("docker.host.teranode1"),
	})

	blockgen, err := td.CallRPC("generate", []interface{}{200})
	t.Logf("blockgen: %s", blockgen)
	require.NoError(t, err)

	block200, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 200)
	t.Logf("block200: %s", block200.Header.Hash())
	require.NoError(t, err)

	td.WaitForBlockHeight(t, block200, blockWait, true)

	td.Stop()
	td.ResetServiceManagerContext(t)

	td2 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:         true,
		EnableP2P:         true,
		EnableValidator:   true,
		SkipRemoveDataDir: true,
		SettingsOverride:  settings.NewSettings("docker.host.teranode1"),
	})

	time.Sleep(10 * time.Second)

	// verify blockheight on node1
	_, err = td2.BlockchainClient.GetBlockByHeight(td2.Ctx, 300)
	require.NoError(t, err)

	// create a mining candidate and test that the previous hash is the tip of the block 300 // TNC2

	// Add cleanup for td2
	t.Cleanup(func() {
		if td2 != nil {
			td2.Stop()
		}
	})
}

func TestTDRestart(t *testing.T) {
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableP2P:       false,
		EnableValidator: true,
	})

	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	_, err = td.CallRPC("generate", []interface{}{1})
	require.NoError(t, err)

	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)

	td.Stop()

	// time.Sleep(10 * time.Second)

	td.ResetServiceManagerContext(t)

	td = daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:         true,
		EnableP2P:         false,
		EnableValidator:   true,
		SkipRemoveDataDir: true,
	})

	td.WaitForBlockHeight(t, block1, blockWait, true)
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
	_, err = td.CallRPC("generate", []interface{}{1})
	require.NoError(t, err)

	for i, subtreeBytes := range candidate.SubtreeHashes {
		subtreeHash := chainhash.Hash(subtreeBytes)

		// // Get the subtree bytes from the store
		subtreeData, err := td.SubtreeStore.Get(td.Ctx, subtreeHash[:], options.WithFileExtension("subtree"))
		require.NoError(t, err, "Failed to get subtree data from store")

		// Parse the subtree
		subtree, err := util.NewSubtreeFromBytes(subtreeData)
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
	// Initialize test daemon with required services
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableP2P:       false,
		EnableValidator: true,
	})

	t.Cleanup(func() {
		td.Stop()
	})

	// Start the blockchain
	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// Generate initial blocks
	initialBlocks := 150
	_, err = td.CallRPC("generate", []interface{}{initialBlocks})
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
		_, err = td.CallRPC("generate", []interface{}{1})
		require.NoError(t, err)

		// // Wait between iterations to allow for subtree size adjustments
		time.Sleep(2 * time.Second)
	}
}
