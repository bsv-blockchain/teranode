//go:build test_docker_daemon || debug

package smoke

import (
	"context"
	"sync"
	"testing"
	"time"

	"encoding/hex"

	"github.com/bitcoin-sv/teranode/daemon"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	helper "github.com/bitcoin-sv/teranode/test/utils"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bk/wif"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/stretchr/testify/require"
)

var (
	testLock sync.Mutex
	// DEBUG DEBUG DEBUG
	blockWait = 20 * time.Second
)

func TestMoveUp(t *testing.T) {
	testLock.Lock()
	defer testLock.Unlock()

	// dependencies := daemon.StartDaemonDependencies(t.Context(), t, true)
	// defer daemon.StopDaemonDependencies(t.Context(), dependencies)

	node2 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC: true,
		EnableP2P: true,
		// EnableFullLogging: true,
		SettingsContext: "docker.host.teranode2",
	})

	defer node2.Stop(t)

	_, err := node2.CallRPC("generate", []any{1})
	require.NoError(t, err)

	node1 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:         true,
		EnableP2P:         true,
		SkipRemoveDataDir: true,
		// EnableFullLogging: true,
		SettingsContext: "docker.host.teranode1",
	})

	defer node1.Stop(t)

	// wait for node1 to catchup to block 1
	err = helper.WaitForNodeBlockHeight(t.Context(), node1.BlockchainClient, 1, blockWait)
	require.NoError(t, err)

	// generate 1 block on node1
	_, err = node1.CallRPC("generate", []any{1})
	require.NoError(t, err)

	// verify block height on node1
	err = helper.WaitForNodeBlockHeight(t.Context(), node1.BlockchainClient, 2, blockWait)
	require.NoError(t, err)

	// verify block height on node2
	err = helper.WaitForNodeBlockHeight(t.Context(), node2.BlockchainClient, 2, blockWait)
	require.NoError(t, err)
}

func TestMoveDownMoveUpWhenNewBlockIsGenerated(t *testing.T) {
	testLock.Lock()
	defer testLock.Unlock()

	// dependencies := daemon.StartDaemonDependencies(t.Context(), t, true)
	// defer daemon.StopDaemonDependencies(t.Context(), dependencies)

	node2 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:         true,
		EnableP2P:         true,
		EnableValidator:   true,
		EnableFullLogging: true,
		SettingsContext:   "docker.host.teranode2",
	})

	// mine 3 blocks on node2
	_, err := node2.CallRPC("generate", []any{300})
	require.NoError(t, err)

	// stop node 2 so that it doesn't sync with node 1
	node2.Stop(t)

	// mine 2 blocks on node1
	node1 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:         true,
		EnableP2P:         true,
		EnableValidator:   true,
		SkipRemoveDataDir: true,
		SettingsContext:   "docker.host.teranode1",
	})

	// // set run state
	// err = node1.BlockchainClient.Run(node1.Ctx, "test")
	// require.NoError(t, err)

	_, err = node1.CallRPC("generate", []any{200})
	require.NoError(t, err)

	// restart node 2 (which is at height 3)
	node2 = daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:         true,
		EnableP2P:         true,
		EnableValidator:   true,
		SkipRemoveDataDir: true,
		SettingsContext:   "docker.host.teranode2",
	})

	_, err = node2.CallRPC("generate", []any{1})
	require.NoError(t, err)

	defer func() {
		node1.Stop(t)
		node2.Stop(t)
	}()

	// verify blockheight on node2
	err = helper.WaitForNodeBlockHeight(t.Context(), node2.BlockchainClient, 300, blockWait)
	require.NoError(t, err)

	// verify blockheight on node1
	err = helper.WaitForNodeBlockHeight(t.Context(), node1.BlockchainClient, 300, blockWait)
	require.NoError(t, err)
}

func TestMoveDownMoveUpWhenNoNewBlockIsGenerated(t *testing.T) {
	testLock.Lock()
	defer testLock.Unlock()

	// dependencies := daemon.StartDaemonDependencies(t.Context(), t, true)
	// defer daemon.StopDaemonDependencies(t.Context(), dependencies)

	node2 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableP2P:       true,
		EnableValidator: true,
		SettingsContext: "docker.host.teranode2",
	})

	// mine 3 blocks on node2
	_, err := node2.CallRPC("generate", []any{300})
	require.NoError(t, err)

	// stop node 2 so that it doesn't sync with node 1
	node2.Stop(t)

	// mine 2 blocks on node1
	node1 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:         true,
		EnableP2P:         true,
		EnableValidator:   true,
		SkipRemoveDataDir: true,
		SettingsContext:   "docker.host.teranode1",
	})

	// // set run state
	// err = node1.BlockchainClient.Run(node1.Ctx, "test")
	// require.NoError(t, err)

	_, err = node1.CallRPC("generate", []any{200})
	require.NoError(t, err)

	// restart node 2 (which is at height 3)
	node2 = daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:         true,
		EnableP2P:         true,
		EnableValidator:   true,
		SkipRemoveDataDir: true,
		SettingsContext:   "docker.host.teranode2",
	})

	defer func() {
		node1.Stop(t)
		node2.Stop(t)
	}()

	// verify blockheight on node2
	err = helper.WaitForNodeBlockHeight(t.Context(), node2.BlockchainClient, 300, blockWait)
	require.NoError(t, err)

	// verify blockheight on node1
	err = helper.WaitForNodeBlockHeight(t.Context(), node1.BlockchainClient, 300, blockWait)
	require.NoError(t, err)
}

func TestTDRestart(t *testing.T) {
	testLock.Lock()
	defer testLock.Unlock()

	// dependencies := daemon.StartDaemonDependencies(t.Context(), t, true)
	// defer daemon.StopDaemonDependencies(t.Context(), dependencies)

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableP2P:       false,
		EnableValidator: true,
		SettingsContext: "docker.host.teranode1",
	})

	// err := td.BlockchainClient.Run(td.Ctx, "test")
	// require.NoError(t, err)

	_, err := td.CallRPC("generate", []any{1})
	require.NoError(t, err)

	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)

	td.Stop(t)

	td.ResetServiceManagerContext(t)

	td = daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:         true,
		EnableP2P:         false,
		EnableValidator:   true,
		SkipRemoveDataDir: true,
		SettingsContext:   "docker.host.teranode1",
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
		td.Stop(t)
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

func TestSkipPolicyChecksOnBlockAcceptance(t *testing.T) {
	testLock.Lock()
	defer testLock.Unlock()

	ctx := context.Background()

	// Start node1 with restrictive policy settings
	node1 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableP2P:       true,
		EnableValidator: true,
		SettingsContext: "docker.host.teranode1", // This has restrictive MaxTxSizePolicy
	})

	// Start node2 with default settings (no size restrictions)
	node2 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableP2P:       true,
		EnableValidator: true,
		SettingsContext: "docker.host.teranode2",
	})

	defer func() {
		node1.Stop(t)
		node2.Stop(t)
	}()

	// Generate initial blocks to get coinbase funds
	_, err := node1.CallRPC("generate", []interface{}{101})
	require.NoError(t, err)

	// Wait for node1 to sync up to block 101
	err = helper.WaitForNodeBlockHeight(ctx, node1.BlockchainClient, 101, blockWait)
	require.NoError(t, err)

	// Create a transaction that exceeds node1's MaxTxSizePolicy
	block1, err := node1.BlockchainClient.GetBlockByHeight(ctx, 1)
	require.NoError(t, err)

	coinbaseTx := block1.CoinbaseTx
	coinbasePrivKey := node2.Settings.BlockAssembly.MinerWalletPrivateKeys[0]
	coinbasePrivateKey, err := wif.DecodeWIF(coinbasePrivKey)
	require.NoError(t, err)

	privateKey, err := bec.NewPrivateKey(bec.S256())
	require.NoError(t, err)

	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	require.NoError(t, err)

	output := coinbaseTx.Outputs[0]
	utxo := &bt.UTXO{
		TxIDHash:      coinbaseTx.TxIDChainHash(),
		Vout:          uint32(0),
		LockingScript: output.LockingScript,
		Satoshis:      output.Satoshis,
	}

	// Create an oversized transaction
	newTx := bt.NewTx()
	err = newTx.FromUTXOs(utxo)
	require.NoError(t, err)

	// Add many outputs to make the transaction exceed MaxTxSizePolicy of node1
	maxTxSize := node1.Settings.Policy.MaxTxSizePolicy
	numOutputs := (maxTxSize / 34) + 1000 // Add extra outputs to ensure we exceed the limit
	satoshisPerOutput := output.Satoshis / uint64(numOutputs)

	t.Logf("Creating transaction with %d outputs to exceed MaxTxSizePolicy of %d bytes", numOutputs, maxTxSize)

	for i := 0; i < numOutputs; i++ {
		err = newTx.AddP2PKHOutputFromAddress(address.AddressString, satoshisPerOutput)
		require.NoError(t, err)
	}

	err = newTx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: coinbasePrivateKey.PrivKey})
	require.NoError(t, err)

	txBytes := hex.EncodeToString(newTx.ExtendedBytes())

	// Try to send the oversized transaction to node1 - should fail
	_, err = node1.CallRPC("sendrawtransaction", []interface{}{txBytes})
	require.Error(t, err, "Expected node1 to reject oversized transaction")
	require.Contains(t, err.Error(), "transaction size in bytes is greater than max tx size policy",
		"Expected error message to indicate transaction size policy violation")

	// Send the same transaction to node2 - should succeed
	// resp, err := node2.CallRPC("sendrawtransaction", []interface{}{txBytes})
	// require.NoError(t, err, "Expected node2 to accept the transaction")
	// t.Logf("Transaction accepted by node2: %s", resp)

	// Mine a block on node2 containing the oversized transaction
	_, err = node2.CallRPC("generate", []interface{}{1})
	require.NoError(t, err)

	// Wait for node1 to receive and accept the block despite the transaction violating its policy
	err = helper.WaitForNodeBlockHeight(ctx, node1.BlockchainClient, 102, blockWait)
	require.NoError(t, err)

	// // Verify that node1 has the transaction in its block
	// block102, err := node1.BlockchainClient.GetBlockByHeight(ctx, 102)
	// require.NoError(t, err)

	// // Check if the transaction is in the block's subtrees
	// err = block102.GetAndValidateSubtrees(ctx, node1.Logger, node1.SubtreeStore, nil)
	// require.NoError(t, err)

	// err = block102.CheckMerkleRoot(ctx)
	// require.NoError(t, err)

	// fallbackGetFunc := func(subtreeHash chainhash.Hash) error {
	// 	return block102.SubTreesFromBytes(subtreeHash[:])
	// }

	// subtree, err := block102.GetSubtrees(ctx, node1.Logger, node1.SubtreeStore, fallbackGetFunc)
	// require.NoError(t, err)

	// txFound := false
	// for i := 0; i < len(subtree); i++ {
	// 	st := subtree[i]
	// 	for _, node := range st.Nodes {
	// 		if node.Hash.String() == newTx.TxIDChainHash().String() {
	// 			txFound = true
	// 			break
	// 		}
	// 	}
	// }

	// assert.True(t, txFound, "Transaction should be found in node1's block despite violating its policy")
}

func TestInvalidBlock(t *testing.T) {
	testLock.Lock()
	defer testLock.Unlock()

	// dependencies := daemon.StartDaemonDependencies(t.Context(), t, true)
	// defer daemon.StopDaemonDependencies(t.Context(), dependencies)

	node1 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		SettingsContext: "docker.host.teranode1",
	})

	time.Sleep(1 * time.Second)

	_, err := node1.CallRPC("generate", []any{3})
	require.NoError(t, err)

	node1BestBlockHeader, node1BestBlockHeaderMeta, err := node1.BlockchainClient.GetBestBlockHeader(t.Context())
	require.NoError(t, err)
	require.Equal(t, uint32(3), node1BestBlockHeaderMeta.Height)

	// Invalidate best block 3
	err = node1.BlockchainClient.InvalidateBlock(t.Context(), node1BestBlockHeader.Hash())
	require.NoError(t, err)

	time.Sleep(1 * time.Second)

	// Best block should be 2
	node1BestBlockHeaderNew, node1BestBlockHeaderMetaNew, err := node1.BlockchainClient.GetBestBlockHeader(t.Context())
	require.NoError(t, err)
	require.NotEqual(t, node1BestBlockHeader.Hash(), node1BestBlockHeaderNew.Hash())
	require.Equal(t, node1BestBlockHeaderMetaNew.Height, uint32(2))
}
