package smoke

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/test"
	"github.com/stretchr/testify/require"
)

const (
	// coinbaseMaturityMultiNode is the number of blocks needed before coinbase can be spent
	coinbaseMaturityMultiNode = 2
)

// createTestNode creates a TestDaemon with the specified node number using MultiNodeSettings.
// Each node gets its own Aerospike container and unique identity.
// The first node (nodeNumber=1) cleans the data directory, subsequent nodes skip removal.
func createTestNode(t *testing.T, nodeNumber int) *daemon.TestDaemon {
	node := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:         true,
		EnableP2P:         true,
		EnableValidator:   true,
		UTXOStoreType:     "aerospike",
		SkipRemoveDataDir: nodeNumber > 1, // Only first node cleans the shared parent data dir; subsequent nodes share it
		SettingsOverrideFunc: func(s *settings.Settings) {
			// Apply MultiNodeSettings for unique identity and separate stores per node
			test.MultiNodeSettings(nodeNumber)(s)

			// Additional overrides specific to this test
			s.P2P.PeerCacheDir = t.TempDir()
			s.ChainCfgParams.CoinbaseMaturity = coinbaseMaturityMultiNode
			s.P2P.SyncCoordinatorPeriodicEvaluationInterval = 1 * time.Second
		},
		FSMState: blockchain.FSMStateRUNNING,
	})

	t.Logf("Node %d created (ClientName: %s, P2P Port: %d, PeerID: %s)",
		nodeNumber, node.Settings.ClientName, node.Settings.P2P.Port, node.Settings.P2P.PeerID)

	return node
}

// Test_NodeB_Inject_After_NodeA_Mined tests that nodeB can sync blocks from nodeA
// after nodeA has already mined blocks.
//
// Scenario:
//  1. Start nodeA
//  2. Mine blocks on nodeA to reach coinbase maturity
//  3. Start nodeB
//  4. Inject nodeA's peer info into nodeB
//  5. Verify nodeB syncs to nodeA's block height
func Test_NodeB_Inject_After_NodeA_Mined(t *testing.T) {
	// Phase 1: Start nodeA and mine blocks
	t.Log("Phase 1: Starting nodeA and mining blocks...")
	nodeA := createTestNode(t, 1)
	defer nodeA.Stop(t)

	// Mine to coinbase maturity
	coinbaseTx := nodeA.MineToMaturityAndGetSpendableCoinbaseTx(t, nodeA.Ctx)
	t.Logf("NodeA mined to maturity, coinbase tx: %s", coinbaseTx.TxIDChainHash().String())

	// Get nodeA's best block before starting nodeB
	nodeABestHeader, nodeAMeta, err := nodeA.BlockchainClient.GetBestBlockHeader(nodeA.Ctx)
	require.NoError(t, err)
	t.Logf("NodeA at height %d, best block: %s", nodeAMeta.Height, nodeABestHeader.Hash().String())

	// Phase 2: Start nodeB and inject nodeA
	t.Log("Phase 2: Starting nodeB and injecting nodeA...")
	nodeB := createTestNode(t, 2)
	defer nodeB.Stop(t)

	// Inject nodeA into nodeB's peer registry
	nodeB.InjectPeer(t, nodeA)
	t.Log("NodeA injected into nodeB's peer registry")

	// Phase 3: Wait for nodeB to sync
	t.Log("Phase 3: Waiting for nodeB to sync to nodeA's block...")
	nodeB.WaitForBlockhash(t, nodeABestHeader.Hash(), 30*time.Second)

	// Verify nodeB reached the same height
	nodeBBestHeader, nodeBMeta, err := nodeB.BlockchainClient.GetBestBlockHeader(nodeB.Ctx)
	require.NoError(t, err)
	require.Equal(t, nodeAMeta.Height, nodeBMeta.Height, "NodeB should be at same height as NodeA")
	require.Equal(t, nodeABestHeader.Hash().String(), nodeBBestHeader.Hash().String(), "NodeB should have same best block as NodeA")

	t.Logf("✓ NodeB synced to height %d, block: %s", nodeBMeta.Height, nodeBBestHeader.Hash().String())
}

// Test_NodeB_Inject_Before_NodeA_Mined tests that re-injecting a peer after mining
// correctly updates the peer registry and triggers sync.
//
// This test differs from Test_NodeB_Inject_After_NodeA_Mined by:
// - Injecting nodeA into nodeB BEFORE mining (at height 0)
// - Mining blocks on nodeA
// - Re-injecting nodeA to update nodeB's registry with the new height
// - Verifying nodeB syncs after the re-injection
//
// Scenario:
//  1. Start nodeA and nodeB (both at genesis)
//  2. Inject nodeA into nodeB (at height 0 - no sync needed yet)
//  3. Mine blocks on nodeA
//  4. Re-inject nodeA into nodeB (updates height, triggers sync)
//  5. Verify nodeB syncs to nodeA's block height
func Test_NodeB_Inject_Before_NodeA_Mined(t *testing.T) {
	// Phase 1: Start both nodes
	t.Log("Phase 1: Starting nodeA and nodeB...")
	nodeA := createTestNode(t, 1)
	defer nodeA.Stop(t)

	nodeB := createTestNode(t, 2)
	defer nodeB.Stop(t)

	// Phase 2: Inject nodeA into nodeB before mining (both at height 0)
	t.Log("Phase 2: Injecting nodeA into nodeB before mining (both at height 0)...")
	nodeB.InjectPeer(t, nodeA)
	t.Log("NodeA injected into nodeB's peer registry at height 0")

	// Phase 3: Mine blocks on nodeA
	t.Log("Phase 3: Mining blocks on nodeA...")
	coinbaseTx := nodeA.MineToMaturityAndGetSpendableCoinbaseTx(t, nodeA.Ctx)
	t.Logf("NodeA mined to maturity, coinbase tx: %s", coinbaseTx.TxIDChainHash().String())

	// Get nodeA's best block
	nodeABestHeader, nodeAMeta, err := nodeA.BlockchainClient.GetBestBlockHeader(nodeA.Ctx)
	require.NoError(t, err)
	t.Logf("NodeA at height %d, best block: %s", nodeAMeta.Height, nodeABestHeader.Hash().String())

	// Phase 4: Re-inject nodeA to update nodeB's registry with the new height
	t.Log("Phase 4: Re-injecting nodeA to update height and trigger sync...")
	nodeB.InjectPeer(t, nodeA)
	t.Log("NodeA re-injected with updated height")

	// Phase 5: Wait for nodeB to sync
	t.Log("Phase 5: Waiting for nodeB to sync to nodeA's block...")
	nodeB.WaitForBlockhash(t, nodeABestHeader.Hash(), 30*time.Second)

	// Verify nodeB reached the same height
	nodeBBestHeader, nodeBMeta, err := nodeB.BlockchainClient.GetBestBlockHeader(nodeB.Ctx)
	require.NoError(t, err)
	require.Equal(t, nodeAMeta.Height, nodeBMeta.Height, "NodeB should be at same height as NodeA")
	require.Equal(t, nodeABestHeader.Hash().String(), nodeBBestHeader.Hash().String(), "NodeB should have same best block as NodeA")

	t.Logf("✓ NodeB synced to height %d, block: %s", nodeBMeta.Height, nodeBBestHeader.Hash().String())
}
