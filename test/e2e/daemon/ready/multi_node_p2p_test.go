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

// Test_NodeB_Inject_After_NodeA_Mined_P2P tests that nodeB can sync blocks from nodeA
// after nodeA has already mined blocks.
//
// Scenario:
//  1. Start nodeA and nodeB
//  2. Mine blocks on nodeA to reach coinbase maturity
//  3. Inject nodeA's peer info into nodeB
//  4. Verify nodeB syncs to nodeA's block height
func Test_NodeB_Inject_After_NodeA_Mined_P2P(t *testing.T) {
	createNode := func(t *testing.T, nodeNumber int) *daemon.TestDaemon {
		return daemon.NewTestDaemon(t, daemon.TestOptions{
			EnableP2P:         true,
			UTXOStoreType:     "aerospike",
			SkipRemoveDataDir: nodeNumber > 1,
			// EnableDebugLogging: true,
			PreserveDataDir: true,
			SettingsOverrideFunc: func(s *settings.Settings) {
				test.MultiNodeSettings(nodeNumber)(s)
				s.P2P.PeerCacheDir = t.TempDir()
				s.P2P.SyncCoordinatorPeriodicEvaluationInterval = 1 * time.Second
				s.GlobalBlockHeightRetention = 1
			},
			FSMState: blockchain.FSMStateRUNNING,
		})
	}

	node1 := createNode(t, 1)
	defer node1.Stop(t)

	node2 := createNode(t, 2)
	defer node2.Stop(t)

	coinbaseTx := node1.MineToMaturityAndGetSpendableCoinbaseTx(t, node1.Ctx)
	t.Logf("Node1 mined to maturity, coinbase tx: %s", coinbaseTx.TxIDChainHash().String())

	// Get nodeA's best block before injecting
	nodeABestHeader, nodeAMeta, err := node1.BlockchainClient.GetBestBlockHeader(node1.Ctx)
	require.NoError(t, err)
	t.Logf("Node1 at height %d, best block: %s", nodeAMeta.Height, nodeABestHeader.Hash().String())

	// Inject nodeA into nodeB — gives sync coordinator nodeA's height and asset URL
	node2.InjectPeer(t, node1)

	// Wait for nodeB to sync
	t.Log("Waiting for node2 to sync to node1's block...")
	node2.WaitForBlockhash(t, nodeABestHeader.Hash(), 30*time.Second)

	nodeBBestHeader, nodeBMeta, err := node2.BlockchainClient.GetBestBlockHeader(node2.Ctx)
	require.NoError(t, err)
	require.Equal(t, nodeAMeta.Height, nodeBMeta.Height, "Node2 should be at same height as Node1")
	require.Equal(t, nodeABestHeader.Hash().String(), nodeBBestHeader.Hash().String(), "Node2 should have same best block as Node1")

	t.Logf("✓ Node2 synced to height %d, block: %s", nodeBMeta.Height, nodeBBestHeader.Hash().String())
}
