package smoke

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/test"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/require"
)

func TestMultiNodeSend2Tx(t *testing.T) {
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
				s.BlockAssembly.InitialMerkleItemsPerSubtree = 1024
				s.BlockAssembly.MaximumMerkleItemsPerSubtree = 32768
			},
			FSMState: blockchain.FSMStateRUNNING,
		})
	}

	node1 := createNode(t, 1)
	defer node1.Stop(t)

	node2 := createNode(t, 2)
	defer node2.Stop(t)

	node1.InjectPeer(t, node2)
	node2.InjectPeer(t, node1)

	// Phase 2: Mine to maturity on node1 and sync node2
	t.Log("Phase 2: Mining to maturity on node1...")
	coinbaseTx := node1.MineToMaturityAndGetSpendableCoinbaseTx(t, node1.Ctx)
	t.Logf("Node1 mined to maturity, coinbase tx: %s", coinbaseTx.TxIDChainHash().String())

	// tx0 spends the coinbase
	tx0 := node1.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(1, coinbaseTx.Outputs[0].Satoshis-1000),
	)

	err := node1.PropagationClient.ProcessTransaction(node1.Ctx, tx0)
	require.NoError(t, err, "Failed to process tx through propagation client")

	time.Sleep(10 * time.Second)

	txHashes, err := node1.BlockAssemblyClient.GetTransactionHashes(node1.Ctx)
	require.NoError(t, err)

	require.Len(t, txHashes, 2, "Node1 should have coinbase placeholder + tx0")

	txHashes, err = node2.BlockAssemblyClient.GetTransactionHashes(node2.Ctx)
	require.NoError(t, err)
	require.Len(t, txHashes, 1, "Node2 should have only the coinbase placeholder")
}
