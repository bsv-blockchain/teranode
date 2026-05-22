package smoke

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/test"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_MultiNode_BlockAssembly_After_Sync_Manual(t *testing.T) {

	createNode := func(t *testing.T, nodeNumber int) *daemon.TestDaemon {
		return daemon.NewTestDaemon(t, daemon.TestOptions{
			UTXOStoreType:     "aerospike",
			SkipRemoveDataDir: nodeNumber > 1,
			// EnableDebugLogging: true,
			PreserveDataDir: true,
			SettingsOverrideFunc: func(s *settings.Settings) {
				test.MultiNodeSettings(nodeNumber)(s)
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

	// Phase 2: Mine to maturity on node1 and sync node2
	t.Log("Phase 2: Mining to maturity on node1...")
	coinbaseTx := node1.MineToMaturityAndGetSpendableCoinbaseTx(t, node1.Ctx)
	t.Logf("Node1 mined to maturity, coinbase tx: %s", coinbaseTx.TxIDChainHash().String())

	block1, err := node1.BlockchainClient.GetBlockByHeight(node1.Ctx, 1)
	require.NoError(t, err)
	err = node2.BlockValidation.ValidateBlock(node2.Ctx, block1, node1.AssetURL, false)
	require.NoError(t, err)

	block2, err := node1.BlockchainClient.GetBlockByHeight(node1.Ctx, 2)
	require.NoError(t, err)
	err = node2.BlockValidation.ValidateBlock(node2.Ctx, block2, node1.AssetURL, false)
	require.NoError(t, err)

	// Phase 3: Create a chain of 10 transactions
	t.Log("Phase 3: Creating chain of 10 transactions...")

	tx := coinbaseTx
	for i := 0; i < 10; i++ {
		tx = node1.CreateTransactionWithOptions(t,
			transactions.WithInput(tx, 0),
			transactions.WithP2PKHOutputs(1, tx.Outputs[0].Satoshis-1000),
		)
		if i < 5 {
			err = node1.PropagationClient.ProcessTransaction(node1.Ctx, tx)
			require.NoError(t, err, "Failed to process tx[%d] through propagation client", i)

			err = node1.WaitForTransactionInBlockAssembly(tx, 10*time.Second)
			require.NoError(t, err, "Tx[%d] not in node1 block assembly", i)
		}

		err = node2.PropagationClient.ProcessTransaction(node2.Ctx, tx)
		require.NoError(t, err, "Failed to process tx[%d] through propagation client", i)

		err = node2.WaitForTransactionInBlockAssembly(tx, 10*time.Second)
		require.NoError(t, err, "Tx[%d] not in node2 block assembly", i)
	}

	// Phase 6: Node1 mines 10 blocks
	t.Log("Phase 6: Node1 mining 9 blocks...")
	for i := 0; i < 10; i++ {
		block := node1.MineAndWait(t, 1)
		err := node2.BlockValidation.ValidateBlock(node2.Ctx, block, node1.AssetURL, false)
		require.NoError(t, err, "Failed to validate block %d", i)
		node2.WaitForBlockHeight(t, block, 10*time.Second)
	}

	// Phase 8: Check no transactions in node1's block assembly
	t.Log("Phase 8: Verifying node1 block assembly is empty...")
	node1Txs, err := node1.BlockAssemblyClient.GetTransactionHashes(node1.Ctx)
	require.NoError(t, err)
	assert.Len(t, node1Txs, 1, "Node1 should have 1 transaction in block assembly")

	// Phase 9: Check node2 has exactly 5 transactions in block assembly (txs 6-10)
	t.Log("Phase 9: Verifying node2 has 6 transactions in block assembly...")
	node2Txs, err := node2.BlockAssemblyClient.GetTransactionHashes(node2.Ctx)
	require.NoError(t, err)
	assert.Len(t, node2Txs, 6, "Node2 should have 6 transactions in block assembly")
	t.Logf("PASS: Node2 has %d transactions in block assembly", len(node2Txs))

	node1.WaitForPruner(t, 10*time.Second)
	node2.WaitForPruner(t, 10*time.Second)

	node2block2 := node2.MineAndWait(t, 1)
	err = node1.BlockValidation.ValidateBlock(node1.Ctx, node2block2, node2.AssetURL, false)
	require.NoError(t, err)

	node1.WaitForBlock(t, node2block2, 10*time.Second)

	node1Txs, err = node1.BlockAssemblyClient.GetTransactionHashes(node1.Ctx)
	require.NoError(t, err)
	assert.Len(t, node1Txs, 1, "Node1 should have 1 transaction in block assembly")

	node2Txs, err = node2.BlockAssemblyClient.GetTransactionHashes(node2.Ctx)
	require.NoError(t, err)
	assert.Len(t, node2Txs, 1, "Node2 should have 1 transaction in block assembly")
}
