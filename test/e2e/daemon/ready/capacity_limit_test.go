package smoke

import (
	"encoding/hex"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/test"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/require"
)

// TestCapacityLimit tests that the MaxUnminedTransactions limit is enforced correctly.
// This test creates two nodes:
// - Node 1: Limited to 2 unmined transactions
// - Node 2: Unlimited unmined transactions
//
// The test verifies:
// 1. Node 1 accepts the first 2 transactions but rejects the 3rd due to capacity limit
// 2. Node 2 accepts all 3 transactions without limit
// 3. When Node 2 mines a block containing all 3 transactions, Node 1 can process the mined block
func TestCapacityLimit(t *testing.T) {
	// Phase 1: Start Node 1 with limited capacity (2 unmined transactions)
	t.Log("Phase 1: Starting Node 1 with MaxUnminedTransactions=2...")
	node1 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:       true,
		EnableP2P:       true,
		EnableValidator: true,
		UTXOStoreType:   "aerospike",
		SettingsOverrideFunc: func(s *settings.Settings) {
			test.MultiNodeSettings(1)(s)
			s.P2P.PeerCacheDir = t.TempDir()
			s.ChainCfgParams.CoinbaseMaturity = 2
			s.P2P.SyncCoordinatorPeriodicEvaluationInterval = 1 * time.Second
			s.BlockAssembly.MaxUnminedTransactions = 2
		},
		FSMState: blockchain.FSMStateRUNNING,
	})
	defer node1.Stop(t)

	t.Logf("Node 1 created with MaxUnminedTransactions=2")

	// Phase 3: Mine to maturity on Node 1
	t.Log("Phase 3: Mining to maturity on Node 1...")
	coinbaseTx := node1.MineToMaturityAndGetSpendableCoinbaseTx(t, node1.Ctx)
	t.Logf("Node 1 mined to maturity, coinbase tx: %s", coinbaseTx.TxIDChainHash().String())

	// Wait for Node 2 to sync
	node1BestHeader, _, err := node1.BlockchainClient.GetBestBlockHeader(node1.Ctx)
	require.NoError(t, err)

	// Phase 2: Start Node 2 with unlimited capacity
	t.Log("Phase 2: Starting Node 2 with unlimited capacity...")
	node2 := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:         true,
		EnableP2P:         true,
		EnableValidator:   true,
		UTXOStoreType:     "aerospike",
		SkipRemoveDataDir: true,
		SettingsOverrideFunc: func(s *settings.Settings) {
			test.MultiNodeSettings(2)(s)
			s.P2P.PeerCacheDir = t.TempDir()
			s.ChainCfgParams.CoinbaseMaturity = 2
			s.P2P.SyncCoordinatorPeriodicEvaluationInterval = 1 * time.Second
			s.BlockAssembly.MaxUnminedTransactions = 0 // 0 = unlimited
		},
		FSMState: blockchain.FSMStateRUNNING,
	})
	defer node2.Stop(t)

	t.Logf("Node 2 created with MaxUnminedTransactions=0 (unlimited)")

	// Connect the nodes
	node2.InjectPeer(t, node1)
	node1.InjectPeer(t, node2)
	t.Log("Nodes connected")

	node2.WaitForBlockhash(t, node1BestHeader.Hash(), 30*time.Second)
	t.Log("Node 2 synced to Node 1")

	// Phase 4: Create 3 transactions
	t.Log("Phase 4: Creating 3 transactions...")
	tx1 := node1.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(1, 30000),
	)
	tx1ID := tx1.TxID()
	t.Logf("Created tx1: %s", tx1ID)

	tx2 := node1.CreateTransactionWithOptions(t,
		transactions.WithInput(tx1, 0),
		transactions.WithP2PKHOutputs(1, 20000),
	)
	tx2ID := tx2.TxID()
	t.Logf("Created tx2: %s", tx2ID)

	tx3 := node1.CreateTransactionWithOptions(t,
		transactions.WithInput(tx2, 0),
		transactions.WithP2PKHOutputs(1, 10000),
	)
	tx3ID := tx3.TxID()
	t.Logf("Created tx3: %s", tx3ID)

	// Phase 5: Send transactions to both nodes via RPC
	t.Log("Phase 5: Sending transactions to both nodes...")

	// Send tx1 to both nodes
	tx1Hex := hex.EncodeToString(tx1.ExtendedBytes())
	_, err = node1.CallRPC(node1.Ctx, "sendrawtransaction", []any{tx1Hex})
	require.NoError(t, err)
	_, err = node2.CallRPC(node2.Ctx, "sendrawtransaction", []any{tx1Hex})
	require.NoError(t, err)
	t.Logf("Sent tx1 to both nodes")

	// Send tx2 to both nodes
	tx2Hex := hex.EncodeToString(tx2.ExtendedBytes())
	_, err = node1.CallRPC(node1.Ctx, "sendrawtransaction", []any{tx2Hex})
	require.NoError(t, err)
	_, err = node2.CallRPC(node2.Ctx, "sendrawtransaction", []any{tx2Hex})
	require.NoError(t, err)
	t.Logf("Sent tx2 to both nodes")

	// Verify Node 1 accepted tx1 and tx2
	node1.WaitForBlockAssemblyToProcessTx(t, tx1ID)
	node1.WaitForBlockAssemblyToProcessTx(t, tx2ID)
	t.Log("Node 1 accepted tx1 and tx2")

	// Verify Node 2 accepted tx1 and tx2
	node2.WaitForBlockAssemblyToProcessTx(t, tx1ID)
	node2.WaitForBlockAssemblyToProcessTx(t, tx2ID)
	t.Log("Node 2 accepted tx1 and tx2")

	// Phase 6: Send tx3 to both nodes - Node 1 should reject due to capacity limit
	t.Log("Phase 6: Sending tx3 to both nodes (Node 1 should reject)...")
	tx3Hex := hex.EncodeToString(tx3.ExtendedBytes())
	_, errNode1 := node1.CallRPC(node1.Ctx, "sendrawtransaction", []any{tx3Hex})
	require.Error(t, errNode1, "Node 1 should reject tx3 due to capacity limit")
	t.Logf("Node 1 rejected tx3 as expected: %v", errNode1)

	_, err = node2.CallRPC(node2.Ctx, "sendrawtransaction", []any{tx3Hex})
	require.NoError(t, err)
	t.Logf("Node 2 accepted tx3")

	// Verify Node 2 accepted tx3
	node2.WaitForBlockAssemblyToProcessTx(t, tx3ID)
	t.Logf("Node 2 has all 3 transactions in block assembly")

	// Phase 7: Node 2 mines a block with all 3 transactions
	t.Log("Phase 7: Node 2 mining a block with all 3 transactions...")
	minedBlock := node2.MineAndWait(t, 1)
	t.Logf("Node 2 mined block: %s (height %d, %d txs)", minedBlock.Hash().String(), minedBlock.Height, minedBlock.TransactionCount)

	// Verify the block contains all 3 transactions plus coinbase (4 total)
	require.Equal(t, uint64(4), minedBlock.TransactionCount, "Block should contain 4 transactions (coinbase + 3 txs)")

	node1.InjectPeer(t, node2)

	// Phase 8: Wait for Node 1 to process the mined block
	t.Log("Phase 8: Waiting for Node 1 to process the mined block...")
	node1.WaitForBlockhash(t, minedBlock.Hash(), 30*time.Second)
	t.Log("Node 1 successfully processed the block containing tx3")

	// Verify Node 1 can retrieve the block
	node1Block, err := node1.BlockchainClient.GetBlock(node1.Ctx, minedBlock.Hash())
	require.NoError(t, err)
	require.Equal(t, uint64(4), node1Block.TransactionCount, "Node 1's block should contain 4 transactions (coinbase + 3 txs)")
	t.Logf("✓ Node 1 successfully processed block with %d transactions", node1Block.TransactionCount)

	// Phase 9: Verify Node 1 can now accept new transactions (capacity available)
	t.Log("Phase 9: Verifying Node 1's capacity is available after mining...")

	// Create a new transaction from the mined block's coinbase
	tx4 := node2.CreateTransactionWithOptions(t,
		transactions.WithInput(tx3, 0),
		transactions.WithP2PKHOutputs(1, 5000),
	)
	tx4Hex := hex.EncodeToString(tx4.ExtendedBytes())

	// Node 1 should now be able to accept tx4 since previous transactions are mined
	_, err = node1.CallRPC(node1.Ctx, "sendrawtransaction", []any{tx4Hex})
	require.NoError(t, err, "Node 1 should accept tx4 after previous transactions were mined")
	t.Logf("✓ Node 1 accepted tx4 (capacity now available)")

	t.Log("✓ Test completed successfully")
}
