package doublespendtest

import (
	"testing"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/require"
)

// TestSameTxBothForks tests a scenario where the same conflicting transaction
// appears in both competing blocks during a fork. This is the "same tx in both forks"
// pattern from PR #246's testConflictingTxReorgMultiOutput.
//
// Scenario:
// - Parent tx with multiple outputs is mined
// - txOriginal spends several outputs from parent and is mined
// - tx1 is submitted to mempool (spends outputs 0-3 from txOriginal)
// - tx1Conflicting is created (spends outputs 2-4 from txOriginal, conflicts on 2,3)
// - tx1Conflicting is placed in BOTH block6a AND block6b (same tx, two forks)
// - Tests that conflict detection handles identical transactions in competing blocks
func TestSameTxBothForksPostgres(t *testing.T) {
	t.Skip("same_tx_both_forks: test disabled pending implementation")
}

func TestSameTxBothForksAerospike(t *testing.T) {
	t.Skip("same_tx_both_forks: test disabled pending implementation")
}

// testSameTxBothForks tests conflict handling when the same transaction appears
// in both forks of a chain split.
//
// Transaction Structure:
//
//	parent (5 outputs)
//	└── txOriginal (spends parent:0,1,2,3,4) -> produces 5 outputs
//	    ├── tx1 (spends txOriginal:0,1,2,3) -> in mempool, rejected from blocks
//	    └── tx1Conflicting (spends txOriginal:2,3,4) -> conflicts on outputs 2,3
//
// Block Structure:
//
//	                      / 6a [tx1Conflicting] (*)
//	0 -> 1 -> ... -> 5 [txOriginal]
//	                      \ 6b [tx1Conflicting] -> 7b (*)
//
// Key test points:
// 1. tx1 is marked as conflicting (lost to tx1Conflicting in blocks)
// 2. tx1Conflicting appears in BOTH forks - should NOT be double-marked
// 3. After reorg to chain B, tx1Conflicting remains valid (not conflicting)
// 4. tx1 remains conflicting regardless of which fork wins
func testSameTxBothForks(t *testing.T, utxoStore string) {
	// Setup test environment with external transactions
	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		UTXOStoreType:        utxoStore,
		SettingsOverrideFunc: externalTxSettingsFunc(),
	})
	defer func() {
		td.Stop(t)
	}()

	// Initialize blockchain
	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// Generate initial blocks and get spendable coinbase
	coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, td.Ctx)

	// Create parent with 5 outputs (external tx)
	parent := td.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, outputAmount),
	)
	t.Logf("Parent: %s (%d outputs)", parent.TxIDChainHash().String(), len(parent.Outputs))

	// Submit and mine parent
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, parent))
	require.NoError(t, td.WaitForTransactionInBlockAssembly(parent, blockWait))
	td.MineAndWait(t, 1)

	_, err = td.BlockchainClient.GetBlockByHeight(td.Ctx, 3)
	require.NoError(t, err)

	// Create txOriginal that spends all 5 outputs from parent
	// This creates a transaction with 5 outputs that can be spent by conflicting txs
	txOriginal := td.CreateTransactionWithOptions(t,
		transactions.WithInput(parent, 0),
		transactions.WithInput(parent, 1),
		transactions.WithInput(parent, 2),
		transactions.WithInput(parent, 3),
		transactions.WithInput(parent, 4),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, 95000), // 5 * 95000 = 475000
	)
	t.Logf("TxOriginal: %s (%d outputs) - spends all parent outputs",
		txOriginal.TxIDChainHash().String(), len(txOriginal.Outputs))

	// Submit and mine txOriginal
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, txOriginal))
	require.NoError(t, td.WaitForTransactionInBlockAssembly(txOriginal, blockWait))
	td.MineAndWait(t, 1)

	block4, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 4)
	require.NoError(t, err)

	// 0 -> 1 -> 2 -> 3 [parent] -> 4 [txOriginal] (*)

	t.Log("Chain established: parent -> txOriginal")

	// Create tx1 that spends outputs 0,1,2,3 from txOriginal (4 inputs)
	// Total: 4 * 95000 = 380000 satoshis
	tx1 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(txOriginal, 0),
		transactions.WithInput(txOriginal, 1),
		transactions.WithInput(txOriginal, 2),
		transactions.WithInput(txOriginal, 3),
		transactions.WithP2PKHOutputs(3, 120000), // 3 * 120000 = 360000
	)
	t.Logf("tx1: %s (%d outputs) - spends txOriginal:0,1,2,3",
		tx1.TxIDChainHash().String(), len(tx1.Outputs))

	// Create tx1Conflicting that spends outputs 2,3,4 from txOriginal (3 inputs)
	// Conflicts with tx1 on outputs 2 and 3
	// Total: 3 * 95000 = 285000 satoshis
	tx1Conflicting := td.CreateTransactionWithOptions(t,
		transactions.WithInput(txOriginal, 2),    // CONFLICT with tx1!
		transactions.WithInput(txOriginal, 3),    // CONFLICT with tx1!
		transactions.WithInput(txOriginal, 4),    // No conflict
		transactions.WithP2PKHOutputs(2, 140000), // 2 * 140000 = 280000
	)
	t.Logf("tx1Conflicting: %s (%d outputs) - spends txOriginal:2,3,4 (conflicts on 2,3)",
		tx1Conflicting.TxIDChainHash().String(), len(tx1Conflicting.Outputs))

	// Submit tx1 to mempool (should be accepted)
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, tx1),
		"tx1 should be accepted to mempool")

	// Submit tx1Conflicting - should be rejected as double-spend
	require.Error(t, td.PropagationClient.ProcessTransaction(td.Ctx, tx1Conflicting),
		"tx1Conflicting should be rejected as double-spend")

	// Create block5a with tx1Conflicting (not tx1!)
	// This tests the scenario where a miner includes the conflicting tx instead of the mempool tx
	_, block5a := td.CreateTestBlock(t, block4, 50001, tx1)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block5a, "legacy", true),
		"Failed to process block5a")

	td.WaitForBlock(t, block5a, blockWait)

	// 0 -> 1 -> 2 -> 3 [parent] -> 4 [txOriginal] -> 5a [tx1Conflicting] (*)

	// Create block5b with the SAME tx1Conflicting (competing fork with identical tx)
	_, block5b := td.CreateTestBlock(t, block4, 50002, tx1Conflicting)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block5b, "legacy", true),
		"Failed to process block5b")

	//                                              / 5a [tx1Conflicting] (*)
	// 0 -> 1 -> 2 -> 3 [parent] -> 4 [txOriginal]
	//                                              \ 5b [tx1Conflicting]

	// Verify chain A (block5a) is winning
	td.WaitForBlockHeight(t, block5a, blockWait, true)

	// Verify conflict status:
	// - tx1 should be conflicting (lost to tx1Conflicting which is mined)
	// - tx1Conflicting should NOT be conflicting (it's in the winning chain)
	td.VerifyConflictingInUtxoStore(t, false, tx1)
	td.VerifyConflictingInUtxoStore(t, true, tx1Conflicting)

	// Both blocks contain the same tx1Conflicting - verify subtrees
	td.VerifyConflictingInSubtrees(t, block5a.Subtrees[0])
	td.VerifyConflictingInSubtrees(t, block5b.Subtrees[0], tx1Conflicting)

	// tx1 should not be in block assembly (conflicting with mined tx)
	td.VerifyNotInBlockAssembly(t, tx1)
	// tx1Conflicting should not be in block assembly (already mined)
	td.VerifyNotInBlockAssembly(t, tx1Conflicting)

	t.Log("Before reorg: tx1 conflicting, tx1Conflicting valid (in both forks)")

	// Now make chain B longer to trigger reorg
	_, block6b := td.CreateTestBlock(t, block5b, 60002) // Empty block
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block6b, block6b.Height, "", "legacy"),
		"Failed to process block6b")

	//                                              / 5a [tx1Conflicting]
	// 0 -> 1 -> 2 -> 3 [parent] -> 4 [txOriginal]
	//                                              \ 5b [tx1Conflicting] -> 6b (*)

	td.WaitForBlockHeight(t, block6b, blockWait, true)

	// After reorg to chain B:
	// - tx1 should STILL be conflicting (tx1Conflicting is still in winning chain)
	// - tx1Conflicting should NOT be conflicting (still in winning chain, just different fork)
	td.VerifyConflictingInUtxoStore(t, true, tx1)
	td.VerifyConflictingInUtxoStore(t, false, tx1Conflicting)

	// Verify tx1Conflicting is NOT double-marked as conflicting
	// (since it appears in both forks, but chain B is now winning)
	td.VerifyOnLongestChainInUtxoStore(t, tx1Conflicting)
	td.VerifyNotOnLongestChainInUtxoStore(t, tx1)

	td.VerifyNotInBlockAssembly(t, tx1)
	td.VerifyNotInBlockAssembly(t, tx1Conflicting)

	t.Log("After reorg: tx1 still conflicting, tx1Conflicting still valid")

	// Verify that tx1 cannot be re-spent (it's already spent by tx1Conflicting)
	_, block7b := td.CreateTestBlock(t, block6b, 70002, tx1)
	require.Error(t, td.BlockValidation.ValidateBlock(td.Ctx, block7b, "legacy", true),
		"Should reject block with already-spent tx1")

	t.Log("Successfully verified same-tx-both-forks scenario")
}
