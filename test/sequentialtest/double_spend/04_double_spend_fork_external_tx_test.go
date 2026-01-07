package doublespendtest

import (
	"testing"

	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/require"
)

// TestDoubleSpendForkExternalTx tests a scenario with two competing chains where
// all transactions are external (multi-UTXO record transactions).
//
// This is a more complex test that verifies chain reorganization works correctly
// when both chains contain external transactions with outputs spread across
// multiple UTXO records.
func TestDoubleSpendForkExternalTxPostgres(t *testing.T) {
	t.Run("double_spend_fork_external_tx", func(t *testing.T) {
		testDoubleSpendForkExternalTx(t, "postgres")
	})
}

func TestDoubleSpendForkExternalTxAerospike(t *testing.T) {
	t.Run("double_spend_fork_external_tx", func(t *testing.T) {
		testDoubleSpendForkExternalTx(t, "aerospike")
	})
}

// testDoubleSpendForkExternalTx tests a scenario with two competing chains:
//
// Transaction Chains (all external with 5 outputs each):
//   - Chain A: txA0 -> txA1 -> txA2 -> txA3 -> txA4
//   - Chain B: txB0 -> txB1 -> txB2 -> txB3 (txA0 and txB0 are double spends)
//
// Block Structure:
//
//	                   / 102a -> 103a [txA0] -> 104a [txA1..txA4]
//	0 -> 1 ... 101 ->
//	                          \ 103b [txB0..txB3] -> 104b -> 105b (*)
//
// Test Flow:
//  1. Mine txA0 in block 103a, then mine txA1-txA4 in block 104a
//  2. Create competing chain B with txB0-txB3 in block 103b (forks from 102a)
//  3. Mine blocks 104b and 105b to make chain B the longest
//  4. Verify external transactions in losing chain A are marked as conflicting
//
// Multi-input spending pattern:
//   - Each chain transaction spends from multiple outputs of the parent
//   - This tests double spend detection across different UTXO records
func testDoubleSpendForkExternalTx(t *testing.T, utxoStore string) {
	// Setup test environment with external transactions
	td, _, txA0, txB0, block102a, _ := setupExternalTxDoubleSpendTest(t, utxoStore, 20)
	defer func() {
		td.Stop(t)
	}()

	t.Logf("External txA0: %s (%d outputs)", txA0.TxIDChainHash().String(), len(txA0.Outputs))
	t.Logf("External txB0 (double spend): %s (%d outputs)", txB0.TxIDChainHash().String(), len(txB0.Outputs))

	// First, mine txA0 so its outputs can be spent
	// 0 -> 1 ... 101 -> 102a [parentTx] -> 103a [txA0]
	_, block103a := td.CreateTestBlock(t, block102a, 10301, txA0)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103a, block103a.Height, "", "legacy"),
		"Failed to process block103a")

	td.WaitForBlockHeight(t, block103a, blockWait, true)

	// Now create chain A external transactions with multi-input spending
	// Each transaction spends from multiple outputs to test UTXO record handling
	txA1 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(txA0, 0),                               // Spend output 0
		transactions.WithInput(txA0, 1),                               // Spend output 1
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, 39000), // 5 * 39000 = 195000
	)
	txA2 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(txA1, 0),
		transactions.WithInput(txA1, 1),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, 15000),
	)
	txA3 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(txA2, 0),
		transactions.WithInput(txA2, 1),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, 5000),
	)
	txA4 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(txA3, 0),
		transactions.WithInput(txA3, 1),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, 1500),
	)

	t.Logf("Chain A txA1: %s (%d outputs)", txA1.TxIDChainHash().String(), len(txA1.Outputs))
	t.Logf("Chain A txA2: %s (%d outputs)", txA2.TxIDChainHash().String(), len(txA2.Outputs))
	t.Logf("Chain A txA3: %s (%d outputs)", txA3.TxIDChainHash().String(), len(txA3.Outputs))
	t.Logf("Chain A txA4: %s (%d outputs)", txA4.TxIDChainHash().String(), len(txA4.Outputs))

	// Create block 104a with chain A external transactions
	subtree104a, block104a := td.CreateTestBlock(t, block103a, 10401, txA1, txA2, txA3, txA4)

	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block104a, block104a.Height, "", "legacy"),
		"Failed to process block104a")

	td.WaitForBlockHeight(t, block104a, blockWait, true)

	// 0 -> 1 ... 101 -> 102a -> 103a [txA0] -> 104a [txA1..txA4] (*)

	// Create chain B (double spend chain)
	// Get block 102a to fork from (chain B forks after 102a, before 103a)
	block102aRefetch, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 102)
	require.NoError(t, err)

	// Create chain B external transactions with multi-input spending
	// txB0 has 5 outputs (from setup), spend from multiple outputs
	txB1 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(txB0, 0),
		transactions.WithInput(txB0, 4),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, 39000),
	)
	txB2 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(txB1, 0),
		transactions.WithInput(txB1, 1),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, 15000),
	)
	txB3 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(txB2, 0),
		transactions.WithInput(txB2, 1),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, 5000),
	)

	t.Logf("Chain B txB1: %s (%d outputs)", txB1.TxIDChainHash().String(), len(txB1.Outputs))
	t.Logf("Chain B txB2: %s (%d outputs)", txB2.TxIDChainHash().String(), len(txB2.Outputs))
	t.Logf("Chain B txB3: %s (%d outputs)", txB3.TxIDChainHash().String(), len(txB3.Outputs))

	// Create block103b with chain B external transactions (forks from 102a)
	_, block103b := td.CreateTestBlock(t, block102aRefetch, 10302, txB0, txB1, txB2, txB3)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103b, block103b.Height, "", "legacy"),
		"Failed to process block103b")

	//                   / 102a -> 103a [txA0] -> 104a [txA1..txA4] (*)
	// 0 -> 1 ... 101 ->
	//                          \ 103b [txB0..txB3]

	// Verify 104a is still the valid block
	td.WaitForBlockHeight(t, block104a, blockWait, true)

	// Switch forks by mining 104b and 105b to make chain B longer
	_, block104b := td.CreateTestBlock(t, block103b, 10402) // Empty block
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block104b, block104b.Height, "", "legacy"),
		"Failed to process block104b")

	_, block105b := td.CreateTestBlock(t, block104b, 10502) // Empty block
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block105b, block105b.Height, "", "legacy"),
		"Failed to process block105b")

	//                   / 102a -> 103a [txA0] -> 104a [txA1..txA4]
	// 0 -> 1 ... 101 ->
	//                          \ 103b [txB0..txB3] -> 104b -> 105b (*)

	// Wait for block assembly to reach height 105
	td.WaitForBlockHeight(t, block105b, blockWait, true)

	// Verify all external txs in chain A (104a) have been marked as conflicting
	td.VerifyConflictingInUtxoStore(t, true, txA0, txA1, txA2, txA3, txA4)
	td.VerifyConflictingInSubtrees(t, subtree104a.RootHash(), txA1, txA2, txA3, txA4)

	// Verify all external txs in chain B (103b) are not marked as conflicting
	td.VerifyConflictingInUtxoStore(t, false, txB0, txB1, txB2, txB3)
	td.VerifyConflictingInSubtrees(t, block103b.Subtrees[0], txB0, txB1, txB2, txB3)

	t.Log("Successfully verified double spend fork with external transactions")
}
