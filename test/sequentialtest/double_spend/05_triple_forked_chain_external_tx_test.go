package doublespendtest

import (
	"testing"

	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/require"
)

// TestTripleForkedChainExternalTx tests a scenario with three competing chains
// where all transactions are external (multi-UTXO record transactions).
//
// This is a complex test that verifies chain reorganization works correctly
// when multiple chains contain external transactions with outputs spread across
// multiple UTXO records and multiple reorganizations occur.
func TestTripleForkedChainExternalTxPostgres(t *testing.T) {
	t.Run("triple_forked_chain_external_tx", func(t *testing.T) {
		testTripleForkedChainExternalTx(t, "postgres")
	})
}

func TestTripleForkedChainExternalTxAerospike(t *testing.T) {
	t.Run("triple_forked_chain_external_tx", func(t *testing.T) {
		testTripleForkedChainExternalTx(t, "aerospike")
	})
}

// testTripleForkedChainExternalTx tests a scenario with three competing chains:
//
// Transaction Chains (all external with 5 outputs each):
//   - Chain A: txA0 -> txA1 -> txA2 -> txA3
//   - Chain B: txB0 -> txB1 -> txB2
//   - Chain C: txC0 -> txC1 -> txC2
//
// Block Structure:
//
//	                102a [txA0] -> 103a [txA1..txA3]
//	               /
//	0 -> 1 ... 101 - 102b -> 103b [txB0..txB2] -> 104b
//	               \
//	                102c -> 103c [txC0..txC2] -> 104c -> 105c (*)
//
// Test Flow:
//  1. Initially chain A (102a->103a) is winning
//  2. Then chain B (102b->103b->104b) becomes winning
//  3. Finally chain C (102c->103c->104c->105c) becomes the ultimate winner
//  4. Verify all transactions in losing chains are marked as conflicting
//
// Multi-input spending pattern:
//   - Each chain transaction spends from multiple outputs of the parent
//   - This tests double spend detection across different UTXO records
func testTripleForkedChainExternalTx(t *testing.T, utxoStore string) {
	// Setup test environment with external transactions
	td, coinbaseTx1, txA0, txB0, block102a, _ := setupExternalTxDoubleSpendTest(t, utxoStore, 25)
	defer td.Stop(t)

	t.Logf("External txA0: %s (%d outputs)", txA0.TxIDChainHash().String(), len(txA0.Outputs))
	t.Logf("External txB0 (double spend): %s (%d outputs)", txB0.TxIDChainHash().String(), len(txB0.Outputs))

	// Create chain A external transactions with multi-input spending
	txA1 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(txA0, 0),
		transactions.WithInput(txA0, 1),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, 39000),
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

	t.Logf("Chain A txA1: %s (%d outputs)", txA1.TxIDChainHash().String(), len(txA1.Outputs))
	t.Logf("Chain A txA2: %s (%d outputs)", txA2.TxIDChainHash().String(), len(txA2.Outputs))
	t.Logf("Chain A txA3: %s (%d outputs)", txA3.TxIDChainHash().String(), len(txA3.Outputs))

	// Create block 103a with chain A external transactions
	subtree103a, block103a := td.CreateTestBlock(t, block102a, 10301, txA1, txA2, txA3)

	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103a, block103a.Height, "", "legacy"),
		"Failed to process block103a")

	//
	//                               txA1
	//                   / 102a ---> txA2 ---> 103a (*)
	//                  /            txA3
	// 0 -> 1 ... 101 /

	// Create chain B (double spend chain)
	block101, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 101)
	require.NoError(t, err)

	// Create block102b from block101
	_, block102b := td.CreateTestBlock(t, block101, 10202) // Empty block
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block102b, block102b.Height, "", "legacy"),
		"Failed to process block102b")

	// Create chain B external transactions with multi-input spending
	// txB0 has 6 outputs (from setup)
	txB1 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(txB0, 0),
		transactions.WithInput(txB0, 1),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, 39000),
	)
	txB2 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(txB1, 0),
		transactions.WithInput(txB1, 1),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, 15000),
	)

	t.Logf("Chain B txB1: %s (%d outputs)", txB1.TxIDChainHash().String(), len(txB1.Outputs))
	t.Logf("Chain B txB2: %s (%d outputs)", txB2.TxIDChainHash().String(), len(txB2.Outputs))

	// Create block103b with chain B external transactions
	subtree103b, block103b := td.CreateTestBlock(t, block102b, 10302, txB0, txB1, txB2)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103b, block103b.Height, "", "legacy"),
		"Failed to process block103b")

	//                         txA1
	//                 102a -> txA2 -> 103a (*)
	//               /         txA3
	// 0 -> 1 ... 101
	//               \         txB0
	//                 102b -> txB1 -> 103b
	//                         txB2

	// Create chain C (triple spend chain)
	// txC0 spends from the same coinbase as txA0 and txB0 to create a triple conflict
	txC0 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx1, 0), // Same output as txA0 and txB0
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, outputAmount),
	)
	txC1 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(txC0, 0),
		transactions.WithInput(txC0, 1),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, 39000),
	)
	txC2 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(txC1, 0),
		transactions.WithInput(txC1, 1),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, 15000),
	)

	t.Logf("Chain C txC0 (triple spend): %s (%d outputs)", txC0.TxIDChainHash().String(), len(txC0.Outputs))
	t.Logf("Chain C txC1: %s (%d outputs)", txC1.TxIDChainHash().String(), len(txC1.Outputs))
	t.Logf("Chain C txC2: %s (%d outputs)", txC2.TxIDChainHash().String(), len(txC2.Outputs))

	// Create block102c from block101
	_, block102c := td.CreateTestBlock(t, block101, 10203) // Empty block
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block102c, block102c.Height, "", "legacy"),
		"Failed to process block102c")

	// Create block103c with chain C external transactions
	_, block103c := td.CreateTestBlock(t, block102c, 10303, txC0, txC1, txC2)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103c, block103c.Height, "", "legacy"),
		"Failed to process block103c")

	//                  102a [txA0] -> 103a [txA1..txA3] (*)
	//                /
	// 0 -> 1 ... 101 - 102b -> 103b [txB0..txB2]
	//                \
	//                  102c -> 103c [txC0..txC2]

	// Verify 103a is still the valid block at height 103
	td.WaitForBlockHeight(t, block103a, blockWait)

	// Make chain B win temporarily by mining 104b
	_, block104b := td.CreateTestBlock(t, block103b, 10402) // Empty block
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block104b, block104b.Height, "", "legacy"),
		"Failed to process block104b")

	//                  102a [txA0] -> 103a [txA1..txA3]
	//                /
	// 0 -> 1 ... 101 - 102b -> 103b [txB0..txB2] -> 104b (*)
	//                \
	//                  102c -> 103c [txC0..txC2]

	// Verify chain B is now winning
	td.WaitForBlockHeight(t, block104b, blockWait)

	// Make chain C the ultimate winner by mining 104c and 105c
	_, block104c := td.CreateTestBlock(t, block103c, 10403) // Empty block
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block104c, block104c.Height, "", "legacy"),
		"Failed to process block104c")

	_, block105c := td.CreateTestBlock(t, block104c, 10503) // Empty block
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block105c, block105c.Height, "", "legacy"),
		"Failed to process block105c")

	//                  102a [txA0] -> 103a [txA1..txA3]
	//                /
	// 0 -> 1 ... 101 - 102b -> 103b [txB0..txB2] -> 104b
	//                \
	//                  102c -> 103c [txC0..txC2] -> 104c -> 105c (*)

	// Wait for block assembly to reach height 105
	td.WaitForBlockHeight(t, block105c, blockWait)

	// Verify all external txs in chain A are marked as conflicting
	td.VerifyConflictingInUtxoStore(t, true, txA1, txA2, txA3)
	td.VerifyConflictingInSubtrees(t, subtree103a.RootHash(), txA1, txA2, txA3)

	// Verify all external txs in chain B are marked as conflicting
	td.VerifyConflictingInUtxoStore(t, true, txB0, txB1, txB2)
	td.VerifyConflictingInSubtrees(t, subtree103b.RootHash(), txB0, txB1, txB2)

	// Verify all external txs in chain C are not marked as conflicting (winning chain)
	td.VerifyConflictingInUtxoStore(t, false, txC0, txC1, txC2)
	td.VerifyConflictingInSubtrees(t, block103c.Subtrees[0], txC0, txC1, txC2)

	t.Log("Successfully verified triple forked chain with external transactions")
}
