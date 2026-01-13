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
//   - Chain B: txB0 -> txB1 -> txB2 (txA0 and txB0 are double spends)
//   - Chain C: txC0 -> txC1 -> txC2 (txC0 is a triple spend with txA0 and txB0)
//
// Block Structure:
//
//	                   / 102a -> 103a [txA0] -> 104a [txA1..txA3]
//	0 -> 1 ... 101 ->         \  103b [txB0..txB2] -> 104b
//	                           \ 103c [txC0..txC2] -> 104c -> 105c (*)
//
// Test Flow:
//  1. Mine txA0 in block 103a, then mine txA1-txA3 in block 104a
//  2. Create chain B with txB0-txB2 in block 103b (forks from 102a)
//  3. Create chain C with txC0-txC2 in block 103c (forks from 102a)
//  4. Make chain B win temporarily by mining 104b
//  5. Make chain C the ultimate winner by mining 104c and 105c
//  6. Verify all transactions in losing chains are marked as conflicting
//
// Multi-input spending pattern:
//   - Each chain transaction spends from multiple outputs of the parent
//   - This tests double spend detection across different UTXO records
func testTripleForkedChainExternalTx(t *testing.T, utxoStore string) {
	// Setup test environment with external transactions
	td, parentTx, txA0, txB0, block102a, _ := setupExternalTxDoubleSpendTest(t, utxoStore, 25)
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

	// Create block 104a with chain A external transactions
	subtree104a, block104a := td.CreateTestBlock(t, block103a, 10401, txA1, txA2, txA3)

	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block104a, block104a.Height, "", "legacy"),
		"Failed to process block104a")

	td.WaitForBlockHeight(t, block104a, blockWait, true)

	// 0 -> 1 ... 101 -> 102a -> 103a [txA0] -> 104a [txA1..txA3] (*)

	// Create chain B external transactions with multi-input spending
	// txB0 has 5 outputs (from setup)
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

	// Create chain C (triple spend chain)
	// txC0 spends from parentTx output 0 (same as txA0 and txB0) to create a triple conflict
	txC0 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(parentTx, 0), // Same output as txA0 and txB0
		transactions.WithInput(parentTx, 3), // Different second output
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, parentTx.Outputs[0].Satoshis/uint64(numOutputsForExternalTx)-100),
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

	// Get block 102a to fork from
	block102aRefetch, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 102)
	require.NoError(t, err)

	// Create block103b with chain B external transactions (forks from 102a)
	subtree103b, block103b := td.CreateTestBlock(t, block102aRefetch, 10302, txB0, txB1, txB2)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103b, block103b.Height, "", "legacy"),
		"Failed to process block103b")

	// Create block103c with chain C external transactions (forks from 102a)
	_, block103c := td.CreateTestBlock(t, block102aRefetch, 10303, txC0, txC1, txC2)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103c, block103c.Height, "", "legacy"),
		"Failed to process block103c")

	//                   / 102a -> 103a [txA0] -> 104a [txA1..txA3] (*)
	// 0 -> 1 ... 101 ->         \  103b [txB0..txB2]
	//                            \ 103c [txC0..txC2]

	// Verify 104a is still the valid block
	td.WaitForBlockHeight(t, block104a, blockWait, true)

	// Make chain B win temporarily by mining 104b and 105b
	_, block104b := td.CreateTestBlock(t, block103b, 10402) // Empty block
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block104b, block104b.Height, "", "legacy"),
		"Failed to process block104b")

	_, block105b := td.CreateTestBlock(t, block104b, 10502) // Empty block
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block105b, block105b.Height, "", "legacy"),
		"Failed to process block105b")

	//                   / 102a -> 103a [txA0] -> 104a [txA1..txA3]
	// 0 -> 1 ... 101 ->         \  103b [txB0..txB2] -> 104b -> 105b (*)
	//                            \ 103c [txC0..txC2]

	// Verify chain B is now winning
	td.WaitForBlockHeight(t, block105b, blockWait, true)

	// Make chain C the ultimate winner by mining 104c, 105c, and 106c
	_, block104c := td.CreateTestBlock(t, block103c, 10403) // Empty block
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block104c, block104c.Height, "", "legacy"),
		"Failed to process block104c")

	_, block105c := td.CreateTestBlock(t, block104c, 10503) // Empty block
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block105c, block105c.Height, "", "legacy"),
		"Failed to process block105c")

	_, block106c := td.CreateTestBlock(t, block105c, 10603) // Empty block
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block106c, block106c.Height, "", "legacy"),
		"Failed to process block106c")

	//                   / 102a -> 103a [txA0] -> 104a [txA1..txA3]
	// 0 -> 1 ... 101 ->         \  103b [txB0..txB2] -> 104b -> 105b
	//                            \ 103c [txC0..txC2] -> 104c -> 105c -> 106c (*)

	// Wait for block assembly to reach height 106
	td.WaitForBlockHeight(t, block106c, blockWait, true)

	// Verify all external txs in chain A are marked as conflicting
	td.VerifyConflictingInUtxoStore(t, true, txA0, txA1, txA2, txA3)
	td.VerifyConflictingInSubtrees(t, subtree104a.RootHash(), txA1, txA2, txA3)

	// Verify all external txs in chain B are marked as conflicting
	td.VerifyConflictingInUtxoStore(t, true, txB0, txB1, txB2)
	td.VerifyConflictingInSubtrees(t, subtree103b.RootHash(), txB0, txB1, txB2)

	// Verify all external txs in chain C are not marked as conflicting (winning chain)
	td.VerifyConflictingInUtxoStore(t, false, txC0, txC1, txC2)
	td.VerifyConflictingInSubtrees(t, block103c.Subtrees[0], txC0, txC1, txC2)

	t.Log("Successfully verified triple forked chain with external transactions")
}
