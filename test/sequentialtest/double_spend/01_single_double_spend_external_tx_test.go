package doublespendtest

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/stretchr/testify/require"
)

// TestSingleDoubleSpendExternalTx tests double-spend handling with external transactions.
// External transactions are multi-UTXO record transactions that exceed utxoBatchSize,
// causing them to be stored externally rather than inline in UTXO records.
//
// This test verifies the same behavior as testSingleDoubleSpend but with external
// transactions that have multiple outputs spread across different UTXO records.
func TestSingleDoubleSpendExternalTxPostgres(t *testing.T) {
	t.Run("single_external_tx_with_one_conflicting_transaction", func(t *testing.T) {
		testSingleDoubleSpendExternalTx(t, "postgres")
	})
}

func TestSingleDoubleSpendExternalTxAerospike(t *testing.T) {
	t.Run("single_external_tx_with_one_conflicting_transaction", func(t *testing.T) {
		testSingleDoubleSpendExternalTx(t, "aerospike")
	})
}

// testSingleDoubleSpendExternalTx tests the handling of double-spend transactions
// using external transactions (multi-UTXO record transactions).
//
// Test flow:
//   - Creates block102b with a double-spend external transaction
//   - Verifies original block102a remains at height 102
//   - Creates block103b to make chain b the longest
//   - Verifies chain reorganization occurs (block103b becomes tip)
//   - Validates final conflict status:
//   - Original tx becomes conflicting (losing chain)
//   - Double-spend tx becomes non-conflicting (winning chain)
//   - Forks back to chain a and verifies proper conflict status updates
//
// External transaction characteristics:
//   - Each transaction has numOutputsForExternalTx (5) outputs
//   - With utxoBatchSize=2, these transactions span 3 UTXO batches
//   - This tests that double spend detection works across UTXO records
func testSingleDoubleSpendExternalTx(t *testing.T, utxoStore string) {
	// Setup test environment with external transactions
	td, _, txA0, txB0, block102a, _ := setupExternalTxDoubleSpendTest(t, utxoStore, 1)
	defer func() {
		td.Stop(t)
	}()

	t.Logf("External txA0: %s (%d outputs)", txA0.TxIDChainHash().String(), len(txA0.Outputs))
	t.Logf("External txB0 (double spend): %s (%d outputs)", txB0.TxIDChainHash().String(), len(txB0.Outputs))

	// create 103A
	// 0 -> 1 ... 101 -> 102a -> 103a (*)
	_, block103a := td.CreateTestBlock(t, block102a, 10301, txA0)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103a, block103a.Height, "", "legacy"),
		"Failed to process block")

	// Create block 102b with a double spend external transaction
	block103b := createConflictingBlockUseExternalRecords(t, td, block103a, []*bt.Tx{txB0}, []*bt.Tx{txA0}, 10202)

	//                   / 102a (*) [txA0 - external] -> 103a (*)
	// 0 -> 1 ... 101 ->
	//                                                \ 103b [txB0 - external, double spend]

	// Create block 103b to make the longest chain...
	_, block104b := td.CreateTestBlock(t, block103b, 10402) // Empty block

	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block104b, block104b.Height, "", "legacy"),
		"Failed to process block")

	td.WaitForBlockHeight(t, block104b, blockWait, true)

	//                   / 102a [txA0 - external] -> 103a
	// 0 -> 1 ... 101 ->
	//                   \ 102b [txB0 - external] -> 103b -> 104b (*)

	// Check the txA0 (external tx) is marked as conflicting
	td.VerifyConflictingInSubtrees(t, block103a.Subtrees[0], txA0)
	td.VerifyConflictingInUtxoStore(t, true, txA0)

	// Check the txA0 has been removed from block assembly
	td.VerifyNotInBlockAssembly(t, txA0)

	// Check the txB0 (external tx) is no longer marked as conflicting
	// It should still be marked as conflicting in the subtree
	td.VerifyConflictingInSubtrees(t, block103b.Subtrees[0], txB0)
	td.VerifyConflictingInUtxoStore(t, false, txB0)

	// Check that the txB0 is not in block assembly, it should have been mined and removed
	td.VerifyNotInBlockAssembly(t, txB0)

	// Fork back to the original chain and check that everything is processed properly
	_, block104a := td.CreateTestBlock(t, block103a, 10401) // Empty block
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block104a, block104a.Height, "", "legacy"),
		"Failed to process block")

	_, block105a := td.CreateTestBlock(t, block104a, 10501) // Empty block
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block105a, block105a.Height, "", "legacy"),
		"Failed to process block")

	td.WaitForBlock(t, block105a, blockWait)

	//                   / 102a [txA0] -> 103a -> 104a -> 105a (*)
	// 0 -> 1 ... 101 ->
	//                   \ 102b [txB0] -> 103b -> 104b

	// Check that the txB0 is not in block assembly, it should have been removed
	td.VerifyNotInBlockAssembly(t, txB0)

	// Check that txB0 has been marked again as conflicting
	td.VerifyConflictingInUtxoStore(t, false, txA0)
	td.VerifyConflictingInUtxoStore(t, true, txB0)

	// Check that both transactions are still marked as conflicting in the subtrees
	td.VerifyConflictingInSubtrees(t, block103a.Subtrees[0], txA0)
	td.VerifyConflictingInSubtrees(t, block103b.Subtrees[0], txB0)

	t.Log("Successfully verified double spend handling with external transactions")
}
