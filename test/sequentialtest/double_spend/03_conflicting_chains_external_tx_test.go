package doublespendtest

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMarkAsConflictingChainsExternalTx tests a scenario where two transaction chains
// conflict with each other, and all transactions are external (multi-UTXO record).
//
// External transactions are stored externally when they exceed utxoBatchSize. This
// tests that conflict detection works correctly across entire chains of external
// transactions spanning multiple UTXO records.
func TestMarkAsConflictingChainsExternalTxPostgres(t *testing.T) {
	t.Run("conflicting_external_transaction_chains", func(t *testing.T) {
		testMarkAsConflictingChainsExternalTx(t, "postgres")
	})
}

func TestMarkAsConflictingChainsExternalTxAerospike(t *testing.T) {
	t.Run("conflicting_external_transaction_chains", func(t *testing.T) {
		testMarkAsConflictingChainsExternalTx(t, "aerospike")
	})
}

// testMarkAsConflictingChainsExternalTx tests a scenario where:
// 1. Two transaction chains conflict with each other
// 2. All transactions in both chains are external (multi-UTXO record)
// 3. All transactions in losing chain should be marked as conflicting
//
// Transaction chains (all external with 5 outputs each):
//   - Chain A: txA0 -> txA1 -> txA2 -> txA3 -> txA4
//   - Chain B: txB1 -> txB2 -> txB3 -> txB4 (txB1 spends from same outputs as txA1)
//
// The double spend is at the second level - txB1 spends outputs 1 and 2 of txA0,
// which conflicts with txA1 that also spends output 1 of txA0.
// This tests double spend detection across different UTXO records of external transactions.
func testMarkAsConflictingChainsExternalTx(t *testing.T, utxoStore string) {
	// Setup test environment with external transactions
	td, _, txA0, _, block102a, _ := setupExternalTxDoubleSpendTest(t, utxoStore, 15)
	defer func() {
		td.Stop(t)
	}()

	t.Logf("External txA0: %s (%d outputs)", txA0.TxIDChainHash().String(), len(txA0.Outputs))

	// txA0 is propagated but not yet mined. First mine it in block 103a.
	// 0 -> 1 ... 101 -> 102a [parentTx] -> 103a [txA0]

	// Create block 103a with txA0
	_, block103a := td.CreateTestBlock(t, block102a, 10301, txA0)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103a, block103a.Height, "", "legacy"),
		"Failed to process block")

	td.WaitForBlockHeight(t, block103a, blockWait, true)

	// Now txA0 is mined with 5 outputs that can be spent
	// txA0 outputs: each has satoshis based on parentTx output amounts

	// Chain A: spend outputs 1 and 2 from txA0
	// This creates external transactions that span different UTXO records
	txA1 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(txA0, 1),                               // Spend output 1
		transactions.WithInput(txA0, 2),                               // Spend output 2
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, 39000), // 5 * 39000 = 195000, leaving room for fees
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

	// Create block 104a with chain A transactions
	subtree104a, block104a := td.CreateTestBlock(t, block103a, 10401, txA1, txA2, txA3, txA4)

	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block104a, block104a.Height, "", "legacy"),
		"Failed to process block")

	// 0 -> 1 ... 101 -> 102a -> 103a [txA0] -> 104a [txA1, txA2, txA3, txA4]

	// Verify chain A transactions are not conflicting
	td.VerifyConflictingInUtxoStore(t, false, txA1, txA2, txA3, txA4)
	td.VerifyConflictingInSubtrees(t, subtree104a.RootHash()) // Should be empty

	// Wait for the block to be processed
	td.WaitForBlockHeight(t, block104a, blockWait, true)

	// Chain B: spend outputs 1 and 4 from txA0 - conflicts with txA1 on output 1!
	// This tests that double spend is detected across different UTXO records
	// Output 1 is shared between txA1 and txB1, output 4 is only in txB1
	txB1 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(txA0, 1), // CONFLICT with txA1!
		transactions.WithInput(txA0, 4), // Additional output from different UTXO record
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
	txB4 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(txB3, 0),
		transactions.WithInput(txB3, 1),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, 1500),
	)

	t.Logf("Chain B txB1 (conflicts with txA1 on output 1): %s (%d outputs)", txB1.TxIDChainHash().String(), len(txB1.Outputs))
	t.Logf("Chain B txB2: %s (%d outputs)", txB2.TxIDChainHash().String(), len(txB2.Outputs))
	t.Logf("Chain B txB3: %s (%d outputs)", txB3.TxIDChainHash().String(), len(txB3.Outputs))
	t.Logf("Chain B txB4: %s (%d outputs)", txB4.TxIDChainHash().String(), len(txB4.Outputs))

	// Create a conflicting block 104b with double spend external transactions
	block104b := createConflictingBlockUseExternalRecords(t, td, block104a,
		[]*bt.Tx{txB1, txB2, txB3, txB4},
		[]*bt.Tx{txA1, txA2, txA3, txA4},
		10402,
	)
	assert.NotNil(t, block104b)

	// Verify 104a is still the valid block
	td.WaitForBlockHeight(t, block104a, blockWait, true)

	//                   / 102a -> 103a [txA0] -> 104a [txA1..txA4] (*)
	// 0 -> 1 ... 101 ->
	//                                         \ 104b [txB1..txB4]

	// Switch forks by mining 105b
	_, block105b := td.CreateTestBlock(t, block104b, 10502) // Empty block

	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block105b, block105b.Height, "", "legacy"),
		"Failed to process block")

	// Wait for block assembly to reach height 105
	td.WaitForBlockHeight(t, block105b, blockWait, true)

	//                   / 102a -> 103a [txA0] -> 104a [txA1..txA4]
	// 0 -> 1 ... 101 ->
	//                                         \ 104b [txB1..txB4] -> 105b (*)

	// Verify all txs in chain A (104a) have been marked as conflicting
	td.VerifyConflictingInUtxoStore(t, true, txA1, txA2, txA3, txA4)
	td.VerifyConflictingInSubtrees(t, subtree104a.RootHash(), txA1, txA2, txA3, txA4)

	// Verify all txs in chain B (104b) are not marked as conflicting
	// They should still be marked as conflicting in the subtrees
	td.VerifyConflictingInUtxoStore(t, false, txB1, txB2, txB3, txB4)
	td.VerifyConflictingInSubtrees(t, block104b.Subtrees[0], txB1, txB2, txB3, txB4)

	t.Log("Successfully verified conflicting external transaction chains")
}
