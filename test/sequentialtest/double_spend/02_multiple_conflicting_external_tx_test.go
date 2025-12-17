package doublespendtest

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMarkAsConflictingMultipleExternalTx tests a scenario where multiple external
// transactions conflict with each other and are in different blocks.
//
// External transactions are multi-UTXO record transactions that exceed utxoBatchSize,
// causing them to be stored externally. This tests that conflict detection works
// correctly when transactions span multiple UTXO records.
func TestMarkAsConflictingMultipleExternalTxPostgres(t *testing.T) {
	t.Run("multiple_conflicting_external_txs_in_different_blocks", func(t *testing.T) {
		testMarkAsConflictingMultipleExternalTx(t, "postgres")
	})
}

func TestMarkAsConflictingMultipleExternalTxAerospike(t *testing.T) {
	t.Run("multiple_conflicting_external_txs_in_different_blocks", func(t *testing.T) {
		testMarkAsConflictingMultipleExternalTx(t, "aerospike")
	})
}

// testMarkAsConflictingMultipleExternalTx tests a scenario where:
// 1. Multiple external transactions conflict with each other
// 2. Conflicting transactions are in different blocks
// 3. All conflicting transactions should be marked as conflicting appropriately
//
// External transaction characteristics:
//   - Each transaction has numOutputsForExternalTx (5) outputs
//   - With utxoBatchSize=2, these transactions span 3 UTXO batches
//   - txA0, txB0, txC0 all spend the same coinbase output (triple spend)
func testMarkAsConflictingMultipleExternalTx(t *testing.T, utxoStore string) {
	// Setup test environment with external transactions
	td, coinbaseTx1, txA0, txB0, block102a, _ := setupExternalTxDoubleSpendTest(t, utxoStore, 10)
	defer td.Stop(t)

	t.Logf("External txA0: %s (%d outputs)", txA0.TxIDChainHash().String(), len(txA0.Outputs))
	t.Logf("External txB0 (double spend): %s (%d outputs)", txB0.TxIDChainHash().String(), len(txB0.Outputs))

	// 0 -> 1 ... 101 -> 102a [txA0 - external]

	// Create block 102b with a double spend external transaction
	block102b := createConflictingExternalBlock(t, td, block102a, []*bt.Tx{txB0}, []*bt.Tx{txA0}, 10202)
	assert.NotNil(t, block102b)

	//                   / 102a (*) [txA0 - external]
	// 0 -> 1 ... 101 ->
	//                   \ 102b [txB0 - external, double spend]

	// Create a third conflicting external transaction (triple spend)
	txC0 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx1, 0),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, outputAmount),
	)
	t.Logf("External txC0 (triple spend): %s (%d outputs)", txC0.TxIDChainHash().String(), len(txC0.Outputs))

	// Create block 102c with a different double spend external transaction
	block102c := createConflictingExternalBlock(t, td, block102a, []*bt.Tx{txC0}, []*bt.Tx{txA0}, 10203)
	assert.NotNil(t, block102c)

	//                   / 102a (*) [txA0 - external]
	// 0 -> 1 ... 101 -> - 102b [txB0 - external]
	//                   \ 102c [txC0 - external]

	// Create block 103b to make chain b the longest
	_, block103b := td.CreateTestBlock(t, block102b, 10302) // Empty block
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103b, block103b.Height, "", "legacy"),
		"Failed to process block")

	td.WaitForBlockHeight(t, block103b, blockWait)

	//                   / 102a [txA0 - external]
	// 0 -> 1 ... 101 -> - 102b [txB0 - external] -> 103b (*)
	//                   \ 102c [txC0 - external]

	// Verify all conflicting external transactions are properly marked
	// txA0 should be conflicting (chain a is losing)
	td.VerifyConflictingInUtxoStore(t, true, txA0)
	td.VerifyConflictingInSubtrees(t, block102a.Subtrees[0], txA0)

	// txB0 should NOT be conflicting (chain b is winning)
	td.VerifyConflictingInUtxoStore(t, false, txB0)
	td.VerifyConflictingInSubtrees(t, block102b.Subtrees[0], txB0)

	// txC0 should be conflicting (chain c is losing)
	td.VerifyConflictingInUtxoStore(t, true, txC0)
	td.VerifyConflictingInSubtrees(t, block102c.Subtrees[0], txC0)

	t.Log("Successfully verified multiple conflicting external transactions in different blocks")
}
