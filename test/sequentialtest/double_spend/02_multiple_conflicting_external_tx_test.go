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
// 1. Multiple external transactions conflict with each other (txA0, txB0, txC0 are triple spends)
// 2. Conflicting transactions are in different blocks/forks
// 3. All transactions in losing chains should be marked as conflicting appropriately
//
// External transaction characteristics:
//   - Each transaction has numOutputsForExternalTx (5) outputs
//   - With utxoBatchSize=2, these transactions span 3 UTXO batches
//   - txA0, txB0, txC0 all spend the same parent transaction output 0 (triple spend)
func testMarkAsConflictingMultipleExternalTx(t *testing.T, utxoStore string) {
	// Setup test environment with external transactions
	td, parentTx, txA0, txB0, block102a, _ := setupExternalTxDoubleSpendTest(t, utxoStore, 10)
	defer func() {
		td.Stop(t)
	}()

	t.Logf("External txA0: %s (%d outputs)", txA0.TxIDChainHash().String(), len(txA0.Outputs))
	t.Logf("External txB0 (double spend): %s (%d outputs)", txB0.TxIDChainHash().String(), len(txB0.Outputs))

	// Create a third conflicting external transaction (triple spend)
	// txC0 spends parentTx outputs 0 and 2 (conflicts with txA0 and txB0 on output 0)
	txC0 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(parentTx, 0),
		transactions.WithInput(parentTx, 2),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, parentTx.Outputs[0].Satoshis/uint64(numOutputsForExternalTx)-100),
	)
	t.Logf("External txC0 (triple spend): %s (%d outputs)", txC0.TxIDChainHash().String(), len(txC0.Outputs))

	// create 103A with txA0
	// 0 -> 1 ... 101 -> 102a -> 103a (*)
	_, block103a := td.CreateTestBlock(t, block102a, 10301, txA0)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103a, block103a.Height, "", "legacy"),
		"Failed to process block")

	// Create block 103b with a double spend external transaction
	block103b := createConflictingBlockUseExternalRecords(t, td, block103a, []*bt.Tx{txB0}, []*bt.Tx{txA0}, 10302)
	assert.NotNil(t, block103b)

	//                   / 102a -> 103a (*) [txA0 - external]
	// 0 -> 1 ... 101 ->
	//                          \ 103b [txB0 - external, double spend]

	// Create block 103c with the third conflicting external transaction
	block103c := createConflictingBlockUseExternalRecords(t, td, block103a, []*bt.Tx{txC0}, []*bt.Tx{txA0}, 10303)
	assert.NotNil(t, block103c)

	//                   / 102a -> 103a (*) [txA0 - external]
	// 0 -> 1 ... 101 ->        \  103b [txB0 - external]
	//                           \ 103c [txC0 - external]

	// Create block 104b to make chain b the longest
	_, block104b := td.CreateTestBlock(t, block103b, 10402) // Empty block
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block104b, block104b.Height, "", "legacy"),
		"Failed to process block")

	td.WaitForBlockHeight(t, block104b, blockWait, true)

	//                   / 102a -> 103a [txA0 - external]
	// 0 -> 1 ... 101 ->        \  103b [txB0 - external] -> 104b (*)
	//                           \ 103c [txC0 - external]

	// Verify all conflicting external transactions are properly marked
	// txA0 should be conflicting (chain a is losing)
	td.VerifyConflictingInUtxoStore(t, true, txA0)
	td.VerifyConflictingInSubtrees(t, block103a.Subtrees[0], txA0)

	// txB0 should NOT be conflicting (chain b is winning)
	td.VerifyConflictingInUtxoStore(t, false, txB0)
	td.VerifyConflictingInSubtrees(t, block103b.Subtrees[0], txB0)

	// txC0 should be conflicting (chain c is losing)
	td.VerifyConflictingInUtxoStore(t, true, txC0)
	td.VerifyConflictingInSubtrees(t, block103c.Subtrees[0], txC0)

	t.Log("Successfully verified multiple conflicting external transactions in different blocks")
}
