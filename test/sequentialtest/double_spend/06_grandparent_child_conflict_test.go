package doublespendtest

import (
	"testing"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/require"
)

// TestGrandparentChildConflict tests a double-spend scenario where:
// - Grandparent transaction has multiple outputs
// - Parent transaction spends one output of grandparent (output 0)
// - ConflictingChild transaction (in a fork) spends the SAME grandparent output (0) AND another grandparent output (1)
//
// This creates a conflict where parent and conflictingChild both try to spend grandparent:0.
func TestGrandparentChildConflictPostgres(t *testing.T) {
	t.Run("grandparent_child_conflict", func(t *testing.T) {
		testGrandparentChildConflict(t, "postgres")
	})
}

func TestGrandparentChildConflictAerospike(t *testing.T) {
	t.Run("grandparent_child_conflict", func(t *testing.T) {
		testGrandparentChildConflict(t, "aerospike")
	})
}

// testGrandparentChildConflict tests this scenario:
//
// Transaction Structure:
//
//	grandparent (5 outputs) -> parent (spends grandparent:0)
//	                       \
//	                        -> conflictingChild (spends grandparent:0 AND grandparent:1)
//
// Block Structure:
//
//	            / 3a [grandparent] -> 4a [parent] (*)
//	0 -> 1 -> 2
//	            \ 3b [grandparent] -> 4b [conflictingChild] -> 5b (*)
//
// The conflict is on grandparent:0 - both parent and conflictingChild spend it.
//
// Expected behavior:
// - When chain A wins: parent is valid, conflictingChild is conflicting
// - When chain B wins: parent is conflicting, conflictingChild is valid
func testGrandparentChildConflict(t *testing.T, utxoStore string) {
	// Setup test environment with external transactions
	td := setupExternalTxDaemon(t, utxoStore)
	defer func() {
		td.Stop(t)
	}()

	// Initialize blockchain
	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// Generate initial blocks
	coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, td.Ctx)

	// Create grandparent with 5 outputs (external tx)
	grandparent := td.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, coinbaseTx.Outputs[0].Satoshis/numOutputsForExternalTx-1000),
	)
	t.Logf("Grandparent: %s (%d outputs)", grandparent.TxIDChainHash().String(), len(grandparent.Outputs))

	// Submit and mine grandparent
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, grandparent))
	require.NoError(t, td.WaitForTransactionInBlockAssembly(grandparent, blockWait))
	td.MineAndWait(t, 1)

	block3a, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 3)
	require.NoError(t, err)

	td.VerifyOnLongestChainInUtxoStore(t, grandparent)

	// Create parent that spends grandparent output 0
	parent := td.CreateTransactionWithOptions(t,
		transactions.WithInput(grandparent, 0),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, outputAmount/numOutputsForExternalTx-1000),
	)
	t.Logf("Parent: %s (%d outputs) - spends grandparent:0", parent.TxIDChainHash().String(), len(parent.Outputs))

	// Submit and mine parent
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, parent))
	require.NoError(t, td.WaitForTransactionInBlockAssembly(parent, blockWait))
	td.MineAndWait(t, 1)

	td.VerifyOnLongestChainInUtxoStore(t, parent)

	block4a, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 4)
	require.NoError(t, err)

	// 0 -> 1 -> 2 -> 3a [grandparent] -> 4a [parent] (*)

	t.Logf("Chain A: block3a [grandparent] -> block4a [parent]")

	// Now create a conflicting child that:
	// 1. Spends grandparent:0 (same as parent - CONFLICT!)
	// 2. Spends grandparent:1 (additional output, no conflict)
	// This tests the case where child conflicts with parent on grandparent:0
	conflictingChild := td.CreateTransactionWithOptions(t,
		transactions.WithInput(grandparent, 0), // CONFLICT with parent on grandparent:0!
		transactions.WithInput(grandparent, 1), // Additional grandparent output
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, outputAmount/numOutputsForExternalTx-1000),
	)
	t.Logf("ConflictingChild: %s (%d outputs) - spends grandparent:0 (CONFLICT) and grandparent:1",
		conflictingChild.TxIDChainHash().String(), len(conflictingChild.Outputs))

	// Create fork: block3b from block2
	block2, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 2)
	require.NoError(t, err)

	// Create block3b with grandparent (same as 3a content)
	_, block3b := td.CreateTestBlock(t, block2, 10302, grandparent)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block3b, block3b.Height, "", "legacy"),
		"Failed to process block3b")

	// Create block4b with conflictingChild
	_, block4b := td.CreateTestBlock(t, block3b, 10402, conflictingChild)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block4b, block4b.Height, "", "legacy"),
		"Failed to process block4b")

	//            / 3a [grandparent] -> 4a [parent] (*)
	// 0 -> 1 -> 2
	//            \ 3b [grandparent] -> 4b [conflictingChild]

	// Verify chain A is still winning
	td.WaitForBlockHeight(t, block4a, blockWait, true)

	// Verify parent is not conflicting (it's in the winning chain)
	td.VerifyConflictingInUtxoStore(t, false, parent)

	// Verify conflictingChild is marked as conflicting (it's in losing chain)
	td.VerifyConflictingInUtxoStore(t, true, conflictingChild)

	t.Log("Verified initial state: parent valid, conflictingChild conflicting")

	// Now make chain B longer by mining block5b
	_, block5b := td.CreateTestBlock(t, block4b, 10502)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block5b, block5b.Height, "", "legacy"),
		"Failed to process block5b")

	//            / 3a [grandparent] -> 4a [parent]
	// 0 -> 1 -> 2
	//            \ 3b [grandparent] -> 4b [conflictingChild] -> 5b (*)

	td.WaitForBlockHeight(t, block5b, blockWait, true)

	// Now chain B is winning
	// Parent should be marked as conflicting
	td.VerifyConflictingInUtxoStore(t, true, parent)

	// ConflictingChild should NOT be conflicting (it's in the winning chain now)
	td.VerifyConflictingInUtxoStore(t, false, conflictingChild)

	t.Log("Verified after reorg: parent conflicting, conflictingChild valid")

	// Verify grandparent is in both chains (should not be conflicting)
	td.VerifyConflictingInSubtrees(t, block3a.Subtrees[0])
	td.VerifyConflictingInSubtrees(t, block3b.Subtrees[0])

	t.Log("Successfully verified grandparent-child conflict scenario")
}

// setupExternalTxDaemon creates a test daemon with external tx settings
func setupExternalTxDaemon(t *testing.T, utxoStoreType string) *daemon.TestDaemon {
	return daemon.NewTestDaemon(t, daemon.TestOptions{
		UTXOStoreType:        utxoStoreType,
		SettingsOverrideFunc: externalTxSettingsFunc(),
	})
}
