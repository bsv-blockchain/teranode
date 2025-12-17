package doublespendtest

import (
	"testing"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/require"
)

// TestGrandparentChildConflict tests a complex double-spend scenario where:
// - Grandparent transaction has multiple outputs
// - Parent transaction spends one output of grandparent
// - Child transaction (in a fork) spends the SAME grandparent output AND an output of parent
//
// This creates an interesting conflict where the child transaction depends on the parent
// but also conflicts with it by spending the same grandparent output.
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
//	                        -> conflictingChild (spends grandparent:0 AND parent:0)
//
// Block Structure:
//
//	        / 2a [grandparent] -> 3a [parent] (*)
//	0 -> 1
//	        \ 2b -> 3b [conflictingChild] -> 4b (*)
//
// The conflictingChild is interesting because:
// 1. It conflicts with parent on grandparent:0 (both spend same output)
// 2. It also spends parent:0, creating a dependency on parent
// 3. But parent is in the other chain, so this creates an invalid situation
//
// Expected behavior: The fork with conflictingChild should fail validation
// because it tries to spend from a parent that doesn't exist in that chain.
func testGrandparentChildConflict(t *testing.T, utxoStore string) {
	// Setup test environment with external transactions
	td := setupExternalTxDaemon(t, utxoStore)
	defer td.Stop(t)

	// Initialize blockchain
	err := td.BlockchainClient.Run(td.Ctx, "test")
	require.NoError(t, err)

	// Generate initial blocks
	coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, td.Ctx)

	// Create grandparent with 5 outputs (external tx)
	grandparent := td.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, outputAmount),
	)
	t.Logf("Grandparent: %s (%d outputs)", grandparent.TxIDChainHash().String(), len(grandparent.Outputs))

	// Submit and mine grandparent
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, grandparent))
	td.MineAndWait(t, 1)

	block2a, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 2)
	require.NoError(t, err)

	// Create parent that spends grandparent output 0
	parent := td.CreateTransactionWithOptions(t,
		transactions.WithInput(grandparent, 0),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, outputAmount/numOutputsForExternalTx-1000),
	)
	t.Logf("Parent: %s (%d outputs) - spends grandparent:0", parent.TxIDChainHash().String(), len(parent.Outputs))

	// Submit and mine parent
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, parent))
	td.MineAndWait(t, 1)

	block3a, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 3)
	require.NoError(t, err)

	// 0 -> 1 -> 2a [grandparent] -> 3a [parent] (*)

	t.Logf("Chain A: block2a [grandparent] -> block3a [parent]")

	// Now create a conflicting child that:
	// 1. Spends grandparent:0 (same as parent - CONFLICT!)
	// 2. Spends grandparent:1 (different output)
	// This tests the case where child conflicts with parent on the same grandparent output
	conflictingChild := td.CreateTransactionWithOptions(t,
		transactions.WithInput(grandparent, 0), // CONFLICT with parent!
		transactions.WithInput(parent, 1),      // Additional output
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, outputAmount/numOutputsForExternalTx-1000),
	)
	t.Logf("ConflictingChild: %s (%d outputs) - spends grandparent:0 (CONFLICT) and grandparent:1",
		conflictingChild.TxIDChainHash().String(), len(conflictingChild.Outputs))

	// Create fork: block2b from block1
	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)

	// Create block2b with grandparent (same as 2a content)
	_, block2b := td.CreateTestBlock(t, block1, 10202, grandparent)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block2b, block2b.Height, "", "legacy"),
		"Failed to process block2b")

	// Create block3b with conflictingChild
	_, block3b := td.CreateTestBlock(t, block2b, 10302, conflictingChild)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block3b, block3b.Height, "", "legacy"),
		"Failed to process block3b")

	//        / 2a [grandparent] -> 3a [parent] (*)
	// 0 -> 1
	//        \ 2b [grandparent] -> 3b [conflictingChild]

	// Verify chain A is still winning
	td.WaitForBlockHeight(t, block3a, blockWait)

	// Verify parent is not conflicting (it's in the winning chain)
	td.VerifyConflictingInUtxoStore(t, false, parent)

	// Verify conflictingChild is marked as conflicting (it's in losing chain)
	td.VerifyConflictingInUtxoStore(t, true, conflictingChild)

	t.Log("Verified initial state: parent valid, conflictingChild conflicting")

	// Now make chain B longer by mining block4b
	_, block4b := td.CreateTestBlock(t, block3b, 10402)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block4b, block4b.Height, "", "legacy"),
		"Failed to process block4b")

	//        / 2a [grandparent] -> 3a [parent]
	// 0 -> 1
	//        \ 2b [grandparent] -> 3b [conflictingChild] -> 4b (*)

	td.WaitForBlockHeight(t, block4b, blockWait)

	// Now chain B is winning
	// Parent should be marked as conflicting
	td.VerifyConflictingInUtxoStore(t, true, parent)

	// ConflictingChild should NOT be conflicting (it's in the winning chain now)
	td.VerifyConflictingInUtxoStore(t, false, conflictingChild)

	t.Log("Verified after reorg: parent conflicting, conflictingChild valid")

	// Verify grandparent is in both chains (should not be conflicting)
	td.VerifyConflictingInSubtrees(t, block2a.Subtrees[0], grandparent)
	td.VerifyConflictingInSubtrees(t, block2b.Subtrees[0], grandparent)

	t.Log("Successfully verified grandparent-child conflict scenario")
}

// setupExternalTxDaemon creates a test daemon with external tx settings
func setupExternalTxDaemon(t *testing.T, utxoStoreType string) *daemon.TestDaemon {
	return daemon.NewTestDaemon(t, daemon.TestOptions{
		UTXOStoreType:        utxoStoreType,
		SettingsOverrideFunc: externalTxSettingsFunc(),
	})
}
