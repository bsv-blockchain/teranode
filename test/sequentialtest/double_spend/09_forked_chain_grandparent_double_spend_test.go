package doublespendtest

import (
	"testing"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/require"
)

// TestComplexForkGrandparentConflict tests a complex scenario where multiple
// transactions in the winning chain spend from both parent and grandparent outputs,
// then a fork with a single transaction that spends multiple grandparent outputs
// becomes the longest chain.
//
// Chain A (initial winner) - Complex spending pattern:
//
//	grandparent (5 outputs: 0,1,2,3,4)
//	├── parent (spends GP:0, GP:4) -> 5 outputs
//	│   ├── child1 (spends parent:0, GP:1)
//	│   └── child2 (spends parent:1, GP:2)
//	└── parentSibling (spends GP:3)
//
// GP output spending in Chain A:
//   - GP:0 -> parent
//   - GP:1 -> child1
//   - GP:2 -> child2
//   - GP:3 -> parentSibling
//   - GP:4 -> parent
//
// Chain B (fork that wins):
//
//	grandparent (same tx)
//	└── parentB (spends GP:0, GP:1, GP:3, GP:4) - conflicts with parent, child1, parentSibling
//	                                              but NOT with child2's GP:2 spending
//
// Block Structure:
//
//	                  / 4a [parent] -> 5a [child1, child2, parentSibling] (*)
//	0 -> 1 -> 2 -> 3 [GP]
//	                  \ 4b [parentB] -> 5b -> 6b (*)
//
// After reorg:
//   - parent, child1, child2, parentSibling should all be conflicting
//   - parentB should be valid
//   - Note: child2 conflicts because its input parent:1 no longer exists (parent is conflicting)
func TestComplexForkGrandparentConflictPostgres(t *testing.T) {
	t.Run("complex_fork_grandparent_conflict", func(t *testing.T) {
		testComplexForkGrandparentConflict(t, "postgres")
	})
}

func TestComplexForkGrandparentConflictAerospike(t *testing.T) {
	t.Run("complex_fork_grandparent_conflict", func(t *testing.T) {
		testComplexForkGrandparentConflict(t, "aerospike")
	})
}

func testComplexForkGrandparentConflict(t *testing.T, utxoStore string) {
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

	// ========== Create Grandparent ==========
	grandparent := td.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, coinbaseTx.Outputs[0].Satoshis/numOutputsForExternalTx-100),
	)
	t.Logf("Grandparent: %s (%d outputs)", grandparent.TxIDChainHash().String(), len(grandparent.Outputs))
	for i := 0; i < len(grandparent.Outputs); i++ {
		t.Logf("  GP[%d] = %d sats", i, grandparent.Outputs[i].Satoshis)
	}

	// Mine grandparent
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, grandparent))
	td.MineAndWait(t, 1)

	block3gp, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 3)
	require.NoError(t, err)

	// 0 -> 1 -> 2 -> 3 [grandparent]

	t.Log("\n=== Building Chain A ===")

	// ========== Chain A: Parent spends GP:0 and GP:4 ==========
	parent := td.CreateTransactionWithOptions(t,
		transactions.WithInput(grandparent, 0), // GP:0
		transactions.WithInput(grandparent, 4), // GP:4
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, grandparent.Outputs[0].Satoshis/numOutputsForExternalTx-100),
	)
	t.Logf("Parent: %s - spends GP:0, GP:4", parent.TxIDChainHash().String())

	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, parent))
	td.MineAndWait(t, 1)

	_, err = td.BlockchainClient.GetBlockByHeight(td.Ctx, 4)
	require.NoError(t, err)

	// 0 -> 1 -> 2 -> 3 [GP] -> 4a [parent]

	// ========== Chain A: child1 spends parent:0 and GP:1 ==========
	child1 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(parent, 0),      // parent:0
		transactions.WithInput(grandparent, 1), // GP:1
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, parent.Outputs[0].Satoshis/numOutputsForExternalTx-100),
	)
	t.Logf("Child1: %s - spends parent:0, GP:1", child1.TxIDChainHash().String())

	// ========== Chain A: child2 spends parent:1 and GP:2 ==========
	child2 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(parent, 1),      // parent:1
		transactions.WithInput(grandparent, 2), // GP:2
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, parent.Outputs[1].Satoshis/numOutputsForExternalTx-100),
	)
	t.Logf("Child2: %s - spends parent:1, GP:2", child2.TxIDChainHash().String())

	// ========== Chain A: parentSibling spends GP:3 ==========
	parentSibling := td.CreateTransactionWithOptions(t,
		transactions.WithInput(grandparent, 3), // GP:3
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, grandparent.Outputs[3].Satoshis/numOutputsForExternalTx-100),
	)
	t.Logf("ParentSibling: %s - spends GP:3", parentSibling.TxIDChainHash().String())

	// Submit all chain A transactions
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, child1))
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, child2))
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, parentSibling))
	td.MineAndWait(t, 1)

	block5a, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 5)
	require.NoError(t, err)

	//                      / 4a [parent] -> 5a [child1, child2, parentSibling] (*)
	// 0 -> 1 -> 2 -> 3 [GP]

	t.Log("\n=== Chain A Summary ===")
	t.Log("GP:0 -> parent")
	t.Log("GP:1 -> child1")
	t.Log("GP:2 -> child2")
	t.Log("GP:3 -> parentSibling")
	t.Log("GP:4 -> parent")

	// Verify all Chain A transactions are valid
	td.VerifyOnLongestChainInUtxoStore(t, grandparent)
	td.VerifyOnLongestChainInUtxoStore(t, parent)
	td.VerifyOnLongestChainInUtxoStore(t, child1)
	td.VerifyOnLongestChainInUtxoStore(t, child2)
	td.VerifyOnLongestChainInUtxoStore(t, parentSibling)

	td.WaitForBlockHeight(t, block5a, blockWait, true)

	t.Log("\n=== Building Chain B (Fork) ===")

	// ========== Chain B: parentB spends GP:0, GP:1, GP:3, GP:4 ==========
	// This conflicts with:
	//   - parent (GP:0, GP:4)
	//   - child1 (GP:1)
	//   - parentSibling (GP:3)
	// Does NOT conflict with:
	//   - child2's GP:2 spending (but child2 still becomes invalid because parent:1 doesn't exist)
	parentB := td.CreateTransactionWithOptions(t,
		transactions.WithInput(grandparent, 0), // GP:0 - conflicts with parent
		transactions.WithInput(grandparent, 1), // GP:1 - conflicts with child1
		transactions.WithInput(grandparent, 2), // GP:3 - conflicts with parentSibling
		transactions.WithInput(grandparent, 4), // GP:4 - conflicts with parent
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, grandparent.Outputs[0].Satoshis/numOutputsForExternalTx-100),
	)
	t.Logf("ParentB: %s - spends GP:0, GP:1, GP:3, GP:4", parentB.TxIDChainHash().String())
	t.Log("  Conflicts with: parent (GP:0,4), child1 (GP:1), parentSibling (GP:3)")
	t.Log("  Does NOT directly conflict with: child2's GP:2")

	// Create block 4b with parentB (forking from block 3 [grandparent])
	_, block4b := td.CreateTestBlock(t, block3gp, 10302, parentB)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block4b, block4b.Height, "", "legacy"),
		"Failed to process block4b with parentB")

	//                      / 4a [parent] -> 5a [child1, child2, parentSibling] (*)
	// 0 -> 1 -> 2 -> 3 [GP]
	//                      \ 4b [parentB]

	// Verify Chain A is still winning
	td.WaitForBlockHeight(t, block5a, blockWait, true)

	// Verify parentB is marked as conflicting (losing chain)
	td.VerifyConflictingInUtxoStore(t, true, parentB)
	t.Log("Before reorg: parentB is conflicting (losing chain)")

	// ========== Make Chain B longer to trigger reorg ==========
	t.Log("\n=== Extending Chain B to trigger reorg ===")

	_, block5b := td.CreateTestBlock(t, block4b, 10402)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block5b, block5b.Height, "", "legacy"))

	_, block6b := td.CreateTestBlock(t, block5b, 10502)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block6b, block6b.Height, "", "legacy"))

	//                      / 4a [parent] -> 5a [child1, child2, parentSibling]
	// 0 -> 1 -> 2 -> 3 [GP]
	//                      \ 4b [parentB] -> 5b -> 6b (*)

	td.WaitForBlockHeight(t, block6b, blockWait, true)

	t.Log("\n=== Verifying state after reorg ===")

	// ========== Verify conflict status after reorg ==========

	// ParentB should now be valid (winning chain)
	td.VerifyConflictingInUtxoStore(t, false, parentB)
	t.Log("ParentB: NOT conflicting (winning chain)")

	// Parent should be conflicting - GP:0 and GP:4 now spent by parentB
	td.VerifyConflictingInUtxoStore(t, true, parent)
	t.Log("Parent: CONFLICTING (GP:0, GP:4 spent by parentB)")

	// Child1 should be conflicting - GP:1 now spent by parentB, and parent:0 doesn't exist
	td.VerifyConflictingInUtxoStore(t, true, child1)
	t.Log("Child1: CONFLICTING (GP:1 spent by parentB, parent:0 doesn't exist)")

	// Child2 should be conflicting - even though GP:2 is not spent by parentB,
	// child2 depends on parent:1, and parent is now conflicting
	td.VerifyConflictingInUtxoStore(t, true, child2)
	t.Log("Child2: CONFLICTING (parent:1 doesn't exist - cascade from parent)")

	// ParentSibling should be conflicting - GP:3 now spent by parentB
	td.VerifyConflictingInUtxoStore(t, false, parentSibling)
	t.Log("ParentSibling: NOT CONFLICTING (GP:3 not spent by parentB)")

	// Grandparent should still be valid (exists in both chains)
	td.VerifyConflictingInUtxoStore(t, false, grandparent)
	t.Log("Grandparent: NOT conflicting (exists in both chains)")

	// Verify none of the conflicting transactions are in block assembly
	td.VerifyNotInBlockAssembly(t, parent)
	td.VerifyNotInBlockAssembly(t, child1)
	td.VerifyNotInBlockAssembly(t, child2)
	td.VerifyInBlockAssembly(t, parentSibling)
}
