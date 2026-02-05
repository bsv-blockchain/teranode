package doublespendtest

import (
	"testing"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/require"
)

// TestGrandparentMultiOutputConflict tests a complex double-spend scenario with
// multiple overlapping outputs:
// - Grandparent transaction has 5 outputs
// - Parent transaction spends outputs 0 and 3 of grandparent
// - Child transaction (in fork) spends outputs 3 and 4 of grandparent + output of parent
//
// The conflict is on grandparent output 3 - both parent and child spend it.
// Additionally, child tries to spend from parent which is in a different chain.
func TestGrandparentMultiOutputConflictPostgres(t *testing.T) {
	t.Run("grandparent_multi_output_conflict", func(t *testing.T) {
		testGrandparentMultiOutputConflict(t, "postgres")
	})
}

func TestGrandparentMultiOutputConflictAerospike(t *testing.T) {
	t.Run("grandparent_multi_output_conflict", func(t *testing.T) {
		testGrandparentMultiOutputConflict(t, "aerospike")
	})
}

// testGrandparentMultiOutputConflict tests this scenario:
//
// Transaction Structure:
//
//	grandparent (5 outputs: 0,1,2,3,4)
//	├── parent (spends grandparent:0 and grandparent:3) -> in chain A
//	└── conflictingChild (spends grandparent:3, grandparent:4, AND parent:0) -> in chain B
//
// The conflict:
// - Both parent and conflictingChild spend grandparent:3
// - conflictingChild also tries to spend parent:0, but parent is in chain A
//
// Block Structure:
//
//	        / 2a [grandparent] -> 3a [parent] (*)
//	0 -> 1
//	        \ 2b [grandparent] -> 3b [conflictingChild] -> 4b (*)
//
// This tests:
// 1. Partial overlap conflict (grandparent:3 spent by both)
// 2. Cross-chain dependency (child depends on parent from other chain)
// 3. Multi-output external transaction handling
func testGrandparentMultiOutputConflict(t *testing.T, utxoStore string) {
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

	// Generate initial blocks
	coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, td.Ctx)

	// Create grandparent with 5 outputs (external tx)
	// Each output has outputAmount (100000) satoshis
	grandparent := td.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, outputAmount),
	)
	t.Logf("Grandparent: %s (%d outputs)", grandparent.TxIDChainHash().String(), len(grandparent.Outputs))
	t.Logf("  Outputs: [0]=%d, [1]=%d, [2]=%d, [3]=%d, [4]=%d",
		grandparent.Outputs[0].Satoshis,
		grandparent.Outputs[1].Satoshis,
		grandparent.Outputs[2].Satoshis,
		grandparent.Outputs[3].Satoshis,
		grandparent.Outputs[4].Satoshis)

	// Submit and mine grandparent
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, grandparent))
	require.NoError(t, td.WaitForTransactionInBlockAssembly(grandparent, blockWait))
	td.MineAndWait(t, 1)

	block3a, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 3)
	require.NoError(t, err)
	require.Equal(t, block3a.Height, uint32(0x3))
	// td.WaitForBlock(t, block3a, blockWait*2)

	// Create parent that spends grandparent outputs 0 and 3 (multi-input from different UTXO records)
	// Total input: 200000 satoshis
	parent := td.CreateTransactionWithOptions(t,
		transactions.WithInput(grandparent, 0),                        // Spend output 0
		transactions.WithInput(grandparent, 3),                        // Spend output 3 - THIS WILL CONFLICT!
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, 39000), // 5 * 39000 = 195000
	)
	t.Logf("Parent: %s (%d outputs) - spends grandparent:0 and grandparent:3",
		parent.TxIDChainHash().String(), len(parent.Outputs))

	// Submit and mine parent
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, parent))
	require.NoError(t, td.WaitForTransactionInBlockAssembly(parent, blockWait))
	td.MineAndWait(t, 1)

	block4a, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 4)
	require.NoError(t, err)

	td.VerifyOnLongestChainInUtxoStore(t, grandparent)
	td.VerifyOnLongestChainInUtxoStore(t, parent)
	td.WaitForBlockBeingMined(t, block4a)

	// 0 -> 1 -> 2a -> 3a [grandparent] -> 4a [parent] (*)

	t.Logf("Chain A established: block2a [grandparent] -> block3a [parent]")

	// Now create a conflicting child that:
	// 1. Spends grandparent:3 (CONFLICT with parent!)
	// 2. Spends grandparent:4 (no conflict)
	// 3. Tries to spend parent:0 (but parent is in other chain!)
	//
	// Note: In the fork, parent doesn't exist, so we can only spend grandparent outputs
	// The conflict is on grandparent:3 which both parent and conflictingChild try to spend
	conflictingChild := td.CreateTransactionWithOptions(t,
		transactions.WithInput(grandparent, 3), // CONFLICT with parent on this output!
		transactions.WithInput(grandparent, 4), // Additional grandparent output
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, 39000),
	)
	t.Logf("ConflictingChild: %s (%d outputs) - spends grandparent:3 (CONFLICT!) and grandparent:4",
		conflictingChild.TxIDChainHash().String(), len(conflictingChild.Outputs))

	// Create fork: block2b from block2
	block2, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 2)
	require.NoError(t, err)

	// Create block2b with grandparent (same tx in both chains)
	_, block3b := td.CreateTestBlock(t, block2, 10202, grandparent)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block3b, block3b.Height, "", "legacy"),
		"Failed to process block3b")

	// Create block3b with conflictingChild
	_, block4b := td.CreateTestBlock(t, block3b, 10302, conflictingChild)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block4b, block4b.Height, "", "legacy"),
		"Failed to process block4b")

	//        / 2a [grandparent] -> 3a [parent] (*)
	// 0 -> 1
	//        \ 2b [grandparent] -> 3b [conflictingChild]

	// Verify chain A is still winning
	td.WaitForBlockHeight(t, block4a, blockWait, true)

	// Verify conflict status before reorg
	t.Log("Verifying conflict status before reorg (chain A winning)...")

	// Parent is in winning chain - should NOT be conflicting
	td.VerifyConflictingInUtxoStore(t, false, parent)

	// ConflictingChild is in losing chain - should be conflicting
	td.VerifyConflictingInUtxoStore(t, true, conflictingChild)

	// Grandparent is in both chains - verify subtrees
	td.VerifyConflictingInSubtrees(t, block4a.Subtrees[0])
	td.VerifyConflictingInSubtrees(t, block4b.Subtrees[0], conflictingChild)

	t.Log("Initial state verified: parent valid, conflictingChild conflicting")

	// Now make chain B longer by mining block4b
	_, block5b := td.CreateTestBlock(t, block4b, 10402)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block5b, block5b.Height, "", "legacy"),
		"Failed to process block5b")

	//        / 2a [grandparent] -> 3a [parent]
	// 0 -> 1
	//        \ 2b [grandparent] -> 3b [conflictingChild] -> 4b (*)

	td.WaitForBlockHeight(t, block5b, blockWait, true)

	// Verify conflict status after reorg
	t.Log("Verifying conflict status after reorg (chain B winning)...")

	// Parent is now in losing chain - should be conflicting
	td.VerifyConflictingInUtxoStore(t, true, parent)

	// ConflictingChild is now in winning chain - should NOT be conflicting
	td.VerifyConflictingInUtxoStore(t, false, conflictingChild)

	t.Log("After reorg verified: parent conflicting, conflictingChild valid")

	// Additional verification: make sure parent is not in block assembly
	td.VerifyNotInBlockAssembly(t, parent)

	// ConflictingChild should also not be in block assembly (already mined)
	td.VerifyNotInBlockAssembly(t, conflictingChild)

	t.Log("Successfully verified grandparent multi-output conflict scenario")

	td.VerifyConflictingInSubtrees(t, block4a.Subtrees[0], parent)
	td.VerifyConflictingInSubtrees(t, block4b.Subtrees[0], conflictingChild)
}

// TestGrandparentChildWithParentDependency tests a scenario where
// the conflicting child tries to spend from BOTH grandparent AND parent.
// This creates an invalid chain since parent doesn't exist in chain B.
func TestGrandparentChildWithParentDependencyPostgres(t *testing.T) {
	t.Run("grandparent_child_with_parent_dependency", func(t *testing.T) {
		testGrandparentChildWithParentDependency(t, "postgres")
	})
}

func TestGrandparentChildWithParentDependencyAerospike(t *testing.T) {
	t.Run("grandparent_child_with_parent_dependency", func(t *testing.T) {
		testGrandparentChildWithParentDependency(t, "aerospike")
	})
}

// testGrandparentChildWithParentDependency tests this scenario:
//
// Transaction Structure:
//
//	grandparent (5 outputs)
//	├── parent (spends grandparent:0, grandparent:3) -> creates 5 outputs
//	│
//	└── child A (in main chain): spends parent:0
//	    child B (in fork): spends grandparent:3, grandparent:4, parent:0
//
// This is interesting because child B:
// 1. Conflicts with parent on grandparent:3
// 2. Tries to spend parent:0, but if parent is invalid, so is child B
//
// The test verifies that this invalid dependency is handled correctly.
func testGrandparentChildWithParentDependency(t *testing.T, utxoStore string) {
	// Setup test environment
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

	// Generate initial blocks
	coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, td.Ctx)

	// Create grandparent with 5 outputs
	grandparent := td.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbaseTx, 0),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, outputAmount),
	)
	t.Logf("Grandparent: %s (%d outputs)", grandparent.TxIDChainHash().String(), len(grandparent.Outputs))

	// Submit and mine grandparent
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, grandparent))
	require.NoError(t, td.WaitForTransactionInBlockAssembly(grandparent, blockWait))
	td.MineAndWait(t, 1)
	block3a, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 3)
	require.NoError(t, err)

	// Create parent spending grandparent:0 and grandparent:3
	parent := td.CreateTransactionWithOptions(t,
		transactions.WithInput(grandparent, 0),
		transactions.WithInput(grandparent, 3),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, 39000),
	)
	t.Logf("Parent: %s (%d outputs) - spends grandparent:0,3", parent.TxIDChainHash().String(), len(parent.Outputs))

	// Submit and mine parent
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, parent))
	require.NoError(t, td.WaitForTransactionInBlockAssembly(parent, blockWait))
	td.MineAndWait(t, 1)

	// Create child A that spends parent:0 (normal child in main chain)
	childA := td.CreateTransactionWithOptions(t,
		transactions.WithInput(parent, 0),
		transactions.WithInput(parent, 1),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, 15000),
	)
	t.Logf("ChildA: %s (%d outputs) - spends parent:0,1", childA.TxIDChainHash().String(), len(childA.Outputs))

	// Submit and mine childA
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, childA))
	require.NoError(t, td.WaitForTransactionInBlockAssembly(childA, blockWait))
	td.MineAndWait(t, 1)

	block4a, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 4)
	require.NoError(t, err)

	// 0 -> 1 -> 2 -> 3 [grandparent] -> 4a [parent] -> 5a [child]

	t.Logf("Main chain: grandparent -> parent -> childA")

	// Now create a conflicting scenario in a fork
	// Child B will spend:
	// - grandparent:3 (CONFLICT with parent!)
	// - grandparent:4 (no conflict)
	childB := td.CreateTransactionWithOptions(t,
		transactions.WithInput(grandparent, 3), // CONFLICT with parent!
		transactions.WithInput(grandparent, 4), // No conflict
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, 39000),
	)
	t.Logf("ChildB: %s (%d outputs) - spends grandparent:3 (CONFLICT!), grandparent:4",
		childB.TxIDChainHash().String(), len(childB.Outputs))

	// Create block4b with childB (skipping parent entirely)
	_, block4b := td.CreateTestBlock(t, block3a, 10302, childB)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block4b, block4b.Height, "", "legacy"),
		"Failed to process block4b")

	//                  / 3a [parent] -> 4a [childA] (*)
	// 0 -> 1 -> 2 [grandparent]
	//                  \ 3b [childB]

	// Verify main chain is winning
	td.WaitForBlockHeight(t, block4a, blockWait, true)

	// Verify conflict status
	td.VerifyConflictingInUtxoStore(t, false, parent)
	td.VerifyConflictingInUtxoStore(t, false, childA)
	td.VerifyConflictingInUtxoStore(t, true, childB)

	t.Log("Before reorg: parent and childA valid, childB conflicting")

	// Make fork longer to trigger reorg
	_, block5b := td.CreateTestBlock(t, block4b, 10402)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block5b, block5b.Height, "", "legacy"))

	_, block6b := td.CreateTestBlock(t, block5b, 10502)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block6b, block6b.Height, "", "legacy"))

	//                  / 3a [parent] -> 4a [childA]
	// 0 -> 1 -> 2 [grandparent]
	//                  \ 3b [childB] -> 4b -> 5b (*)

	td.WaitForBlockHeight(t, block6b, blockWait, true)

	// After reorg:
	// - parent should be conflicting (spends grandparent:3 which childB also spends)
	// - childA should be conflicting (depends on parent which is now conflicting)
	// - childB should be valid (in winning chain)
	td.VerifyConflictingInUtxoStore(t, true, parent)
	td.VerifyConflictingInUtxoStore(t, true, childA) // childA depends on parent!
	td.VerifyConflictingInUtxoStore(t, false, childB)

	t.Log("After reorg: parent and childA conflicting (cascade), childB valid")

	t.Log("Successfully verified grandparent-child with parent dependency scenario")
}
