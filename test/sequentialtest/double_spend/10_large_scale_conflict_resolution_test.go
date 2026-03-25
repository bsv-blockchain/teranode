package doublespendtest

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDeepChainConflictResolutionPostgres tests conflict resolution across a 10-level deep
// spending chain. This would have caught the unbounded recursion bug in PR #614 where
// recursive conflict resolution hung for 6+ hours on mainnet block 941681.
func TestDeepChainConflictResolutionPostgres(t *testing.T) {
	t.Run("deep_chain_conflict_resolution", func(t *testing.T) {
		testDeepChainConflictResolution(t, "postgres")
	})
}

func TestDeepChainConflictResolutionAerospike(t *testing.T) {
	t.Run("deep_chain_conflict_resolution", func(t *testing.T) {
		testDeepChainConflictResolution(t, "aerospike")
	})
}

// testDeepChainConflictResolution builds a 10-level deep spending chain (txA0 -> txA9),
// mines it across multiple blocks, then creates a fork that double-spends the root.
// When the fork wins, conflict resolution must traverse all 10 levels to mark
// every descendant as conflicting.
//
// Chain A (mined first):
//
//	txA0 -> txA1 -> txA2 -> txA3 -> txA4 -> txA5 -> txA6 -> txA7 -> txA8 -> txA9
//
// Chain B (fork, conflicts with txA0):
//
//	txB0 (double-spends parentTx:0)
//
// Block layout:
//
//	                / 103a [txA0] -> 104a [txA1..txA4] -> 105a [txA5..txA9]
//	102a [parentTx]
//	                \ 103b [txB0] -> 104b -> 105b -> 106b (wins by length)
func testDeepChainConflictResolution(t *testing.T, utxoStore string) {
	td, _, txA0, txB0, block102a, _ := setupExternalTxDoubleSpendTest(t, utxoStore, 50)
	defer func() {
		td.Stop(t)
	}()

	t.Logf("txA0 (root): %s (%d outputs)", txA0.TxIDChainHash().String(), len(txA0.Outputs))

	// Mine txA0 in block 103a
	_, block103a := td.CreateTestBlock(t, block102a, 50301, txA0)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103a, block103a.Height, "", "legacy"))
	td.WaitForBlockHeight(t, block103a, blockWait, true)

	// Build a 10-level deep chain from txA0. Each tx spends outputs 0,1 from its parent
	// and creates 5 outputs. Amounts decrease each level but stay above dust+fee threshold.
	chainAmounts := []uint64{
		5_000_000, // txA1
		1_950_000, // txA2
		760_000,   // txA3
		295_000,   // txA4
		115_000,   // txA5
		44_000,    // txA6
		17_000,    // txA7
		6_500,     // txA8
		2_400,     // txA9
	}

	chainA := make([]*bt.Tx, 10)
	chainA[0] = txA0

	prev := txA0
	for i := 0; i < 9; i++ {
		tx := td.CreateTransactionWithOptions(t,
			transactions.WithInput(prev, 0),
			transactions.WithInput(prev, 1),
			transactions.WithP2PKHOutputs(numOutputsForExternalTx, chainAmounts[i]),
		)
		chainA[i+1] = tx
		t.Logf("Chain A txA%d: %s (%d outputs, %d sats/output)", i+1,
			tx.TxIDChainHash().String(), len(tx.Outputs), chainAmounts[i])
		prev = tx
	}

	// Mine chain A across two blocks
	// Block 104a: txA1..txA4
	subtree104a, block104a := td.CreateTestBlock(t, block103a, 50401, chainA[1], chainA[2], chainA[3], chainA[4])
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block104a, block104a.Height, "", "legacy"))
	td.WaitForBlockHeight(t, block104a, blockWait, true)

	// Block 105a: txA5..txA9
	subtree105a, block105a := td.CreateTestBlock(t, block104a, 50501, chainA[5], chainA[6], chainA[7], chainA[8], chainA[9])
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block105a, block105a.Height, "", "legacy"))
	td.WaitForBlockHeight(t, block105a, blockWait, true)

	// Verify chain A txs are not conflicting
	td.VerifyConflictingInUtxoStore(t, false, chainA[1], chainA[2], chainA[3], chainA[4])
	td.VerifyConflictingInSubtrees(t, subtree104a.RootHash())
	td.VerifyConflictingInUtxoStore(t, false, chainA[5], chainA[6], chainA[7], chainA[8], chainA[9])
	td.VerifyConflictingInSubtrees(t, subtree105a.RootHash())

	// Create fork: block 103b with txB0 (conflicts with txA0 on parentTx:0)
	block103b := createConflictingBlockUseExternalRecords(t, td, block103a,
		[]*bt.Tx{txB0},
		[]*bt.Tx{txA0},
		50302,
	)
	assert.NotNil(t, block103b)

	// Chain A is still winning (tip at 105a vs 103b)
	td.WaitForBlockHeight(t, block105a, blockWait, true)

	// Mine empty blocks on chain B to overtake chain A
	_, block104b := td.CreateTestBlock(t, block103b, 50402)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block104b, block104b.Height, "", "legacy"))

	_, block105b := td.CreateTestBlock(t, block104b, 50502)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block105b, block105b.Height, "", "legacy"))

	// Block 106b makes chain B longer — triggers reorg and conflict resolution
	// This is the critical section: the old recursive code would hang here
	reorgStart := time.Now()

	_, block106b := td.CreateTestBlock(t, block105b, 50602)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block106b, block106b.Height, "", "legacy"))
	td.WaitForBlockHeight(t, block106b, 30*time.Second, true)

	reorgDuration := time.Since(reorgStart)
	t.Logf("Reorg with 10-level deep conflict resolution completed in %v", reorgDuration)
	require.Less(t, reorgDuration, 30*time.Second,
		"Conflict resolution took too long — possible regression to recursive traversal")

	// Verify ALL chain A transactions are now conflicting (the full 10-level cascade)
	td.VerifyConflictingInUtxoStore(t, true, chainA[0])
	td.VerifyConflictingInUtxoStore(t, true, chainA[1], chainA[2], chainA[3], chainA[4])
	td.VerifyConflictingInSubtrees(t, subtree104a.RootHash(), chainA[1], chainA[2], chainA[3], chainA[4])
	td.VerifyConflictingInUtxoStore(t, true, chainA[5], chainA[6], chainA[7], chainA[8], chainA[9])
	td.VerifyConflictingInSubtrees(t, subtree105a.RootHash(), chainA[5], chainA[6], chainA[7], chainA[8], chainA[9])

	// Verify txB0 is not conflicting (it's on the winning chain)
	td.VerifyConflictingInUtxoStore(t, false, txB0)

	t.Log("Successfully verified 10-level deep conflict resolution")
}

// TestWideTreeConflictResolutionPostgres tests conflict resolution across a wide transaction
// tree where each node has multiple children. This exercises the BFS fan-out that triggered
// the map-mutation-during-range bug in the old recursive code.
func TestWideTreeConflictResolutionPostgres(t *testing.T) {
	t.Run("wide_tree_conflict_resolution", func(t *testing.T) {
		testWideTreeConflictResolution(t, "postgres")
	})
}

func TestWideTreeConflictResolutionAerospike(t *testing.T) {
	t.Run("wide_tree_conflict_resolution", func(t *testing.T) {
		testWideTreeConflictResolution(t, "aerospike")
	})
}

// testWideTreeConflictResolution builds a 3-level wide tree with 4 children at level 1
// and 2 grandchildren per child at level 2 (~12 total txs), then double-spends the root.
//
// Chain A tree (all txs have 5 outputs each):
//
//	txA_root (spends parentTx:0,1)
//	├── child0 (spends root:0)
//	│   ├── gc0 (spends child0:0)
//	│   └── gc1 (spends child0:1)
//	├── child1 (spends root:1)
//	│   ├── gc2 (spends child1:0)
//	│   └── gc3 (spends child1:1)
//	├── child2 (spends root:2)
//	│   ├── gc4 (spends child2:0)
//	│   └── gc5 (spends child2:1)
//	└── child3 (spends root:3)
//	    └── gc6 (spends child3:0)
//
// Chain B (fork):
//
//	txB_root (double-spends parentTx:0)
//
// Block layout:
//
//	                / 103a [txA_root] -> 104a [child0..3, gc0..6]
//	102a [parentTx]
//	                \ 103b [txB_root] -> 104b -> 105b (wins by length)
func testWideTreeConflictResolution(t *testing.T, utxoStore string) {
	td, _, txARoot, txBRoot, block102a, _ := setupExternalTxDoubleSpendTest(t, utxoStore, 51)
	defer func() {
		td.Stop(t)
	}()

	t.Logf("txA_root: %s (%d outputs)", txARoot.TxIDChainHash().String(), len(txARoot.Outputs))

	// Mine txA_root in block 103a
	_, block103a := td.CreateTestBlock(t, block102a, 51301, txARoot)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103a, block103a.Height, "", "legacy"))
	td.WaitForBlockHeight(t, block103a, blockWait, true)

	// Build wide tree: 4 children from txA_root, each spending a different output
	childAmount := uint64(1_000_000)
	gcAmount := uint64(100_000)

	child0 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(txARoot, 0),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, childAmount),
	)
	child1 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(txARoot, 1),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, childAmount),
	)
	child2 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(txARoot, 2),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, childAmount),
	)
	child3 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(txARoot, 3),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, childAmount),
	)

	t.Logf("child0: %s", child0.TxIDChainHash().String())
	t.Logf("child1: %s", child1.TxIDChainHash().String())
	t.Logf("child2: %s", child2.TxIDChainHash().String())
	t.Logf("child3: %s", child3.TxIDChainHash().String())

	// Grandchildren: 2 per child (except child3 which gets 1)
	gc0 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(child0, 0),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, gcAmount),
	)
	gc1 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(child0, 1),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, gcAmount),
	)
	gc2 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(child1, 0),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, gcAmount),
	)
	gc3 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(child1, 1),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, gcAmount),
	)
	gc4 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(child2, 0),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, gcAmount),
	)
	gc5 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(child2, 1),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, gcAmount),
	)
	gc6 := td.CreateTransactionWithOptions(t,
		transactions.WithInput(child3, 0),
		transactions.WithP2PKHOutputs(numOutputsForExternalTx, gcAmount),
	)

	t.Logf("Created 7 grandchildren (gc0-gc6)")

	// Mine all tree txs in block 104a (children first, then grandchildren — dependency order)
	allTreeTxs := []*bt.Tx{child0, child1, child2, child3, gc0, gc1, gc2, gc3, gc4, gc5, gc6}
	subtree104a, block104a := td.CreateTestBlock(t, block103a, 51401, allTreeTxs...)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block104a, block104a.Height, "", "legacy"))
	td.WaitForBlockHeight(t, block104a, blockWait, true)

	// Verify tree txs are not conflicting
	td.VerifyConflictingInUtxoStore(t, false, allTreeTxs...)
	td.VerifyConflictingInSubtrees(t, subtree104a.RootHash())

	// Create fork: block 103b with txB_root (conflicts with txA_root on parentTx:0)
	block103b := createConflictingBlockUseExternalRecords(t, td, block103a,
		[]*bt.Tx{txBRoot},
		[]*bt.Tx{txARoot},
		51302,
	)
	assert.NotNil(t, block103b)

	// Chain A still winning (104a vs 103b)
	td.WaitForBlockHeight(t, block104a, blockWait, true)

	// Mine empty blocks on chain B to overtake
	_, block104b := td.CreateTestBlock(t, block103b, 51402)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block104b, block104b.Height, "", "legacy"))

	// Block 105b makes chain B longer — triggers reorg
	reorgStart := time.Now()

	_, block105b := td.CreateTestBlock(t, block104b, 51502)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block105b, block105b.Height, "", "legacy"))
	td.WaitForBlockHeight(t, block105b, 30*time.Second, true)

	reorgDuration := time.Since(reorgStart)
	t.Logf("Reorg with wide tree conflict resolution completed in %v", reorgDuration)
	require.Less(t, reorgDuration, 30*time.Second,
		"Conflict resolution took too long — possible regression to recursive traversal")

	// Verify the entire tree is now conflicting (root + 4 children + 7 grandchildren = 12 txs)
	td.VerifyConflictingInUtxoStore(t, true, txARoot)
	td.VerifyConflictingInUtxoStore(t, true, allTreeTxs...)
	td.VerifyConflictingInSubtrees(t, subtree104a.RootHash(), allTreeTxs...)

	// Verify txB_root is not conflicting (winning chain)
	td.VerifyConflictingInUtxoStore(t, false, txBRoot)

	t.Log("Successfully verified wide tree conflict resolution (12 transactions)")
}
