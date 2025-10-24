package subtreevalidation

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/stretchr/testify/require"
)

// TestPrepareTxsPerLevel_ExternalParents tests that transactions with parents
// outside the subtree are not incorrectly assigned to level 0.
//
// This test demonstrates the bug where transactions with external parents
// (parents not in the current subtree) are assigned level 0, causing them
// to be validated before their parents are available, resulting in TX_NOT_FOUND errors.
func TestPrepareTxsPerLevel_ExternalParents(t *testing.T) {
	t.Run("transaction with external parent should not be level 0", func(t *testing.T) {
		s := &Server{}

		// Create a simple chain:
		// parentTx (external, not in subtree) -> childTx (in subtree)
		parentTx := tx1.Clone()
		childTx := bt.NewTx()

		// Make childTx spend from parentTx (which is NOT in the subtree)
		err := childTx.FromUTXOs(&bt.UTXO{
			TxIDHash:      parentTx.TxIDChainHash(),
			Vout:          0,
			LockingScript: parentTx.Outputs[0].LockingScript,
			Satoshis:      parentTx.Outputs[0].Satoshis,
		})
		require.NoError(t, err)

		err = childTx.AddP2PKHOutputFromScript(parentTx.Outputs[0].LockingScript, 1000)
		require.NoError(t, err)

		// Create transactions slice with ONLY childTx (parentTx is external)
		transactions := []missingTx{
			{
				tx:  childTx,
				idx: 0,
			},
		}

		// Call prepareTxsPerLevel
		maxLevel, txsPerLevel, err := s.prepareTxsPerLevel(context.Background(), transactions)
		require.NoError(t, err)

		// BUG DEMONSTRATION: Currently, childTx is assigned to level 0 because its parent
		// is not in the subtree, so len(dependencies[childTx]) == 0
		//
		// This is INCORRECT because childTx has a parent (just not in the subtree).
		// When childTx is validated at level 0, it will fail with TX_NOT_FOUND
		// because parentTx is not available yet.

		// Verify the current buggy behaviour exists
		currentMaxLevel := maxLevel
		currentLevel0Count := len(txsPerLevel[0])

		// Document what currently happens (will pass, showing the bug exists)
		t.Logf("Current buggy behaviour: maxLevel=%d, level0_count=%d", currentMaxLevel, currentLevel0Count)

		// This assertion will FAIL when the bug is fixed
		// The bug is that transactions with external parents are assigned to level 0
		if currentMaxLevel == 0 && currentLevel0Count == 1 {
			t.Fatal("BUG CONFIRMED: Transaction with external parent is incorrectly assigned to level 0. " +
				"This will cause TX_NOT_FOUND errors during validation because the parent transaction " +
				"is not in the subtree and won't be available when this transaction is validated.")
		}
	})

	t.Run("mixed transactions with internal and external parents", func(t *testing.T) {
		s := &Server{}

		// Create a more complex scenario:
		// externalParent (not in subtree) -> tx1 (in subtree) -> tx2 (in subtree)
		externalParent := parentTx1.Clone()
		tx1InSubtree := bt.NewTx()
		tx2InSubtree := bt.NewTx()

		// tx1 spends from external parent
		err := tx1InSubtree.FromUTXOs(&bt.UTXO{
			TxIDHash:      externalParent.TxIDChainHash(),
			Vout:          0,
			LockingScript: externalParent.Outputs[0].LockingScript,
			Satoshis:      externalParent.Outputs[0].Satoshis,
		})
		require.NoError(t, err)

		err = tx1InSubtree.AddP2PKHOutputFromScript(externalParent.Outputs[0].LockingScript, 5000)
		require.NoError(t, err)

		// tx2 spends from tx1 (both in subtree)
		err = tx2InSubtree.FromUTXOs(&bt.UTXO{
			TxIDHash:      tx1InSubtree.TxIDChainHash(),
			Vout:          0,
			LockingScript: tx1InSubtree.Outputs[0].LockingScript,
			Satoshis:      tx1InSubtree.Outputs[0].Satoshis,
		})
		require.NoError(t, err)

		err = tx2InSubtree.AddP2PKHOutputFromScript(tx1InSubtree.Outputs[0].LockingScript, 4000)
		require.NoError(t, err)

		// Only tx1 and tx2 are in the subtree (externalParent is not)
		transactions := []missingTx{
			{
				tx:  tx1InSubtree,
				idx: 0,
			},
			{
				tx:  tx2InSubtree,
				idx: 1,
			},
		}

		maxLevel, txsPerLevel, err := s.prepareTxsPerLevel(context.Background(), transactions)
		require.NoError(t, err)

		// Find which level each tx is at
		tx1Level := -1
		tx2Level := -1

		for level, txs := range txsPerLevel {
			for _, mtx := range txs {
				if mtx.tx.TxID() == tx1InSubtree.TxID() {
					tx1Level = level
				}
				if mtx.tx.TxID() == tx2InSubtree.TxID() {
					tx2Level = level
				}
			}
		}

		t.Logf("Current behaviour: tx1_level=%d, tx2_level=%d, maxLevel=%d", tx1Level, tx2Level, maxLevel)

		// BUG CONFIRMED: tx1 is at level 0 even though it has an external parent
		// When tx1 is validated at level 0, it will try to look up externalParent
		// and fail with TX_NOT_FOUND, exactly like the error in the bug report.
		if tx1Level == 0 && tx2Level == 1 {
			t.Fatal("BUG CONFIRMED: tx1 is at level 0 despite having an external parent. " +
				"This causes TX_NOT_FOUND when trying to validate tx1 before its parent is available. " +
				"tx2 is correctly at level 1 (depends on tx1 which is in subtree).")
		}
	})

	t.Run("all transactions have external parents", func(t *testing.T) {
		s := &Server{}

		// Scenario: Multiple independent transactions, all with external parents
		// None of them depend on each other, but all depend on external txs
		externalParent1 := parentTx1.Clone()
		externalParent2 := tx1.Clone()

		childTx1 := bt.NewTx()
		childTx2 := bt.NewTx()

		// childTx1 spends from externalParent1 (not in subtree)
		err := childTx1.FromUTXOs(&bt.UTXO{
			TxIDHash:      externalParent1.TxIDChainHash(),
			Vout:          0,
			LockingScript: externalParent1.Outputs[0].LockingScript,
			Satoshis:      externalParent1.Outputs[0].Satoshis,
		})
		require.NoError(t, err)

		err = childTx1.AddP2PKHOutputFromScript(externalParent1.Outputs[0].LockingScript, 3000)
		require.NoError(t, err)

		// childTx2 spends from externalParent2 (not in subtree)
		err = childTx2.FromUTXOs(&bt.UTXO{
			TxIDHash:      externalParent2.TxIDChainHash(),
			Vout:          0,
			LockingScript: externalParent2.Outputs[0].LockingScript,
			Satoshis:      externalParent2.Outputs[0].Satoshis,
		})
		require.NoError(t, err)

		err = childTx2.AddP2PKHOutputFromScript(externalParent2.Outputs[0].LockingScript, 2000)
		require.NoError(t, err)

		// Both child transactions are in the subtree, but their parents are not
		transactions := []missingTx{
			{
				tx:  childTx1,
				idx: 0,
			},
			{
				tx:  childTx2,
				idx: 1,
			},
		}

		maxLevel, txsPerLevel, err := s.prepareTxsPerLevel(context.Background(), transactions)
		require.NoError(t, err)

		t.Logf("Current behaviour: maxLevel=%d, level0_count=%d", maxLevel, len(txsPerLevel[0]))

		// BUG CONFIRMED: Both transactions are assigned to level 0
		// This is problematic because when these transactions are validated,
		// they will all fail with TX_NOT_FOUND errors for their parent transactions.
		if maxLevel == 0 && len(txsPerLevel[0]) == 2 {
			t.Fatal("BUG CONFIRMED: Both transactions are at level 0 despite having external parents. " +
				"During validation, both will fail with TX_NOT_FOUND when trying to look up their " +
				"parent transactions that are not in the subtree.")
		}
	})
}
