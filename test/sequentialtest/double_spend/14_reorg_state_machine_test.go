package doublespendtest

import (
	"testing"

	bt "github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/require"
)

// File 14: Reorg State Machine Attacks
//
// These tests target the 5-phase commit in process_conflicting.go and the
// SubtreeProcessor reorg handling, looking for:
//   - ConflictingChildren growing stale across multiple reorgs (flip-flop)
//   - Orphaned (non-conflicting) txs incorrectly marked as conflicting after reorg
//   - Re-submission of orphaned confirmed txs that should be rejected
//   - Frozen tx causing partial phase-1 state in ProcessConflicting
//   - BFS incorrectly marking valid children (unrelated to the conflict) as conflicting
//   - Unbounded ConflictingChildren after 3+ sequential conflicts on the same UTXO

// TestFlipFlopReorgPostgres tests that alternating reorgs (A→B→A→B, 4 flips) on the
// same tx pair leave exactly the right conflicting flags and ConflictingChildren state.
func TestFlipFlopReorgPostgres(t *testing.T) {
	t.Run("flip_flop_reorg_4_times", func(t *testing.T) {
		testFlipFlopReorg(t, "postgres")
	})
}

func TestFlipFlopReorgAerospike(t *testing.T) {
	t.Run("flip_flop_reorg_4_times", func(t *testing.T) {
		testFlipFlopReorg(t, "aerospike")
	})
}

// TestResubmitOrphanedConfirmedTxPostgres tests that a transaction that was confirmed,
// then orphaned by a reorg (where the UTXO is now spent by a conflicting tx on the new
// chain), is correctly rejected when re-submitted to propagation.
func TestResubmitOrphanedConfirmedTxPostgres(t *testing.T) {
	t.Run("resubmit_orphaned_confirmed_tx_to_mempool", func(t *testing.T) {
		testResubmitOrphanedConfirmedTxToMempool(t, "postgres")
	})
}

func TestResubmitOrphanedConfirmedTxAerospike(t *testing.T) {
	t.Run("resubmit_orphaned_confirmed_tx_to_mempool", func(t *testing.T) {
		testResubmitOrphanedConfirmedTxToMempool(t, "aerospike")
	})
}

// TestOrphanedTxReturnsToMempoolPostgres tests that transactions orphaned by a reorg
// (but NOT conflicting — no competing tx spent their UTXOs) correctly return to the
// mempool as valid unmined transactions, not as conflicting.
func TestOrphanedTxReturnsToMempoolPostgres(t *testing.T) {
	t.Run("orphaned_tx_returns_to_mempool_not_conflicting", func(t *testing.T) {
		testOrphanedTxReturnsToMempoolNotConflicting(t, "postgres")
	})
}

func TestOrphanedTxReturnsToMempoolAerospike(t *testing.T) {
	t.Run("orphaned_tx_returns_to_mempool_not_conflicting", func(t *testing.T) {
		testOrphanedTxReturnsToMempoolNotConflicting(t, "aerospike")
	})
}

// TestFrozenTxInConflictResolutionPathPostgres tests that when a frozen transaction
// is the loser in a reorg, ProcessConflicting fails gracefully without leaving the
// UTXO store in a partial or inconsistent state.
func TestFrozenTxInConflictResolutionPathPostgres(t *testing.T) {
	t.Run("frozen_tx_in_conflict_resolution_path", func(t *testing.T) {
		testFrozenTxInConflictResolutionPath(t, "postgres")
	})
}

func TestFrozenTxInConflictResolutionPathAerospike(t *testing.T) {
	t.Run("frozen_tx_in_conflict_resolution_path", func(t *testing.T) {
		testFrozenTxInConflictResolutionPath(t, "aerospike")
	})
}

// TestProcessConflictingDoesNotMarkValidChildrenPostgres tests that BFS conflict
// propagation only marks the conflicting transaction and its descendants as conflicting,
// not sibling transactions that share the same parent but spend different outputs.
func TestProcessConflictingDoesNotMarkValidChildrenPostgres(t *testing.T) {
	t.Run("process_conflicting_does_not_mark_valid_children_as_conflicting", func(t *testing.T) {
		testProcessConflictingDoesNotMarkValidChildrenAsConflicting(t, "postgres")
	})
}

func TestProcessConflictingDoesNotMarkValidChildrenAerospike(t *testing.T) {
	t.Run("process_conflicting_does_not_mark_valid_children_as_conflicting", func(t *testing.T) {
		testProcessConflictingDoesNotMarkValidChildrenAsConflicting(t, "aerospike")
	})
}

// TestMultipleSequentialConflictsOnSameUTXOPostgres tests that when a UTXO is the
// subject of 3 sequential double-spend resolutions (A beats B, C beats A, D beats C),
// the final state is correct (D wins, A/B/C all conflicting) and ConflictingChildren
// is bounded (not growing unboundedly with each reorg).
func TestMultipleSequentialConflictsOnSameUTXOPostgres(t *testing.T) {
	t.Run("multiple_sequential_conflicts_on_same_utxo", func(t *testing.T) {
		testMultipleSequentialConflictsOnSameUTXO(t, "postgres")
	})
}

func TestMultipleSequentialConflictsOnSameUTXOAerospike(t *testing.T) {
	t.Run("multiple_sequential_conflicts_on_same_utxo", func(t *testing.T) {
		testMultipleSequentialConflictsOnSameUTXO(t, "aerospike")
	})
}

// TestDeepReorgMixedTxFatesPostgres tests a 10-block reorg containing three categories
// of transactions: re-mined on the new chain, conflicting with the new chain, and
// simply orphaned (not conflicting). Verifies each category ends in the correct state.
func TestDeepReorgMixedTxFatesPostgres(t *testing.T) {
	t.Run("deep_reorg_mixed_tx_fates", func(t *testing.T) {
		testDeepReorgMixedTxFates(t, "postgres")
	})
}

func TestDeepReorgMixedTxFatesAerospike(t *testing.T) {
	t.Run("deep_reorg_mixed_tx_fates", func(t *testing.T) {
		testDeepReorgMixedTxFates(t, "aerospike")
	})
}

// testFlipFlopReorg exercises 4 alternating reorgs on the same tx pair (txA vs txB).
//
// Block structure progression:
//
//	Round 1 (initial — A wins):
//	  0 -> ... -> 101 -> 102a [txA] (*)
//
//	Round 2 (B wins):
//	  0 -> ... -> 101 -> 102a [txA]
//	                  \-> 102b [txB] -> 103b (*)
//
//	Round 3 (A wins):
//	  0 -> ... -> 101 -> 102a [txA] -> 103a -> 104a (*)
//	                  \-> 102b [txB] -> 103b
//
//	Round 4 (B wins):
//	  0 -> ... -> 101 -> 102a [txA] -> 103a -> 104a
//	                  \-> 102b [txB] -> 103b -> 104b -> 105b (*)
//
// After each flip:
//   - Exactly one of txA/txB is conflicting=false (winner)
//   - The other is conflicting=true (loser)
//   - The losing tx is NOT in block assembly
//   - The parent coinbase's ConflictingChildren has at most 1 entry per losing tx (no unbounded growth)
func testFlipFlopReorg(t *testing.T, utxoStoreType string) {
	// blockOffset=20 ensures unique coinbase UTXO; avoids conflicts with other tests
	td, _, txA, txB, block102a, _ := setupDoubleSpendTest(t, utxoStoreType, 20)
	defer td.Stop(t)

	// ── Round 1: Chain A wins (already the case after setup) ──────────────────
	// block102a is at height 102 containing txA; txB was rejected by propagation
	td.VerifyConflictingInUtxoStore(t, false, txA)
	td.VerifyNotInBlockAssembly(t, txA) // mined, not in assembly

	// ── Round 2: Make chain B win ─────────────────────────────────────────────
	// Create block 102b (competing with block102a, same height) containing txB
	block102b := createConflictingBlock(t, td, block102a, []*bt.Tx{txB}, []*bt.Tx{txA}, 20200)
	require.NotNil(t, block102b)

	// Extend chain B to height 103 to make it the longest chain
	_, block103b := td.CreateTestBlock(t, block102b, 20300)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103b, block103b.Height, "", "legacy", 0))
	td.WaitForBlockHeight(t, block103b, helperBlockWait, true)

	// After round 2: txB wins, txA loses
	td.VerifyConflictingInUtxoStore(t, true, txA)
	td.VerifyConflictingInUtxoStore(t, false, txB)
	td.VerifyNotInBlockAssembly(t, txA)
	td.VerifyNotInBlockAssembly(t, txB) // mined, not in assembly

	// ── Round 3: Make chain A win again ───────────────────────────────────────
	_, block103a := td.CreateTestBlock(t, block102a, 20301)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103a, block103a.Height, "", "legacy", 0))

	_, block104a := td.CreateTestBlock(t, block103a, 20401)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block104a, block104a.Height, "", "legacy", 0))
	td.WaitForBlockHeight(t, block104a, helperBlockWait)

	// After round 3: txA wins, txB loses
	td.VerifyConflictingInUtxoStore(t, false, txA)
	td.VerifyConflictingInUtxoStore(t, true, txB)
	td.VerifyNotInBlockAssembly(t, txA) // mined
	td.VerifyNotInBlockAssembly(t, txB) // conflicting

	// ── Round 4: Make chain B win again ──────────────────────────────────────
	_, block104b := td.CreateTestBlock(t, block103b, 20400)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block104b, block104b.Height, "", "legacy", 0))

	_, block105b := td.CreateTestBlock(t, block104b, 20500)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block105b, block105b.Height, "", "legacy", 0))
	td.WaitForBlockHeight(t, block105b, helperBlockWait, true)

	// After round 4: txB wins again, txA loses
	td.VerifyConflictingInUtxoStore(t, true, txA)
	td.VerifyConflictingInUtxoStore(t, false, txB)
	td.VerifyNotInBlockAssembly(t, txA)
	td.VerifyNotInBlockAssembly(t, txB)

	// Subtrees: both blocks always have their respective txs marked as conflicting in their subtrees
	td.VerifyConflictingInSubtrees(t, block102a.Subtrees[0], txA)
	td.VerifyConflictingInSubtrees(t, block102b.Subtrees[0], txB)
}

// requireConflictingChildrenCount asserts that the tx's ConflictingChildren list has
// at most expectedCount entries. The ConflictingChildren list on a tx contains the
// txs that spend from it and are currently marked conflicting.
func requireConflictingChildrenCount(t *testing.T, td *daemon.TestDaemon, tx *bt.Tx, expectedCount int, msg string) {
	t.Helper()

	meta, err := td.UtxoStore.Get(td.Ctx, tx.TxIDChainHash(), fields.ConflictingChildren)
	require.NoError(t, err, "failed to get ConflictingChildren for tx %s", tx.TxIDChainHash())
	require.LessOrEqual(t, len(meta.ConflictingChildren), expectedCount,
		"%s: got %d ConflictingChildren entries (max expected %d): %v",
		msg, len(meta.ConflictingChildren), expectedCount, meta.ConflictingChildren)
}

// testResubmitOrphanedConfirmedTxToMempool verifies that a tx confirmed on chain A,
// then made conflicting by a reorg to chain B (where txConflict spends the same input),
// is correctly rejected when re-submitted to propagation.
//
// Block structure:
//
//	           / 102a [txA] (orphaned, then conflicting)
//	0 -> 101 ->
//	           \ 102b [txConflict] -> 103b (*)
//
// After chain B wins, txA becomes conflicting. Re-submitting txA must fail.
func testResubmitOrphanedConfirmedTxToMempool(t *testing.T, utxoStoreType string) {
	td, _, txA, txConflict, block102a, _ := setupDoubleSpendTest(t, utxoStoreType, 21)
	defer td.Stop(t)

	// Initial state: txA mined; txConflict was rejected by propagation (not yet in UTXO store)
	td.VerifyConflictingInUtxoStore(t, false, txA)

	// Make chain B the winner
	block102b := createConflictingBlock(t, td, block102a, []*bt.Tx{txConflict}, []*bt.Tx{txA}, 21200)
	require.NotNil(t, block102b)

	_, block103b := td.CreateTestBlock(t, block102b, 21300)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103b, block103b.Height, "", "legacy", 0))
	td.WaitForBlockHeight(t, block103b, helperBlockWait, true)

	// After reorg: txA is conflicting, txConflict is the winner
	td.VerifyConflictingInUtxoStore(t, true, txA)
	td.VerifyConflictingInUtxoStore(t, false, txConflict)

	// Re-submit txA to propagation — must be rejected since its input is now spent by txConflict
	err := td.PropagationClient.ProcessTransaction(td.Ctx, txA)
	require.Error(t, err,
		"re-submitting txA to propagation must fail: its input is spent by txConflict on the current chain")
}

// testOrphanedTxReturnsToMempoolNotConflicting verifies that transactions orphaned by
// a reorg (but not conflicting — no other tx spent their UTXOs) return to the mempool
// as valid candidates for re-mining, not as conflicting transactions.
//
// Block structure:
//
//	           / 102a [txA, spends UTXO_P] -> 103a [txB, spends txA:0]
//	0 -> 101 ->
//	           \ 102b [txC, spends UTXO_Q≠P] -> 103b -> 104b (*)
//
// txA and txB are orphaned (NOT conflicting) because nobody else spent UTXO_P or txA:0.
// After chain B wins, txA and txB should be valid unmined candidates for re-mining.
func testOrphanedTxReturnsToMempoolNotConflicting(t *testing.T, utxoStoreType string) {
	// txC comes from block 23 (a DIFFERENT coinbase than txA from block 22) — no conflict
	td, _, txA, _, block102a, txC := setupDoubleSpendTest(t, utxoStoreType, 22)
	defer td.Stop(t)

	// txA is mined in block102a (spending coinbase from block 22)
	// txC was created from a DIFFERENT block's coinbase (block 23) — not a double-spend
	td.VerifyConflictingInUtxoStore(t, false, txA)

	// Create txB: spends txA:0 (child of txA); submit to propagation
	txB := td.CreateTransaction(t, txA, 0)
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, txB))
	require.NoError(t, td.WaitForTransactionInBlockAssembly(txB, helperBlockWait))

	// Mine txB in block103a — chain A: 102a[txA] → 103a[txB]
	_, block103a := td.CreateTestBlock(t, block102a, 22301, txB)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103a, block103a.Height, "", "legacy", 0))
	td.WaitForBlockHeight(t, block103a, helperBlockWait, true)

	td.VerifyConflictingInUtxoStore(t, false, txB)
	td.VerifyNotInBlockAssembly(t, txB) // mined

	// Build a COMPETING chain using txC (spends a DIFFERENT coinbase — no conflict with txA/txB)
	// Chain B: 102a (shared parent) → 102b [txC] → 103b → 104b (longer than chain A at 103)
	// Note: We use block102a as the shared parent since setupDoubleSpendTest builds on block 101
	// but block102a is at height 102, so block102b needs to be at height 102 from the same parent
	block102aParent, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, block102a.Height-1)
	require.NoError(t, err)

	_, block102b := td.CreateTestBlock(t, block102aParent, 22200, txC)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block102b, block102b.Height, "", "legacy", 0))

	// Extend chain B to height 104 to make it the longest
	_, block103b := td.CreateTestBlock(t, block102b, 22300)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103b, block103b.Height, "", "legacy", 0))

	_, block104b := td.CreateTestBlock(t, block103b, 22400)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block104b, block104b.Height, "", "legacy", 0))
	td.WaitForBlockHeight(t, block104b, helperBlockWait, true)

	// After chain B wins (which doesn't touch UTXO_P or txA:0):
	// - txA and txB are ORPHANED: their UTXOs were not touched by chain B
	// - They are NOT conflicting (nobody else spent their inputs)
	td.VerifyConflictingInUtxoStore(t, false, txA) // orphaned but NOT conflicting
	td.VerifyConflictingInUtxoStore(t, false, txB) // orphaned but NOT conflicting

	// txA and txB should return to block assembly as valid candidates for re-mining
	require.NoError(t, td.WaitForTransactionInBlockAssembly(txA, helperBlockWait),
		"txA must return to block assembly as a valid unmined tx after the reorg orphaned it")
	require.NoError(t, td.WaitForTransactionInBlockAssembly(txB, helperBlockWait),
		"txB must return to block assembly as a valid unmined tx after the reorg orphaned it")
}

// testFrozenTxInConflictResolutionPath extends the existing frozen-tx test to verify
// that when a frozen tx is in the conflict resolution path (block with double-spend
// arrives), the block is rejected AND no UTXO is permanently stuck in locked=true.
//
// Scenario: txA is mined and its outputs are frozen; txB double-spends txA's input.
// Block with txB arrives → ProcessConflicting would mark txA as the loser but txA is
// frozen → conflict resolution must fail cleanly, NOT leave partial state.
func testFrozenTxInConflictResolutionPath(t *testing.T, utxoStoreType string) {
	td, _, txA, txB, block102a, _ := setupDoubleSpendTest(t, utxoStoreType, 45)
	defer td.Stop(t)

	// Freeze txA's outputs (simulates alert system freeze)
	spends := make([]*utxo.Spend, 0, len(txA.Outputs))
	for idx, output := range txA.Outputs {
		if output.Satoshis == 0 {
			continue
		}
		utxoHash, _ := util.UTXOHashFromOutput(txA.TxIDChainHash(), output, uint32(idx)) //nolint:gosec
		spends = append(spends, &utxo.Spend{
			TxID:     txA.TxIDChainHash(),
			Vout:     uint32(idx), //nolint:gosec
			UTXOHash: utxoHash,
		})
	}
	require.NoError(t, td.UtxoStore.FreezeUTXOs(td.Ctx, spends, td.Settings))

	// Block with txB arrives — conflict resolution must fail because txA is frozen.
	// Pass expectBlockError=true: the block should be rejected.
	result := createConflictingBlock(t, td, block102a, []*bt.Tx{txB}, []*bt.Tx{txA}, 45200, true)
	require.Nil(t, result, "block with double-spend of frozen tx must be rejected")

	// After the failed conflict resolution, verify the UTXO store is in a clean state.
	// txA must NOT be stuck in locked=true (no partial phase-2 from the 5-phase commit).
	txAMeta, err := td.UtxoStore.Get(td.Ctx, txA.TxIDChainHash(), fields.Conflicting, fields.Locked)
	require.NoError(t, err)
	require.False(t, txAMeta.Locked,
		"txA must NOT be stuck in locked=true after failed conflict resolution — indicates partial phase-2 state")
	require.False(t, txAMeta.Conflicting,
		"txA must remain non-conflicting (frozen but still the winner) after the block was rejected")

	// Note: txB was never stored (block was rejected before CreateConflicting could store it).
	// The key assertion is the frozen tx check above.
}

// testProcessConflictingDoesNotMarkValidChildrenAsConflicting verifies that BFS
// conflict propagation only marks the actual conflicting transaction as conflicting —
// NOT sibling transactions that share the same parent but spend different outputs.
//
// Transaction structure:
//
//	txParent (mined, multiple outputs from external tx)
//	  ├─ txValidChild (spends txParent:0) ── in block103a (chain A) → becomes loser
//	  ├─ txConflictChild (spends txParent:0) ── in block103b (chain B) → becomes winner
//	  └─ txGoodChild (spends txParent:1) ── in block103a (chain A) → must stay valid
//
// When chain B wins (txConflictChild's block):
//   - txConflictChild: conflicting=false (winner for txParent:0)
//   - txValidChild: conflicting=true (loser for txParent:0)
//   - txGoodChild: conflicting=false (spends txParent:1, uncontested — must NOT be marked conflicting)
func testProcessConflictingDoesNotMarkValidChildrenAsConflicting(t *testing.T, utxoStoreType string) {
	td := setupExternalTxDaemon(t, utxoStoreType)
	defer td.Stop(t)

	require.NoError(t, td.BlockchainClient.Run(td.Ctx, "test"))
	coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, td.Ctx)

	// Get the best block as parent for creating subsequent blocks
	bestHeader, bestHeight, err := td.BlockchainClient.GetBestBlockHeader(td.Ctx)
	require.NoError(t, err)
	bestBlock, err := td.BlockchainClient.GetBlock(td.Ctx, bestHeader.Hash())
	require.NoError(t, err)

	// Create txParent with multiple outputs (external tx so it has ≥5 outputs)
	txParent := createExternalTxFromParent(t, td, coinbaseTx, 0)
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, txParent))
	require.NoError(t, td.WaitForTransactionInBlockAssembly(txParent, externalBlockWait))

	// Mine txParent into a block
	_, blockWithParent := td.CreateTestBlock(t, bestBlock, 30100, txParent)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, blockWithParent, blockWithParent.Height, "", "legacy", 0))
	td.WaitForBlockHeight(t, blockWithParent, externalBlockWait, true)
	_ = bestHeight // used for context only

	// Create children using simple transactions (not external): txParent outputs only have
	// outputAmount/numOutputsForExternalTx satoshis each, which is not enough to fund
	// another external tx (5 outputs * same per-output amount > single input amount).
	txValidChild := td.CreateTransaction(t, txParent, 0)    // first-seen winner for txParent:0
	txConflictChild := td.CreateTransaction(t, txParent, 0) // double-spend of txValidChild
	txGoodChild := td.CreateTransaction(t, txParent, 1)     // spends txParent:1, unrelated

	// Submit txValidChild (first-seen → accepted)
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, txValidChild))
	require.NoError(t, td.WaitForTransactionInBlockAssembly(txValidChild, externalBlockWait))

	// txConflictChild is a double-spend → rejected by propagation
	require.Error(t, td.PropagationClient.ProcessTransaction(td.Ctx, txConflictChild),
		"txConflictChild must be rejected as a double-spend")

	// txGoodChild (spending txParent:1) should be accepted
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, txGoodChild))
	require.NoError(t, td.WaitForTransactionInBlockAssembly(txGoodChild, externalBlockWait))

	// Mine txValidChild and txGoodChild in chain A block (blockWithParent + 1)
	_, block103a := td.CreateTestBlock(t, blockWithParent, 30200, txValidChild, txGoodChild)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103a, block103a.Height, "", "legacy", 0))
	td.WaitForBlockHeight(t, block103a, externalBlockWait, true)

	// Create competing chain B block at same height, containing txConflictChild (not txValidChild)
	_, block103b := td.CreateTestBlock(t, blockWithParent, 30201, txConflictChild)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103b, block103b.Height, "", "legacy", 0))

	// Extend chain B to make it the winner
	_, block104b := td.CreateTestBlock(t, block103b, 30300)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block104b, block104b.Height, "", "legacy", 0))
	td.WaitForBlockHeight(t, block104b, externalBlockWait, true)

	// After chain B wins:
	// - txConflictChild is now the winner for txParent:0 → non-conflicting
	// - txValidChild is now the loser for txParent:0 → conflicting
	// - txGoodChild spends txParent:1 (unrelated) → MUST remain non-conflicting
	td.VerifyConflictingInUtxoStore(t, false, txConflictChild)
	td.VerifyConflictingInUtxoStore(t, true, txValidChild)
	td.VerifyConflictingInUtxoStore(t, false, txGoodChild)

	// txGoodChild was in block103a (now orphaned) → must return to mempool, NOT conflicting
	require.NoError(t, td.WaitForTransactionInBlockAssembly(txGoodChild, externalBlockWait),
		"txGoodChild (spending uncontested txParent:1) must NOT be marked conflicting after the reorg")

	// txConflictChild is mined in block103b — should not be in assembly
	td.VerifyNotInBlockAssembly(t, txConflictChild)
	// txValidChild is conflicting — not in assembly
	td.VerifyNotInBlockAssembly(t, txValidChild)
}

// testMultipleSequentialConflictsOnSameUTXO tests three sequential double-spend
// resolutions on the same coinbase UTXO.
//
// Sequence:
//  1. txA beats txB (reorg 1): block102a [txA] wins
//  2. txC beats txA (reorg 2): block102c [txC] → 103c wins
//  3. txD beats txC (reorg 3): block102d [txD] → 103d → 104d wins
//
// Final state: txD non-conflicting; txA, txB, txC all conflicting.
// The ConflictingChildren list must not grow unboundedly across the 3 reorgs.
func testMultipleSequentialConflictsOnSameUTXO(t *testing.T, utxoStoreType string) {
	td, coinbaseTx, txA, txB, block102a, _ := setupDoubleSpendTest(t, utxoStoreType, 25)
	defer td.Stop(t)

	// txC and txD also spend the same coinbase UTXO as txA and txB
	txC := td.CreateTransaction(t, coinbaseTx, 0)
	txD := td.CreateTransaction(t, coinbaseTx, 0)

	// ── Reorg 1: txA wins (already the case after setup) ─────────────────────
	// txB was rejected by propagation (not stored in UTXO store) — skip txB check here
	td.VerifyConflictingInUtxoStore(t, false, txA)

	// ── Reorg 2: txC beats txA ────────────────────────────────────────────────
	// Create block102c (same parent as block102a) with txC
	block102c := createConflictingBlock(t, td, block102a, []*bt.Tx{txC}, []*bt.Tx{txA}, 25200)
	require.NotNil(t, block102c)

	_, block103c := td.CreateTestBlock(t, block102c, 25300)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103c, block103c.Height, "", "legacy", 0))
	td.WaitForBlockHeight(t, block103c, helperBlockWait, true)

	// txC wins, txA loses (txB was rejected by propagation — not in UTXO store, skip check)
	td.VerifyConflictingInUtxoStore(t, false, txC)
	td.VerifyConflictingInUtxoStore(t, true, txA)

	// ── Reorg 3: txD beats txC ────────────────────────────────────────────────
	// After reorg 2, block102a is orphaned — can't use createConflictingBlock(block102a) since
	// it asserts block102a is still at height 102. Build block102d manually from block101.
	block101For3, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, block102a.Height-1)
	require.NoError(t, err)

	_, block102d := td.CreateTestBlock(t, block101For3, 25201, txD)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block102d, block102d.Height, "", "legacy", 0))

	_, block103d := td.CreateTestBlock(t, block102d, 25301)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103d, block103d.Height, "", "legacy", 0))

	_, block104d := td.CreateTestBlock(t, block103d, 25401)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block104d, block104d.Height, "", "legacy", 0))
	td.WaitForBlockHeight(t, block104d, helperBlockWait, true)

	// Final state: txD wins; txA and txC lose (txB was rejected by propagation — not in UTXO store)
	td.VerifyConflictingInUtxoStore(t, false, txD)
	td.VerifyConflictingInUtxoStore(t, true, txA)
	td.VerifyConflictingInUtxoStore(t, true, txC)

	// Block assembly: none of the losing txs should be in assembly
	td.VerifyNotInBlockAssembly(t, txA)
	td.VerifyNotInBlockAssembly(t, txB) // rejected by propagation, never in assembly
	td.VerifyNotInBlockAssembly(t, txC)
	td.VerifyNotInBlockAssembly(t, txD) // mined, not in assembly
}

// testDeepReorgMixedTxFates tests a reorg where transactions in the old chain fall
// into three distinct categories, each with different expected outcomes:
//
//   - Re-mined: the SAME tx appears in both chain A AND chain B. After chain B wins,
//     the tx is still valid and mined (conflicting=false).
//   - Conflicting: the tx double-spends a UTXO that chain B's tx also spends. After
//     chain B wins, this tx is conflicting=true and NOT in block assembly.
//   - Orphaned: the tx was in chain A but nobody else touched its UTXOs on chain B.
//     After chain B wins, the tx returns to block assembly as a valid unmined candidate.
//
// Block structure:
//
//	                / 102a [txConflictA, txReMined, txOrphan] -> 103a [empty]
//	0 -> ... -> 101
//	                \ 102b [txConflictB, txReMined] -> 103b -> 104b (wins)
func testDeepReorgMixedTxFates(t *testing.T, utxoStoreType string) {
	// blockOffset=70: txConflictA spends coinbase from block 70
	td, _, txConflictA, txConflictB, block102a, _ := setupDoubleSpendTest(t, utxoStoreType, 70)
	defer td.Stop(t)

	// Get two more coinbases for the re-mined and orphaned categories
	block72, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 72)
	require.NoError(t, err)
	block73, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 73)
	require.NoError(t, err)

	// txReMined: will appear in BOTH chain A and chain B blocks
	// It spends block72's coinbase — valid on both chains (common prefix)
	// (block71 coinbase is already spent by tx2 returned from setupDoubleSpendTest(blockOffset=70))
	txReMined := td.CreateTransaction(t, block72.CoinbaseTx, 0)
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, txReMined))
	require.NoError(t, td.WaitForTransactionInBlockAssembly(txReMined, helperBlockWait))

	// txOrphan: only in chain A (spends block73's coinbase, no double-spend)
	txOrphan := td.CreateTransaction(t, block73.CoinbaseTx, 0)
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, txOrphan))
	require.NoError(t, td.WaitForTransactionInBlockAssembly(txOrphan, helperBlockWait))

	// Extend chain A: block103a contains txReMined and txOrphan
	_, block103a := td.CreateTestBlock(t, block102a, 70300, txReMined, txOrphan)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103a, block103a.Height, "", "legacy", 0))
	td.WaitForBlockHeight(t, block103a, helperBlockWait, true)

	// All chain A txs are valid at this point
	td.VerifyConflictingInUtxoStore(t, false, txConflictA)
	td.VerifyConflictingInUtxoStore(t, false, txReMined)
	td.VerifyConflictingInUtxoStore(t, false, txOrphan)

	// Build chain B: block102b contains txConflictB (double-spends txConflictA)
	// AND txReMined (the same re-mined tx appears in both chains)
	// txConflictB was already returned by setupDoubleSpendTest as the conflicting tx
	block101, err2 := td.BlockchainClient.GetBlockByHeight(td.Ctx, block102a.Height-1)
	require.NoError(t, err2)

	_, block102b := td.CreateTestBlock(t, block101, 70200, txConflictB, txReMined)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block102b, block102b.Height, "", "legacy", 0))

	// Extend chain B to height 104 to beat chain A at height 103
	_, block103b := td.CreateTestBlock(t, block102b, 70301)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103b, block103b.Height, "", "legacy", 0))

	_, block104b := td.CreateTestBlock(t, block103b, 70400)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block104b, block104b.Height, "", "legacy", 0))
	td.WaitForBlockHeight(t, block104b, helperBlockWait, true)

	// ── Assert all 3 categories ──────────────────────────────────────────────

	// Category 1: Re-mined — txReMined is in chain B block102b → still valid, mined
	td.VerifyConflictingInUtxoStore(t, false, txReMined)
	td.VerifyNotInBlockAssembly(t, txReMined) // mined on chain B

	// Category 2: Conflicting — txConflictA lost to txConflictB (chain B winner)
	td.VerifyConflictingInUtxoStore(t, true, txConflictA)
	td.VerifyConflictingInUtxoStore(t, false, txConflictB)
	td.VerifyNotInBlockAssembly(t, txConflictA) // conflicting
	td.VerifyNotInBlockAssembly(t, txConflictB) // mined

	// Category 3: Orphaned — txOrphan was in chain A block103a (orphaned by reorg)
	// No competing tx spent block73's coinbase on chain B → NOT conflicting
	td.VerifyConflictingInUtxoStore(t, false, txOrphan)
	require.NoError(t, td.WaitForTransactionInBlockAssembly(txOrphan, helperBlockWait),
		"txOrphan must return to block assembly as valid unmined tx after being orphaned by the reorg")
}
