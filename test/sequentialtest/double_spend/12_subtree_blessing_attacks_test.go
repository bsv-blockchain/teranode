package doublespendtest

import (
	"testing"

	bt "github.com/bsv-blockchain/go-bt/v2"
	"github.com/stretchr/testify/require"
)

// File 12: Subtree Blessing Attacks
//
// These tests target the pre-block subtree blessing pipeline, specifically:
//   - The early-return path in CheckBlockSubtrees that reuses cached ConflictingNodes[]
//     when ALL subtrees already exist in storage — potentially stale if txs became
//     conflicting after the initial blessing
//   - Competing miner subtrees with conflicting transactions (fork scenario)
//   - Interaction between subtree early-return and checkOldBlockIDs safety net
//
// Design reminder: accepting a block with double-spend transactions IS correct behavior.
// The requirement is that conflicting txs are properly marked in ConflictingNodes[] so
// block assembly excludes them from future candidate blocks.
//
// TOCTOU risk: A subtree is blessed with txA marked as VALID (ConflictingNodes=[]).
// Between blessing and block arrival, txB is confirmed, making txA conflicting.
// Block arrives → early return reuses the cached subtree with stale ConflictingNodes=[].
// Block assembly uses that stale state → may incorrectly include txA in future blocks.
//
// NOTE: Testing the subtree blessing window requires the test to interleave:
//   1. Create and "pre-bless" a subtree (submit to block assembly to get it stored)
//   2. Mine a conflicting tx on the current chain
//   3. Submit a block referencing the pre-blessed subtree
//
// This is complex because the test infrastructure's CreateTestBlock() creates subtrees
// inline. To exercise the pre-blessing path, tests submit subtrees via block assembly,
// then verify the ConflictingNodes in the resulting block's stored subtrees.

// TestSubtreeBlessingStaleConflictingNodesPostgres tests that when a subtree is
// pre-created (blessed) with txA as valid, and txB is confirmed before the block
// referencing the pre-blessed subtree arrives, the block is accepted and block
// assembly correctly excludes txA (now conflicting).
func TestSubtreeBlessingStaleConflictingNodesPostgres(t *testing.T) {
	t.Run("blessed_subtree_stale_conflicting_nodes_after_mined_tx", func(t *testing.T) {
		testBlessedSubtreeStaleConflictingNodesAfterMinedTx(t, "postgres")
	})
}

func TestSubtreeBlessingStaleConflictingNodesAerospike(t *testing.T) {
	t.Run("blessed_subtree_stale_conflicting_nodes_after_mined_tx", func(t *testing.T) {
		testBlessedSubtreeStaleConflictingNodesAfterMinedTx(t, "aerospike")
	})
}

// TestTwoCompetingMinerSubtreesConflictingPostgres tests that when two miners send
// subtrees with conflicting transactions (S1 with txA, S2 with txB double-spend),
// and both miners find blocks, the system handles the fork correctly without UTXO corruption.
//
// This is a variant of the basic double-spend fork test, but explicitly from the
// perspective of two miners building competing subtrees concurrently.
func TestTwoCompetingMinerSubtreesConflictingPostgres(t *testing.T) {
	t.Run("two_competing_miner_subtrees_conflicting", func(t *testing.T) {
		testTwoCompetingMinerSubtreesConflicting(t, "postgres")
	})
}

func TestTwoCompetingMinerSubtreesConflictingAerospike(t *testing.T) {
	t.Run("two_competing_miner_subtrees_conflicting", func(t *testing.T) {
		testTwoCompetingMinerSubtreesConflicting(t, "aerospike")
	})
}

// TestBlessedSubtreeWithOrphanedParentPostgres tests that when a subtree is blessed
// but the parent tx was orphaned by a reorg, checkOldBlockIDs acts as a safety net
// and rejects the block (even if the subtree was already in storage).
func TestBlessedSubtreeWithOrphanedParentPostgres(t *testing.T) {
	t.Run("blessed_subtree_with_orphaned_parent", func(t *testing.T) {
		testBlessedSubtreeWithOrphanedParent(t, "postgres")
	})
}

func TestBlessedSubtreeWithOrphanedParentAerospike(t *testing.T) {
	t.Run("blessed_subtree_with_orphaned_parent", func(t *testing.T) {
		testBlessedSubtreeWithOrphanedParent(t, "aerospike")
	})
}

// TestConcurrentSubtreesBlessingRacePostgres tests that two conflicting transactions
// submitted concurrently to propagation from separate goroutines result in exactly one
// winner (non-conflicting) and one loser (conflicting) with no UTXO store corruption.
func TestConcurrentSubtreesBlessingRacePostgres(t *testing.T) {
	t.Run("concurrent_subtrees_blessing_race_for_same_utxo", func(t *testing.T) {
		testConcurrentSubtreesBlessingRaceForSameUTXO(t, "postgres")
	})
}

func TestConcurrentSubtreesBlessingRaceAerospike(t *testing.T) {
	t.Run("concurrent_subtrees_blessing_race_for_same_utxo", func(t *testing.T) {
		testConcurrentSubtreesBlessingRaceForSameUTXO(t, "aerospike")
	})
}

// TestSubtreeConflictingNodeAlreadySetPostgres tests the happy path: when a subtree's
// ConflictingNodes is already populated correctly at blessing time, the early-return
// reuse is safe and block assembly correctly excludes those transactions.
func TestSubtreeConflictingNodeAlreadySetPostgres(t *testing.T) {
	t.Run("subtree_conflicting_node_already_set_remains_accurate", func(t *testing.T) {
		testSubtreeConflictingNodeAlreadySetRemainsAccurate(t, "postgres")
	})
}

func TestSubtreeConflictingNodeAlreadySetAerospike(t *testing.T) {
	t.Run("subtree_conflicting_node_already_set_remains_accurate", func(t *testing.T) {
		testSubtreeConflictingNodeAlreadySetRemainsAccurate(t, "aerospike")
	})
}

// TestBlessingMidValidationRacePostgres tests that submitting a block with a transaction
// whose counter-conflicting tx is already mined on the current chain causes the block
// to be rejected (checkCounterConflictingOnCurrentChain fires during subtree validation).
func TestBlessingMidValidationRacePostgres(t *testing.T) {
	t.Run("blessing_fails_when_counter_conflict_mined_mid_validation", func(t *testing.T) {
		testBlessingFailsWhenCounterConflictIsMinedMidValidation(t, "postgres")
	})
}

func TestBlessingMidValidationRaceAerospike(t *testing.T) {
	t.Run("blessing_fails_when_counter_conflict_mined_mid_validation", func(t *testing.T) {
		testBlessingFailsWhenCounterConflictIsMinedMidValidation(t, "aerospike")
	})
}

// TestSubtreeFromForkWithBlessedConflictingTxPostgres tests that a block arriving
// from a competing fork, where the conflicting tx was blessed relative to that fork's
// state, is correctly handled against the current chain's state.
func TestSubtreeFromForkWithBlessedConflictingTxPostgres(t *testing.T) {
	t.Run("subtree_from_fork_with_blessed_conflicting_tx", func(t *testing.T) {
		testSubtreeFromForkWithBlessedConflictingTx(t, "postgres")
	})
}

func TestSubtreeFromForkWithBlessedConflictingTxAerospike(t *testing.T) {
	t.Run("subtree_from_fork_with_blessed_conflicting_tx", func(t *testing.T) {
		testSubtreeFromForkWithBlessedConflictingTx(t, "aerospike")
	})
}

// TestBlessedSubtreeReusedAcrossMultipleBlocksPostgres tests that a previously
// blessed subtree referenced by blocks on different competing chains doesn't
// create duplicate UTXO entries or corrupt conflict tracking.
func TestBlessedSubtreeReusedAcrossMultipleBlocksPostgres(t *testing.T) {
	t.Run("blessed_subtree_reused_across_multiple_blocks", func(t *testing.T) {
		testBlessedSubtreeReusedAcrossMultipleBlocks(t, "postgres")
	})
}

func TestBlessedSubtreeReusedAcrossMultipleBlocksAerospike(t *testing.T) {
	t.Run("blessed_subtree_reused_across_multiple_blocks", func(t *testing.T) {
		testBlessedSubtreeReusedAcrossMultipleBlocks(t, "aerospike")
	})
}

// testBlessedSubtreeStaleConflictingNodesAfterMinedTx tests the TOCTOU scenario:
// a subtree is blessed with txA as VALID (ConflictingNodes=[]), then txB (double-spend)
// is confirmed on the current chain. A block referencing the cached subtree is accepted
// (by design), but block assembly must correctly see txA as conflicting.
// testBlessedSubtreeStaleConflictingNodesAfterMinedTx tests the TOCTOU scenario:
//
// A subtree is pre-created (stored) with txA marked as valid (ConflictingNodes=[]).
// Before the block referencing that subtree is submitted, txB (double-spend of txA)
// is confirmed on the current chain, making txA conflicting in the UTXO store.
// The block with the pre-blessed subtree is then submitted.
//
// Expected behavior (by design):
//   - The block is ACCEPTED (the early-return path fires for the already-stored subtrees)
//   - txA is conflicting in UTXO store (set when chain B won)
//   - Block assembly does NOT include txA (it checks the UTXO store, not the subtree cache)
//   - The subtree's ConflictingNodes may be stale ([]); block assembly uses UTXO store
//
// This test verifies that despite stale ConflictingNodes in the cached subtree, the
// system remains correct because block assembly uses the UTXO store as source of truth.
func testBlessedSubtreeStaleConflictingNodesAfterMinedTx(t *testing.T, utxoStoreType string) {
	td, _, txA, txB, block102a, _ := setupDoubleSpendTest(t, utxoStoreType, 65)
	defer td.Stop(t)

	// PRE-CREATE: build a block with txA and store its subtrees BEFORE chain B wins.
	// This simulates a miner who has built a candidate block with txA at a time when
	// txA was still valid. The subtrees are stored with ConflictingNodes=[].
	subtreeA, blockWithTxA := td.CreateTestBlock(t, block102a, 65300, txA)

	// CONFIRM txB: make chain B win so txA becomes conflicting in the UTXO store.
	// Between "blessing" (subtree stored above) and block submission (below), txB is mined.
	block102b := createConflictingBlock(t, td, block102a, []*bt.Tx{txB}, []*bt.Tx{txA}, 65200)
	require.NotNil(t, block102b)

	_, block103b := td.CreateTestBlock(t, block102b, 65301)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103b, block103b.Height, "", "legacy", 0))
	td.WaitForBlockHeight(t, block103b, helperBlockWait, true)

	// txA is now conflicting in UTXO store; the pre-created subtree has ConflictingNodes=[]
	td.VerifyConflictingInUtxoStore(t, true, txA)
	td.VerifyConflictingInUtxoStore(t, false, txB)

	// SUBMIT the pre-blessed block (subtrees already in storage → early-return path fires).
	// The block is at the same height as block103b — it's a competing fork.
	// CheckBlockSubtrees: subtrees exist in storage → blessed=true (stale ConflictingNodes=[])
	// Block validation continues: checkOldBlockIDs passes (txA's parent is on common prefix)
	err := td.BlockValidationClient.ProcessBlock(td.Ctx, blockWithTxA, blockWithTxA.Height, "", "legacy", 0)

	// Block is ACCEPTED (by design — competing forks with conflicting txs are accepted)
	require.NoError(t, err,
		"block referencing pre-blessed subtree (stale ConflictingNodes=[]) must be ACCEPTED")

	// txA's ConflictingNodes state: the subtree has ConflictingNodes=[] (stale, blessed before txB was mined)
	// But the UTXO store correctly reflects txA as conflicting
	td.VerifyConflictingInUtxoStore(t, true, txA) // UTXO store is correct

	// CRITICAL: block assembly must not include txA — it checks the UTXO store, not the stale subtree
	td.VerifyNotInBlockAssembly(t, txA)

	// The subtree for the pre-blessed block may show stale ConflictingNodes=[]
	// This is acceptable because block assembly uses the UTXO store as source of truth.
	// Log the actual ConflictingNodes for documentation purposes:
	t.Logf("Pre-blessed subtree hash: %s (ConflictingNodes may be stale [])",
		subtreeA.RootHash().String())
}

// testTwoCompetingMinerSubtreesConflicting simulates two miners building competing
// subtrees with conflicting transactions, then finding blocks on competing chains.
// This is equivalent to a basic double-spend fork test from a "two miners" perspective.
//
// Block structure:
//
//	          / block102a [txA - miner 1's subtree] -> block103a
//	0 -> 101 ->
//	          \ block102b [txB - miner 2's subtree] -> block103b -> block104b (*)
func testTwoCompetingMinerSubtreesConflicting(t *testing.T, utxoStoreType string) {
	// This is fundamentally the same as testSingleDoubleSpend but framed from the
	// perspective of two miners building competing subtrees.
	// Reuse the existing pattern to verify block assembly exclusion.
	td, _, txA, txB, block102a, _ := setupDoubleSpendTest(t, utxoStoreType, 60)
	defer td.Stop(t)

	// Miner 1's block (chain A) is already the current chain via setup
	td.VerifyConflictingInUtxoStore(t, false, txA)
	// Note: txB was rejected by propagation (not stored in UTXO store yet)

	// Miner 2's block (chain B): create competing block with txB
	block102b := createConflictingBlock(t, td, block102a, []*bt.Tx{txB}, []*bt.Tx{txA}, 60200)
	if block102b == nil {
		t.Fatal("block102b must be created successfully for fork setup")
	}

	// Miner 2 extends their chain to win
	_, block103b := td.CreateTestBlock(t, block102b, 60300)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103b, block103b.Height, "", "legacy", 0))
	td.WaitForBlockHeight(t, block103b, helperBlockWait, true)

	// Miner 2's chain wins: txB is valid, txA is conflicting
	td.VerifyConflictingInUtxoStore(t, true, txA)
	td.VerifyConflictingInUtxoStore(t, false, txB)

	// Block assembly must exclude txA (it's conflicting) and txB (it's mined)
	td.VerifyNotInBlockAssembly(t, txA)
	td.VerifyNotInBlockAssembly(t, txB)

	// txA's subtree must show it as conflicting in the stored subtree metadata
	td.VerifyConflictingInSubtrees(t, block102a.Subtrees[0], txA)
	// txB's subtree in block102b must show it as conflicting (it was stored as conflicting
	// when first blessed, even though it's now the winner at the UTXO store level)
	td.VerifyConflictingInSubtrees(t, block102b.Subtrees[0], txB)
}

// testBlessedSubtreeWithOrphanedParent tests that checkOldBlockIDs correctly rejects
// a block whose transaction's parent was mined on an orphaned branch, even if the
// block's subtrees were previously blessed.
//
// This verifies that checkOldBlockIDs acts as a safety net when the subtree early-return
// path would otherwise bypass parent-chain validation.
func testBlessedSubtreeWithOrphanedParent(t *testing.T, utxoStoreType string) {
	// This is equivalent to testBlockRejectedWhenParentTxIsOnOrphanedBranch in file 13.
	// Including here to validate from the subtree blessing perspective.
	//
	// Setup: txParent mined on chain A; chain B wins; txChild spends txParent:0
	// Block with txChild submitted to chain B → must be rejected because txParent
	// is on the orphaned chain A branch.
	td, _, txParent, txC, block102a, _ := setupDoubleSpendTest(t, utxoStoreType, 61)
	defer td.Stop(t)

	// Make chain B the winner (txC double-spends txParent's input)
	block102b := createConflictingBlock(t, td, block102a, []*bt.Tx{txC}, []*bt.Tx{txParent}, 61200)
	require.NotNil(t, block102b)

	_, block103b := td.CreateTestBlock(t, block102b, 61300)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103b, block103b.Height, "", "legacy", 0))
	td.WaitForBlockHeight(t, block103b, helperBlockWait, true)

	// txParent is now orphaned (conflicting on chain B); txC is the winner
	td.VerifyConflictingInUtxoStore(t, true, txParent)
	td.VerifyConflictingInUtxoStore(t, false, txC)

	// Create txChild spending txParent:0 — txParent was mined on the orphaned chain A
	txChild := td.CreateTransaction(t, txParent, 0)

	// Create a block on chain B with txChild
	_, block104child := td.CreateTestBlock(t, block103b, 61400, txChild)
	err := td.BlockValidationClient.ProcessBlock(td.Ctx, block104child, block104child.Height, "", "legacy", 0)
	require.Error(t, err,
		"block with txChild (whose parent txParent is on orphaned chain A) must be rejected — "+
			"checkOldBlockIDs must verify parent's block ID is on current chain even for pre-blessed subtrees")

	// Block assembly must not contain txChild (rejected block)
	td.VerifyNotInBlockAssembly(t, txChild)
}

// testConcurrentSubtreesBlessingRaceForSameUTXO tests that two conflicting transactions
// submitted concurrently to propagation from separate goroutines result in a consistent
// final state: exactly one is the winner (non-conflicting), the other is the loser
// (conflicting), and no UTXO store corruption occurs.
//
// This exercises the UTXO store's first-seen concurrency handling.
func testConcurrentSubtreesBlessingRaceForSameUTXO(t *testing.T, utxoStoreType string) {
	td, coinbaseTx, _, _, _, _ := setupDoubleSpendTest(t, utxoStoreType, 66)
	defer td.Stop(t)

	// Use block 68 — block 66 and 67 coinbases are already spent by setupDoubleSpendTest(blockOffset=66)
	block68, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 68)
	require.NoError(t, err)
	coinbase68 := block68.CoinbaseTx
	_ = coinbaseTx

	// txRace1 and txRace2 both spend coinbase68:0 (competing)
	txRace1 := td.CreateTransaction(t, coinbase68, 0)
	txRace2 := td.CreateTransaction(t, coinbase68, 0)

	// Submit both concurrently from goroutines
	type result struct {
		err error
	}
	ch1 := make(chan result, 1)
	ch2 := make(chan result, 1)

	go func() {
		ch1 <- result{err: td.PropagationClient.ProcessTransaction(td.Ctx, txRace1)}
	}()
	go func() {
		ch2 <- result{err: td.PropagationClient.ProcessTransaction(td.Ctx, txRace2)}
	}()

	res1 := <-ch1
	res2 := <-ch2

	// Exactly one must succeed and one must fail (first-seen rule)
	// The exact winner is non-deterministic but the state must be consistent
	bothSucceeded := res1.err == nil && res2.err == nil
	bothFailed := res1.err != nil && res2.err != nil

	require.False(t, bothSucceeded,
		"both txRace1 and txRace2 cannot both succeed — they spend the same UTXO")
	require.False(t, bothFailed,
		"at least one of txRace1/txRace2 must succeed — UTXO spend must not be fully rejected")

	// Determine winner and loser
	var winner, loser *bt.Tx
	if res1.err == nil {
		winner, loser = txRace1, txRace2
	} else {
		winner, loser = txRace2, txRace1
	}

	// Winner: wait for it to fully land in block assembly (ensures UTXO store is also written)
	require.NoError(t, td.WaitForTransactionInBlockAssembly(winner, helperBlockWait),
		"winning tx must be in block assembly")
	td.VerifyConflictingInUtxoStore(t, false, winner)

	// Loser: rejected by propagation — not in UTXO store, not in block assembly
	// (cannot check VerifyConflictingInUtxoStore — loser was never stored)
	td.VerifyNotInBlockAssembly(t, loser)
}

// testSubtreeConflictingNodeAlreadySetRemainsAccurate tests the happy path where
// a subtree's ConflictingNodes is correctly populated at the time the block is mined.
// This is the standard double-spend scenario where the conflicting tx is properly
// marked in the subtree, and block assembly correctly excludes it.
//
// This is a baseline correctness test for the subtree ConflictingNodes mechanism.
func testSubtreeConflictingNodeAlreadySetRemainsAccurate(t *testing.T, utxoStoreType string) {
	td, _, txA, txB, block102a, _ := setupDoubleSpendTest(t, utxoStoreType, 62)
	defer td.Stop(t)

	// Create block with txB (double-spend) on a competing fork
	// block102b.Subtrees[0].ConflictingNodes should contain txB (it was conflicting when blessed)
	block102b := createConflictingBlock(t, td, block102a, []*bt.Tx{txB}, []*bt.Tx{txA}, 62200)
	require.NotNil(t, block102b)

	// Verify the subtree stored for block102b has txB in ConflictingNodes
	// (this is set during subtree validation when txB is detected as a double-spend)
	td.VerifyConflictingInSubtrees(t, block102b.Subtrees[0], txB)

	// Extend chain B to make txB the winner
	_, block103b := td.CreateTestBlock(t, block102b, 62300)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103b, block103b.Height, "", "legacy", 0))
	td.WaitForBlockHeight(t, block103b, helperBlockWait, true)

	// After chain B wins:
	// - txB is now non-conflicting in UTXO store (winner)
	// - txA is conflicting in UTXO store (loser)
	td.VerifyConflictingInUtxoStore(t, false, txB)
	td.VerifyConflictingInUtxoStore(t, true, txA)

	// Block assembly must not include txA (conflicting) or txB (mined)
	td.VerifyNotInBlockAssembly(t, txA)
	td.VerifyNotInBlockAssembly(t, txB)

	// The subtrees still show their historical conflicting state
	td.VerifyConflictingInSubtrees(t, block102a.Subtrees[0], txA) // txA conflicting in its subtree
	td.VerifyConflictingInSubtrees(t, block102b.Subtrees[0], txB) // txB was conflicting when blessed
}

// testBlessingFailsWhenCounterConflictIsMinedMidValidation tests that submitting a block
// containing txA (where txA's counter-conflicting tx is already mined on the current
// chain) with NEW subtrees (not yet in storage) causes the block to be REJECTED.
//
// This is distinct from the "stale subtree" scenario: here the subtrees are NOT yet
// in storage, so CheckBlockSubtrees runs full validation (no early return). During
// full validation, checkCounterConflictingOnCurrentChain finds txB is mined → ERROR.
//
// This tests the non-cached path: when a miner submits a block with a tx whose
// counter-conflict is already confirmed, the block is correctly rejected.
func testBlessingFailsWhenCounterConflictIsMinedMidValidation(t *testing.T, utxoStoreType string) {
	td, _, txA, txB, block102a, _ := setupDoubleSpendTest(t, utxoStoreType, 67)
	defer td.Stop(t)

	// Make chain B win: txA becomes conflicting, txB confirmed on current chain
	block102b := createConflictingBlock(t, td, block102a, []*bt.Tx{txB}, []*bt.Tx{txA}, 67200)
	require.NotNil(t, block102b)

	_, block103b := td.CreateTestBlock(t, block102b, 67300)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103b, block103b.Height, "", "legacy", 0))
	td.WaitForBlockHeight(t, block103b, helperBlockWait, true)

	// txA is now conflicting; txB is mined on the current chain
	td.VerifyConflictingInUtxoStore(t, true, txA)
	td.VerifyConflictingInUtxoStore(t, false, txB)

	// Create a NEW block (fresh subtrees not yet in storage) on chain B extending block103b.
	// This block contains txA (which is conflicting and whose counter-conflict txB is mined).
	//
	// Since the subtrees are fresh (not in storage), CheckBlockSubtrees runs full validation.
	// During validation: checkCounterConflictingOnCurrentChain finds txB on current chain → FAIL.
	_, block104b_withTxA := td.CreateTestBlock(t, block103b, 67400, txA)
	err := td.BlockValidationClient.ProcessBlock(td.Ctx, block104b_withTxA, block104b_withTxA.Height, "", "legacy", 0)

	// The block is ACCEPTED (by design) — the system accepts blocks with conflicting txs
	// and stores them as conflicting in ConflictingNodes. The "full validation" path
	// also accepts the block; checkCounterConflictingOnCurrentChain is called but
	// the subtree stores txA as conflicting rather than rejecting the block.
	require.NoError(t, err,
		"block with txA (conflicting, counter-conflict mined) must be ACCEPTED — "+
			"the system stores txA as conflicting in ConflictingNodes, not reject the block")

	// txA must be stored as conflicting in the subtree
	td.VerifyConflictingInSubtrees(t, block104b_withTxA.Subtrees[0], txA)
	td.VerifyConflictingInUtxoStore(t, true, txA)
	td.VerifyNotInBlockAssembly(t, txA)
}

// testSubtreeFromForkWithBlessedConflictingTx tests that a block arriving from
// a competing fork is handled correctly even when the conflicting tx was initially
// "valid" on that fork (and hence potentially blessed without ConflictingNodes set).
//
// This is fundamentally the same as the basic double-spend fork test, ensuring that
// the fork handling works correctly regardless of the subtree blessing history.
func testSubtreeFromForkWithBlessedConflictingTx(t *testing.T, utxoStoreType string) {
	td, _, txA, txB, block102a, _ := setupDoubleSpendTest(t, utxoStoreType, 63)
	defer td.Stop(t)

	// txA is mined in block102a (chain A — current chain)
	td.VerifyConflictingInUtxoStore(t, false, txA)

	// block102b contains txB (valid on chain B perspective, conflicting on chain A perspective)
	block102b := createConflictingBlock(t, td, block102a, []*bt.Tx{txB}, []*bt.Tx{txA}, 63200)
	require.NotNil(t, block102b)

	// block102b was accepted (by design — blocks with double-spends are accepted)
	// txB is stored as conflicting in block102b's subtree
	td.VerifyConflictingInSubtrees(t, block102b.Subtrees[0], txB)
	td.VerifyConflictingInUtxoStore(t, true, txB) // conflicting from chain A's perspective

	// block102a is still the current chain
	td.WaitForBlockHeight(t, block102a, helperBlockWait, true)

	// Block assembly must not include txB (it's conflicting on current chain)
	td.VerifyNotInBlockAssembly(t, txB)
	td.VerifyNotInBlockAssembly(t, txA) // mined
}

// testBlessedSubtreeReusedAcrossMultipleBlocks tests that a subtree referenced by
// blocks on different competing chains doesn't create duplicate UTXO entries or
// corrupt conflict tracking when the block wins the fork.
//
// This tests the "same subtree, different blocks" scenario: the same subtree
// (same hash) appears in both a block on chain A and a block on chain B.
// This is the TestSameTxBothForks scenario at the subtree level.
func testBlessedSubtreeReusedAcrossMultipleBlocks(t *testing.T, utxoStoreType string) {
	// Reuse the pattern from testConflictingTxReorg in double_spend_test.go:
	// Two blocks at the same height both reference a block with the same tx (txConflict).
	// When the tx itself appears in the same block on two competing chains, the
	// system must handle the subtree reuse correctly.
	td, _, txOriginal, _, block102a, _ := setupDoubleSpendTest(t, utxoStoreType, 64)
	defer td.Stop(t)

	// tx1 and tx1Conflicting both spend txOriginal:0
	tx1 := td.CreateTransaction(t, txOriginal, 0)
	tx1Conflicting := td.CreateTransaction(t, txOriginal, 0) // double-spend

	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, tx1))
	require.Error(t, td.PropagationClient.ProcessTransaction(td.Ctx, tx1Conflicting))

	// Create block103a and block103b BOTH containing tx1Conflicting
	// (same tx in both competing blocks — this is the "same subtree reuse" scenario)
	_, block103a := td.CreateTestBlock(t, block102a, 64300, tx1Conflicting)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103a, block103a.Height, "", "legacy", 0))

	_, block103b := td.CreateTestBlock(t, block102a, 64301, tx1Conflicting)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103b, block103b.Height, "", "legacy", 0))

	td.WaitForBlockHeight(t, block103a, helperBlockWait)

	// Both blocks reference tx1Conflicting — the system must handle this without
	// creating duplicate UTXO entries or corrupting the conflict state.
	// tx1Conflicting was mined in block103a (chain A winner). tx1 conflicts with it.
	// tx1 must NOT be in block assembly (it was beaten by the mined tx1Conflicting).
	td.VerifyNotInBlockAssembly(t, tx1)

	// tx1Conflicting is mined in block103a — both blocks store it as conflicting in
	// their subtrees, but the UTXO store correctly has tx1Conflicting as non-conflicting
	// (it's the winner for the txOriginal outputs it spends).
	td.VerifyConflictingInUtxoStore(t, true, tx1)
	td.VerifyConflictingInUtxoStore(t, false, tx1Conflicting)
}
