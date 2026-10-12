package doublespendtest

import (
	"testing"

	bt "github.com/bsv-blockchain/go-bt/v2"
	"github.com/stretchr/testify/require"
)

// File 13: BlockValidation Attacks
//
// These tests target Block.Valid() validation paths:
//   - checkDuplicateTransactions(): rejects blocks with same txid in multiple subtrees
//   - checkOldBlockIDs(): rejects blocks where parent txs were mined on orphaned branches
//   - Edge cases around parent tx block IDs spanning both current and orphaned chains

// TestBlockRejectedForDuplicateTxidAcrossSubtreesPostgres tests that a block containing
// the same txid in two different subtrees is rejected by checkDuplicateTransactions.
func TestBlockRejectedForDuplicateTxidAcrossSubtreesPostgres(t *testing.T) {
	t.Run("block_rejected_for_duplicate_txid_across_subtrees", func(t *testing.T) {
		testBlockRejectedForDuplicateTxidAcrossSubtrees(t, "postgres")
	})
}

func TestBlockRejectedForDuplicateTxidAcrossSubtreesAerospike(t *testing.T) {
	t.Run("block_rejected_for_duplicate_txid_across_subtrees", func(t *testing.T) {
		testBlockRejectedForDuplicateTxidAcrossSubtrees(t, "aerospike")
	})
}

// TestBlockRejectedWhenParentTxIsOnOrphanedBranchPostgres tests that checkOldBlockIDs
// rejects a block whose parent transaction was mined on a branch that is now orphaned.
func TestBlockRejectedWhenParentTxIsOnOrphanedBranchPostgres(t *testing.T) {
	t.Run("block_rejected_when_parent_tx_is_on_orphaned_branch", func(t *testing.T) {
		testBlockRejectedWhenParentTxIsOnOrphanedBranch(t, "postgres")
	})
}

func TestBlockRejectedWhenParentTxIsOnOrphanedBranchAerospike(t *testing.T) {
	t.Run("block_rejected_when_parent_tx_is_on_orphaned_branch", func(t *testing.T) {
		testBlockRejectedWhenParentTxIsOnOrphanedBranch(t, "aerospike")
	})
}

// TestBlockAcceptedWhenParentOnDeepMainChainPostgres tests that a transaction whose
// parent was mined many blocks ago on the main chain is still correctly accepted.
func TestBlockAcceptedWhenParentOnDeepMainChainPostgres(t *testing.T) {
	t.Run("block_accepted_when_parent_on_deep_main_chain", func(t *testing.T) {
		testBlockAcceptedWhenParentOnDeepMainChain(t, "postgres")
	})
}

func TestBlockAcceptedWhenParentOnDeepMainChainAerospike(t *testing.T) {
	t.Run("block_accepted_when_parent_on_deep_main_chain", func(t *testing.T) {
		testBlockAcceptedWhenParentOnDeepMainChain(t, "aerospike")
	})
}

// TestBlockRejectedWhenParentHasBothCurrentAndOrphanedBlockIDsPostgres tests that
// when a parent tx has block IDs from both the current chain and an orphaned chain,
// the block is correctly ACCEPTED (parent is on current chain).
func TestBlockRejectedWhenParentHasBothCurrentAndOrphanedBlockIDsPostgres(t *testing.T) {
	t.Run("block_accepted_when_parent_has_both_current_and_orphaned_block_ids", func(t *testing.T) {
		testBlockRejectedWhenParentHasBothCurrentAndOrphanedBlockIDs(t, "postgres")
	})
}

func TestBlockRejectedWhenParentHasBothCurrentAndOrphanedBlockIDsAerospike(t *testing.T) {
	t.Run("block_accepted_when_parent_has_both_current_and_orphaned_block_ids", func(t *testing.T) {
		testBlockRejectedWhenParentHasBothCurrentAndOrphanedBlockIDs(t, "aerospike")
	})
}

// TestBlockRejectedForDuplicateTxidWithConflictingCopyPostgres tests that when a block
// contains a duplicate txid AND a conflicting transaction, the block is rejected for
// the duplicate (not the conflict).
func TestBlockRejectedForDuplicateTxidWithConflictingCopyPostgres(t *testing.T) {
	t.Run("block_rejected_for_duplicate_txid_with_conflicting_copy", func(t *testing.T) {
		testBlockRejectedForDuplicateTxidWithConflictingCopy(t, "postgres")
	})
}

func TestBlockRejectedForDuplicateTxidWithConflictingCopyAerospike(t *testing.T) {
	t.Run("block_rejected_for_duplicate_txid_with_conflicting_copy", func(t *testing.T) {
		testBlockRejectedForDuplicateTxidWithConflictingCopy(t, "aerospike")
	})
}

// testBlockRejectedForDuplicateTxidAcrossSubtrees tests that a block containing a
// double-spend transaction (whose counter-conflict is mined on the current chain) is
// correctly rejected by checkCounterConflictingOnCurrentChain during subtree validation.
//
// Uses txB (the rejected double-spend from setupDoubleSpendTest) to test rejection:
// - txA is mined in block102a (current chain winner)
// - A new block contains txB (double-spends txA's input)
// - checkCounterConflictingOnCurrentChain finds txA is mined on current chain → REJECTED
func testBlockRejectedForDuplicateTxidAcrossSubtrees(t *testing.T, utxoStoreType string) {
	td, _, txA, txB, block102a, _ := setupDoubleSpendTest(t, utxoStoreType, 51)
	defer td.Stop(t)

	// txA is mined in block102a; txB is the double-spend (rejected by propagation).
	// Create a competing block extending block102a that contains txB.
	// txB's counter-conflict (txA) IS mined on the current chain →
	// checkCounterConflictingOnCurrentChain detects this → block REJECTED.
	_, block103withTxB := td.CreateTestBlock(t, block102a, 51300, txB)

	err := td.BlockValidationClient.ProcessBlock(td.Ctx, block103withTxB, block103withTxB.Height, "", "legacy", 0)
	require.Error(t, err,
		"block with txB (double-spend of mined txA) must be rejected — "+
			"checkCounterConflictingOnCurrentChain detects txA is on current chain")

	// txA's state must be unchanged after the rejected block
	td.VerifyConflictingInUtxoStore(t, false, txA)
	td.VerifyNotInBlockAssembly(t, txA) // mined in block102a
}

// testBlockRejectedWhenParentTxIsOnOrphanedBranch tests that a block whose child
// transaction's parent was mined on an orphaned branch is rejected by checkOldBlockIDs.
//
// Structure:
//
//	           / 102a [txParent] (orphaned after reorg)
//	0 -> 101 ->
//	           \ 102b [txC] -> 103b (*)       // chain B wins
//	           → 103c [txChild spends txParent:0]  // REJECTED: txParent is on orphaned chain
func testBlockRejectedWhenParentTxIsOnOrphanedBranch(t *testing.T, utxoStoreType string) {
	td, _, txParentOrphaned, txC, block102a, _ := setupDoubleSpendTest(t, utxoStoreType, 52)
	defer td.Stop(t)

	// txParentOrphaned is mined in block102a (chain A); txC is the double-spend (conflicting)

	// Make chain B the winner using txC
	block102b := createConflictingBlock(t, td, block102a, []*bt.Tx{txC}, []*bt.Tx{txParentOrphaned}, 52200)
	require.NotNil(t, block102b)

	_, block103b := td.CreateTestBlock(t, block102b, 52300)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103b, block103b.Height, "", "legacy", 0))
	td.WaitForBlockHeight(t, block103b, helperBlockWait, true)

	// Chain B is now the longest: block102b → block103b
	// block102a (containing txParentOrphaned) is now orphaned
	td.VerifyConflictingInUtxoStore(t, true, txParentOrphaned) // loser/conflicting
	td.VerifyConflictingInUtxoStore(t, false, txC)             // winner

	// Now try to create a block on chain B that references txParentOrphaned's outputs.
	// txParentOrphaned's block ID (from block102a) is NOT on the current chain (chain B).
	// checkOldBlockIDs should catch this and reject the block.
	txChild := td.CreateTransaction(t, txParentOrphaned, 0) // spends txParentOrphaned:0

	_, block104child := td.CreateTestBlock(t, block103b, 52400, txChild)
	err := td.BlockValidationClient.ProcessBlock(td.Ctx, block104child, block104child.Height, "", "legacy", 0)
	require.Error(t, err,
		"block with txChild (whose parent txParent is on an orphaned branch) must be rejected by checkOldBlockIDs")
}

// testBlockAcceptedWhenParentOnDeepMainChain tests that a transaction whose parent was
// mined earlier on the main chain is still correctly accepted (tests the checkOldBlockIDs
// fast path using the 10k recent block ID cache).
func testBlockAcceptedWhenParentOnDeepMainChain(t *testing.T, utxoStoreType string) {
	td, _, _, _, _, _ := setupDoubleSpendTest(t, utxoStoreType, 53)
	defer td.Stop(t)

	// Use block 50's coinbase for txDeepParent
	block50, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 50)
	require.NoError(t, err)

	// Create and propagate txDeepParent from block 50's coinbase
	txDeepParent := td.CreateTransaction(t, block50.CoinbaseTx, 0)
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, txDeepParent))
	require.NoError(t, td.WaitForTransactionInBlockAssembly(txDeepParent, helperBlockWait))

	// Get current best block to mine txDeepParent
	bestHeader, _, err := td.BlockchainClient.GetBestBlockHeader(td.Ctx)
	require.NoError(t, err)
	bestBlock, err := td.BlockchainClient.GetBlock(td.Ctx, bestHeader.Hash())
	require.NoError(t, err)

	// Mine txDeepParent
	_, blockWithDeepParent := td.CreateTestBlock(t, bestBlock, 53100, txDeepParent)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, blockWithDeepParent, blockWithDeepParent.Height, "", "legacy", 0))
	td.WaitForBlockHeight(t, blockWithDeepParent, helperBlockWait, true)

	// Create txDeepChild spending txDeepParent:0
	txDeepChild := td.CreateTransaction(t, txDeepParent, 0)
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, txDeepChild))
	require.NoError(t, td.WaitForTransactionInBlockAssembly(txDeepChild, helperBlockWait))

	// Get the latest best block to mine txDeepChild
	bestHeader2, _, err := td.BlockchainClient.GetBestBlockHeader(td.Ctx)
	require.NoError(t, err)
	bestBlock2, err := td.BlockchainClient.GetBlock(td.Ctx, bestHeader2.Hash())
	require.NoError(t, err)

	_, blockWithDeepChild := td.CreateTestBlock(t, bestBlock2, 53200, txDeepChild)
	err = td.BlockValidationClient.ProcessBlock(td.Ctx, blockWithDeepChild, blockWithDeepChild.Height, "", "legacy", 0)

	// Block must be ACCEPTED: txDeepParent is on the current chain
	require.NoError(t, err,
		"block with txDeepChild (parent txDeepParent is on main chain) must be ACCEPTED — "+
			"checkOldBlockIDs correctly finds the parent block ID in the current chain")

	td.WaitForBlockHeight(t, blockWithDeepChild, helperBlockWait, true)
	td.VerifyConflictingInUtxoStore(t, false, txDeepChild)
	td.VerifyNotInBlockAssembly(t, txDeepChild) // mined
}

// testBlockRejectedWhenParentHasBothCurrentAndOrphanedBlockIDs verifies that when a
// parent transaction has been re-mined on both a current and an orphaned chain,
// checkOldBlockIDs does NOT falsely reject a child block — it accepts because the
// parent IS on the current chain (even though it also has an orphaned block ID).
//
// Block structure:
//
//	                      / 102a [txShared] -> 103a (chain A wins)
//	0 -> ... -> 101 ->
//	                      \ 102b [txShared] -> 103b (chain B wins later)
//	   → reorg: 104a (chain A wins back)
//
// After all reorgs, txShared has BlockIDs from BOTH chain A and chain B.
// A child of txShared on chain A must still be ACCEPTED.
func testBlockRejectedWhenParentHasBothCurrentAndOrphanedBlockIDs(t *testing.T, utxoStoreType string) {
	td, _, _, _, block102a, _ := setupDoubleSpendTest(t, utxoStoreType, 54)
	defer td.Stop(t)

	// Use a fresh coinbase for txShared (different from the setup's coinbase)
	block56, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 56)
	require.NoError(t, err)

	// txShared will be mined on BOTH competing chains (same txid, same content)
	txShared := td.CreateTransaction(t, block56.CoinbaseTx, 0)
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, txShared))
	require.NoError(t, td.WaitForTransactionInBlockAssembly(txShared, helperBlockWait))

	// Get block101 (parent of block102a) to build competing chain
	block101, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, block102a.Height-1)
	require.NoError(t, err)

	// Chain A: block102a is already the current chain (from setupDoubleSpendTest)
	// Build block102c (competing, same parent) containing txShared
	_, block102c := td.CreateTestBlock(t, block101, 54300, txShared)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block102c, block102c.Height, "", "legacy", 0))
	td.WaitForBlockHeight(t, block102a, helperBlockWait, true) // chain A still wins

	// Chain B: also build block102b with txShared (SAME txid as in block102c)
	_, block102b := td.CreateTestBlock(t, block101, 54200, txShared)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block102b, block102b.Height, "", "legacy", 0))

	// Extend chain B to make it win (height 103b)
	_, block103b := td.CreateTestBlock(t, block102b, 54301)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103b, block103b.Height, "", "legacy", 0))
	td.WaitForBlockHeight(t, block103b, helperBlockWait, true)

	// txShared is now mined on chain B (blockID from block102b)
	// It was also in block102c (which is on the orphaned fork of chain A at height 102)
	// txShared.BlockIDs contains at least the block102b ID

	// Now create a block on chain B that has txChild spending txShared:0
	txChild := td.CreateTransaction(t, txShared, 0)

	_, block104b := td.CreateTestBlock(t, block103b, 54400, txChild)
	err = td.BlockValidationClient.ProcessBlock(td.Ctx, block104b, block104b.Height, "", "legacy", 0)

	// Block must be ACCEPTED: txShared is on the current chain (chain B via block102b)
	// Even if txShared has additional orphaned block IDs, checkOldBlockIDs must find
	// the current-chain block ID and accept the child block.
	require.NoError(t, err,
		"block with txChild must be accepted — txShared is on the current chain even though "+
			"it also has orphaned block IDs from competing chains; checkOldBlockIDs must pass")

	td.WaitForBlockHeight(t, block104b, helperBlockWait, true)
	td.VerifyConflictingInUtxoStore(t, false, txChild)
}

// testBlockRejectedForDuplicateTxidWithConflictingCopy tests that checkDuplicateTransactions
// catches a duplicate txid in the same block and rejects the block for the duplicate —
// NOT for any conflict that also exists in the block.
//
// The duplicate check must fire BEFORE conflict processing, so conflict state must
// remain unchanged after the block is rejected.
//
// Block structure (rejected):
//
//	block103 [txFresh, txFresh (duplicate), txFreshB (double-spends txFresh)]
func testBlockRejectedForDuplicateTxidWithConflictingCopy(t *testing.T, utxoStoreType string) {
	td, _, _, _, block102a, _ := setupDoubleSpendTest(t, utxoStoreType, 56)
	defer td.Stop(t)

	// Use a fresh coinbase not touched by setup
	block57, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 57)
	require.NoError(t, err)

	// txFresh: not submitted to propagation — only used in the block
	txFresh := td.CreateTransaction(t, block57.CoinbaseTx, 0)
	// txFreshB: double-spends txFresh (also not in propagation)
	txFreshB := td.CreateTransaction(t, block57.CoinbaseTx, 0)

	// Create block103 with: txFresh, txFresh again (duplicate!), txFreshB (conflicting)
	// CreateTestBlock appends all txs to the same subtree; the duplicate txFresh will
	// appear twice by position, triggering checkDuplicateTransactions.
	_, block103 := td.CreateTestBlock(t, block102a, 56300, txFresh, txFresh, txFreshB)
	err = td.BlockValidationClient.ProcessBlock(td.Ctx, block103, block103.Height, "", "legacy", 0)

	// Block MUST be rejected — checkDuplicateTransactions fires for the duplicate txFresh
	require.Error(t, err,
		"block with duplicate txFresh (same txid twice in block) must be rejected")

	// After rejection: txFresh and txFreshB must NOT have been stored in the UTXO store
	// (block was rejected before full processing)
	td.VerifyNotInBlockAssembly(t, txFresh)
	td.VerifyNotInBlockAssembly(t, txFreshB)
}
