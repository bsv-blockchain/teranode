//go:build teraslab

// Package doublespendtest also exercises the full double-spend / conflict suite
// against the TeraSlab UTXO store. These variants are gated behind the
// `teraslab` build tag because running them requires a TeraSlab server image and
// Docker, which most contributors will not have. They reuse the exact same
// backend-agnostic test bodies as the Aerospike and Postgres variants, only
// swapping the UTXO store type to "teraslab".
//
// Run with:  go test -tags teraslab ./test/sequentialtest/double_spend/...
// Override the image with the TERASLAB_IMAGE env var (defaults to
// ghcr.io/icellan/teraslab:latest).

package doublespendtest

import "testing"

func TestSingleDoubleSpendExternalTxTeraslab(t *testing.T) {
	t.Run("single_external_tx_with_one_conflicting_transaction", func(t *testing.T) {
		testSingleDoubleSpendExternalTx(t, "teraslab")
	})
}

func TestMarkAsConflictingMultipleExternalTxTeraslab(t *testing.T) {
	t.Run("multiple_conflicting_external_txs_in_different_blocks", func(t *testing.T) {
		testMarkAsConflictingMultipleExternalTx(t, "teraslab")
	})
}

func TestMarkAsConflictingChainsExternalTxTeraslab(t *testing.T) {
	t.Run("conflicting_external_transaction_chains", func(t *testing.T) {
		testMarkAsConflictingChainsExternalTx(t, "teraslab")
	})
}

func TestDoubleSpendForkExternalTxTeraslab(t *testing.T) {
	t.Run("double_spend_fork_external_tx", func(t *testing.T) {
		testDoubleSpendForkExternalTx(t, "teraslab")
	})
}

func TestTripleForkedChainExternalTxTeraslab(t *testing.T) {
	t.Run("triple_forked_chain_external_tx", func(t *testing.T) {
		testTripleForkedChainExternalTx(t, "teraslab")
	})
}

func TestGrandparentChildConflictTeraslab(t *testing.T) {
	t.Run("grandparent_child_conflict", func(t *testing.T) {
		testGrandparentChildConflict(t, "teraslab")
	})
}

func TestGrandparentMultiOutputConflictTeraslab(t *testing.T) {
	t.Run("grandparent_multi_output_conflict", func(t *testing.T) {
		testGrandparentMultiOutputConflict(t, "teraslab")
	})
}

func TestGrandparentChildWithParentDependencyTeraslab(t *testing.T) {
	t.Run("grandparent_child_with_parent_dependency", func(t *testing.T) {
		testGrandparentChildWithParentDependency(t, "teraslab")
	})
}

func TestSameChainGrandparentDoubleSpendTeraslab(t *testing.T) {
	t.Run("same_chain_grandparent_double_spend", func(t *testing.T) {
		testSameChainGrandparentDoubleSpend(t, "teraslab")
	})
}

func TestComplexForkGrandparentConflictTeraslab(t *testing.T) {
	t.Run("complex_fork_grandparent_conflict", func(t *testing.T) {
		testComplexForkGrandparentConflict(t, "teraslab")
	})
}

func TestDeepChainConflictResolutionTeraslab(t *testing.T) {
	t.Run("deep_chain_conflict_resolution", func(t *testing.T) {
		testDeepChainConflictResolution(t, "teraslab")
	})
}

func TestWideTreeConflictResolutionTeraslab(t *testing.T) {
	t.Run("wide_tree_conflict_resolution", func(t *testing.T) {
		testWideTreeConflictResolution(t, "teraslab")
	})
}

func TestReorgResetRecoversAssemblyTxTeraslab(t *testing.T) {
	t.Run("reset_recovers_assembly_tx_with_mined_utxo_state", func(t *testing.T) {
		testReorgResetRecoversAssemblyTx(t, "teraslab")
	})
}

func TestSubtreeBlessingStaleConflictingNodesTeraslab(t *testing.T) {
	t.Run("blessed_subtree_stale_conflicting_nodes_after_mined_tx", func(t *testing.T) {
		testBlessedSubtreeStaleConflictingNodesAfterMinedTx(t, "teraslab")
	})
}

func TestTwoCompetingMinerSubtreesConflictingTeraslab(t *testing.T) {
	t.Run("two_competing_miner_subtrees_conflicting", func(t *testing.T) {
		testTwoCompetingMinerSubtreesConflicting(t, "teraslab")
	})
}

func TestBlessedSubtreeWithOrphanedParentTeraslab(t *testing.T) {
	t.Run("blessed_subtree_with_orphaned_parent", func(t *testing.T) {
		testBlessedSubtreeWithOrphanedParent(t, "teraslab")
	})
}

func TestConcurrentSubtreesBlessingRaceTeraslab(t *testing.T) {
	t.Run("concurrent_subtrees_blessing_race_for_same_utxo", func(t *testing.T) {
		testConcurrentSubtreesBlessingRaceForSameUTXO(t, "teraslab")
	})
}

func TestSubtreeConflictingNodeAlreadySetTeraslab(t *testing.T) {
	t.Run("subtree_conflicting_node_already_set_remains_accurate", func(t *testing.T) {
		testSubtreeConflictingNodeAlreadySetRemainsAccurate(t, "teraslab")
	})
}

func TestBlessingMidValidationRaceTeraslab(t *testing.T) {
	t.Run("blessing_fails_when_counter_conflict_mined_mid_validation", func(t *testing.T) {
		testBlessingFailsWhenCounterConflictIsMinedMidValidation(t, "teraslab")
	})
}

func TestSubtreeFromForkWithBlessedConflictingTxTeraslab(t *testing.T) {
	t.Run("subtree_from_fork_with_blessed_conflicting_tx", func(t *testing.T) {
		testSubtreeFromForkWithBlessedConflictingTx(t, "teraslab")
	})
}

func TestBlessedSubtreeReusedAcrossMultipleBlocksTeraslab(t *testing.T) {
	t.Run("blessed_subtree_reused_across_multiple_blocks", func(t *testing.T) {
		testBlessedSubtreeReusedAcrossMultipleBlocks(t, "teraslab")
	})
}

func TestBlockRejectedForDuplicateTxidAcrossSubtreesTeraslab(t *testing.T) {
	t.Run("block_rejected_for_duplicate_txid_across_subtrees", func(t *testing.T) {
		testBlockRejectedForDuplicateTxidAcrossSubtrees(t, "teraslab")
	})
}

func TestBlockRejectedWhenParentTxIsOnOrphanedBranchTeraslab(t *testing.T) {
	t.Run("block_rejected_when_parent_tx_is_on_orphaned_branch", func(t *testing.T) {
		testBlockRejectedWhenParentTxIsOnOrphanedBranch(t, "teraslab")
	})
}

func TestBlockAcceptedWhenParentOnDeepMainChainTeraslab(t *testing.T) {
	t.Run("block_accepted_when_parent_on_deep_main_chain", func(t *testing.T) {
		testBlockAcceptedWhenParentOnDeepMainChain(t, "teraslab")
	})
}

func TestBlockRejectedWhenParentHasBothCurrentAndOrphanedBlockIDsTeraslab(t *testing.T) {
	t.Run("block_accepted_when_parent_has_both_current_and_orphaned_block_ids", func(t *testing.T) {
		testBlockRejectedWhenParentHasBothCurrentAndOrphanedBlockIDs(t, "teraslab")
	})
}

func TestBlockRejectedForDuplicateTxidWithConflictingCopyTeraslab(t *testing.T) {
	t.Run("block_rejected_for_duplicate_txid_with_conflicting_copy", func(t *testing.T) {
		testBlockRejectedForDuplicateTxidWithConflictingCopy(t, "teraslab")
	})
}

func TestFlipFlopReorgTeraslab(t *testing.T) {
	t.Run("flip_flop_reorg_4_times", func(t *testing.T) {
		testFlipFlopReorg(t, "teraslab")
	})
}

func TestResubmitOrphanedConfirmedTxTeraslab(t *testing.T) {
	t.Run("resubmit_orphaned_confirmed_tx_to_mempool", func(t *testing.T) {
		testResubmitOrphanedConfirmedTxToMempool(t, "teraslab")
	})
}

func TestOrphanedTxReturnsToMempoolTeraslab(t *testing.T) {
	t.Run("orphaned_tx_returns_to_mempool_not_conflicting", func(t *testing.T) {
		testOrphanedTxReturnsToMempoolNotConflicting(t, "teraslab")
	})
}

func TestFrozenTxInConflictResolutionPathTeraslab(t *testing.T) {
	t.Run("frozen_tx_in_conflict_resolution_path", func(t *testing.T) {
		testFrozenTxInConflictResolutionPath(t, "teraslab")
	})
}

func TestProcessConflictingDoesNotMarkValidChildrenTeraslab(t *testing.T) {
	t.Run("process_conflicting_does_not_mark_valid_children_as_conflicting", func(t *testing.T) {
		testProcessConflictingDoesNotMarkValidChildrenAsConflicting(t, "teraslab")
	})
}

func TestMultipleSequentialConflictsOnSameUTXOTeraslab(t *testing.T) {
	t.Run("multiple_sequential_conflicts_on_same_utxo", func(t *testing.T) {
		testMultipleSequentialConflictsOnSameUTXO(t, "teraslab")
	})
}

func TestDeepReorgMixedTxFatesTeraslab(t *testing.T) {
	t.Run("deep_reorg_mixed_tx_fates", func(t *testing.T) {
		testDeepReorgMixedTxFates(t, "teraslab")
	})
}

func TestCrossForkSpendingTransactionTeraslab(t *testing.T) {
	t.Run("cross_fork_spending_transaction", func(t *testing.T) {
		testCrossForkSpendingTransaction(t, "teraslab")
	})
}

func TestSpendingFromConflictingParentInBlockTeraslab(t *testing.T) {
	t.Run("spending_from_conflicting_parent_in_block", func(t *testing.T) {
		testSpendingFromConflictingParentInBlock(t, "teraslab")
	})
}

func TestMultiInputTxPartialSpendRollbackTeraslab(t *testing.T) {
	t.Run("multi_input_tx_partial_spend_rollback", func(t *testing.T) {
		testMultiInputTxPartialSpendRollback(t, "teraslab")
	})
}

func TestIntraBlockDoubleSpendAcrossSubtreesTeraslab(t *testing.T) {
	t.Run("intra_block_double_spend_across_subtrees", func(t *testing.T) {
		testIntraBlockDoubleSpendAcrossSubtrees(t, "teraslab")
	})
}

func TestHundredLevelDeepChainConflictTeraslab(t *testing.T) {
	t.Run("hundred_level_deep_chain_conflict", func(t *testing.T) {
		testHundredLevelDeepChainConflict(t, "teraslab")
	})
}

func TestDiamondConflictGraphTeraslab(t *testing.T) {
	t.Run("diamond_conflict_graph", func(t *testing.T) {
		testDiamondConflictGraph(t, "teraslab")
	})
}

func TestSameTxBothForksReEnableTeraslab(t *testing.T) {
	t.Run("same_tx_both_forks_re_enable", func(t *testing.T) {
		testSameTxBothForksReEnable(t, "teraslab")
	})
}

func TestSameTxBothForksTeraslab(t *testing.T) {
	t.Skip("same_tx_both_forks: test disabled pending implementation")
}

func TestConflictingChildrenListWith50EntriesTeraslab(t *testing.T) {
	t.Run("conflicting_children_list_with_50_entries", func(t *testing.T) {
		testConflictingChildrenListWith50Entries(t, "teraslab")
	})
}

func TestStaleConflictingChildrenAfterMultipleReorgsTeraslab(t *testing.T) {
	t.Run("stale_conflicting_children_after_multiple_reorgs", func(t *testing.T) {
		testStaleConflictingChildrenAfterMultipleReorgs(t, "teraslab")
	})
}

func TestResubmitDoubleSpendLongAfterOriginalMinedTeraslab(t *testing.T) {
	t.Run("resubmit_double_spend_long_after_original_mined", func(t *testing.T) {
		testResubmitDoubleSpendLongAfterOriginalMined(t, "teraslab")
	})
}

func TestLockedUTXORetryOnPhase2WindowTeraslab(t *testing.T) {
	t.Run("locked_utxo_retry_on_phase2_window", func(t *testing.T) {
		testLockedUTXORetryOnPhase2Window(t, "teraslab")
	})
}

func TestZeroOutputTransactionConflictTeraslab(t *testing.T) {
	t.Run("zero_output_transaction_conflict", func(t *testing.T) {
		testZeroOutputTransactionConflict(t, "teraslab")
	})
}

func TestDoubleSpendTeraslab(t *testing.T) {
	t.Run("single_tx_with_one_conflicting_transaction", func(t *testing.T) {
		testSingleDoubleSpend(t, "teraslab")
	})
	// t.Run("multiple conflicting txs in same block", func(t *testing.T) {
	// 	testMarkAsConflictingMultipleSameBlock(t, "teraslab")
	// })
	t.Run("multiple_conflicting_txs_in_different_blocks", func(t *testing.T) {
		testMarkAsConflictingMultiple(t, "teraslab")
	})
	t.Run("conflicting_transaction_chains", func(t *testing.T) {
		testMarkAsConflictingChains(t, "teraslab")
	})
	t.Run("double_spend_fork", func(t *testing.T) {
		testDoubleSpendFork(t, "teraslab")
	})
	t.Run("double_spend_in_subsequent_block", func(t *testing.T) {
		testDoubleSpendInSubsequentBlock(t, "teraslab")
	})
	t.Run("triple_forked_chain", func(t *testing.T) {
		testTripleForkedChain(t, "teraslab")
	})
	t.Run("test_non_conflicting_tx_after_reorg", func(t *testing.T) {
		testNonConflictingTxReorg(t, "teraslab")
	})
	t.Run("test_conflicting_tx_processed_after_reorg", func(t *testing.T) {
		testConflictingTxReorg(t, "teraslab")
	})
	t.Run("test_non_conflicting_tx_after_block_assembly_reset", func(t *testing.T) {
		testNonConflictingTxBlockAssemblyReset(t, "teraslab")
	})
	t.Run("test_double_spend_fork_with_nested_txs", func(t *testing.T) {
		testDoubleSpendForkWithNestedTXs(t, "teraslab")
	})
	t.Run("test_double_spend_with_frozen_tx", func(t *testing.T) {
		testSingleDoubleSpendFrozenTx(t, "teraslab")
	})
	// this test is not working yet, waiting for #2853
	// t.Run("test_double_spend_not_mined_for_long", func(t *testing.T) {
	// 	testSingleDoubleSpendNotMinedForLong(t, "teraslab")
	// })
}
