package packedsql

import (
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
)

func TestConformance(t *testing.T) {
	t.Run("store", func(t *testing.T) { tests.Store(t, newTestStore(t)) })
	t.Run("spend", func(t *testing.T) { tests.Spend(t, newTestStore(t)) })
	t.Run("restore", func(t *testing.T) { tests.Restore(t, newTestStore(t)) })
	t.Run("freeze", func(t *testing.T) { tests.Freeze(t, newTestStore(t)) })
	t.Run("reassign", func(t *testing.T) { tests.ReAssign(t, newTestStore(t)) })
	t.Run("set mined", func(t *testing.T) { tests.SetMined(t, newTestStore(t)) })
	t.Run("conflicting", func(t *testing.T) { tests.Conflicting(t, newTestStore(t)) })
	t.Run("conflict WAL", func(t *testing.T) { tests.ConflictWAL(t, newTestStore(t)) })
	t.Run("conflict WAL crash recovery", func(t *testing.T) { tests.ConflictWALCrashRecovery(t, newTestStore(t)) })
	t.Run("unspend idempotent", func(t *testing.T) { tests.UnspendIdempotent(t, newTestStore(t)) })
	t.Run("spend error types", func(t *testing.T) { tests.SpendErrorTypes(t, newTestStore(t)) })
	t.Run("get spend not found", func(t *testing.T) { tests.GetSpendNotFound(t, newTestStore(t)) })
	t.Run("set block height zero", func(t *testing.T) { tests.SetBlockHeightZero(t, newTestStore(t)) })
	t.Run("set block state contract", func(t *testing.T) { tests.SetBlockStateContract(t, newTestStore(t)) })
	t.Run("set block state snapshot under concurrency", func(t *testing.T) {
		tests.SetBlockStateSnapshotUnderConcurrency(t, newTestStore(t))
	})
	t.Run("set locked behavior", func(t *testing.T) { tests.SetLockedBehavior(t, newTestStore(t)) })
	t.Run("set conflicting behavior", func(t *testing.T) { tests.SetConflictingBehavior(t, newTestStore(t)) })
	t.Run("set mined unmined since", func(t *testing.T) { tests.SetMinedUnminedSince(t, newTestStore(t)) })
	t.Run("spend idempotent", func(t *testing.T) { tests.SpendIdempotent(t, newTestStore(t)) })
	t.Run("spend and create", func(t *testing.T) { tests.SpendAndCreate(t, newTestStore(t)) })
	t.Run("spend and create create only", func(t *testing.T) { tests.SpendAndCreateCreateOnly(t, newTestStore(t)) })
	t.Run("spend and create spend only", func(t *testing.T) { tests.SpendAndCreateSpendOnly(t, newTestStore(t)) })
	t.Run("spend and create tx exists keeps spends", func(t *testing.T) {
		tests.SpendAndCreateTxExistsKeepsSpends(t, newTestStore(t))
	})
	t.Run("spend and create spend error surfaces per input", func(t *testing.T) {
		tests.SpendAndCreateSpendErrorSurfacesPerInput(t, newTestStore(t))
	})
	t.Run("spend and create invalid options", func(t *testing.T) {
		tests.SpendAndCreateInvalidOptions(t, newTestStore(t))
	})
	t.Run("set mined with spent", func(t *testing.T) { tests.SetMinedWithSpent(t, newTestStore(t)) })
}

func TestConflictRecoveryConformance(t *testing.T) {
	t.Run("descendants", func(t *testing.T) { tests.ConflictRecoveryDescendants(t, newTestStore(t)) })
	t.Run("failures", func(t *testing.T) { tests.ConflictRecoveryFailures(t, newTestStore(t)) })
	t.Run("persistence", func(t *testing.T) { tests.ConflictRecoveryPersistence(t, newTestStore(t)) })
	t.Run("unexpected owner", func(t *testing.T) { tests.ConflictRecoveryUnexpectedOwner(t, newTestStore(t)) })
	t.Run("related winners", func(t *testing.T) { tests.ConflictRecoveryRelatedWinners(t, newTestStore(t)) })
	t.Run("confirmed winner", func(t *testing.T) { tests.ConflictRecoveryConfirmedWinner(t, newTestStore(t)) })
	t.Run("forward own block", func(t *testing.T) { tests.ConflictRecoveryForwardOwnBlock(t, newTestStore(t)) })
	t.Run("confirmed loser not demoted", func(t *testing.T) { tests.ConflictRecoveryConfirmedLoserNotDemoted(t, newTestStore(t)) })
	t.Run("confirmed loser owns winning input", func(t *testing.T) { tests.ConflictRecoveryConfirmedLoserOwnsWinningInput(t, newTestStore(t)) })
	t.Run("confirmed loser mixed chains", func(t *testing.T) { tests.ConflictRecoveryConfirmedLoserMixedChains(t, newTestStore(t)) })
	t.Run("foreign lock", func(t *testing.T) { tests.ConflictRecoveryForeignLock(t, newTestStore(t)) })
	t.Run("wide cascade", func(t *testing.T) { tests.ConflictRecoveryWideCascade(t, newTestStore(t)) })
	t.Run("late loser child", func(t *testing.T) { tests.ConflictRecoveryLateLoserChild(t, newTestStore(t)) })
	t.Run("late winner child", func(t *testing.T) { tests.ConflictRecoveryLateWinnerChild(t, newTestStore(t)) })
	t.Run("late free input spender", func(t *testing.T) { tests.ConflictRecoveryLateFreeInputSpender(t, newTestStore(t)) })
	t.Run("early failure", func(t *testing.T) { tests.ConflictRecoveryEarlyFailure(t, newTestStore(t)) })
	t.Run("refused after rollback", func(t *testing.T) { tests.ConflictRecoveryRefusedAfterRollback(t, newTestStore(t)) })
	t.Run("heal stale forward", func(t *testing.T) { tests.ConflictRecoveryHealStaleForward(t, newTestStore(t)) })
	t.Run("outsider owned input", func(t *testing.T) { tests.ConflictRecoveryOutsiderOwnedInput(t, newTestStore(t)) })
	t.Run("reverse then readd", func(t *testing.T) { tests.ConflictRecoveryReverseThenReadd(t, newTestStore(t)) })
	t.Run("reverse keeps foreign lock", func(t *testing.T) { tests.ConflictRecoveryReverseKeepsForeignLock(t, newTestStore(t)) })
	t.Run("legacy intent", func(t *testing.T) { tests.ConflictRecoveryLegacyIntent(t, newTestStore(t)) })
	t.Run("cancelled verify", func(t *testing.T) { tests.ConflictRecoveryCancelledVerify(t, newTestStore(t)) })
	t.Run("set conflicting children", func(t *testing.T) { tests.ConflictRecoverySetConflictingChildren(t, newTestStore(t), true) })
	t.Run("reverse completion failure", func(t *testing.T) { tests.ConflictRecoveryReverseCompletionFailure(t, newTestStore(t)) })
	t.Run("winner with conflicting child", func(t *testing.T) { tests.ConflictRecoveryWinnerWithConflictingChild(t, newTestStore(t)) })
	t.Run("partial reverse readd", func(t *testing.T) { tests.ConflictRecoveryPartialReverseReadd(t, newTestStore(t)) })
	t.Run("freed input taken", func(t *testing.T) { tests.ConflictRecoveryFreedInputTaken(t, newTestStore(t)) })
	t.Run("block keyed intent", func(t *testing.T) { tests.ConflictRecoveryBlockKeyedIntent(t, newTestStore(t)) })
	t.Run("superset winners", func(t *testing.T) { tests.ConflictRecoverySupersetWinners(t, newTestStore(t)) })
	t.Run("superset refused phases", func(t *testing.T) { tests.ConflictRecoverySupersetRefusedPhases(t, newTestStore(t)) })
	t.Run("freed winning input taken", func(t *testing.T) { tests.ConflictRecoveryFreedWinningInputTaken(t, newTestStore(t)) })
	t.Run("applied winner pruned parent", func(t *testing.T) { tests.ConflictRecoveryAppliedWinnerPrunedParent(t, newTestStore(t)) })
	t.Run("legacy key", func(t *testing.T) { tests.ConflictRecoveryLegacyKey(t, newTestStore(t)) })
	t.Run("rollback step failures", func(t *testing.T) { tests.ConflictRecoveryRollbackStepFailures(t, newTestStore(t)) })
	t.Run("replan then heal", func(t *testing.T) { tests.ConflictRecoveryReplanThenHeal(t, newTestStore(t)) })
	t.Run("rollback retry two winners", func(t *testing.T) { tests.ConflictRecoveryRollbackRetryTwoWinners(t, newTestStore(t)) })
}

func TestParentOutputsConformance(t *testing.T) {
	t.Run("contract", func(t *testing.T) { tests.ParentOutputsForValidation(t, newTestStore(t)) })
	t.Run("reads outputs, never inputs", func(t *testing.T) { tests.ParentOutputsReadsOutputsNotInputs(t, newTestStore(t)) })
}

func TestSpendAndCreateMultiConformance(t *testing.T) {
	t.Run("matches a loop of SpendAndCreate", func(t *testing.T) { tests.SpendAndCreateMultiMatchesLoop(t, newTestStore(t)) })
	t.Run("a spend fails partway", func(t *testing.T) { tests.SpendAndCreateMultiSpendFailsPartway(t, newTestStore(t)) })
	t.Run("a parent in the list exists", func(t *testing.T) { tests.SpendAndCreateMultiParentExists(t, newTestStore(t)) })
	t.Run("a refusal writes nothing", func(t *testing.T) { tests.SpendAndCreateMultiRefusalWritesNothing(t, newTestStore(t)) })
	t.Run("repeat at every cut point", func(t *testing.T) { tests.SpendAndCreateMultiRepeatAtCutPoints(t, newTestStore(t)) })
	t.Run("refusals", func(t *testing.T) { tests.SpendAndCreateMultiRefusals(t, newTestStore(t)) })
	t.Run("an empty list", func(t *testing.T) { tests.SpendAndCreateMultiEmptyList(t, newTestStore(t)) })
	t.Run("levels parallel and ordered", func(t *testing.T) { tests.SpendAndCreateMultiLevelsParallelAndOrdered(t, newTestStore(t)) })
	t.Run("concurrency bound", func(t *testing.T) { tests.SpendAndCreateMultiConcurrencyBound(t, newTestStore(t)) })
	t.Run("result mapping", func(t *testing.T) { tests.SpendAndCreateMultiResultMapping(t, newTestStore(t)) })
	t.Run("options pass through", func(t *testing.T) { tests.SpendAndCreateMultiOptionsPassThrough(t, newTestStore(t)) })
	t.Run("subtree indexes", func(t *testing.T) { tests.SpendAndCreateMultiSubtreeIdxs(t, newTestStore(t)) })
	t.Run("cancelled between levels", func(t *testing.T) { tests.SpendAndCreateMultiCancelledBetweenLevels(t, newTestStore(t)) })
	t.Run("parents deduplicated", func(t *testing.T) { tests.SpendAndCreateMultiParentsDeduplicated(t, newTestStore(t)) })
	t.Run("below checkpoint matches a loop", func(t *testing.T) { tests.SpendAndCreateMultiBelowCheckpointMatchesLoop(t, newTestStore(t)) })
	t.Run("below checkpoint two waves", func(t *testing.T) { tests.SpendAndCreateMultiBelowCheckpointTwoWaves(t, newTestStore(t)) })
	t.Run("below checkpoint create fails", func(t *testing.T) { tests.SpendAndCreateMultiBelowCheckpointCreateFails(t, newTestStore(t)) })
	t.Run("below checkpoint existing respent", func(t *testing.T) { tests.SpendAndCreateMultiBelowCheckpointExistingRespent(t, newTestStore(t)) })
	t.Run("below checkpoint missing parent", func(t *testing.T) { tests.SpendAndCreateMultiBelowCheckpointMissingParent(t, newTestStore(t)) })
	t.Run("below checkpoint existing parent pruned", func(t *testing.T) { tests.SpendAndCreateMultiBelowCheckpointExistingParentPruned(t, newTestStore(t)) })
	t.Run("below checkpoint second wave missing parent", func(t *testing.T) {
		tests.SpendAndCreateMultiBelowCheckpointSecondWaveMissingParent(t, newTestStore(t))
	})
	t.Run("below checkpoint cancel mid-call", func(t *testing.T) { tests.SpendAndCreateMultiBelowCheckpointCancelMidCall(t, newTestStore(t)) })
	t.Run("below checkpoint rollback outlives deadline", func(t *testing.T) {
		tests.SpendAndCreateMultiBelowCheckpointRollbackOutlivesDeadline(t, newTestStore(t))
	})
}
