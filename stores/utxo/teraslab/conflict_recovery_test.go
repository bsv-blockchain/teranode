//go:build teraslab

package teraslab_test

// Conflict-recovery parity suite for the TeraSlab backend: the same shared
// contract tests the Aerospike backend wires (stores/utxo/tests/conflict_recovery.go),
// exercising crash-replay, cascades, rollback and healing against the live server.
// Each case gets a fresh container (clean UTXO + WAL), mirroring the per-test
// isolation the rest of the TeraSlab suite uses. The ownership-specific cases the
// Aerospike backend skips (SetConflictLocks/ReleaseConflictLocks are no-ops here —
// see conflict_recovery.go) are skipped here too.

import (
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
)

func runConflictRecovery(t *testing.T, fn func(*testing.T, utxo.Store)) {
	t.Helper()
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	fn(t, store)
}

func TestConflictRecoveryWideCascade(t *testing.T) {
	runConflictRecovery(t, tests.ConflictRecoveryWideCascade)
}
func TestConflictRecoveryLateLoserChild(t *testing.T) {
	runConflictRecovery(t, tests.ConflictRecoveryLateLoserChild)
}
func TestConflictRecoveryLateWinnerChild(t *testing.T) {
	runConflictRecovery(t, tests.ConflictRecoveryLateWinnerChild)
}
func TestConflictRecoveryLateFreeInputSpender(t *testing.T) {
	runConflictRecovery(t, tests.ConflictRecoveryLateFreeInputSpender)
}
func TestConflictRecoveryEarlyFailure(t *testing.T) {
	runConflictRecovery(t, tests.ConflictRecoveryEarlyFailure)
}
func TestConflictRecoveryRefusedAfterRollback(t *testing.T) {
	runConflictRecovery(t, tests.ConflictRecoveryRefusedAfterRollback)
}
func TestConflictRecoveryHealStaleForward(t *testing.T) {
	runConflictRecovery(t, tests.ConflictRecoveryHealStaleForward)
}
func TestConflictRecoveryReverseCompletionFailure(t *testing.T) {
	runConflictRecovery(t, tests.ConflictRecoveryReverseCompletionFailure)
}
func TestConflictRecoveryWinnerWithConflictingChild(t *testing.T) {
	runConflictRecovery(t, tests.ConflictRecoveryWinnerWithConflictingChild)
}
func TestConflictRecoveryReverseThenReadd(t *testing.T) {
	runConflictRecovery(t, tests.ConflictRecoveryReverseThenReadd)
}
func TestConflictRecoveryLegacyIntent(t *testing.T) {
	runConflictRecovery(t, tests.ConflictRecoveryLegacyIntent)
}
func TestConflictRecoveryCancelledVerify(t *testing.T) {
	runConflictRecovery(t, tests.ConflictRecoveryCancelledVerify)
}
func TestConflictRecoveryPartialReverseReadd(t *testing.T) {
	runConflictRecovery(t, tests.ConflictRecoveryPartialReverseReadd)
}
func TestConflictRecoveryFreedInputTaken(t *testing.T) {
	runConflictRecovery(t, tests.ConflictRecoveryFreedInputTaken)
}
func TestConflictRecoveryBlockKeyedIntent(t *testing.T) {
	runConflictRecovery(t, tests.ConflictRecoveryBlockKeyedIntent)
}
func TestConflictRecoverySupersetWinners(t *testing.T) {
	runConflictRecovery(t, tests.ConflictRecoverySupersetWinners)
}
func TestConflictRecoverySupersetRefusedPhases(t *testing.T) {
	runConflictRecovery(t, tests.ConflictRecoverySupersetRefusedPhases)
}
func TestConflictRecoveryFreedWinningInputTaken(t *testing.T) {
	runConflictRecovery(t, tests.ConflictRecoveryFreedWinningInputTaken)
}
func TestConflictRecoveryAppliedWinnerPrunedParent(t *testing.T) {
	runConflictRecovery(t, tests.ConflictRecoveryAppliedWinnerPrunedParent)
}
func TestConflictRecoveryLegacyKey(t *testing.T) {
	runConflictRecovery(t, tests.ConflictRecoveryLegacyKey)
}
func TestConflictRecoveryRollbackStepFailures(t *testing.T) {
	runConflictRecovery(t, tests.ConflictRecoveryRollbackStepFailures)
}
func TestConflictRecoveryRollbackRetryTwoWinners(t *testing.T) {
	runConflictRecovery(t, tests.ConflictRecoveryRollbackRetryTwoWinners)
}
func TestConflictRecoveryReplanThenHeal(t *testing.T) {
	runConflictRecovery(t, tests.ConflictRecoveryReplanThenHeal)
}

func TestConflictRecoverySetConflictingChildren(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	tests.ConflictRecoverySetConflictingChildren(t, store, true)
}
