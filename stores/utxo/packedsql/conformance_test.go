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
