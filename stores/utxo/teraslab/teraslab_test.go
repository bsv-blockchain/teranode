package teraslab_test

import (
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo"
	teraslabstore "github.com/bsv-blockchain/teranode/stores/utxo/teraslab"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
)

// Ensure Store implements the utxo.Store interface.
var _ utxo.Store = (*teraslabstore.Store)(nil)

func TestStore(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	tests.Store(t, store)
}

func TestSpend(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	tests.Spend(t, store)
}

func TestRestore(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	tests.Restore(t, store)
}

func TestFreeze(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	tests.Freeze(t, store)
}

func TestReAssign(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	tests.ReAssign(t, store)
}

func TestSetMined(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	tests.SetMined(t, store)
}

func TestConflicting(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	tests.Conflicting(t, store)
}

func TestSanity(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	tests.Sanity(t, store)
}

func TestGetSpendNotFound(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	tests.GetSpendNotFound(t, store)
}

func TestSpendErrorTypes(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	tests.SpendErrorTypes(t, store)
}

func TestSetBlockHeightZero(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	tests.SetBlockHeightZero(t, store)
}

func TestSetLockedBehavior(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	tests.SetLockedBehavior(t, store)
}

func TestSetConflictingBehavior(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	tests.SetConflictingBehavior(t, store)
}

func TestSetMinedUnminedSince(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	tests.SetMinedUnminedSince(t, store)
}

func TestSpendIdempotent(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	tests.SpendIdempotent(t, store)
}

func TestSetMinedWithSpent(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	tests.SetMinedWithSpent(t, store)
}
