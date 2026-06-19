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
