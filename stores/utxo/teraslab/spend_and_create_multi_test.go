//go:build teraslab

package teraslab_test

// SpendAndCreateMulti parity suite for the TeraSlab backend: the same shared
// contract tests the SQL/Aerospike backends wire. TeraSlab delegates to the
// shared DefaultSpendAndCreateMulti (see spend_and_create.go).

import (
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
)

func runMulti(t *testing.T, fn func(*testing.T, utxo.Store)) {
	t.Helper()
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	fn(t, store)
}

func TestSpendAndCreateMultiMatchesLoop(t *testing.T) {
	runMulti(t, tests.SpendAndCreateMultiMatchesLoop)
}
func TestSpendAndCreateMultiSpendFailsPartway(t *testing.T) {
	runMulti(t, tests.SpendAndCreateMultiSpendFailsPartway)
}
func TestSpendAndCreateMultiParentExists(t *testing.T) {
	runMulti(t, tests.SpendAndCreateMultiParentExists)
}
func TestSpendAndCreateMultiRefusalWritesNothing(t *testing.T) {
	runMulti(t, tests.SpendAndCreateMultiRefusalWritesNothing)
}
func TestSpendAndCreateMultiRepeatAtCutPoints(t *testing.T) {
	runMulti(t, tests.SpendAndCreateMultiRepeatAtCutPoints)
}
