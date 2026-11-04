//go:build teraslab

package teraslab_test

// ParentOutputsForValidation coverage for the TeraSlab backend.
//
// The shared tests.ParentOutputsForValidation is intentionally NOT wired: it
// asserts a snapshot-seeded tx (WithMinedBlockInfo BlockID 0) reads back Mined,
// which the TeraSlab 0.9.1 server cannot represent — it treats a create-time
// BlockID of 0 as unmined and stores no block entry (icellan/teraslab#105). The
// read contract (answers come from the parent's own outputs, never a child's
// input copy) is covered by tests.ParentOutputsReadsOutputsNotInputs, which the
// store passes.

import (
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
)

func TestParentOutputsReadsOutputsNotInputs(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()
	tests.ParentOutputsReadsOutputsNotInputs(t, store)
}
