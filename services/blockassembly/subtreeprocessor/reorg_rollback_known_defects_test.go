//go:build bagaps

package subtreeprocessor

// Known-defect proofs for the subtree processor's reorg rollback
// (services/blockassembly/spec.md). Each asserts the spec-correct behaviour and FAILS
// against the current implementation, so it sits behind the bagaps build tag; run with:
//
//	go test -tags bagaps -run '^TestKnownDefect_' ./services/blockassembly/subtreeprocessor/
//
// When the defect is fixed, drop the TestKnownDefect_ prefix and the build tag, and
// remove its Conformance Gaps entry from spec.md.

import (
	"testing"

	"github.com/bsv-blockchain/teranode/model"
	"github.com/stretchr/testify/require"
)

// TestKnownDefect_MoveBackBuildFailureLeavesMovedBackTxsInTheMap: when a fork reorg
// fails, the tx map MUST agree with the subtrees it leaves in place.
//
// A failure inside the move-back's bulk build puts back the subtrees the move-back
// started from, but the move-back has already written the moved-back block's
// transactions into the tx map in place, and nothing takes them out. The map then
// holds a1, which no installed subtree does. Only a reset may run after a failed fork
// reorg, and it clears the map, which hides this.
//
// Spec: BA-REORG-007 (tracked as GAP-BA-004).
func TestKnownDefect_MoveBackBuildFailureLeavesMovedBackTxsInTheMap(t *testing.T) {
	f := newStepRestoreFixture(t, stepRestoreOptions{})

	err := f.stp.Reorg([]*model.Block{f.blockABadBuild}, []*model.Block{f.blockF, f.blockF2})
	require.ErrorContains(t, err, "error bulk building subtrees", "the reorg must fail inside the move-back's bulk build")

	require.NoError(t, f.stp.CheckSubtreeProcessor(), "the tx map must agree with the subtrees after a failed move-back")
	require.False(t, f.stp.GetCurrentTxMap().Exists(f.a1), "a1 is in no installed subtree, so it must not be in the map")
}
