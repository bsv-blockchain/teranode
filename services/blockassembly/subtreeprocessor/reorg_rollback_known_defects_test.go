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

	"github.com/stretchr/testify/require"
)

// TestKnownDefect_PartialFailureRollbackLeavesMovedBackTx: when a reorg fails half
// way, the rollback MUST restore block assembly's internal state, not only the tip. A
// tx that only the moved-back block carried must not stay in the tx map, and the
// subtree processor's invariants must hold.
//
// Today reorgBlocks snapshots currentTxMap by pointer before the move-back, and the
// move-back adds txBack to that same map in place. The rollback reassigns the
// already-mutated map, so txBack survives and the tx map no longer matches the
// restored subtrees. In production handleReorg falls back to a full reset on reorg
// failure, which hides this.
//
// Spec: BA-REORG-007 (tracked as GAP-BA-006).
func TestKnownDefect_PartialFailureRollbackLeavesMovedBackTx(t *testing.T) {
	r := runPartialFailureReorg(t)
	require.Error(t, r.reorgErr, "the injected mid-reorg failure must surface as an error")

	require.NoError(t, r.stp.CheckSubtreeProcessor(),
		"rolled-back reorg must restore subtree processor invariants")
	require.False(t, r.stp.GetCurrentTxMap().Exists(r.txBack),
		"move-back-only tx must not remain in currentTxMap after rollback")
}
