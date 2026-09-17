package aerospike

import "testing"

// TestUseNativeForSubOp_FlagOffNeverNative pins the routing gate from the other side of
// TestUseNativeForSubOp_Fencing: with the setting off, no sub-op is ever native, and with
// it on, the sub-ops that are NOT fenced do take the native path. The fenced set itself —
// unspend (#899) and the four freeze-aware sub-ops (#1422) — is asserted in
// TestUseNativeForSubOp_Fencing, so a change there fails exactly one test.
func TestUseNativeForSubOp_FlagOffNeverNative(t *testing.T) {
	// Flag OFF: never native, regardless of sub-op.
	off := &Store{}
	for _, op := range []uint8{subOpSpend, subOpSpendMulti, subOpUnspend, subOpSetMined, subOpSetLocked} {
		if off.useNativeForSubOp(op) {
			t.Fatalf("flag off must never use native (sub-op %d)", op)
		}
	}

	// Flag ON: the sub-ops with no fencing reason route natively.
	on := &Store{}
	on.useNativeTeranodeOps.Store(true)

	for _, op := range []uint8{
		subOpSetMined, subOpReassign, subOpSetConflicting, subOpPreserveUntil, subOpSetLocked,
		subOpIncrementSpentExtraRec, subOpSetDeleteAtHeight, subOpAddDeletedChildren,
	} {
		if !on.useNativeForSubOp(op) {
			t.Fatalf("sub-op %d should use the native path when the flag is on", op)
		}
	}
}
