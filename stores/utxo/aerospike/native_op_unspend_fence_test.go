package aerospike

import "testing"

// TestUseNativeForSubOp_FencesUnspend locks the #899 safety invariant: the
// native operate-path is never used for unspend, even when native ops are
// enabled, because the server-fork subOpUnspend dispatcher's SpendingData
// ownership enforcement cannot be verified from the client (the startup probe
// exercises only setLocked). Unspend therefore stays on the UDF/Lua path, which
// always enforces the #766 ownership check. All other sub-ops keep the native
// path when the flag is on.
func TestUseNativeForSubOp_FencesUnspend(t *testing.T) {
	// Flag OFF: never native, regardless of sub-op.
	off := &Store{}
	for _, op := range []uint8{subOpSpend, subOpUnspend, subOpSetLocked} {
		if off.useNativeForSubOp(op) {
			t.Fatalf("flag off must never use native (sub-op %d)", op)
		}
	}

	// Flag ON with replay protection proven: every sub-op is native EXCEPT
	// unspend, which is fenced to UDF.
	on := &Store{}
	on.useNativeTeranodeOps.Store(true)
	on.nativeReplayProtection.Store(true)

	if on.useNativeForSubOp(subOpUnspend) {
		t.Fatal("subOpUnspend must be fenced to the UDF path even with native ops on (#899)")
	}

	for _, op := range []uint8{
		subOpSpend, subOpSpendMulti, subOpSetMined, subOpFreeze, subOpUnfreeze,
		subOpReassign, subOpSetConflicting, subOpPreserveUntil, subOpSetLocked,
		subOpAddDeletedChildren,
	} {
		if !on.useNativeForSubOp(op) {
			t.Fatalf("sub-op %d should use the native path when the flag is on", op)
		}
	}
}

// TestUseNativeForSubOp_FencesReplayProtectionSubOpsOnly: a dispatcher that
// passes the first-seen probe but not the replay-protection probe keeps the
// native path for every other sub-op, and runs spend, spendMulti and
// addDeletedChildren through the UDF, where the marker and the idempotent report
// are enforced in-repo. Before, the failed probe demoted the whole store.
func TestUseNativeForSubOp_FencesReplayProtectionSubOpsOnly(t *testing.T) {
	unproven := &Store{}
	unproven.useNativeTeranodeOps.Store(true)

	for _, op := range []uint8{subOpSpend, subOpSpendMulti, subOpAddDeletedChildren, subOpUnspend} {
		if unproven.useNativeForSubOp(op) {
			t.Fatalf("sub-op %d must stay on the UDF path until replay protection is proven", op)
		}
	}

	for _, op := range []uint8{
		subOpSetMined, subOpFreeze, subOpUnfreeze, subOpReassign, subOpSetConflicting,
		subOpPreserveUntil, subOpSetLocked, subOpIncrementSpentExtraRec, subOpSetDeleteAtHeight,
	} {
		if !unproven.useNativeForSubOp(op) {
			t.Fatalf("sub-op %d should keep the native path when only replay protection is unproven", op)
		}
	}
}
