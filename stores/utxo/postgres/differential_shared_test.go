package postgres

import (
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
)

// These wire the shared gold-standard differential tests (defined in
// stores/utxo/tests/tests.go) to the postgres backend. Each pins a divergence
// found while reviewing PR #684 where the postgres store originally disagreed
// with the aerospike/sql gold standard. Those divergences have since been fixed
// in this PR, so these now act as regression/parity tests: they must PASS on
// postgres exactly as they do on aerospike and sql. The per-test comments below
// record the original postgres bug each one guards against.

// TestSetMinedUnsetOnMissingTx_Postgres: SetMinedMulti(UnsetMined=true) on a
// missing tx must be a tolerated no-op (Interface.go:295-303). Postgres returns
// a StorageError (mined.go:244-248).
func TestSetMinedUnsetOnMissingTx_Postgres(t *testing.T) {
	store, _ := setupTestStore(t)
	tests.SetMinedUnsetOnMissingTx(t, store)
}

// TestUnsetMinedPreservesUnminedSince_Postgres: unsetting one of several
// non-longest-chain block entries must preserve UnminedSince. Postgres clobbers
// it to 0 via the ELSE NULL arm (mined.go:230-232).
func TestUnsetMinedPreservesUnminedSince_Postgres(t *testing.T) {
	store, _ := setupTestStore(t)
	tests.UnsetMinedPreservesUnminedSinceWhenNonLCBlocksRemain(t, store)
}

// TestRemoveBlockIDsKeepsParallelArraysAligned_Postgres: RemoveBlockIDs must drop
// the matching entry from all three parallel arrays. Postgres previously trimmed
// only block_ids (remove_block_ids.go), misaligning block_heights/subtree_idxs.
func TestRemoveBlockIDsKeepsParallelArraysAligned_Postgres(t *testing.T) {
	store, _ := setupTestStore(t)
	tests.RemoveBlockIDsKeepsParallelArraysAligned(t, store)
}

// TestUnspendFlagAsLockedLocksParent_Postgres: Unspend(flagAsLocked=true) must
// reverse the spend and lock the parent atomically (spend.go:681-687).
func TestUnspendFlagAsLockedLocksParent_Postgres(t *testing.T) {
	store, _ := setupTestStore(t)
	tests.UnspendFlagAsLockedLocksParent(t, store)
}

// TestUnfreezeAndReassignNotFrozenErr_Postgres: UnFreezeUTXOs/ReAssignUTXO on a
// non-frozen output must return a typed error (not a silent no-op), matching the
// aerospike gold standard. Guards the guarded-UPDATE + RowsAffected diagnosis in
// alert_system.go.
func TestUnfreezeAndReassignNotFrozenErr_Postgres(t *testing.T) {
	store, _ := setupTestStore(t)
	tests.UnfreezeAndReassignNotFrozenErr(t, store)
}
