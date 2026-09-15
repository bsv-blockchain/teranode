package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/stretchr/testify/require"
)

// The suite used to be opt-in behind UTXOSET_CONFORMANCE, because it failed while the store
// was incomplete and a permanently red package makes a real regression indistinguishable from
// the known gap. It passes now, so the gate is gone: it runs with everything else, and a
// failure means a regression rather than a to-do list.

// The store-agnostic conformance suite is this store's specification. Every other
// implementation (postgres, aerospike, sql) is held to it, and the same invariants must
// hold here regardless of how radically the storage model differs underneath.
//
// Each subtest is named for the capability it pins, so a failure says what is missing rather
// than merely that something is. They all pass; a suite not listed here is one this store does
// not implement the entry point for, not one that is failing quietly.
func TestConformance(t *testing.T) {
	t.Run("Store", func(t *testing.T) {
		db, _ := newTestStore(t)
		tests.Store(t, db)
	})

	t.Run("Spend", func(t *testing.T) {
		db, _ := newTestStore(t)
		tests.Spend(t, db)
	})

	t.Run("Freeze", func(t *testing.T) {
		db, _ := newTestStore(t)
		tests.Freeze(t, db)
	})

	// Reassigning a frozen coin, the alert system's confiscation path. It is the one place a
	// coin's spending rules change under it, and this store holds the rules themselves rather
	// than a digest of them, so it needs hash_override to carry what the new output hashes to.
	t.Run("ReAssign", func(t *testing.T) {
		db, _ := newTestStore(t)
		tests.ReAssign(t, db)
	})

	t.Run("SetMined", func(t *testing.T) {
		db, _ := newTestStore(t)
		tests.SetMined(t, db)
	})

	t.Run("Conflicting", func(t *testing.T) {
		db, _ := newTestStore(t)
		tests.Conflicting(t, db)
	})

	t.Run("Restore", func(t *testing.T) {
		db, _ := newTestStore(t)
		tests.Restore(t, db)
	})

	t.Run("UnspendIdempotent", func(t *testing.T) {
		db, _ := newTestStore(t)
		tests.UnspendIdempotent(t, db)
	})

	t.Run("SetMinedWithSpent", func(t *testing.T) {
		db, _ := newTestStore(t)
		tests.SetMinedWithSpent(t, db)
	})

	// The full round trip through the membership table: a longest-chain stamp settles the
	// transaction and moves it into tx_mined, MarkTransactionsOnLongestChain(false) moves it
	// back out with the mempool marker at the current tip, and (true) settles it again. It was
	// parked while only the outward move existed.
	t.Run("SetMinedUnminedSince", func(t *testing.T) {
		db, _ := newTestStore(t)
		tests.SetMinedUnminedSince(t, db)
	})

	// The delete-at-height lifecycle: a mempool-created tx stamped mined on the longest
	// chain moves into tx_mined, every output gets spent, and Prune(1_000_000) at that
	// height drops every membership window below the journal-retention cutoff wholesale
	// (there is no per-row DAH sweep in this design — see pruner.go). The coins are
	// already gone from the spend, so once the window holding the tx's identity is
	// dropped, a lookup misses.
	t.Run("MinedThenSpendAllPrunes", func(t *testing.T) {
		db, _ := newTestStore(t)

		svc, err := db.GetPrunerService()
		require.NoError(t, err)

		tests.MinedThenSpendAllPrunes(t, db, svc)
	})

	// The six SpendAndCreate entry points. The spec named them as ones this design should
	// enable, and they are the cross-store contract for the option C1's own-output coin guard
	// turns on: WithCreateOnly skips the spend phase, which is the path a mempool create takes
	// when the validator's CreateConflicting branch fires.
	//
	// The package's own spend_and_create_batch_test.go covers similar ground, but it is written
	// against this store's internals. These are written against the interface, which is what
	// makes them a contract rather than a second opinion.
	t.Run("SpendAndCreate", func(t *testing.T) {
		db, _ := newTestStore(t)
		tests.SpendAndCreate(t, db)
	})

	t.Run("SpendAndCreateCreateOnly", func(t *testing.T) {
		db, _ := newTestStore(t)
		tests.SpendAndCreateCreateOnly(t, db)
	})

	t.Run("SpendAndCreateSpendOnly", func(t *testing.T) {
		db, _ := newTestStore(t)
		tests.SpendAndCreateSpendOnly(t, db)
	})

	t.Run("SpendAndCreateTxExistsKeepsSpends", func(t *testing.T) {
		db, _ := newTestStore(t)
		tests.SpendAndCreateTxExistsKeepsSpends(t, db)
	})

	t.Run("SpendAndCreateSpendErrorSurfacesPerInput", func(t *testing.T) {
		db, _ := newTestStore(t)
		tests.SpendAndCreateSpendErrorSurfacesPerInput(t, db)
	})

	t.Run("SpendAndCreateInvalidOptions", func(t *testing.T) {
		db, _ := newTestStore(t)
		tests.SpendAndCreateInvalidOptions(t, db)
	})

	// The conflict-resolution write-ahead log. Block assembly reads it once at startup and
	// replays whatever a crash left half-finished, so an intent that does not survive is a
	// conflict resolution that silently never completes.
	t.Run("ConflictWAL", func(t *testing.T) {
		db, _ := newTestStore(t)
		tests.ConflictWAL(t, db)
	})

	// Crash recovery for that log: for each step boundary of the forward and reverse
	// conflict-resolution operations it rebuilds the on-disk state a SIGKILL would leave and
	// replays. It is the hardest suite here because the parent is MINED throughout, so every
	// read the driver makes on it -- its inputs, its spenders, its locked flag -- has to reach
	// tx_mined rather than the identity table.
	t.Run("ConflictWALCrashRecovery", func(t *testing.T) {
		db, _ := newTestStore(t)
		tests.ConflictWALCrashRecovery(t, db)
	})

	// The conflicting flag from the outside: GetSpend reports CONFLICTING on the coin, Get
	// reports it on the metadata, a spend of that coin fails with ErrTxConflicting, and the
	// contested parent names the child without becoming conflicting itself.
	t.Run("SetConflictingBehavior", func(t *testing.T) {
		db, _ := newTestStore(t)
		tests.SetConflictingBehavior(t, db)
	})

	// The lock from the outside, and the round trip: OK, locked, a spend refused with
	// ErrTxLocked, unlocked, OK, and the same spend now accepted.
	t.Run("SetLockedBehavior", func(t *testing.T) {
		db, _ := newTestStore(t)
		tests.SetLockedBehavior(t, db)
	})

	// Re-spending a coin with the SAME spending transaction is a no-op success, not a double
	// spend. Block validation replays a block it has already applied, and a store that raised
	// there could never re-apply one.
	t.Run("SpendIdempotent", func(t *testing.T) {
		db, _ := newTestStore(t)
		tests.SpendIdempotent(t, db)
	})

	// The four ways a spend is refused, each with its own error, because the validator
	// behaves differently for each: a parent it has never seen, a claim about the coin that
	// does not match, a coinbase inside its maturity window, and a coin some other
	// transaction already took -- which must also name the transaction that took it.
	t.Run("SpendErrorTypes", func(t *testing.T) {
		db, _ := newTestStore(t)
		tests.SpendErrorTypes(t, db)
	})

	// Not-found is a STATUS from GetSpend, never an error. A caller asking about an outpoint
	// the store does not hold is asking a legitimate question.
	t.Run("GetSpendNotFound", func(t *testing.T) {
		db, _ := newTestStore(t)
		tests.GetSpendNotFound(t, db)
	})

	// Height zero is refused, because it is the unconfirmed sentinel throughout this store and
	// accepting it would make every maturity and retention test read true.
	t.Run("SetBlockHeightZero", func(t *testing.T) {
		db, _ := newTestStore(t)
		tests.SetBlockHeightZero(t, db)
	})

	// The two halves of the block-state snapshot contract (issue 1443). The write side: one
	// SetBlockState and the snapshot and both single-field getters agree, because DAH and
	// maturity read the height directly and would otherwise freeze at a stale one. The read
	// side: a pair some single writer actually published, never one assembled from two loads,
	// which only shows up while a writer is mid-update.
	t.Run("SetBlockStateContract", func(t *testing.T) {
		db, _ := newTestStore(t)
		tests.SetBlockStateContract(t, db)
	})

	t.Run("SetBlockStateSnapshotUnderConcurrency", func(t *testing.T) {
		db, _ := newTestStore(t)
		tests.SetBlockStateSnapshotUnderConcurrency(t, db)
	})

	// Sanity used to be LEFT OUT everywhere: tests.go:739 built its spending transaction with
	// a zero-value bt.FeeQuote{}, which carries no fee types, so go-bt's ChangeToAddress
	// returned "feetype not found" and the require failed before any store was touched. Fixed
	// in the shared suite (bt.NewFeeQuote()), and this is the first store to turn it on.
	//
	// It covers a width nothing else here does: a thousand transactions created, each spent by
	// a child, and all thousand outpoints then asked about by name. That is the only scale in
	// the suite at which a statement whose cost is a function of the table rather than the
	// batch would show up as a stall rather than as a wrong answer.
	t.Run("Sanity", func(t *testing.T) {
		db, _ := newTestStore(t)
		tests.Sanity(t, db)
	})
}

// BenchmarkConformance is the shared suite's own benchmark: create, spend, unspend, delete,
// through the interface, one round per iteration.
//
// It is a benchmark rather than a test, so it needs its own top-level function -- t.Run cannot
// give it a *testing.B -- and `go test` without -bench does not run it at all. That is the
// -short skip the task asked for, provided by the toolchain rather than by a guard here, so
// nothing about it lengthens an ordinary run. Measured on the container instance at 3.0 ms per
// round, so it is cheap enough to keep.
func BenchmarkConformance(b *testing.B) {
	db, _ := benchStore(b)

	tests.Benchmark(b, db)
}
