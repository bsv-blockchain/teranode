package aerospike

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// ConflictingIndexVerdict reports whether the conflicting index self-test has
// finished and, if it has, whether it trusted the index.
func (s *Store) ConflictingIndexVerdict() (done, usable bool) {
	switch s.conflictingIndexState.Load() {
	case conflictingIndexUsable:
		return true, true
	case conflictingIndexUnusable:
		return true, false
	default:
		return false, false
	}
}

// StopConflictingIndexSelfTest stops the background self-test, for tests that
// never close their store.
func (s *Store) StopConflictingIndexSelfTest() {
	s.stopConflictingIndexSelfTest()
}

// SetChildDAHBeforeWrite installs the hook that runs before SetDAHForChildRecords enqueues its page
// writes, and clears it when given nil.
func SetChildDAHBeforeWrite(f func()) {
	childDAHBeforeWrite = f
}

// UsesNativeOps reports whether the store engaged the native operate-path at start-up and still has it.
func (s *Store) UsesNativeOps() bool {
	return s.useNativeTeranodeOps.Load()
}

// UsesNativeVerifiedMarker reports whether the pruner's replay-marker write uses native sub-op 15.
func (s *Store) UsesNativeVerifiedMarker() bool {
	return s.useNativeVerifiedMarkerWrite()
}

// UsesNativeMark reports whether MarkTransactionsOnLongestChain sends native sub-op 14.
func (s *Store) UsesNativeMark() bool {
	return s.useNativeMark()
}

// UsesNativeTeranodeOps reports whether the store sends native ops at all, so a test can prove a
// sub-op 14 rejection leaves the other native ops on.
func (s *Store) UsesNativeTeranodeOps() bool {
	return s.useNativeTeranodeOps.Load()
}

// SetNativeMarkForTest turns the native sub-op 14 path on or off, so a test against a stock server can
// drive the mark's native result handling through SetBatchOperateFn.
func (s *Store) SetNativeMarkForTest(on bool) {
	s.useNativeTeranodeOps.Store(on)
	s.useNativeMarkLongestChain.Store(on)
}

// DetectNativeMarkForTest runs the sub-op 14 start-up probe on its own.
func (s *Store) DetectNativeMarkForTest(ctx context.Context) bool {
	return s.detectNativeMarkLongestChainSupport(ctx)
}

// ConflictWALIndexName is the name of the WAL index.
func (s *Store) ConflictWALIndexName() string {
	return s.conflictWALIndexName()
}

// EnsureConflictWALIndex creates the WAL index when it is missing, as New does.
func (s *Store) EnsureConflictWALIndex() {
	s.ensureConflictWALIndex()
}

// QueryConflictWALIndex reads the WAL through its index alone and returns how many roots it found,
// and whether the index was readable.
func (s *Store) QueryConflictWALIndex(ctx context.Context) (int, bool, error) {
	records, readable, err := s.queryConflictWALIndex(ctx)
	return len(records), readable, err
}

// ConflictWALFallbackScans is the number of WAL reads that scanned the set, for this process.
func ConflictWALFallbackScans() float64 {
	return testutil.ToFloat64(prometheusConflictWALFallbackScans)
}

// RollbackIdempotentMatchesForTest runs the abort path's rollback for a failed
// spend by tx whose only successful inputs were the given idempotent matches,
// with an unwinnable sibling failure so no transient window suppresses it.
func (s *Store) RollbackIdempotentMatchesForTest(tx *bt.Tx, idempotent []*utxo.Spend) {
	s.rollbackPartialSpends(tx, &spendCompletionResult{idempotentSpends: idempotent, rollbackNeeded: true, sawUnwinnable: true}, "test")
}
