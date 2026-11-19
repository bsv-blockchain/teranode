package aerospike

import (
	"context"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
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

// SetAfterChildAdoptionFailure installs the hook that runs after a create fails to give the surviving
// pagination children its attempt token, and clears it when given nil.
func SetAfterChildAdoptionFailure(f func()) {
	afterChildAdoptionFailure = f
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

// FailChildAdoption makes the batch that gives surviving pagination children the attempt token
// fail, and delegates every other batch to the real client. With batchLevel the batch call itself
// returns the error, otherwise each record carries it. RestoreBatchOperate undoes it.
func (s *Store) FailChildAdoption(batchLevel bool) {
	failure := aerospike.ErrTimeout

	s.batchOperateFn = func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
		if len(records) > 0 {
			if bw, ok := records[0].(*aerospike.BatchWrite); ok && bw.Policy != nil && bw.Policy.RecordExistsAction == aerospike.UPDATE_ONLY {
				if batchLevel {
					return failure
				}

				for _, record := range records {
					record.BatchRec().Err = failure
				}

				return nil
			}
		}

		return s.client.BatchOperate(policy, records)
	}
}

// RestoreBatchOperate removes the override that FailChildAdoption installed.
func (s *Store) RestoreBatchOperate() {
	s.batchOperateFn = nil
}

// FinalizeFilteredOutResult reads the result of a finalize write that the server filtered out.
// inDoubt says that the client sent the write more than once.
func (s *Store) FinalizeFilteredOutResult(txHash *chainhash.Hash, attempt []byte, inDoubt bool) error {
	return s.finalizeFilteredOutResult(txHash, attempt, inDoubt)
}

// WriteInDoubt reports whether the client sent the write of a batch record more than once.
func WriteInDoubt(rec *aerospike.BatchRecord) bool {
	return writeInDoubt(rec)
}
