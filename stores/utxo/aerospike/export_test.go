package aerospike

import (
	"github.com/bsv-blockchain/aerospike-client-go/v8"
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
