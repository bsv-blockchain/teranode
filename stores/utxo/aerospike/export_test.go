package aerospike

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/stores/utxo"
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

// UsesNativeOps reports whether the store engaged the native operate-path at start-up and still has it.
func (s *Store) UsesNativeOps() bool {
	return s.useNativeTeranodeOps.Load()
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

// SetMaxParentLanePositions lowers the most parent lane positions a create writes, and returns the
// function that puts it back.
func SetMaxParentLanePositions(n int) (restore func()) {
	was := maxParentLanePositions
	maxParentLanePositions = n

	return func() { maxParentLanePositions = was }
}

// ConflictWALFallbackScans is the number of this store's WAL reads that scanned the set.
func (s *Store) ConflictWALFallbackScans() int64 {
	return s.conflictWALScans.Load()
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

// SetBatcherWait sets how long a submitter waits for a batcher to deliver.
func (s *Store) SetBatcherWait(d time.Duration) {
	s.batcherWait = d
}

// RollbackIdempotentMatchesForTest runs the abort path's rollback for a failed
// spend by tx whose only successful inputs were the given idempotent matches,
// with an unwinnable sibling failure so no transient window suppresses it.
func (s *Store) RollbackIdempotentMatchesForTest(tx *bt.Tx, idempotent []*utxo.Spend) {
	s.rollbackPartialSpends(tx, &spendCompletionResult{idempotentSpends: idempotent, rollbackNeeded: true, sawUnwinnable: true}, "test")
}

// SetRollbackBeforeProbeForTest installs the hook rollbackPartialSpends runs right before its
// spender-existence probe, and clears it when given nil.
func (s *Store) SetRollbackBeforeProbeForTest(f func()) {
	s.rollbackBeforeProbe = f
}

// RollbackRequiredParentSpendsForTest runs the rollback of a failed
// RequireParentOutputs call (the second wave of DefaultSpendAndCreateMulti)
// that wrote the given spends. rollbackNeeded says whether its failure was a
// validation error (utxo.IsSpendRollbackError) rather than an infrastructure one.
func (s *Store) RollbackRequiredParentSpendsForTest(tx *bt.Tx, written []*utxo.Spend, rollbackNeeded bool) {
	s.rollbackPartialSpends(tx, &spendCompletionResult{spentSpends: written, requireParentOutput: true, rollbackNeeded: rollbackNeeded}, "test")
}

// ResolveSpendCompletionsForTest resolves the answered spends of a call by tx,
// each carrying its own Err, and returns the spends the call wrote and the
// number of inputs that succeeded.
func (s *Store) ResolveSpendCompletionsForTest(ctx context.Context, tx *bt.Tx, spends []*utxo.Spend) (written []*utxo.Spend, succeeded int) {
	items := make([]*batchSpend, len(spends))

	for i, spend := range spends {
		items[i] = &batchSpend{spend: spend}
		items[i].completed.Store(true)
		items[i].published.Store(true)
	}

	res := s.resolveSpendCompletions(ctx, tx, items, false)

	return res.spentSpends, res.succeeded
}

// SpenderState is the creating state of the record of a spender, as spenderOwnsSpend reads it.
type SpenderState struct {
	Absent          bool
	Creating        bool
	RollingBack     bool
	Hold            bool
	HasUnminedSince bool
	UnminedSince    int
}

// SpenderOwnsSpend runs spenderOwnsSpend for a match recorded at recordedHeight.
func SpenderOwnsSpend(st SpenderState, recordedHeight uint32, hasRecordedHeight bool) bool {
	return spenderOwnsSpend(
		creatingState{absent: st.Absent, creating: st.Creating, rollingBack: st.RollingBack, hold: st.Hold, hasUnminedSince: st.HasUnminedSince, unminedSince: st.UnminedSince},
		&batchSpend{recordedHeight: recordedHeight, hasRecordedHeight: hasRecordedHeight},
	)
}

// LeaveSharedClientOpenOnClose makes Close and CloseClient leave the process-wide client cache
// alone. GetAerospikeClient hands every store on one host the same client, so a test store on a
// server other tests share must not close that client for all of them; TestMain closes it.
func (s *Store) LeaveSharedClientOpenOnClose() {
	u := *s.url
	u.Host = "client-shared-with-other-test-stores"
	s.url = &u
}

// prunerServiceHolder is the test using the process-wide pruner service singleton, if any.
var (
	prunerServiceHolderMu   sync.Mutex
	prunerServiceHolderCond = sync.NewCond(&prunerServiceHolderMu)
	prunerServiceHolder     string
)

// UsePrunerServiceForTest gives the calling test the pruner service singleton until it ends:
// GetPrunerService hands every store in the process the same service, built for the first store
// that asks, so two tests on different servers that prune at the same time would share one. It
// waits for any other test using it, and resets the singleton now and when the test ends. A
// subtest of the holder, or the holder asking again, goes straight through.
func UsePrunerServiceForTest(t testing.TB) {
	t.Helper()

	prunerServiceHolderMu.Lock()

	holds := func() bool {
		return prunerServiceHolder != "" && (t.Name() == prunerServiceHolder || strings.HasPrefix(t.Name(), prunerServiceHolder+"/"))
	}

	if holds() {
		prunerServiceHolderMu.Unlock()
		ResetPrunerServiceForTests()

		return
	}

	for prunerServiceHolder != "" {
		prunerServiceHolderCond.Wait()
	}

	prunerServiceHolder = t.Name()
	prunerServiceHolderMu.Unlock()

	ResetPrunerServiceForTests()

	t.Cleanup(func() {
		ResetPrunerServiceForTests()

		prunerServiceHolderMu.Lock()
		prunerServiceHolder = ""
		prunerServiceHolderMu.Unlock()
		prunerServiceHolderCond.Broadcast()
	})
}
