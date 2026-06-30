package aerospike

import (
	"sync"
	"time"
)

// batchTimerPool recycles the per-submit-wait timers used across the UTXO store
// batcher callsites (Create / Get / Spend / SetLocked / IncrementSpentRecords /
// setDAHForChildRecords). Each batcher submit-wait previously allocated a fresh
// time.NewTimer per operation; at millions of UTXO ops per block during
// big-block catchup that churn dominated allocations (~5% / ~108GB over a 30s
// profile window) for a timer that — with batcherWait at ~2m40s post-#1172 —
// effectively never fires. Reusing the timers removes that allocation while
// preserving the leak-guard semantics: the timer still fires to release a caller
// parked on a wedged batch.
//
// Go 1.23+ timer semantics make recycling safe without manual channel draining:
// after Stop or Reset returns it is guaranteed that no stale value will be
// delivered on the timer's channel, so a recycled timer Reset to a new deadline
// cannot fire a leftover tick from a previous use.
var batchTimerPool sync.Pool

// acquireBatchTimer returns a running timer that fires after d, reusing a pooled
// timer when one is available. Every call MUST be paired with releaseBatchTimer
// (typically `defer releaseBatchTimer(timer)`) so the timer is stopped and
// returned to the pool.
func acquireBatchTimer(d time.Duration) *time.Timer {
	if v := batchTimerPool.Get(); v != nil {
		t := v.(*time.Timer)
		t.Reset(d)

		return t
	}

	return time.NewTimer(d)
}

// releaseBatchTimer stops t and returns it to the pool for reuse. It is safe to
// call whether or not the timer has already fired: Go 1.23+ Stop guarantees no
// stale tick survives on the channel for the next acquireBatchTimer/Reset.
func releaseBatchTimer(t *time.Timer) {
	t.Stop()
	batchTimerPool.Put(t)
}
