package netsync

import (
	"sync"
	"time"
)

// commitRateSamples is how many recent commits the rate is measured over. It is a count, not a
// time window, on purpose: while the chain waits on one slow block no commits arrive, and a
// rate over the last minute would fall to zero and shrink the read-ahead at exactly the moment
// it matters. The last 256 commits describe how fast the node commits, however long ago the
// last one was.
const commitRateSamples = 256

// commitRateTracker measures how many blocks a second the node is putting into the chain. The
// read-ahead depth uses it to turn a lead in time into a count of blocks.
type commitRateTracker struct {
	mu    sync.Mutex
	times [commitRateSamples]time.Time
	next  int
	count int
}

func newCommitRateTracker() *commitRateTracker {
	return &commitRateTracker{}
}

// note records one block joining the chain at t. Safe on a nil tracker, so managers built as
// struct literals in tests need none.
func (r *commitRateTracker) note(t time.Time) {
	if r == nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.times[r.next] = t
	r.next = (r.next + 1) % commitRateSamples

	if r.count < commitRateSamples {
		r.count++
	}
}

// rate is blocks per second over the recorded commits, or zero while fewer than two have been
// seen or they share one instant.
func (r *commitRateTracker) rate() float64 {
	if r == nil {
		return 0
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.count < 2 {
		return 0
	}

	newest := r.times[(r.next-1+commitRateSamples)%commitRateSamples]
	oldest := r.times[(r.next-r.count+commitRateSamples)%commitRateSamples]

	span := newest.Sub(oldest).Seconds()
	if span <= 0 {
		return 0
	}

	return float64(r.count-1) / span
}

// paceGapCap is the most one gap between two commits adds to the pace's time. A longer gap is the
// chain waiting for a block, not the chain's speed.
const paceGapCap = time.Second

// pace is blocks a second over the recorded commits with each gap capped at paceGapCap: the
// chain's speed when it does not wait. Zero while fewer than two commits are recorded.
func (r *commitRateTracker) pace() float64 {
	if r == nil {
		return 0
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.count < 2 {
		return 0
	}

	var span time.Duration

	first := (r.next - r.count + commitRateSamples) % commitRateSamples
	for i := 1; i < r.count; i++ {
		a := r.times[(first+i-1)%commitRateSamples]
		b := r.times[(first+i)%commitRateSamples]
		span += min(b.Sub(a), paceGapCap)
	}

	if span <= 0 {
		return 0
	}

	return float64(r.count-1) / span.Seconds()
}
