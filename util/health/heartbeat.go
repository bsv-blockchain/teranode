// Package health provides progress tracking for liveness probes.
package health

import (
	"sync/atomic"
	"time"
)

// Heartbeat records the last time a loop proved it could still make progress,
// so a liveness probe can tell a wedged service from a healthy one (issue 1447).
//
// The distinction that makes this safe is WHAT gets recorded. A heartbeat must
// mean "this loop is still servicing its work channel", NOT "this loop did
// useful work" — a node with no blocks arriving is perfectly healthy, and on
// mainnet the gap between blocks is routinely tens of minutes. Loops therefore
// beat on a periodic tick inside the same select they service work on: if the
// loop is deadlocked, or wedged inside a handler, the tick cannot be served
// either and the heartbeat goes stale. That is exactly the silent freeze a
// liveness probe exists to catch, and it stays quiet when the node is merely
// idle.
//
// The zero value is usable and reports healthy until the first Beat, so a
// service that has not started its loop yet is never killed during startup.
// That matters more than it looks: a service is constructed during Init but its
// loop may not start until well into Start, behind work that is legitimately
// unbounded — waiting on pending block validation, reloading a large unmined
// set from disk. Beating before the loop owns the heartbeat would age it
// through that entire preamble and report a perfectly healthy, still-starting
// node as wedged; worse, a restart sends the node back through the same
// preamble, so it could never finish starting.
//
// The beat is stored as a time.Time rather than a plain unix-nanosecond count
// so it keeps its monotonic reading. Age therefore measures elapsed time, not
// wall-clock difference: an NTP correction or a host clock step cannot age the
// heartbeat past the timeout and restart a node that is working fine.
type Heartbeat struct {
	lastBeat atomic.Pointer[time.Time] // nil = never beaten
	now      func() time.Time
}

// Beat records that the loop is still making progress.
func (h *Heartbeat) Beat() {
	t := h.clock()
	h.lastBeat.Store(&t)
}

// BeatIfStarted records progress only once a loop has claimed the heartbeat
// with a first Beat, and does nothing before that.
//
// Work that can run BOTH during startup and from inside the loop must use this
// rather than Beat. Beating from the startup path would move the heartbeat off
// its never-beaten state part-way through a preamble that is legitimately
// unbounded, so the remainder of that preamble would age the heartbeat and the
// probe would report a healthy, still-starting node as wedged.
func (h *Heartbeat) BeatIfStarted() {
	if h.lastBeat.Load() == nil {
		return
	}

	h.Beat()
}

// Age returns how long since the last beat. A Heartbeat that has never beaten
// reports zero age: nothing has claimed responsibility for it yet, so it must
// not be read as a stall.
func (h *Heartbeat) Age() time.Duration {
	last := h.lastBeat.Load()
	if last == nil {
		return 0
	}

	age := h.clock().Sub(*last)
	if age < 0 {
		// A backwards clock step must not be read as a stall.
		return 0
	}

	return age
}

// Stalled reports whether the last beat is older than deadline, along with the
// age that decision was made on. A deadline of zero or less disables the check,
// so an operator can turn the probe's restart behaviour off without redeploying
// different code.
//
// The age is returned rather than left to a second Age call so a caller can
// report the number the decision actually used. Reading twice would let the
// loop beat in between and produce a message that contradicts its own verdict
// — "has not made progress for 0s".
func (h *Heartbeat) Stalled(deadline time.Duration) (age time.Duration, stalled bool) {
	if deadline <= 0 {
		return 0, false
	}

	age = h.Age()

	return age, age > deadline
}

// SetLastBeatForTest forces the last-beat time. Test-only seam so a caller can
// stand in for a loop that has stopped being serviced without sleeping.
func (h *Heartbeat) SetLastBeatForTest(t time.Time) {
	h.lastBeat.Store(&t)
}

func (h *Heartbeat) clock() time.Time {
	if h.now != nil {
		return h.now()
	}

	return time.Now()
}
