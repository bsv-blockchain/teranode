package netsync

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCommitRateTracker(t *testing.T) {
	r := newCommitRateTracker()
	require.Zero(t, r.rate(), "no commits, no rate")

	now := time.Now()
	r.note(now)
	require.Zero(t, r.rate(), "one commit is not a rate")

	r.note(now.Add(time.Second))
	require.InDelta(t, 1.0, r.rate(), 1e-9)

	for i := 2; i <= 1000; i++ {
		r.note(now.Add(time.Duration(i) * 100 * time.Millisecond))
	}

	require.InDelta(t, 10.0, r.rate(), 1e-6, "the rate follows the most recent commits, not the whole history")

	var nilTracker *commitRateTracker
	require.Zero(t, nilTracker.rate(), "a manager built without a tracker reads no rate")
	nilTracker.note(now)
}

// The pace is the chain's speed when it does not wait: a gap between two commits counts at most
// paceGapCap, so a stop for a missing block does not slow it. The deadline rule divides by it, and
// a higher pace gives shorter, safer deadlines.
func TestCommitRatePaceIgnoresLongWaits(t *testing.T) {
	r := newCommitRateTracker()
	at := time.Unix(1_000_000, 0)

	// Nine commits 0.2 s apart, then a 300 s stop, then nine more 0.2 s apart.
	for range 9 {
		r.note(at)
		at = at.Add(200 * time.Millisecond)
	}

	at = at.Add(300 * time.Second)
	for range 9 {
		r.note(at)
		at = at.Add(200 * time.Millisecond)
	}

	require.Less(t, r.rate(), 0.1, "the mean rate includes the stop")
	require.InDelta(t, 17.0/(16*0.2+1.0), r.pace(), 0.01, "the stop counts as one second")

	var nilTracker *commitRateTracker
	require.Zero(t, nilTracker.pace())
	require.Zero(t, newCommitRateTracker().pace())
}
