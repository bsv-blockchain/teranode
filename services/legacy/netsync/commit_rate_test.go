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
