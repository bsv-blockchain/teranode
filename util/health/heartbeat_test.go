package health

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// stalled drops the age Stalled returns, for the cases asserting only the verdict.
func stalled(h *Heartbeat, deadline time.Duration) bool {
	_, out := h.Stalled(deadline)
	return out
}

func TestHeartbeatStalled(t *testing.T) {
	t.Run("a fresh heartbeat is not stalled", func(t *testing.T) {
		var h Heartbeat
		h.Beat()

		require.False(t, stalled(&h, time.Minute))
	})

	t.Run("a stale heartbeat is stalled", func(t *testing.T) {
		now := time.Now()
		h := &Heartbeat{now: func() time.Time { return now }}
		h.Beat()

		now = now.Add(2 * time.Minute)
		age, out := h.Stalled(time.Minute)
		require.True(t, out)
		require.Equal(t, 2*time.Minute, age, "the reported age must be the one the verdict used")
		require.Equal(t, 2*time.Minute, h.Age())

		// A beat clears it.
		h.Beat()
		require.False(t, stalled(h, time.Minute))
	})

	t.Run("a zero deadline disables the check", func(t *testing.T) {
		now := time.Now()
		h := &Heartbeat{now: func() time.Time { return now }}
		h.Beat()

		now = now.Add(24 * time.Hour)
		require.False(t, stalled(h, 0), "an operator who has not opted in must never be restarted")
		require.False(t, stalled(h, -time.Second))
	})

	t.Run("a heartbeat that never beat is not a stall", func(t *testing.T) {
		// A service constructed but not yet started must not be killed.
		var h Heartbeat

		require.Zero(t, h.Age())
		require.False(t, stalled(&h, time.Nanosecond))
	})

	t.Run("a backwards clock step is not a stall", func(t *testing.T) {
		now := time.Now()
		h := &Heartbeat{now: func() time.Time { return now }}
		h.Beat()

		now = now.Add(-time.Hour)
		require.Zero(t, h.Age())
		require.False(t, stalled(h, time.Second))
	})
}

// TestBeatIfStartedIsSilentUntilTheLoopOwnsTheHeartbeat pins the rule that keeps
// a node from killing itself while it is still starting. Work reachable BOTH
// from the startup preamble and from inside the loop must not start the clock:
// the preamble is legitimately unbounded, and a restart re-enters it, so a beat
// there turns a slow start into a permanent crash loop.
func TestBeatIfStartedIsSilentUntilTheLoopOwnsTheHeartbeat(t *testing.T) {
	now := time.Now()
	h := &Heartbeat{now: func() time.Time { return now }}

	h.BeatIfStarted()

	now = now.Add(time.Hour)
	require.Zero(t, h.Age(), "startup work must leave the heartbeat unclaimed")
	require.False(t, stalled(h, time.Second), "a still-starting service must never be restarted")

	// Once the loop claims it, the same call records progress — otherwise long
	// work running inside the loop would be indistinguishable from a wedge.
	h.Beat()
	now = now.Add(time.Hour)
	require.Equal(t, time.Hour, h.Age())

	h.BeatIfStarted()
	require.Zero(t, h.Age(), "once started, progress must refresh the heartbeat")
}

// TestDisableReportsHealthyAfterADeliberateStop pins the shutdown half: a loop
// that exits on purpose disables its heartbeat, and from then on the probe must
// read it as never beaten, not as a stall that keeps growing through the drain.
func TestDisableReportsHealthyAfterADeliberateStop(t *testing.T) {
	now := time.Now()
	h := &Heartbeat{now: func() time.Time { return now }}
	h.Beat()

	now = now.Add(time.Hour)
	require.True(t, stalled(h, time.Minute), "precondition: the heartbeat must be stale before Disable")

	h.Disable()
	require.False(t, stalled(h, time.Minute), "a disabled heartbeat must not report a stall")
	require.Zero(t, h.Age())

	// BeatIfStarted must not re-arm it: nothing owns the heartbeat any more.
	h.BeatIfStarted()
	require.Nil(t, h.lastBeat.Load(), "BeatIfStarted must not re-arm a disabled heartbeat")
}
