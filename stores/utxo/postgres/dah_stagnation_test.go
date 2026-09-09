package postgres

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestClassifyStall(t *testing.T) {
	threshold := 900 * time.Second

	cases := []struct {
		name          string
		sinceProgress time.Duration
		backlog       int64
		want          stallLevel
	}{
		{"no backlog never alarms regardless of age", 24 * time.Hour, 0, stallNone},
		{"fresh progress with backlog", 10 * time.Second, 5000, stallNone},
		{"just under warn threshold", 449 * time.Second, 5000, stallNone},
		{"warn at threshold/2", 450 * time.Second, 5000, stallWarn},
		{"still warn just under threshold", 899 * time.Second, 5000, stallWarn},
		{"error at threshold", 900 * time.Second, 5000, stallError},
		{"error far past threshold", 36 * time.Hour, 1, stallError},
		{"tiny backlog still alarms", 900 * time.Second, 1, stallError},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, classifyStall(tc.sinceProgress, tc.backlog, threshold))
		})
	}
}

func TestClassifyStallDisabledThreshold(t *testing.T) {
	// threshold <= 0 disables the alarm entirely (explicit ops opt-out).
	require.Equal(t, stallNone, classifyStall(24*time.Hour, 5000, 0))
}

// TestStagnationTrackerRewindThenResume covers the reorg-rewind blind spot: a
// running-max tracker would treat post-rewind re-sweeping (950 -> 960 -> 970,
// all below the old 1000 peak) as "no progress" and fire a sustained false
// stallError during healthy recovery. The tracker must compare against the
// PREVIOUS tick's raw value, so any forward motion counts as an advance.
func TestStagnationTrackerRewindThenResume(t *testing.T) {
	threshold := 900 * time.Second
	now := time.Unix(1_000_000, 0)

	var tr stagnationTracker

	var wm [numPartitions]int64
	wm[3] = 1000
	tr.prime(wm, now)

	// Reorg rewinds partition 3's watermark to 950: NOT an advance, but the
	// baseline must move down to 950 so the coming re-sweep registers.
	now = now.Add(60 * time.Second)
	level, _ := tr.observe(3, 950, 500, now, threshold)
	require.Equal(t, stallNone, level)

	// A full threshold later the sweep has moved 950 -> 960, still below the
	// old 1000 peak. This IS an advance: no alarm, and the clock refreshes.
	now = now.Add(threshold)
	level, since := tr.observe(3, 960, 490, now, threshold)
	require.Equal(t, stallNone, level, "post-rewind forward progress below the old peak must count as an advance")
	require.Equal(t, time.Duration(0), since)

	// And again another full threshold later, 960 -> 970: still healthy.
	now = now.Add(threshold)
	level, _ = tr.observe(3, 970, 480, now, threshold)
	require.Equal(t, stallNone, level)
}

// TestStagnationTrackerEscalation walks a genuinely frozen watermark with
// backlog through the decision table as fake time passes: None below
// threshold/2, Warn at threshold/2, Error at threshold.
func TestStagnationTrackerEscalation(t *testing.T) {
	threshold := 900 * time.Second
	start := time.Unix(1_000_000, 0)

	var tr stagnationTracker

	var wm [numPartitions]int64
	wm[0] = 100
	tr.prime(wm, start)

	// 60s frozen: below threshold/2 -> None.
	level, since := tr.observe(0, 100, 50, start.Add(60*time.Second), threshold)
	require.Equal(t, stallNone, level)
	require.Equal(t, 60*time.Second, since)

	// 450s frozen: at threshold/2 -> Warn.
	level, since = tr.observe(0, 100, 50, start.Add(450*time.Second), threshold)
	require.Equal(t, stallWarn, level)
	require.Equal(t, 450*time.Second, since)

	// 900s frozen: at threshold -> Error.
	level, since = tr.observe(0, 100, 50, start.Add(900*time.Second), threshold)
	require.Equal(t, stallError, level)
	require.Equal(t, 900*time.Second, since)
}

// TestStagnationTrackerAdvanceResetsEscalation: after a stallError, an advance
// drops straight back to None and restarts the frozen clock from that tick.
func TestStagnationTrackerAdvanceResetsEscalation(t *testing.T) {
	threshold := 900 * time.Second
	start := time.Unix(1_000_000, 0)

	var tr stagnationTracker

	var wm [numPartitions]int64
	wm[5] = 200
	tr.prime(wm, start)

	// Frozen past the threshold -> Error.
	level, _ := tr.observe(5, 200, 50, start.Add(threshold), threshold)
	require.Equal(t, stallError, level)

	// Watermark advances -> None, clock reset.
	advancedAt := start.Add(threshold + 60*time.Second)
	level, since := tr.observe(5, 201, 49, advancedAt, threshold)
	require.Equal(t, stallNone, level)
	require.Equal(t, time.Duration(0), since)

	// Frozen again, but not yet threshold/2 since the advance -> still None.
	level, _ = tr.observe(5, 201, 49, advancedAt.Add(threshold/2-time.Second), threshold)
	require.Equal(t, stallNone, level)

	// Frozen a full threshold since the advance -> Error again.
	level, _ = tr.observe(5, 201, 49, advancedAt.Add(threshold), threshold)
	require.Equal(t, stallError, level)
}

// TestStagnationTrackerNoBacklogNeverEscalates: a frozen watermark with zero
// backlog is a caught-up sweep, not a stall, no matter how old.
func TestStagnationTrackerNoBacklogNeverEscalates(t *testing.T) {
	threshold := 900 * time.Second
	start := time.Unix(1_000_000, 0)

	var tr stagnationTracker

	var wm [numPartitions]int64
	wm[7] = 300
	tr.prime(wm, start)

	level, _ := tr.observe(7, 300, 0, start.Add(24*time.Hour), threshold)
	require.Equal(t, stallNone, level)

	level, _ = tr.observe(7, 300, 0, start.Add(48*time.Hour), threshold)
	require.Equal(t, stallNone, level)
}

// TestZeroTipTrackerEscalation walks a persistently zero/negative safeTip
// through the same None -> Warn -> Error decision table as the per-partition
// tracker (via classifyStall's backlog=1 sentinel: tip unknown means backlog
// cannot be ruled out), but with a different logging cadence: Warnf must fire
// once on the transition into Warn, and Errorf must repeat every tick once in
// Error — this is the escalation clock for "broken/zero safeTip", the exact
// silent-stall class the monitor exists to kill.
func TestZeroTipTrackerEscalation(t *testing.T) {
	threshold := 900 * time.Second
	start := time.Unix(1_000_000, 0)

	var z zeroTipTracker

	level, since, shouldLog := z.observe(start, threshold)
	require.Equal(t, stallNone, level)
	require.Equal(t, time.Duration(0), since)
	require.False(t, shouldLog)

	level, since, shouldLog = z.observe(start.Add(450*time.Second), threshold)
	require.Equal(t, stallWarn, level)
	require.Equal(t, 450*time.Second, since)
	require.True(t, shouldLog, "first tick crossing into Warn must log")

	// Still in the Warn window a tick later: must NOT log again.
	level, _, shouldLog = z.observe(start.Add(480*time.Second), threshold)
	require.Equal(t, stallWarn, level)
	require.False(t, shouldLog, "Warn only logs once on the transition, not every tick")

	// At threshold: Error, and it logs.
	level, since, shouldLog = z.observe(start.Add(900*time.Second), threshold)
	require.Equal(t, stallError, level)
	require.Equal(t, 900*time.Second, since)
	require.True(t, shouldLog)

	// Past threshold, still Error: logs EVERY tick, unlike Warn.
	level, _, shouldLog = z.observe(start.Add(960*time.Second), threshold)
	require.Equal(t, stallError, level)
	require.True(t, shouldLog, "Error alarm must repeat every tick")
}

// TestZeroTipTrackerResetOnRecovery: once safeTip goes positive again, reset()
// must clear the clock so a later zero-tip episode starts counting from zero
// rather than reading as still-stalled from the old episode.
func TestZeroTipTrackerResetOnRecovery(t *testing.T) {
	threshold := 900 * time.Second
	start := time.Unix(1_000_000, 0)

	var z zeroTipTracker

	level, _, _ := z.observe(start, threshold)
	require.Equal(t, stallNone, level)

	level, _, _ = z.observe(start.Add(900*time.Second), threshold)
	require.Equal(t, stallError, level)

	// safeTip recovers above 0.
	z.reset()

	// A fresh zero-tip episode must NOT be treated as still stalled.
	level, since, shouldLog := z.observe(start.Add(910*time.Second), threshold)
	require.Equal(t, stallNone, level)
	require.Equal(t, time.Duration(0), since)
	require.False(t, shouldLog)
}

// TestZeroTipTrackerDisabledThreshold: threshold<=0 stays fully silent even
// after a long zero-tip stretch, consistent with classifyStall's disabled path.
func TestZeroTipTrackerDisabledThreshold(t *testing.T) {
	start := time.Unix(1_000_000, 0)

	var z zeroTipTracker

	level, _, shouldLog := z.observe(start.Add(24*time.Hour), 0)
	require.Equal(t, stallNone, level)
	require.False(t, shouldLog)
}

// TestStagnationTrackerPrimeIsIdempotent: prime() only baselines once; later
// calls (the monitor calls it every tick) must not reset a frozen clock.
func TestStagnationTrackerPrimeIsIdempotent(t *testing.T) {
	threshold := 900 * time.Second
	start := time.Unix(1_000_000, 0)

	var tr stagnationTracker

	var wm [numPartitions]int64
	wm[1] = 400
	tr.prime(wm, start)

	// A later prime call must NOT re-baseline lastAdvance.
	tr.prime(wm, start.Add(800*time.Second))

	level, since := tr.observe(1, 400, 50, start.Add(900*time.Second), threshold)
	require.Equal(t, stallError, level)
	require.Equal(t, 900*time.Second, since)
}
