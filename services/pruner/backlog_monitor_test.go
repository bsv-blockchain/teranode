package pruner

import (
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// fakeClock is a manually advanced clock for deterministic monitor tests.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func defaultTestConfig() BacklogConfig {
	return BacklogConfig{
		Enabled:           true,
		MaxLagBlocks:      3,
		MaxIncompleteRuns: 2,
		HeadroomWarnRatio: 0.8,
		HeadroomWindow:    3,
		FailReadiness:     true,
		LogRepeatInterval: 5 * time.Minute,
	}
}

func newTestMonitor(cfg BacklogConfig) (*BacklogMonitor, *fakeClock) {
	clk := &fakeClock{t: time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)}
	return NewBacklogMonitor(cfg, ulogger.TestLogger{}, clk.now), clk
}

func TestBacklogConfigFromSettings(t *testing.T) {
	cfg := BacklogConfigFromSettings(settings.PrunerSettings{
		BacklogMonitorEnabled:    true,
		BacklogMaxLagBlocks:      4,
		BacklogMaxIncompleteRuns: 5,
		BacklogHeadroomWarnRatio: 0.7,
		BacklogHeadroomWindow:    8,
		BacklogFailReadiness:     false,
		BacklogLogRepeatInterval: time.Minute,
	})
	require.Equal(t, BacklogConfig{
		Enabled: true, MaxLagBlocks: 4, MaxIncompleteRuns: 5, HeadroomWarnRatio: 0.7,
		HeadroomWindow: 8, FailReadiness: false, LogRepeatInterval: time.Minute,
	}, cfg)
}

func TestBacklogState_String(t *testing.T) {
	require.Equal(t, "OK", BacklogOK.String())
	require.Equal(t, "AT_RISK", BacklogAtRisk.String())
	require.Equal(t, "BEHIND", BacklogBehind.String())
}

func TestBacklogMonitor_HealthyPrunerStaysOK(t *testing.T) {
	m, clk := newTestMonitor(defaultTestConfig())
	for h := uint32(100); h < 110; h++ {
		m.OnPruneRequested(h)
		m.OnRunStarted(h)
		clk.advance(2 * time.Minute)
		m.OnRunFinished(h, 1_000_000, 2*time.Minute, true)
		clk.advance(8 * time.Minute)
	}
	s := m.Snapshot()
	require.Equal(t, BacklogOK, s.State)
	require.Equal(t, uint32(109), s.LastCompletedHeight)
	require.Equal(t, uint32(0), s.LagBlocks)
	require.InDelta(t, 0.2, s.HeadroomRatio, 1e-9) // 2m runs / 10m interval
}

func TestBacklogMonitor_LagBeyondThresholdIsBehindAndRecovers(t *testing.T) {
	m, clk := newTestMonitor(defaultTestConfig())
	m.OnPruneRequested(100) // seeds lastCompleted=99
	m.OnRunStarted(100)
	// Run for 100 never finishes while more blocks arrive.
	for h := uint32(101); h <= 102; h++ {
		clk.advance(10 * time.Minute)
		require.NotEqual(t, BacklogEventEntered, m.OnPruneRequested(h), "lag %d must not trip", h-99)
	}
	clk.advance(10 * time.Minute)
	require.Equal(t, BacklogEventEntered, m.OnPruneRequested(103)) // lag = 103-99 = 4 > 3
	require.Equal(t, BacklogBehind, m.Snapshot().State)

	// Coalesced run covering everything up to 103 completes.
	require.Equal(t, BacklogEventRecovered, m.OnRunFinished(103, 5, time.Minute, true))
	s := m.Snapshot()
	require.Equal(t, BacklogOK, s.State)
	require.Equal(t, uint32(0), s.LagBlocks)
}

func TestBacklogMonitor_LagExactlyAtThresholdIsNotBehind(t *testing.T) {
	m, clk := newTestMonitor(defaultTestConfig())
	m.OnPruneRequested(100) // seeds lastCompleted=99, lag=1
	clk.advance(time.Minute)
	m.OnPruneRequested(102) // lag=3, threshold is "> 3"
	require.Equal(t, BacklogOK, m.Snapshot().State)
	require.Equal(t, uint32(3), m.Snapshot().LagBlocks)
}

func TestBacklogMonitor_IncompleteStreak(t *testing.T) {
	m, clk := newTestMonitor(defaultTestConfig())
	m.OnPruneRequested(100)
	m.OnRunStarted(100)
	require.Equal(t, BacklogEventNone, m.OnRunFinished(100, 0, time.Second, false))
	require.Equal(t, 1, m.Snapshot().IncompleteStreak)
	clk.advance(time.Second)
	require.Equal(t, BacklogEventEntered, m.OnRunFinished(100, 0, time.Second, false))
	require.Equal(t, BacklogBehind, m.Snapshot().State)
	require.Equal(t, 2, m.Snapshot().IncompleteStreak)

	require.Equal(t, BacklogEventRecovered, m.OnRunFinished(100, 10, time.Second, true))
	require.Equal(t, 0, m.Snapshot().IncompleteStreak)
	require.Equal(t, BacklogOK, m.Snapshot().State)
}

func TestBacklogMonitor_HeadroomAtRiskAfterWarmup(t *testing.T) {
	m, clk := newTestMonitor(defaultTestConfig()) // window 3
	// Runs take 9m of every 10m interval: ratio 0.9.
	h := uint32(200)
	m.OnPruneRequested(h)
	for i := 0; i < 2; i++ {
		clk.advance(9 * time.Minute)
		m.OnRunFinished(h, 1, 9*time.Minute, true)
		clk.advance(time.Minute)
		h++
		m.OnPruneRequested(h)
		require.Equal(t, BacklogOK, m.Snapshot().State, "warm-up must suppress AT_RISK (sample %d)", i+1)
	}
	clk.advance(9 * time.Minute)
	m.OnRunFinished(h, 1, 9*time.Minute, true) // 3rd duration
	clk.advance(time.Minute)
	h++
	require.Equal(t, BacklogEventEntered, m.OnPruneRequested(h)) // 3rd interval -> window full
	s := m.Snapshot()
	require.Equal(t, BacklogAtRisk, s.State)
	require.InDelta(t, 0.9, s.HeadroomRatio, 1e-9)
}

func TestBacklogMonitor_BehindTakesPrecedenceOverAtRisk(t *testing.T) {
	cfg := defaultTestConfig()
	cfg.HeadroomWindow = 1
	m, clk := newTestMonitor(cfg)
	m.OnPruneRequested(100)
	clk.advance(time.Minute)
	m.OnRunFinished(100, 1, 50*time.Second, true) // ratio pending interval
	clk.advance(time.Minute)
	m.OnPruneRequested(101) // interval 2m, ratio 50s/120s < 0.8 -> OK
	m.OnRunStarted(101)
	for h := uint32(102); h <= 105; h++ {
		clk.advance(time.Second) // tiny intervals push ratio way up too
		m.OnPruneRequested(h)
	}
	require.Equal(t, BacklogBehind, m.Snapshot().State)
}

func TestBacklogMonitor_SkipsSuspendLagCheck(t *testing.T) {
	m, clk := newTestMonitor(defaultTestConfig())
	m.OnPruneRequested(100)
	m.OnRunFinished(100, 1, time.Second, true)
	for h := uint32(101); h <= 120; h++ {
		clk.advance(time.Minute)
		m.OnPruneRequested(h)
		m.OnSkipped(h, "catchup_mode")
	}
	s := m.Snapshot()
	require.Equal(t, BacklogOK, s.State, "catchup skips must not produce BEHIND")
	require.True(t, s.Suspended)
	require.Equal(t, "catchup_mode", s.SuspendReason)
	require.Equal(t, uint32(20), s.LagBlocks, "lag is still reported while suspended")
	require.Equal(t, 0, s.IncompleteStreak)

	// Leaving the skip: the next real run lifts the suspension WITHOUT paging for
	// the lag that built up while skipping (nothing was the pruner's fault), and
	// re-seeds so a genuine stall afterwards is still detected.
	require.Equal(t, BacklogEventNone, m.OnRunStarted(120))
	s = m.Snapshot()
	require.False(t, s.Suspended)
	require.Equal(t, BacklogOK, s.State)
	require.Equal(t, uint32(119), s.LastCompletedHeight)

	for h := uint32(121); h <= 122; h++ {
		clk.advance(time.Minute)
		m.OnPruneRequested(h)
	}
	require.Equal(t, BacklogOK, m.Snapshot().State) // lag 3
	clk.advance(time.Minute)
	require.Equal(t, BacklogEventEntered, m.OnPruneRequested(123)) // lag 4 > 3
}

// A fresh chain with pruner_min_block_height=300 must not page at height 301
// just because the first 300 heights were (deliberately) never pruned.
func TestBacklogMonitor_MinHeightFreshChainDoesNotPage(t *testing.T) {
	m, clk := newTestMonitor(defaultTestConfig())
	for h := uint32(1); h <= 300; h++ {
		clk.advance(10 * time.Second)
		m.OnPruneRequested(h)
		m.OnSkipped(h, "below_min_height")
	}
	clk.advance(10 * time.Second)
	m.OnPruneRequested(301)
	require.Equal(t, BacklogEventNone, m.OnRunStarted(301))
	require.Equal(t, BacklogOK, m.Snapshot().State)
	require.Equal(t, BacklogEventNone, m.OnRunFinished(301, 0, time.Millisecond, true))
	require.Equal(t, BacklogOK, m.Snapshot().State)
}

// With the default pruner_skipDuringCatchup=false the pruner keeps running
// during catchup, so requests race ahead of completions and arrive at
// sub-second intervals. Neither may produce BEHIND or poison the headroom
// window; once RUNNING again, a real stall must still be detected.
func TestBacklogMonitor_CatchupSuspendsLagAndHeadroom(t *testing.T) {
	cfg := defaultTestConfig()
	cfg.HeadroomWindow = 2
	m, clk := newTestMonitor(cfg)
	m.OnPruneRequested(100)
	m.OnCatchupState(true)
	m.OnRunStarted(100)
	for h := uint32(101); h <= 500; h++ {
		clk.advance(100 * time.Millisecond)
		m.OnPruneRequested(h)
	}
	m.OnRunFinished(100, 1, time.Minute, true)
	s := m.Snapshot()
	require.Equal(t, BacklogOK, s.State)
	require.True(t, s.Suspended)
	require.Equal(t, "catchup_in_progress", s.SuspendReason)
	require.Equal(t, 0.0, s.HeadroomRatio, "catchup intervals must not enter the headroom window")

	require.Equal(t, BacklogEventNone, m.OnCatchupState(false))
	s = m.Snapshot()
	require.False(t, s.Suspended)
	require.Equal(t, BacklogOK, s.State)
	require.Equal(t, uint32(499), s.LastCompletedHeight)

	m.OnRunStarted(500)
	for h := uint32(501); h <= 503; h++ {
		clk.advance(10 * time.Minute)
		m.OnPruneRequested(h)
	}
	require.Equal(t, BacklogBehind, m.Snapshot().State) // 503-499 = 4 > 3
}

// Timeouts are failures, not deliberate skips: a pruner that can never run
// because block assembly is wedged must go BEHIND, not stay silently OK.
func TestBacklogMonitor_TimeoutSkipsCountAsIncomplete(t *testing.T) {
	for _, reason := range []string{"block_assembly_timeout", "mined_status_timeout", "fsm_error"} {
		t.Run(reason, func(t *testing.T) {
			m, clk := newTestMonitor(defaultTestConfig())
			m.OnPruneRequested(100)
			require.Equal(t, BacklogEventNone, m.OnSkipped(100, reason))
			s := m.Snapshot()
			require.False(t, s.Suspended)
			require.Equal(t, 1, s.IncompleteStreak)
			clk.advance(10 * time.Minute)
			m.OnPruneRequested(101)
			require.Equal(t, BacklogEventEntered, m.OnSkipped(101, reason))
			s = m.Snapshot()
			require.Equal(t, BacklogBehind, s.State)
			require.Contains(t, s.Message(), reason)

			m.OnRunStarted(102)
			require.Equal(t, BacklogEventRecovered, m.OnRunFinished(102, 1, time.Second, true))
			require.Equal(t, 0, m.Snapshot().IncompleteStreak)
		})
	}
}

func TestBacklogMonitor_DisabledChecks(t *testing.T) {
	cfg := defaultTestConfig()
	cfg.MaxLagBlocks = 0
	cfg.MaxIncompleteRuns = 0
	cfg.HeadroomWarnRatio = 0
	m, clk := newTestMonitor(cfg)
	m.OnPruneRequested(100)
	for h := uint32(101); h < 200; h++ {
		clk.advance(time.Second)
		m.OnPruneRequested(h)
		m.OnRunFinished(100, 0, time.Hour, false)
	}
	require.Equal(t, BacklogOK, m.Snapshot().State)
}

// Review Focus #1.
func TestBacklogMonitor_RestartSeedsThenDetects(t *testing.T) {
	m, clk := newTestMonitor(defaultTestConfig())
	require.Equal(t, BacklogEventNone, m.OnPruneRequested(5000))
	s := m.Snapshot()
	require.Equal(t, BacklogOK, s.State)
	require.Equal(t, uint32(4999), s.LastCompletedHeight)
	require.Equal(t, uint32(1), s.LagBlocks)

	m.OnRunStarted(5000) // stuck run
	for h := uint32(5001); h <= 5003; h++ {
		clk.advance(10 * time.Minute)
		m.OnPruneRequested(h)
	}
	require.Equal(t, BacklogBehind, m.Snapshot().State) // lag 4 > 3
}

// Review Focus #2 and #3.
func TestBacklogMonitor_LowerRequestDoesNotRewindOrUnderflow(t *testing.T) {
	m, clk := newTestMonitor(defaultTestConfig())
	m.OnPruneRequested(300)
	clk.advance(time.Second)
	m.OnPruneRequested(250) // stale initial-pruning signal / reorg
	require.Equal(t, uint32(300), m.Snapshot().RequestedHeight)

	m.OnRunFinished(400, 1, time.Second, true) // finished above requested
	s := m.Snapshot()
	require.Equal(t, uint32(400), s.LastCompletedHeight)
	require.Equal(t, uint32(0), s.LagBlocks, "lag must clamp at 0, not wrap")

	m.OnRunFinished(350, 1, time.Second, true) // lower completion must not rewind
	require.Equal(t, uint32(400), m.Snapshot().LastCompletedHeight)
}

// Review Focus #4.
func TestBacklogMonitor_ZeroIntervalsGiveZeroRatio(t *testing.T) {
	cfg := defaultTestConfig()
	cfg.HeadroomWindow = 2
	m, _ := newTestMonitor(cfg) // clock never advances: every interval is 0
	for h := uint32(1); h <= 4; h++ {
		m.OnPruneRequested(h)
		m.OnRunFinished(h, 1, time.Minute, true)
	}
	r := m.Snapshot().HeadroomRatio
	require.False(t, math.IsNaN(r) || math.IsInf(r, 0))
	require.Equal(t, 0.0, r)
	require.Equal(t, BacklogOK, m.Snapshot().State)
}

// Review Focus #5.
func TestBacklogMonitor_NilAndDisabledAreNoOps(t *testing.T) {
	var nilMon *BacklogMonitor
	require.NotPanics(t, func() {
		nilMon.OnPruneRequested(1)
		nilMon.OnSkipped(1, "x")
		nilMon.OnRunStarted(1)
		nilMon.OnRunFinished(1, 1, time.Second, false)
	})
	require.Equal(t, BacklogOK, nilMon.Snapshot().State)

	cfg := defaultTestConfig()
	cfg.Enabled = false
	m, _ := newTestMonitor(cfg)
	for i := 0; i < 10; i++ {
		require.Equal(t, BacklogEventNone, m.OnRunFinished(1, 0, time.Hour, false))
	}
	require.Equal(t, BacklogOK, m.Snapshot().State)
	require.Equal(t, 0, m.Snapshot().IncompleteStreak)
}

func TestBacklogSnapshot_MessageHasNoDoubleQuotes(t *testing.T) {
	s := BacklogSnapshot{
		State: BacklogBehind, RequestedHeight: 456, LastCompletedHeight: 442, LagBlocks: 14,
		IncompleteStreak: 3, HeadroomRatio: 1.4, LastRunRecords: 2_000_000_000, LastRunDuration: 30 * time.Minute,
	}
	msg := s.Message()
	require.NotContains(t, msg, `"`)
	require.Contains(t, msg, "pruner BEHIND")
	require.Contains(t, msg, "14 blocks behind (requested 456, last completed 442)")
	require.Contains(t, msg, "3 consecutive incomplete runs")
	require.InDelta(t, 2_000_000_000.0/1800.0, s.RecordsPerSecond(), 1e-6)
	require.Equal(t, 0.0, BacklogSnapshot{}.RecordsPerSecond())
}

// recordingLogger captures Infof/Warnf/Errorf lines by level.
type recordingLogger struct {
	ulogger.TestLogger
	mu    sync.Mutex
	lines map[string][]string
}

func newRecordingLogger() *recordingLogger {
	return &recordingLogger{lines: map[string][]string{}}
}

func (l *recordingLogger) add(level, format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines[level] = append(l.lines[level], fmt.Sprintf(format, args...))
}

func (l *recordingLogger) Infof(format string, args ...interface{})  { l.add("INFO", format, args...) }
func (l *recordingLogger) Warnf(format string, args ...interface{})  { l.add("WARN", format, args...) }
func (l *recordingLogger) Errorf(format string, args ...interface{}) { l.add("ERROR", format, args...) }

func (l *recordingLogger) count(level string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.lines[level])
}

func TestBacklogMonitor_LogsBehindAtErrorWithRepeatCadence(t *testing.T) {
	log := newRecordingLogger()
	clk := &fakeClock{t: time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)}
	m := NewBacklogMonitor(defaultTestConfig(), log, clk.now) // repeat 5m

	m.OnPruneRequested(100)
	m.OnRunStarted(100)
	for h := uint32(101); h <= 104; h++ {
		clk.advance(time.Minute)
		m.OnPruneRequested(h)
	}
	require.Equal(t, 1, log.count("ERROR"), "rising edge logs once")
	require.Contains(t, log.lines["ERROR"][0], "pruner BEHIND")

	clk.advance(time.Minute) // 1m after the edge: no repeat yet
	m.OnPruneRequested(105)
	require.Equal(t, 1, log.count("ERROR"))

	clk.advance(5 * time.Minute) // past the 5m cadence
	require.Equal(t, BacklogEventContinues, m.OnPruneRequested(106))
	require.Equal(t, 2, log.count("ERROR"))

	m.OnRunFinished(106, 1, time.Second, true)
	require.Equal(t, 1, log.count("INFO"), "recovery logs at INFO")
	require.Contains(t, log.lines["INFO"][0], "recovered")
}

func TestBacklogMonitor_AtRiskLogsAtWarn(t *testing.T) {
	log := newRecordingLogger()
	cfg := defaultTestConfig()
	cfg.HeadroomWindow = 1
	clk := &fakeClock{t: time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)}
	m := NewBacklogMonitor(cfg, log, clk.now)

	m.OnPruneRequested(100)
	clk.advance(10 * time.Minute)
	m.OnRunFinished(100, 1, 9*time.Minute, true)
	m.OnPruneRequested(101) // interval 10m, duration 9m -> 0.9
	require.Equal(t, BacklogAtRisk, m.Snapshot().State)
	require.Equal(t, 1, log.count("WARN"))
	require.Equal(t, 0, log.count("ERROR"))
}

func TestBacklogMonitor_PublishesMetrics(t *testing.T) {
	initPrometheusMetrics()
	m, clk := newTestMonitor(defaultTestConfig())

	m.OnPruneRequested(100)
	m.OnRunStarted(100)
	for h := uint32(101); h <= 104; h++ {
		clk.advance(time.Minute)
		m.OnPruneRequested(h)
	}
	require.Equal(t, 2.0, testutil.ToFloat64(prunerBacklogState))
	require.Equal(t, 5.0, testutil.ToFloat64(prunerLagBlocks))
	require.Equal(t, 99.0, testutil.ToFloat64(prunerLastCompletedHeight))

	before := testutil.ToFloat64(prunerIncompleteRunsTotal)
	m.OnRunFinished(104, 0, time.Second, false)
	require.Equal(t, before+1, testutil.ToFloat64(prunerIncompleteRunsTotal))

	m.OnRunFinished(104, 3600, time.Hour, true)
	require.Equal(t, 0.0, testutil.ToFloat64(prunerBacklogState))
	require.Equal(t, 104.0, testutil.ToFloat64(prunerLastCompletedHeight))
	require.InDelta(t, 1.0, testutil.ToFloat64(prunerLastRunRecordsPerSecond), 1e-9)
}
