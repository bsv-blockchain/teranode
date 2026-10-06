package pruner

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// BacklogState is the pruner's ability to keep up with records due for deletion.
// It deliberately ignores total UTXO-set size, which grows legitimately on
// mainnet; only records whose deleteAtHeight has passed are the pruner's job.
type BacklogState int

const (
	// BacklogOK means the pruner keeps up with due records.
	BacklogOK BacklogState = iota
	// BacklogAtRisk means runs consume most of the block interval: no headroom.
	BacklogAtRisk
	// BacklogBehind means due records are accumulating faster than they are pruned.
	BacklogBehind
)

func (s BacklogState) String() string {
	switch s {
	case BacklogAtRisk:
		return "AT_RISK"
	case BacklogBehind:
		return "BEHIND"
	default:
		return "OK"
	}
}

// BacklogEvent tells the caller (and tests) what an observation changed.
type BacklogEvent int

const (
	// BacklogEventNone means nothing to report.
	BacklogEventNone BacklogEvent = iota
	// BacklogEventEntered means the state changed to AT_RISK or BEHIND.
	BacklogEventEntered
	// BacklogEventContinues is the rate-limited repeat while not OK.
	BacklogEventContinues
	// BacklogEventRecovered means the state returned to OK.
	BacklogEventRecovered
)

// BacklogConfig holds the monitor thresholds. A zero threshold disables its check.
type BacklogConfig struct {
	Enabled           bool
	MaxLagBlocks      uint32
	MaxIncompleteRuns int
	HeadroomWarnRatio float64
	HeadroomWindow    int
	FailReadiness     bool
	LogRepeatInterval time.Duration
}

// BacklogConfigFromSettings maps the pruner_backlog* settings onto a BacklogConfig.
func BacklogConfigFromSettings(p settings.PrunerSettings) BacklogConfig {
	return BacklogConfig{
		Enabled:           p.BacklogMonitorEnabled,
		MaxLagBlocks:      p.BacklogMaxLagBlocks,
		MaxIncompleteRuns: p.BacklogMaxIncompleteRuns,
		HeadroomWarnRatio: p.BacklogHeadroomWarnRatio,
		HeadroomWindow:    p.BacklogHeadroomWindow,
		FailReadiness:     p.BacklogFailReadiness,
		LogRepeatInterval: p.BacklogLogRepeatInterval,
	}
}

// BacklogSnapshot is a point-in-time copy of the monitor's view.
type BacklogSnapshot struct {
	State               BacklogState
	RequestedHeight     uint32
	LastCompletedHeight uint32
	LagBlocks           uint32
	IncompleteStreak    int
	HeadroomRatio       float64
	LastRunRecords      int64
	LastRunDuration     time.Duration
	Suspended           bool
	SuspendReason       string
	IncompleteReason    string
}

// RecordsPerSecond is the throughput of the last finished run, 0 if unknown.
func (s BacklogSnapshot) RecordsPerSecond() float64 {
	if s.LastRunDuration <= 0 {
		return 0
	}

	return float64(s.LastRunRecords) / s.LastRunDuration.Seconds()
}

// Message renders the snapshot for logs and the health check. It must never
// contain a double quote: health.CheckAll embeds it in JSON unescaped.
func (s BacklogSnapshot) Message() string {
	msg := fmt.Sprintf("pruner %s: %d blocks behind (requested %d, last completed %d), %d consecutive incomplete runs, last run %d records in %s (%.0f rec/s), headroom %.2fx block interval",
		s.State, s.LagBlocks, s.RequestedHeight, s.LastCompletedHeight, s.IncompleteStreak,
		s.LastRunRecords, s.LastRunDuration.Round(time.Second), s.RecordsPerSecond(), s.HeadroomRatio)

	if s.IncompleteStreak > 0 && s.IncompleteReason != "" {
		msg += fmt.Sprintf(", last incomplete cause %s", s.IncompleteReason)
	}

	if s.Suspended {
		msg += fmt.Sprintf(", lag check suspended (%s)", s.SuspendReason)
	}

	return msg
}

// BacklogMonitor detects the pruner falling behind records due for deletion.
// It is store-agnostic: it only consumes events the pruner server already sees.
// All methods are safe on a nil receiver and are no-ops when disabled.
type BacklogMonitor struct {
	mu     sync.Mutex
	cfg    BacklogConfig
	logger ulogger.Logger
	now    func() time.Time

	seeded        bool
	requested     uint32
	lastCompleted uint32
	lastRequestAt time.Time
	intervals     []time.Duration
	durations     []time.Duration
	streak        int
	suspended     bool
	suspendReason string
	// catchingUp is set while the FSM is not RUNNING. With the default
	// pruner_skipDuringCatchup=false the pruner keeps running during catchup,
	// so this suspends the lag check independently of skip-suspension.
	catchingUp bool
	// lastIncompleteReason names what ended the current incomplete streak.
	lastIncompleteReason string
	lastRecords          int64
	lastDuration         time.Duration

	state     BacklogState
	lastLogAt time.Time
}

// NewBacklogMonitor creates a monitor. now may be nil (time.Now).
func NewBacklogMonitor(cfg BacklogConfig, logger ulogger.Logger, now func() time.Time) *BacklogMonitor {
	if now == nil {
		now = time.Now
	}

	return &BacklogMonitor{cfg: cfg, logger: logger, now: now}
}

func (m *BacklogMonitor) active() bool {
	return m != nil && m.cfg.Enabled
}

// OnPruneRequested records that a block notification asked the pruner to prune
// up to height. Called when the signal is enqueued, not when the worker
// dequeues it, so lag keeps growing while a long run is in progress.
func (m *BacklogMonitor) OnPruneRequested(height uint32) BacklogEvent {
	if !m.active() {
		return BacklogEventNone
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.now()

	if !m.seeded {
		// A fresh process cannot know what was completed before it started.
		// Seeding to height-1 starts it OK; a real stall still shows up as
		// further requests arrive without a completion.
		m.seeded = true
		if height > 0 {
			m.lastCompleted = height - 1
		}
	} else if !m.lagSuspendedLocked() {
		// Catchup and deliberate skips produce request bursts that say nothing
		// about the steady-state block interval; keep them out of the window.
		m.intervals = pushWindow(m.intervals, now.Sub(m.lastRequestAt), m.window())
	}

	m.lastRequestAt = now

	if height > m.requested {
		m.requested = height
	}

	return m.evaluateLocked()
}

// OnSkipped records that the worker did not prune height. Deliberate skips
// (below_min_height, catchup_mode) suspend the lag check until the next
// OnRunStarted. Any other reason (block_assembly_timeout, mined_status_timeout,
// fsm_error) is a failure: the pruner wanted to run and could not, so it counts
// toward the incomplete-run streak instead of hiding a pruner that never runs.
func (m *BacklogMonitor) OnSkipped(_ uint32, reason string) BacklogEvent {
	if !m.active() {
		return BacklogEventNone
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if isDeliberateSkip(reason) {
		m.suspended = true
		m.suspendReason = reason
	} else {
		m.streak++
		m.lastIncompleteReason = reason
		m.onIncompleteRunLocked()
	}

	return m.evaluateLocked()
}

// OnCatchupState records whether the node is catching up (FSM not RUNNING).
// While catching up the lag check is suspended; on leaving catchup the
// completed height is re-seeded so the catchup backlog does not page.
func (m *BacklogMonitor) OnCatchupState(catchingUp bool) BacklogEvent {
	if !m.active() {
		return BacklogEventNone
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if catchingUp {
		m.catchingUp = true
	} else if m.catchingUp {
		m.catchingUp = false
		m.reseedLocked(m.requested)
	}

	return m.evaluateLocked()
}

func isDeliberateSkip(reason string) bool {
	return reason == "below_min_height" || reason == "catchup_mode"
}

// reseedLocked moves lastCompleted up to height-1 so that lag accumulated while
// the check was suspended (through no fault of the pruner) is not reported.
func (m *BacklogMonitor) reseedLocked(height uint32) {
	if height > 0 && height-1 > m.lastCompleted {
		m.lastCompleted = height - 1
	}
}

// OnRunStarted records that the worker passed every skip check for height and
// is about to run the prune phases. It lifts any skip suspension.
func (m *BacklogMonitor) OnRunStarted(height uint32) BacklogEvent {
	if !m.active() {
		return BacklogEventNone
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.suspended {
		m.reseedLocked(height)
	}

	m.suspended = false
	m.suspendReason = ""

	return m.evaluateLocked()
}

// OnRunFinished records the outcome of phase 2 for height. ok=false means the
// run returned an error (including exhausted partition retries).
func (m *BacklogMonitor) OnRunFinished(height uint32, records int64, duration time.Duration, ok bool) BacklogEvent {
	if !m.active() {
		return BacklogEventNone
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.durations = pushWindow(m.durations, duration, m.window())

	if ok {
		m.streak = 0
		m.lastIncompleteReason = ""
		m.lastRecords = records
		m.lastDuration = duration
		m.seeded = true

		if height > m.lastCompleted {
			m.lastCompleted = height
		}
	} else {
		m.streak++
		m.lastIncompleteReason = "phase2_error"
		m.onIncompleteRunLocked()
	}

	return m.evaluateLocked()
}

// Snapshot returns the current view. Safe on a nil or disabled monitor.
func (m *BacklogMonitor) Snapshot() BacklogSnapshot {
	if !m.active() {
		return BacklogSnapshot{State: BacklogOK}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	return m.snapshotLocked()
}

// HealthCheck reports the backlog state for the pruner readiness probe. Only
// readiness can fail, only while BEHIND, and only if FailReadiness is set:
// being behind must never get the pod killed by a liveness probe.
func (m *BacklogMonitor) HealthCheck(_ context.Context, checkLiveness bool) (int, string, error) {
	if !m.active() {
		return http.StatusOK, "pruner backlog monitor disabled", nil
	}

	s := m.Snapshot()
	msg := s.Message()

	if !checkLiveness && s.State == BacklogBehind && m.cfg.FailReadiness {
		return http.StatusServiceUnavailable, msg, errors.NewServiceUnavailableError(msg)
	}

	return http.StatusOK, msg, nil
}

func (m *BacklogMonitor) window() int {
	if m.cfg.HeadroomWindow < 1 {
		return 1
	}

	return m.cfg.HeadroomWindow
}

func (m *BacklogMonitor) lagSuspendedLocked() bool {
	return m.suspended || m.catchingUp
}

func (m *BacklogMonitor) lagLocked() uint32 {
	if m.requested > m.lastCompleted {
		return m.requested - m.lastCompleted
	}

	return 0
}

func (m *BacklogMonitor) headroomLocked() float64 {
	if len(m.durations) == 0 || len(m.intervals) == 0 {
		return 0
	}

	meanInterval := meanDuration(m.intervals)
	if meanInterval <= 0 {
		return 0
	}

	return meanDuration(m.durations).Seconds() / meanInterval.Seconds()
}

func (m *BacklogMonitor) computeStateLocked() BacklogState {
	if !m.lagSuspendedLocked() && m.cfg.MaxLagBlocks > 0 && m.lagLocked() > m.cfg.MaxLagBlocks {
		return BacklogBehind
	}

	if m.cfg.MaxIncompleteRuns > 0 && m.streak >= m.cfg.MaxIncompleteRuns {
		return BacklogBehind
	}

	w := m.window()
	if m.cfg.HeadroomWarnRatio > 0 && len(m.durations) >= w && len(m.intervals) >= w &&
		m.headroomLocked() >= m.cfg.HeadroomWarnRatio {
		return BacklogAtRisk
	}

	return BacklogOK
}

func (m *BacklogMonitor) snapshotLocked() BacklogSnapshot {
	reason := m.suspendReason
	if reason == "" && m.catchingUp {
		reason = "catchup_in_progress"
	}

	return BacklogSnapshot{
		State:               m.state,
		RequestedHeight:     m.requested,
		LastCompletedHeight: m.lastCompleted,
		LagBlocks:           m.lagLocked(),
		IncompleteStreak:    m.streak,
		HeadroomRatio:       m.headroomLocked(),
		LastRunRecords:      m.lastRecords,
		LastRunDuration:     m.lastDuration,
		Suspended:           m.lagSuspendedLocked(),
		SuspendReason:       reason,
		IncompleteReason:    m.lastIncompleteReason,
	}
}

// evaluateLocked recomputes the state and decides what to report.
func (m *BacklogMonitor) evaluateLocked() BacklogEvent {
	prev := m.state
	next := m.computeStateLocked()
	m.state = next
	now := m.now()

	event := BacklogEventNone

	switch {
	case next != prev && next == BacklogOK:
		event = BacklogEventRecovered
	case next != prev:
		event = BacklogEventEntered
	case next != BacklogOK && m.cfg.LogRepeatInterval > 0 && now.Sub(m.lastLogAt) >= m.cfg.LogRepeatInterval:
		event = BacklogEventContinues
	}

	if event != BacklogEventNone {
		m.lastLogAt = now
		m.logEventLocked(event)
	}

	m.publishMetricsLocked()

	return event
}

// logEventLocked writes the event at a severity matching the state: BEHIND is
// ERROR, AT_RISK is WARN, recovery is INFO.
func (m *BacklogMonitor) logEventLocked(event BacklogEvent) {
	if m.logger == nil {
		return
	}

	msg := m.snapshotLocked().Message()

	switch {
	case event == BacklogEventRecovered:
		m.logger.Infof("[pruner] backlog recovered: %s", msg)
	case m.state == BacklogBehind:
		m.logger.Errorf("[pruner] %s", msg)
	case m.state == BacklogAtRisk:
		m.logger.Warnf("[pruner] %s", msg)
	}
}

// publishMetricsLocked mirrors the snapshot into Prometheus. Metrics are
// registered in Server.Init; before that (or in tests that never call
// initPrometheusMetrics) they are nil and this is a no-op.
func (m *BacklogMonitor) publishMetricsLocked() {
	if prunerBacklogState == nil {
		return
	}

	s := m.snapshotLocked()
	prunerBacklogState.Set(float64(s.State))
	prunerLagBlocks.Set(float64(s.LagBlocks))
	prunerLastCompletedHeight.Set(float64(s.LastCompletedHeight))
	prunerHeadroomRatio.Set(s.HeadroomRatio)
	prunerLastRunRecordsPerSecond.Set(s.RecordsPerSecond())
}

// onIncompleteRunLocked counts a failed phase-2 run.
func (m *BacklogMonitor) onIncompleteRunLocked() {
	if prunerIncompleteRunsTotal != nil {
		prunerIncompleteRunsTotal.Inc()
	}
}

func pushWindow(w []time.Duration, d time.Duration, size int) []time.Duration {
	w = append(w, d)
	if len(w) > size {
		w = w[len(w)-size:]
	}

	return w
}

func meanDuration(w []time.Duration) time.Duration {
	if len(w) == 0 {
		return 0
	}

	var sum time.Duration
	for _, d := range w {
		sum += d
	}

	return sum / time.Duration(len(w))
}
