package validator

import (
	"context"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	inmemorykafka "github.com/bsv-blockchain/teranode/util/kafka/in_memory_kafka"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// fakeConsumer records PauseAll/ResumeAll calls.
type fakeConsumer struct {
	mu      sync.Mutex
	pauses  int
	resumes int
}

func (f *fakeConsumer) PauseAll() {
	f.mu.Lock()
	f.pauses++
	f.mu.Unlock()
}

func (f *fakeConsumer) ResumeAll() {
	f.mu.Lock()
	f.resumes++
	f.mu.Unlock()
}

func (f *fakeConsumer) pauseCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.pauses
}

func (f *fakeConsumer) resumeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.resumes
}

// fakeReader is a controllable queue-stats source. windowMillis is the
// double-spend window the "producer" reports, which is the value the controller
// must base its control decision on.
type fakeReader struct {
	mu           sync.Mutex
	ageMillis    int64
	windowMillis int64
	err          error
}

func (f *fakeReader) set(ageMillis int64, err error) {
	f.mu.Lock()
	f.ageMillis = ageMillis
	f.err = err
	f.mu.Unlock()
}

// setWithWindow sets the reported head age and the reported drain floor together.
func (f *fakeReader) setWithWindow(ageMillis, windowMillis int64, err error) {
	f.mu.Lock()
	f.ageMillis = ageMillis
	f.windowMillis = windowMillis
	f.err = err
	f.mu.Unlock()
}

func (f *fakeReader) GetBlockAssemblyQueueStats(_ context.Context) (blockassembly.QueueStats, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.err != nil {
		return blockassembly.QueueStats{}, f.err
	}

	return blockassembly.QueueStats{
		HeadAge:           time.Duration(f.ageMillis) * time.Millisecond,
		DoubleSpendWindow: time.Duration(f.windowMillis) * time.Millisecond,
	}, nil
}

func testBackpressureConfig() settings.ValidatorKafkaBackpressureSettings {
	return settings.ValidatorKafkaBackpressureSettings{
		Enabled:             true,
		PauseQueueAge:       500 * time.Millisecond,
		ResumeQueueAge:      100 * time.Millisecond,
		PollInterval:        5 * time.Millisecond,
		ReadTimeout:         100 * time.Millisecond,
		MaxPause:            30 * time.Second,
		MaxFailOpenCooldown: time.Second,
		StaleErrorLimit:     3,
	}
}

func newTestController(reader queueStatsReader, consumer pausableConsumer) *kafkaBackpressureController {
	initPrometheusMetrics()
	return newKafkaBackpressureController(ulogger.TestLogger{}, testBackpressureConfig(), 0, reader, consumer)
}

// TestBackpressure_HysteresisPauseThenResume covers the core watermark logic
// deterministically: exactly one pause when the age crosses the high watermark,
// no flapping while the age sits between the watermarks, and exactly one resume
// once it falls to the low watermark. The real-consumer in-flight semantics are
// covered separately by TestBackpressure_InMemoryKafkaInFlightAndResumeByAge.
func TestBackpressure_HysteresisPauseThenResume(t *testing.T) {
	consumer := &fakeConsumer{}
	c := newTestController(&fakeReader{}, consumer)

	// Below the high watermark: nothing happens.
	c.evaluate(400 * time.Millisecond)
	require.Equal(t, 0, consumer.pauseCount())

	// At/above the high watermark: exactly one pause.
	c.evaluate(600 * time.Millisecond)
	require.Equal(t, 1, consumer.pauseCount())
	require.True(t, c.paused.Load())

	// Between the watermarks (still above resume): no flapping, still paused.
	for _, age := range []time.Duration{590, 400, 200, 150} {
		c.evaluate(age * time.Millisecond)
	}
	require.Equal(t, 1, consumer.pauseCount())
	require.Equal(t, 0, consumer.resumeCount())
	require.True(t, c.paused.Load())

	// Drop to the low watermark: exactly one resume.
	c.evaluate(100 * time.Millisecond)
	require.Equal(t, 1, consumer.resumeCount())
	require.False(t, c.paused.Load())
}

// TestBackpressure_MaxPauseForcesResume verifies a pause held past MaxPause is
// force-resumed even while the queue is still hot.
func TestBackpressure_MaxPauseForcesResume(t *testing.T) {
	consumer := &fakeConsumer{}
	c := newTestController(&fakeReader{}, consumer)

	base := time.Unix(1000, 0)
	now := base
	c.now = func() time.Time { return now }

	c.evaluate(600 * time.Millisecond)
	require.True(t, c.paused.Load())

	// Still hot, but not yet past the cap: no resume.
	now = base.Add(29 * time.Second)
	c.evaluate(600 * time.Millisecond)
	require.Equal(t, 0, consumer.resumeCount())

	// Past the cap: fail-open resume even though still hot.
	now = base.Add(31 * time.Second)
	c.evaluate(600 * time.Millisecond)
	require.Equal(t, 1, consumer.resumeCount())
	require.False(t, c.paused.Load())
}

// TestBackpressure_DoubleSpendWindowExcludedFromControl verifies the control
// decision is based on the head age PAST the double-spend drain floor: a healthy
// hold-back (head aged only up to the floor plus a small epsilon) must not pause,
// while a genuine stall past the floor must.
//
// It is driven through tick() with the window carried by the READ, because that —
// not the reader's own setting — is now the control input.
func TestBackpressure_DoubleSpendWindowExcludedFromControl(t *testing.T) {
	ctx := context.Background()
	reader := &fakeReader{}
	consumer := &fakeConsumer{}
	c := newTestController(reader, consumer)

	// Healthy hold-back: raw head age = window + 50ms → effectiveAge 50ms, below
	// the 500ms pause watermark. Must not pause.
	reader.setWithWindow(1050, 1000, nil)
	c.tick(ctx)
	require.Equal(t, 0, consumer.pauseCount(), "a healthy hold-back at the drain floor must not pause")
	require.False(t, c.paused.Load())

	// Genuine stall: raw head age = window + 600ms → effectiveAge 600ms, above
	// the pause watermark. Must pause.
	reader.setWithWindow(1600, 1000, nil)
	c.tick(ctx)
	require.Equal(t, 1, consumer.pauseCount(), "a stall past the drain floor must pause")
	require.True(t, c.paused.Load())
}

// TestBackpressure_UsesReportedDoubleSpendWindow pins that the REPORTED window is
// what the control decision subtracts, not the reader's own setting.
//
// The two live in independent per-process settings contexts, and a mismatch used to
// break the feature silently in both directions: block assembly at 10s with the
// validator at 0 left the effective age permanently above the pause watermark, so
// the controller paused on the first tick and stayed in the pause/max-pause cycle
// forever; the reverse clamped the effective age to 0 and made the feature inert.
func TestBackpressure_UsesReportedDoubleSpendWindow(t *testing.T) {
	ctx := context.Background()
	reader := &fakeReader{}
	consumer := &fakeConsumer{}
	c := newTestController(reader, consumer)

	// The reader's own idea of the window is zero (the default), while the producer
	// reports 500ms. Head age 600ms → effective 100ms, which is below the 500ms
	// pause watermark: no pause. Keying on the local value would have paused here.
	require.Equal(t, time.Duration(0), c.localDoubleSpendWindow, "precondition: the local setting disagrees")

	reader.setWithWindow(600, 500, nil)
	c.tick(ctx)

	require.Equal(t, 0, consumer.pauseCount(), "the reported window must be subtracted, not the local one")
	require.False(t, c.paused.Load())

	// A stall past the REPORTED floor still pauses, so the feature is not merely
	// disabled by the subtraction.
	reader.setWithWindow(1100, 500, nil)
	c.tick(ctx)
	require.Equal(t, 1, consumer.pauseCount())
}

// TestBackpressure_LogsWindowMismatchOnce pins that a persistent mismatch is
// operator-visible without spamming at the 50ms poll cadence: one line the first
// time, and one more only when the REPORTED value changes.
func TestBackpressure_LogsWindowMismatchOnce(t *testing.T) {
	consumer := &fakeConsumer{}
	c := newTestController(&fakeReader{}, consumer)
	c.localDoubleSpendWindow = 10 * time.Second

	// First disagreement: logged.
	require.Equal(t, 500*time.Millisecond, c.observeReportedWindow(500*time.Millisecond))
	require.True(t, c.windowMismatchLogged, "the first mismatch must be logged")

	// Same reported value over many ticks: not logged again.
	for i := 0; i < 100; i++ {
		c.observeReportedWindow(500 * time.Millisecond)
		require.True(t, c.windowMismatchLogged, "a persistent mismatch must not re-log every tick")
	}

	// The reported value changes: the latch resets so the new disagreement is
	// logged once.
	c.observeReportedWindow(2 * time.Second)
	require.True(t, c.windowMismatchLogged)
	require.Equal(t, 2*time.Second, c.lastReportedWindow)

	// Agreement clears nothing but must not be reported as a mismatch.
	c.observeReportedWindow(10 * time.Second)
	require.False(t, c.windowMismatchLogged, "matching values must not be reported as a mismatch")
}

// TestBackpressure_NegativeReportedWindowClampedToZero pins the trust boundary: a
// negative reported window would be ADDED to the effective age by the subtraction
// in evaluate and could invert the control decision, so a bad peer must not be able
// to steer it.
func TestBackpressure_NegativeReportedWindowClampedToZero(t *testing.T) {
	ctx := context.Background()
	reader := &fakeReader{}
	consumer := &fakeConsumer{}
	c := newTestController(reader, consumer)

	require.Equal(t, time.Duration(0), c.observeReportedWindow(-5*time.Second))

	// Head age below the watermark with a negative reported window: the clamp means
	// the effective age stays 400ms and no pause happens. Without it, 400ms minus a
	// negative 5s would read as 5.4s and pause.
	reader.setWithWindow(400, -5000, nil)
	c.tick(ctx)

	require.Equal(t, 0, consumer.pauseCount(), "a negative reported window must not be able to force a pause")
	require.Equal(t, time.Duration(0), c.reportedWindow)
}

// TestBackpressure_MaxPauseCooldownBoundsDutyCycle verifies that after a
// max-pause fail-open resume the controller does not immediately re-pause while
// the signal stays hot — and that the suppression is PROPORTIONAL and CAPPED rather
// than lasting a full MaxPause.
//
// The old behaviour suppressed backpressure for a whole MaxPause (30s by default).
// During a sustained stall that is 30s of hard shedding with a positive queue cap,
// or 30s of unbounded growth toward the very OOM the feature exists to prevent with
// the default unbounded queue. A 30s pause now yields
// min(30s/4, MaxFailOpenCooldown) = min(7.5s, 1s) = 1s of suppression.
func TestBackpressure_MaxPauseCooldownBoundsDutyCycle(t *testing.T) {
	consumer := &fakeConsumer{}
	c := newTestController(&fakeReader{}, consumer)

	base := time.Unix(2000, 0)
	now := base
	c.now = func() time.Time { return now }

	// Hot → pause.
	c.evaluate(600 * time.Millisecond)
	require.True(t, c.paused.Load())
	require.Equal(t, 1, consumer.pauseCount())

	// Held hot past MaxPause → fail-open resume, proportional cooldown armed.
	now = base.Add(31 * time.Second)
	c.evaluate(600 * time.Millisecond)
	require.False(t, c.paused.Load())
	require.Equal(t, 1, consumer.resumeCount())
	require.Equal(t, time.Second, c.failOpenCooldown,
		"31s/4 = 7.75s, capped by MaxFailOpenCooldown to 1s - not a full MaxPause")

	// Still hot but inside the cooldown → must NOT re-pause.
	now = base.Add(31*time.Second + 900*time.Millisecond)
	c.evaluate(600 * time.Millisecond)
	require.False(t, c.paused.Load())
	require.Equal(t, 1, consumer.pauseCount(), "must not re-pause during the cooldown")

	// First tick at or after the cooldown → re-pause allowed. Under the old
	// MaxPause-long latch the queue would have been unprotected for another 29s.
	now = base.Add(32 * time.Second)
	c.evaluate(600 * time.Millisecond)
	require.True(t, c.paused.Load())
	require.Equal(t, 2, consumer.pauseCount(), "re-pause allowed once the proportional cooldown elapses")
}

// TestBackpressure_StaleSignalResumeArmsCooldown is the A7 regression guard.
//
// The stale-signal fail-open resume used to arm no cooldown at all, so a flapping
// stats endpoint cycled StaleErrorLimit error ticks paused, one tick resumed, then
// re-paused on the very next hot read — a ~75% paused duty cycle with the default
// limit of 3, which is precisely the wedge the controller's comment claimed could
// not happen. That resume is a fail-open, not an observed drain, so it must arm the
// same cooldown the max-pause resume does.
func TestBackpressure_StaleSignalResumeArmsCooldown(t *testing.T) {
	ctx := context.Background()
	reader := &fakeReader{}
	consumer := &fakeConsumer{}
	c := newTestController(reader, consumer)

	base := time.Unix(4000, 0)
	now := base
	c.now = func() time.Time { return now }

	// A hot read pauses the consumer.
	reader.set(600, nil)
	c.tick(ctx)
	require.True(t, c.paused.Load())
	require.Equal(t, 1, consumer.pauseCount())

	// StaleErrorLimit consecutive read errors → fail-open resume.
	readErr := errors.NewProcessingError("queue stats unavailable")
	reader.set(0, readErr)

	for i := 0; i < c.cfg.StaleErrorLimit; i++ {
		now = now.Add(c.cfg.PollInterval)
		c.tick(ctx)
	}

	require.False(t, c.paused.Load(), "the stale signal fails open")
	require.Equal(t, 1, consumer.resumeCount())
	require.NotZero(t, c.failOpenCooldown, "a stale-signal resume must arm a cooldown, not resume bare")

	// A hot read on the very next tick must NOT re-pause. This is the assertion that
	// fails without the fix.
	now = now.Add(c.cfg.PollInterval)
	reader.set(600, nil)
	c.tick(ctx)

	require.False(t, c.paused.Load(), "a hot read on the tick after a fail-open resume must not re-pause")
	require.Equal(t, 1, consumer.pauseCount())

	// Past the cooldown, protection returns.
	now = now.Add(c.failOpenCooldown)
	c.tick(ctx)

	require.True(t, c.paused.Load(), "protection must return once the cooldown elapses")
	require.Equal(t, 2, consumer.pauseCount())
}

// TestBackpressure_CooldownFlooredAtTwicePollInterval pins the other end of the
// clamp: a very short pause still arms at least two poll intervals of cooldown, so
// a fast-flapping signal cannot busy-toggle pause/resume on consecutive ticks.
func TestBackpressure_CooldownFlooredAtTwicePollInterval(t *testing.T) {
	consumer := &fakeConsumer{}
	c := newTestController(&fakeReader{}, consumer)

	base := time.Unix(5000, 0)
	now := base
	c.now = func() time.Time { return now }

	c.pauseStart = base
	c.paused.Store(true)

	// A 4ms pause would yield 1ms of cooldown on the raw formula — less than a
	// single poll interval, i.e. no suppression at all in practice.
	now = base.Add(4 * time.Millisecond)
	c.armFailOpenCooldown()

	require.Equal(t, 2*c.cfg.PollInterval, c.failOpenCooldown,
		"a tiny pause must still arm the anti-busy-toggle floor")
}

// TestBackpressure_MaxPauseCooldownClearedByDrain verifies a genuine drain to
// the resume watermark clears the cooldown latch immediately, re-arming normal
// pause behaviour without waiting out the full cooldown.
func TestBackpressure_MaxPauseCooldownClearedByDrain(t *testing.T) {
	consumer := &fakeConsumer{}
	c := newTestController(&fakeReader{}, consumer)

	base := time.Unix(3000, 0)
	now := base
	c.now = func() time.Time { return now }

	c.evaluate(600 * time.Millisecond)
	require.True(t, c.paused.Load())

	now = base.Add(31 * time.Second)
	c.evaluate(600 * time.Millisecond)
	require.False(t, c.paused.Load())
	require.NotZero(t, c.failOpenCooldown, "precondition: a cooldown is armed")

	// A genuine drain to the low watermark clears the latch. The timestamp is kept
	// strictly INSIDE the armed cooldown, so this proves the drain cleared it rather
	// than the cooldown having simply elapsed.
	now = base.Add(31*time.Second + 100*time.Millisecond)
	c.evaluate(50 * time.Millisecond)
	require.False(t, c.paused.Load())
	require.Zero(t, c.failOpenCooldown, "a genuine drain clears the cooldown early")

	// Immediately hot again → pause allowed (latch cleared by the drain).
	c.evaluate(600 * time.Millisecond)
	require.True(t, c.paused.Load(), "a drain clears the cooldown so a later spike can pause again")
}

// TestBackpressure_FailOpenOnStaleSignal verifies that after StaleErrorLimit
// consecutive read errors while paused the controller resumes, and that a
// subsequent good read resets the error streak.
func TestBackpressure_FailOpenOnStaleSignal(t *testing.T) {
	reader := &fakeReader{}
	consumer := &fakeConsumer{}
	c := newTestController(reader, consumer)

	ctx := context.Background()

	// A hot read pauses the consumer.
	reader.set(600, nil)
	c.tick(ctx)
	require.True(t, c.paused.Load())
	require.Equal(t, 1, consumer.pauseCount())

	// Two errors: below the limit, stay paused.
	readErr := errors.NewProcessingError("queue stats unavailable")
	reader.set(0, readErr)
	c.tick(ctx)
	c.tick(ctx)
	require.Equal(t, 0, consumer.resumeCount())
	require.True(t, c.paused.Load())

	// Third consecutive error hits the limit: fail-open resume.
	c.tick(ctx)
	require.Equal(t, 1, consumer.resumeCount())
	require.False(t, c.paused.Load())

	// A good read resets the error streak.
	reader.set(50, nil)
	c.tick(ctx)
	require.Equal(t, 0, c.consecutiveErrors)
}

// TestBackpressure_FailOpenResetsErrorStreak verifies that the fail-open resume
// itself clears the consecutive-error streak (and the read-errors gauge with it),
// so it does not climb without bound while reads keep failing.
func TestBackpressure_FailOpenResetsErrorStreak(t *testing.T) {
	reader := &fakeReader{}
	consumer := &fakeConsumer{}
	c := newTestController(reader, consumer)

	ctx := context.Background()

	// A hot read pauses the consumer.
	reader.set(600, nil)
	c.tick(ctx)
	require.True(t, c.paused.Load())

	// StaleErrorLimit consecutive errors trip the fail-open resume.
	reader.set(0, errors.NewProcessingError("queue stats unavailable"))
	for i := 0; i < c.cfg.StaleErrorLimit; i++ {
		c.tick(ctx)
	}
	require.Equal(t, 1, consumer.resumeCount())
	require.False(t, c.paused.Load())

	// The streak is cleared by the resume itself, not only by a later good read.
	require.Equal(t, 0, c.consecutiveErrors)

	// A further failed read starts a fresh streak rather than continuing to climb.
	c.tick(ctx)
	require.Equal(t, 1, c.consecutiveErrors)
}

// TestBackpressure_RunZeroPollIntervalDoesNotPanic verifies the run loop clamps a
// non-positive poll interval instead of panicking in time.NewTicker. The settings
// loader disables the controller in that case, but a hand-built Settings — as the
// repo's own tests use — can still reach run().
func TestBackpressure_RunZeroPollIntervalDoesNotPanic(t *testing.T) {
	initPrometheusMetrics()

	cfg := testBackpressureConfig()
	cfg.PollInterval = 0

	c := newKafkaBackpressureController(ulogger.TestLogger{}, cfg, 0, &fakeReader{}, &fakeConsumer{})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // the run loop returns on the already-cancelled context after the ticker is built

	require.NotPanics(t, func() { c.run(ctx) })
}

// deadlineRecordingReader records the deadline of the context each read is handed,
// so the per-poll bound tick actually applies is observable rather than inferred.
// It records the time REMAINING at the moment of the read rather than the absolute
// deadline, so the assertion is exact instead of racing the scheduler.
type deadlineRecordingReader struct {
	mu        sync.Mutex
	remaining time.Duration
	hasDDL    bool
	calls     int

	ageMillis int64
}

func (r *deadlineRecordingReader) GetBlockAssemblyQueueStats(ctx context.Context) (blockassembly.QueueStats, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.calls++

	var deadline time.Time

	deadline, r.hasDDL = ctx.Deadline()
	r.remaining = time.Until(deadline)

	return blockassembly.QueueStats{HeadAge: time.Duration(r.ageMillis) * time.Millisecond}, nil
}

// TestBackpressure_ZeroReadTimeoutStillReads covers the missing lower bound on
// ReadTimeout. PollInterval is clamped in run(), but ReadTimeout was passed to
// context.WithTimeout verbatim: a zero deadline has already expired when the read
// starts, so EVERY read fails and the controller silently never pauses anything —
// the failure mode the feature exists to prevent, with no signal that it is off.
// The settings loader disables the controller on a non-positive value, but a
// hand-built Settings (as the repo's own tests use) still reaches tick.
func TestBackpressure_ZeroReadTimeoutStillReads(t *testing.T) {
	initPrometheusMetrics()

	cfg := testBackpressureConfig()
	cfg.ReadTimeout = 0

	reader := &deadlineRecordingReader{ageMillis: 600}
	consumer := &fakeConsumer{}

	c := newKafkaBackpressureController(ulogger.TestLogger{}, cfg, 0, reader, consumer)

	c.tick(context.Background())

	require.Equal(t, 1, reader.calls, "the read was attempted")
	require.True(t, reader.hasDDL, "the read is still bounded by a deadline")
	require.Positive(t, reader.remaining, "a zero configured timeout must not hand the read an already-expired deadline")
	require.LessOrEqual(t, reader.remaining, defaultKafkaBackpressureReadTimeout,
		"the fallback deadline is the documented default, not an unbounded read")

	require.Equal(t, 0, c.consecutiveErrors, "the read succeeded rather than failing on an expired context")
	require.True(t, c.paused.Load(), "a hot queue must still pause; a zero read timeout silently disabled that")
	require.Equal(t, 1, consumer.pauseCount())
}

// TestBackpressure_DarkSignalWarnsOnceWhileRunning covers the other half of the
// error-streak behaviour. While the consumer is RUNNING there is no pause to fail
// open from, so the streak keeps climbing — that is deliberate (it is the "how long
// has the signal been dark" reading) and is asserted here so it cannot be
// "corrected" into a reset that loses the measurement. What was missing is
// visibility: a dark signal was only ever reported at Debugf. Exactly one warning
// per dark stretch, re-armed by a good read.
func TestBackpressure_DarkSignalWarnsOnceWhileRunning(t *testing.T) {
	initPrometheusMetrics()

	logger := &capturingLogger{}
	reader := &fakeReader{}
	consumer := &fakeConsumer{}

	c := newKafkaBackpressureController(logger, testBackpressureConfig(), 0, reader, consumer)

	ctx := context.Background()
	const darkSignalWarning = "queue-stats signal dark after"

	ticks := c.cfg.StaleErrorLimit + 3

	reader.set(0, errors.NewProcessingError("queue stats unavailable"))

	for i := 0; i < ticks; i++ {
		c.tick(ctx)
	}

	require.False(t, c.paused.Load(), "precondition: the consumer was never paused, so the fail-open arm never runs")
	require.Equal(t, 0, consumer.pauseCount())
	require.Equal(t, 0, consumer.resumeCount())

	require.Equal(t, ticks, c.consecutiveErrors,
		"with the consumer running the streak is meant to climb — it is the dark-signal duration, and only a good read clears it")
	require.Equal(t, float64(ticks), testutil.ToFloat64(prometheusKafkaBackpressureReadErrors))

	require.Equal(t, 1, strings.Count(logger.joined(), darkSignalWarning),
		"a dark signal must be visible above Debugf, and exactly once — not at the poll cadence")

	// A good read clears the streak and re-arms the warning.
	reader.set(50, nil)
	c.tick(ctx)
	require.Equal(t, 0, c.consecutiveErrors)

	reader.set(0, errors.NewProcessingError("queue stats unavailable"))

	for i := 0; i < c.cfg.StaleErrorLimit; i++ {
		c.tick(ctx)
	}

	require.Equal(t, 2, strings.Count(logger.joined(), darkSignalWarning),
		"a fresh dark stretch after the signal came back warns again")
}

// TestBackpressure_DisabledOrNilClient verifies startKafkaBackpressure is a safe
// no-op when disabled or when a required client is nil.
func TestBackpressure_DisabledOrNilClient(t *testing.T) {
	initPrometheusMetrics()

	logger := ulogger.TestLogger{}
	ctx := context.Background()

	t.Run("disabled", func(t *testing.T) {
		tSettings := settings.NewSettings()
		tSettings.Validator.KafkaBackpressure.Enabled = false

		v := &Server{logger: logger, settings: tSettings}
		require.NotPanics(t, func() { v.startKafkaBackpressure(ctx) })
	})

	t.Run("enabled but nil clients", func(t *testing.T) {
		tSettings := settings.NewSettings()
		tSettings.Validator.KafkaBackpressure.Enabled = true

		v := &Server{logger: logger, settings: tSettings}
		require.NotPanics(t, func() { v.startKafkaBackpressure(ctx) })
	})

	t.Run("disabled with non-nil clients starts no goroutine", func(t *testing.T) {
		tSettings := settings.NewSettings()
		tSettings.Validator.KafkaBackpressure.Enabled = false

		// Both clients are non-nil so this proves the disabled-branch short-circuit,
		// not merely the nil guard. The mock has no expectations: any queue-stats
		// read (which is the only thing that could precede a pause/resume) would be
		// recorded, so asserting zero calls proves no controller goroutine ran.
		baMock := blockassembly.NewMock()

		consumer := newInMemoryConsumer(t, "mvp-disabled")
		defer func() { _ = consumer.Close() }()

		v := &Server{
			logger:              logger,
			settings:            tSettings,
			consumerClient:      consumer,
			blockAssemblyClient: baMock,
		}

		require.NotPanics(t, func() { v.startKafkaBackpressure(ctx) })
		require.Never(t, func() bool { return len(baMock.Calls) > 0 }, 100*time.Millisecond, 20*time.Millisecond,
			"disabled controller must not read queue stats or pause/resume")
	})
}

// newInMemoryConsumer builds a real KafkaConsumerGroup backed by the in-memory
// broker on a unique topic, so tests exercise the actual PauseAll/ResumeAll path
// rather than a hand-rolled double.
func newInMemoryConsumer(t *testing.T, topic string) *kafka.KafkaConsumerGroup {
	t.Helper()

	kafkaURL, err := url.Parse("memory://localhost/" + topic)
	require.NoError(t, err)

	consumer, err := kafka.NewKafkaConsumerGroupFromURL(ulogger.TestLogger{}, kafkaURL, topic+"-group", true, nil)
	require.NoError(t, err)

	return consumer
}

// TestBackpressure_InMemoryKafkaRealPauseResume drives the controller against a
// real in-memory KafkaConsumerGroup and proves that PauseAll actually stops
// message delivery to the handler and ResumeAll restarts it — i.e. the real
// consumer pause/resume path is exercised, not just a fake.
func TestBackpressure_InMemoryKafkaRealPauseResume(t *testing.T) {
	initPrometheusMetrics()

	const topic = "mvp-real-pause-resume"
	broker := inmemorykafka.GetSharedBroker()
	broker.DropTopic(topic)

	consumer := newInMemoryConsumer(t, topic)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		_ = consumer.Close()
		broker.DropTopic(topic)
	})

	var processed atomic.Int64
	consumer.Start(ctx, func(*kafka.KafkaMessage) error {
		processed.Add(1)
		return nil
	}, kafka.WithLogErrorAndMoveOn())

	require.Eventually(t, func() bool { return broker.HasConsumer(topic) }, 2*time.Second, 5*time.Millisecond)

	// Baseline: not paused, messages flow.
	require.NoError(t, broker.Produce(ctx, topic, nil, []byte("m0")))
	require.Eventually(t, func() bool { return processed.Load() == 1 }, 2*time.Second, 5*time.Millisecond)

	reader := &fakeReader{}
	c := newTestController(reader, consumer)

	// Hot read → controller pauses the real consumer.
	reader.set(600, nil)
	c.tick(ctx)
	require.True(t, c.paused.Load())

	// Messages produced while paused must not reach the handler.
	require.NoError(t, broker.Produce(ctx, topic, nil, []byte("m1")))
	require.Never(t, func() bool { return processed.Load() > 1 }, 200*time.Millisecond, 20*time.Millisecond,
		"a paused consumer must not deliver new messages")

	// Cool read → controller resumes; the backlog now flows.
	reader.set(50, nil)
	c.tick(ctx)
	require.False(t, c.paused.Load())
	require.Eventually(t, func() bool { return processed.Load() == 2 }, 2*time.Second, 5*time.Millisecond)
}

// TestBackpressure_InMemoryKafkaInFlightAndResumeByAge proves the documented
// in-flight semantics against a real in-memory consumer: a record already being
// processed when PauseAll fires still completes, and — crucially — the resume is
// driven by the observed queue-head age falling to the low watermark, never by
// the mere fact that the controller paused ("paused" is not treated as
// "drained").
func TestBackpressure_InMemoryKafkaInFlightAndResumeByAge(t *testing.T) {
	initPrometheusMetrics()

	const topic = "mvp-inflight-resume-by-age"
	broker := inmemorykafka.GetSharedBroker()
	broker.DropTopic(topic)

	consumer := newInMemoryConsumer(t, topic)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		_ = consumer.Close()
		broker.DropTopic(topic)
	})

	var processed atomic.Int64
	enteredFirst := make(chan struct{})
	releaseFirst := make(chan struct{})
	var firstOnce sync.Once

	consumer.Start(ctx, func(*kafka.KafkaMessage) error {
		// Hold only the first record in-flight until the test releases it.
		firstOnce.Do(func() {
			close(enteredFirst)
			<-releaseFirst
		})
		processed.Add(1)
		return nil
	}, kafka.WithLogErrorAndMoveOn())

	require.Eventually(t, func() bool { return broker.HasConsumer(topic) }, 2*time.Second, 5*time.Millisecond)

	// Produce the first record and wait until it is actually in the handler.
	require.NoError(t, broker.Produce(ctx, topic, nil, []byte("inflight")))
	select {
	case <-enteredFirst:
	case <-time.After(2 * time.Second):
		t.Fatal("first record never reached the handler")
	}

	reader := &fakeReader{}
	c := newTestController(reader, consumer)

	// Pause while the first record is still in-flight.
	reader.set(600, nil)
	c.tick(ctx)
	require.True(t, c.paused.Load())

	// The in-flight record completes despite the pause.
	close(releaseFirst)
	require.Eventually(t, func() bool { return processed.Load() == 1 }, 2*time.Second, 5*time.Millisecond,
		"an already-fetched record must finish processing even after PauseAll")

	// A second record produced while paused must not be delivered.
	require.NoError(t, broker.Produce(ctx, topic, nil, []byte("backlog")))

	// Age stays hot (between the watermarks) across several ticks: the controller
	// must NOT resume just because it paused — resume is age-driven.
	for i := 0; i < 5; i++ {
		reader.set(400, nil)
		c.tick(ctx)
	}
	require.True(t, c.paused.Load(), "controller must not treat 'paused' as 'drained'")
	require.Equal(t, int64(1), processed.Load(), "no new delivery while paused")

	// Age falls to the low watermark → resume, and the backlog flows.
	reader.set(50, nil)
	c.tick(ctx)
	require.False(t, c.paused.Load())
	require.Eventually(t, func() bool { return processed.Load() == 2 }, 2*time.Second, 5*time.Millisecond)
}

// TestBackpressure_ShutdownResumes drives the real run loop and verifies that a
// context cancel while paused triggers a final resume, so a restart never
// inherits a paused consumer. Exercised under -race for the goroutine + shared
// paused flag.
func TestBackpressure_ShutdownResumes(t *testing.T) {
	reader := &fakeReader{}
	reader.set(600, nil) // permanently hot so the controller pauses

	consumer := &fakeConsumer{}
	c := newTestController(reader, consumer)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		c.run(ctx)
		close(done)
	}()

	require.Eventually(t, c.paused.Load, 2*time.Second, 5*time.Millisecond, "controller should pause while hot")

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("run did not return after cancel")
	}

	require.False(t, c.paused.Load(), "consumer must be resumed on shutdown")
	require.GreaterOrEqual(t, consumer.resumeCount(), 1)
}
