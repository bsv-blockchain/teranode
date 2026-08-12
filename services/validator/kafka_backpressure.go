package validator

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// cooldownDutyDivisor sets how much of the pause that preceded a fail-open
// resume is spent suppressing the next pause: cooldown = pausedFor / divisor,
// then clamped. At 4 a pause can occupy at most ~80% of a pause+cooldown cycle
// instead of the ~100% a same-tick re-pause produced, while the cooldown stays
// short enough that a genuinely overloaded node regains protection quickly.
const cooldownDutyDivisor = 4

// queueStatsReader is the slim signal source the controller reads each tick. It
// is satisfied by blockassembly.ClientI and deliberately narrow so the read can
// never touch the subtree-processor main loop (GetBlockAssemblyQueueStats is
// backed by atomic loads only). It returns native Go types, so the controller
// never imports the block-assembly protobuf.
type queueStatsReader interface {
	GetBlockAssemblyQueueStats(ctx context.Context) (blockassembly.QueueStats, error)
}

// pausableConsumer is the lever the controller pulls. It is satisfied by
// kafka.KafkaConsumerGroupI. PauseAll stops future fetches only; records already
// returned by PollFetches keep processing in per-partition goroutines, so a
// bounded amount of ingest continues after a pause — the resume decision
// therefore gates on the observed queue-head age falling below the low
// watermark, never on "we paused, so it must be drained".
type pausableConsumer interface {
	PauseAll()
	ResumeAll()
}

// kafkaBackpressureController pauses and resumes the validator's transaction
// Kafka consumer based on the block-assembly ingest queue-head age, with
// hysteresis (distinct pause/resume watermarks), a hard per-pause cap, and
// fail-open resume when the signal is unavailable. It complements — and never
// replaces — the block-assembly hard shed and queue cap: pausing lets bursts
// ride on Kafka's durable log so fewer transactions hit the shed path.
//
// All mutable state except the paused flag is touched only from the single run
// goroutine. The paused flag is an atomic so tests and shutdown can observe it.
type kafkaBackpressureController struct {
	logger   ulogger.Logger
	cfg      settings.ValidatorKafkaBackpressureSettings
	reader   queueStatsReader
	consumer pausableConsumer

	// localDoubleSpendWindow is this process's copy of the block-assembly drain
	// floor. It is DIAGNOSTIC ONLY and must never feed a control decision: the
	// hold-back is applied by the block-assembly process from ITS settings
	// context, and the two are independent per-process settings that can differ
	// silently in both directions. The control value is the window the producer
	// reports on each queue-stats read (reportedWindow); this field exists so a
	// disagreement between the two can be logged.
	localDoubleSpendWindow time.Duration

	// reportedWindow is the drain floor carried by the most recent successful
	// read — the value evaluate subtracts from the raw head age. It is written
	// only by tick and read only by evaluate, both on the single run goroutine.
	reportedWindow time.Duration

	// lastReportedWindow and windowMismatchLogged bound the mismatch warning to
	// one line per distinct reported value, so a persistent misconfiguration
	// cannot spam at the poll cadence.
	lastReportedWindow   time.Duration
	windowMismatchLogged bool

	// now supplies the current time; overridable in tests for deterministic
	// max-pause behaviour.
	now func() time.Time

	// paused reflects whether the consumer is currently paused by this
	// controller. Atomic so shutdown/tests can read it safely.
	paused atomic.Bool

	// pauseStart is when the current pause began; valid only while paused.
	pauseStart time.Time

	// failOpenResumeAt is when the last fail-open resume happened, and
	// failOpenCooldown is how long a new pause stays suppressed from that
	// instant; both zero when no cooldown is armed.
	//
	// The invariant: a resume that was NOT a genuine drain to the low watermark
	// arms a bounded cooldown. That covers both fail-open arms — the max-pause
	// forced resume and the stale-signal resume — because in neither case is the
	// queue known to have drained, so a re-pause on the very next tick would
	// wedge ingest to a near-zero duty cycle. The cooldown is proportional to
	// the pause it follows (see cooldownDutyDivisor) and capped by
	// cfg.MaxFailOpenCooldown, so backpressure is never suppressed for a full
	// MaxPause window. A genuine drain to the resume watermark clears it early.
	failOpenResumeAt time.Time
	failOpenCooldown time.Duration

	// consecutiveErrors counts back-to-back failed reads for fail-open.
	consecutiveErrors int
}

// newKafkaBackpressureController builds a controller. It does not start any
// goroutine; call run to begin.
//
// localDoubleSpendWindow is the reader process's own copy of the block-assembly
// drain floor. It is retained for mismatch logging only — the control decision
// uses the window each read reports.
func newKafkaBackpressureController(logger ulogger.Logger, cfg settings.ValidatorKafkaBackpressureSettings,
	localDoubleSpendWindow time.Duration, reader queueStatsReader, consumer pausableConsumer) *kafkaBackpressureController {
	return &kafkaBackpressureController{
		logger:                 logger,
		cfg:                    cfg,
		localDoubleSpendWindow: localDoubleSpendWindow,
		reader:                 reader,
		consumer:               consumer,
		now:                    time.Now,
	}
}

// run drives the poll loop until the context is cancelled. On exit it always
// resumes a paused consumer so a shutdown (or an unexpected loop exit) never
// leaves ingest wedged.
func (c *kafkaBackpressureController) run(ctx context.Context) {
	c.logger.Infof("[Validator] kafka backpressure controller started (pause>=%s, resume<=%s, poll=%s, maxPause=%s, maxFailOpenCooldown=%s)",
		c.cfg.PauseQueueAge, c.cfg.ResumeQueueAge, c.cfg.PollInterval, c.cfg.MaxPause, c.cfg.MaxFailOpenCooldown)

	ticker := time.NewTicker(c.cfg.PollInterval)
	defer ticker.Stop()
	defer c.resumeOnExit()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.tick(ctx)
		}
	}
}

// tick performs one poll-and-decide cycle. The read is bounded by a per-poll
// deadline so the controller can never inherit a downstream stall.
func (c *kafkaBackpressureController) tick(ctx context.Context) {
	readCtx, cancel := context.WithTimeout(ctx, c.cfg.ReadTimeout)
	stats, err := c.reader.GetBlockAssemblyQueueStats(readCtx)
	cancel()

	if err != nil {
		c.onReadError(err)
		return
	}

	// A good read clears the fail-open error streak.
	c.consecutiveErrors = 0
	prometheusKafkaBackpressureReadErrors.Set(0)

	c.reportedWindow = c.observeReportedWindow(stats.DoubleSpendWindow)

	c.evaluate(stats.HeadAge)
}

// observeReportedWindow sanity-clamps the drain floor carried by a read and logs
// a disagreement with this process's own setting.
//
// The clamp is a trust boundary, not defensiveness for its own sake: a negative
// or absurd reported window would be ADDED to the effective age by the
// subtraction in evaluate and could invert the control decision, so a bad peer
// must not be able to steer it. A negative value is treated as zero.
//
// The warning fires the first time the reported value differs from the local one
// and again only when the reported value itself changes, so a persistent
// mismatch cannot spam at the poll cadence.
func (c *kafkaBackpressureController) observeReportedWindow(reported time.Duration) time.Duration {
	if reported < 0 {
		reported = 0
	}

	if reported != c.lastReportedWindow {
		c.lastReportedWindow = reported
		c.windowMismatchLogged = false
	}

	if reported != c.localDoubleSpendWindow && !c.windowMismatchLogged {
		c.windowMismatchLogged = true

		c.logger.Warnf("[Validator] kafka backpressure: block assembly reports doubleSpendWindow=%s but this process is configured with %s; using the reported value for control (align blockassembly_doubleSpendWindow across both settings contexts)",
			reported, c.localDoubleSpendWindow)
	}

	return reported
}

// onReadError applies the fail-open policy: after StaleErrorLimit consecutive
// failed reads, a paused consumer is resumed rather than left wedged on a signal
// that has gone dark.
//
// That resume is a fail-open, not an observed drain, so it arms the same cooldown
// the max-pause resume does. Without it a flapping stats endpoint cycles
// StaleErrorLimit error ticks paused, one tick resumed, then re-pauses on the
// next hot read — a paused duty cycle approaching StaleErrorLimit/(limit+1).
func (c *kafkaBackpressureController) onReadError(err error) {
	c.consecutiveErrors++
	prometheusKafkaBackpressureReadErrors.Set(float64(c.consecutiveErrors))

	c.logger.Debugf("[Validator] kafka backpressure: queue-stats read failed (%d/%d): %v",
		c.consecutiveErrors, c.cfg.StaleErrorLimit, err)

	if c.consecutiveErrors >= c.cfg.StaleErrorLimit && c.paused.Load() {
		c.armFailOpenCooldown()
		c.resume(fmt.Sprintf("stale signal: %d consecutive read errors", c.consecutiveErrors))
	}
}

// armFailOpenCooldown records a fail-open resume and computes how long a new
// pause stays suppressed:
//
//	cooldown = clamp(pausedFor / cooldownDutyDivisor, 2*PollInterval, MaxFailOpenCooldown)
//
// The cap bounds how long the queue grows unprotected after a fail-open — the A6
// defect was suppressing backpressure for a full MaxPause (30s by default), which
// on an unbounded queue is 30s of growth toward the OOM the feature exists to
// prevent. The floor bounds the paused duty cycle from the other side, so a
// fast-flapping signal cannot busy-toggle pause/resume every tick.
//
// The floor is applied last: the settings loader already guarantees
// MaxFailOpenCooldown >= 2*PollInterval, and if a hand-built config violates that
// the anti-busy-toggle floor is the property worth keeping.
//
// It must be called BEFORE resume, which is what consumes pauseStart.
func (c *kafkaBackpressureController) armFailOpenCooldown() {
	cooldown := c.now().Sub(c.pauseStart) / cooldownDutyDivisor

	if cooldown > c.cfg.MaxFailOpenCooldown {
		cooldown = c.cfg.MaxFailOpenCooldown
	}

	if floor := 2 * c.cfg.PollInterval; cooldown < floor {
		cooldown = floor
	}

	c.failOpenResumeAt = c.now()
	c.failOpenCooldown = cooldown
}

// evaluate applies the hysteresis decision to a freshly-read queue-head age.
// The control decision is based on the effective age — the raw head age minus
// the drain floor the PRODUCER reported (reportedWindow) — so a healthy hold-back
// (head aged only up to the floor) does not read as a stall, and a difference
// between the two processes' settings cannot silently invert the decision.
func (c *kafkaBackpressureController) evaluate(age time.Duration) {
	effectiveAge := age - c.reportedWindow
	if effectiveAge < 0 {
		effectiveAge = 0
	}

	if !c.paused.Load() {
		// Within a fail-open cooldown: suppress a new pause so a persistently
		// dark/hot signal cannot re-pause every tick. Clear the latch on a
		// genuine drain to the resume watermark or once the armed cooldown
		// elapses, then fall through to normal evaluation.
		if !c.failOpenResumeAt.IsZero() {
			if effectiveAge <= c.cfg.ResumeQueueAge || c.now().Sub(c.failOpenResumeAt) >= c.failOpenCooldown {
				c.failOpenResumeAt = time.Time{}
				c.failOpenCooldown = 0
			} else {
				return
			}
		}

		if effectiveAge >= c.cfg.PauseQueueAge {
			c.pause(effectiveAge)
		}

		return
	}

	// Already paused: resume on the low watermark, or when the pause has run
	// past its hard cap (fail-open) even if the queue is still hot.
	if effectiveAge <= c.cfg.ResumeQueueAge {
		c.resume(fmt.Sprintf("queue-head age %s fell to resume watermark %s", effectiveAge, c.cfg.ResumeQueueAge))
		return
	}

	if pausedFor := c.now().Sub(c.pauseStart); pausedFor >= c.cfg.MaxPause {
		// The queue is still hot, so this resume is a fail-open: arm a bounded,
		// proportional cooldown rather than suppressing backpressure for a whole
		// MaxPause window.
		c.armFailOpenCooldown()
		c.resume(fmt.Sprintf("max-pause %s reached while age still %s (cooldown %s)", c.cfg.MaxPause, effectiveAge, c.failOpenCooldown))
	}
}

// pause suspends the consumer and records the pause start.
func (c *kafkaBackpressureController) pause(age time.Duration) {
	c.consumer.PauseAll()
	c.pauseStart = c.now()
	c.paused.Store(true)

	prometheusKafkaBackpressurePaused.Set(1)
	prometheusKafkaBackpressurePauseTotal.Inc()

	c.logger.Warnf("[Validator] kafka backpressure: paused tx consumer (queue-head age %s >= pause watermark %s)",
		age, c.cfg.PauseQueueAge)
}

// resume resumes the consumer, records how long it was paused, and clears the
// paused flag. reason is a caller-built, human-readable cause.
func (c *kafkaBackpressureController) resume(reason string) {
	c.consumer.ResumeAll()

	pausedFor := c.now().Sub(c.pauseStart)
	c.paused.Store(false)

	prometheusKafkaBackpressurePaused.Set(0)
	prometheusKafkaBackpressureResumeTotal.Inc()
	prometheusKafkaBackpressurePausedSecondsTotal.Add(pausedFor.Seconds())

	c.logger.Warnf("[Validator] kafka backpressure: resumed tx consumer after %s (%s)", pausedFor, reason)
}

// resumeOnExit is the shutdown/exit guard: if still paused, resume so a restart
// (or a stalled controller) never inherits a paused consumer.
func (c *kafkaBackpressureController) resumeOnExit() {
	if c.paused.Load() {
		c.resume("controller shutting down")
	}
}

// startKafkaBackpressure launches the backpressure controller goroutine when it
// is enabled and both the Kafka consumer and block-assembly client are wired.
// It is a safe no-op otherwise (disabled config, or a nil client — e.g. in tests
// or a local-validator deployment without a Kafka consumer).
//
// The controller is bound to the CONSUMER's context, not to the caller's, so it
// shares the lifetime of the thing it controls by construction: Stop's
// consumerCancel is what ends it, its resume-on-exit runs, and a Start/Stop cycle
// leaks no goroutine. Bound to the Start context instead, run's select on
// ctx.Done() would never fire on Stop and the controller would keep pausing and
// resuming a closed consumer.
//
// Ordering preference, not a guarantee: Stop cancels the consumer context before
// closing the consumer, so resume-on-exit has a chance to observe a live client — but
// nothing joins this goroutine, so the two can interleave. That is acceptable because
// the property is immaterial: pause state is client-local and dies with the client
// (franz-go's on the kgo.Client that closeClient closes, the in-memory arm's on the
// consumer group object built per consumer), so a resume landing after Close changes
// nothing a later Start could observe. The closed guard in
// KafkaConsumerGroup.setFetchPaused is what makes such a late call safe rather than
// merely pointless.
//
// The consumer context is nil when Start has not run (a Stop-before-Start, and the
// controller unit tests); fall back to the passed context so the caller still
// gets a controller with a cancellable lifetime.
func (v *Server) startKafkaBackpressure(ctx context.Context) {
	cfg := v.settings.Validator.KafkaBackpressure

	if !cfg.Enabled {
		return
	}

	if v.consumerClient == nil || v.blockAssemblyClient == nil {
		v.logger.Warnf("[Validator] kafka backpressure enabled but consumer or block-assembly client is nil; controller not started")
		return
	}

	controllerCtx := v.consumerContext()
	if controllerCtx == nil {
		controllerCtx = ctx
	}

	controller := newKafkaBackpressureController(v.logger, cfg, v.settings.BlockAssembly.DoubleSpendWindow, v.blockAssemblyClient, v.consumerClient)
	go controller.run(controllerCtx)
}
