package validator

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// queueStatsReader is the slim signal source the controller reads each tick. It
// is satisfied by blockassembly.ClientI and deliberately narrow so the read can
// never touch the subtree-processor main loop (GetBlockAssemblyQueueStats is
// backed by atomic loads only).
type queueStatsReader interface {
	GetBlockAssemblyQueueStats(ctx context.Context) (*blockassembly_api.QueueStatsMessage, error)
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

	// now supplies the current time; overridable in tests for deterministic
	// max-pause behaviour.
	now func() time.Time

	// paused reflects whether the consumer is currently paused by this
	// controller. Atomic so shutdown/tests can read it safely.
	paused atomic.Bool

	// pauseStart is when the current pause began; valid only while paused.
	pauseStart time.Time

	// consecutiveErrors counts back-to-back failed reads for fail-open.
	consecutiveErrors int
}

// newKafkaBackpressureController builds a controller. It does not start any
// goroutine; call run to begin.
func newKafkaBackpressureController(logger ulogger.Logger, cfg settings.ValidatorKafkaBackpressureSettings,
	reader queueStatsReader, consumer pausableConsumer) *kafkaBackpressureController {
	return &kafkaBackpressureController{
		logger:   logger,
		cfg:      cfg,
		reader:   reader,
		consumer: consumer,
		now:      time.Now,
	}
}

// run drives the poll loop until the context is cancelled. On exit it always
// resumes a paused consumer so a shutdown (or an unexpected loop exit) never
// leaves ingest wedged.
func (c *kafkaBackpressureController) run(ctx context.Context) {
	c.logger.Infof("[Validator] kafka backpressure controller started (pause>=%s, resume<=%s, poll=%s, maxPause=%s)",
		c.cfg.PauseQueueAge, c.cfg.ResumeQueueAge, c.cfg.PollInterval, c.cfg.MaxPause)

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

	if err != nil || stats == nil {
		c.onReadError(err)
		return
	}

	// A good read clears the fail-open error streak.
	c.consecutiveErrors = 0
	prometheusKafkaBackpressureReadErrors.Set(0)

	age := time.Duration(stats.QueueHeadAgeMillis) * time.Millisecond
	c.evaluate(age)
}

// onReadError applies the fail-open policy: after StaleErrorLimit consecutive
// failed reads, a paused consumer is resumed rather than left wedged on a signal
// that has gone dark.
func (c *kafkaBackpressureController) onReadError(err error) {
	c.consecutiveErrors++
	prometheusKafkaBackpressureReadErrors.Set(float64(c.consecutiveErrors))

	c.logger.Debugf("[Validator] kafka backpressure: queue-stats read failed (%d/%d): %v",
		c.consecutiveErrors, c.cfg.StaleErrorLimit, err)

	if c.consecutiveErrors >= c.cfg.StaleErrorLimit && c.paused.Load() {
		c.resume(fmt.Sprintf("stale signal: %d consecutive read errors", c.consecutiveErrors))
	}
}

// evaluate applies the hysteresis decision to a freshly-read queue-head age.
func (c *kafkaBackpressureController) evaluate(age time.Duration) {
	if !c.paused.Load() {
		if age >= c.cfg.PauseQueueAge {
			c.pause(age)
		}

		return
	}

	// Already paused: resume on the low watermark, or when the pause has run
	// past its hard cap (fail-open) even if the queue is still hot.
	if age <= c.cfg.ResumeQueueAge {
		c.resume(fmt.Sprintf("queue-head age %s fell to resume watermark %s", age, c.cfg.ResumeQueueAge))
		return
	}

	if pausedFor := c.now().Sub(c.pauseStart); pausedFor >= c.cfg.MaxPause {
		c.resume(fmt.Sprintf("max-pause %s reached while age still %s", c.cfg.MaxPause, age))
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
func (v *Server) startKafkaBackpressure(ctx context.Context) {
	cfg := v.settings.Validator.KafkaBackpressure

	if !cfg.Enabled {
		return
	}

	if v.consumerClient == nil || v.blockAssemblyClient == nil {
		v.logger.Warnf("[Validator] kafka backpressure enabled but consumer or block-assembly client is nil; controller not started")
		return
	}

	controller := newKafkaBackpressureController(v.logger, cfg, v.blockAssemblyClient, v.consumerClient)
	go controller.run(ctx)
}
