package validator

import (
	"context"
	"net/url"
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

// fakeReader is a controllable queue-stats source.
type fakeReader struct {
	mu        sync.Mutex
	ageMillis int64
	err       error
}

func (f *fakeReader) set(ageMillis int64, err error) {
	f.mu.Lock()
	f.ageMillis = ageMillis
	f.err = err
	f.mu.Unlock()
}

func (f *fakeReader) GetBlockAssemblyQueueStats(_ context.Context) (int64, time.Duration, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.err != nil {
		return 0, 0, f.err
	}

	return 0, time.Duration(f.ageMillis) * time.Millisecond, nil
}

func testBackpressureConfig() settings.ValidatorKafkaBackpressureSettings {
	return settings.ValidatorKafkaBackpressureSettings{
		Enabled:         true,
		PauseQueueAge:   500 * time.Millisecond,
		ResumeQueueAge:  100 * time.Millisecond,
		PollInterval:    5 * time.Millisecond,
		ReadTimeout:     100 * time.Millisecond,
		MaxPause:        30 * time.Second,
		StaleErrorLimit: 3,
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
func TestBackpressure_DoubleSpendWindowExcludedFromControl(t *testing.T) {
	consumer := &fakeConsumer{}
	c := newTestController(&fakeReader{}, consumer)
	c.doubleSpendWindow = 1000 * time.Millisecond

	// Healthy hold-back: raw head age = window + 50ms → effectiveAge 50ms, below
	// the 500ms pause watermark. Must not pause.
	c.evaluate(1050 * time.Millisecond)
	require.Equal(t, 0, consumer.pauseCount(), "a healthy hold-back at the drain floor must not pause")
	require.False(t, c.paused.Load())

	// Genuine stall: raw head age = window + 600ms → effectiveAge 600ms, above
	// the pause watermark. Must pause.
	c.evaluate(1600 * time.Millisecond)
	require.Equal(t, 1, consumer.pauseCount(), "a stall past the drain floor must pause")
	require.True(t, c.paused.Load())
}

// TestBackpressure_MaxPauseCooldownBoundsDutyCycle verifies that after a
// max-pause fail-open resume the controller does not immediately re-pause while
// the signal stays hot: it must wait out a MaxPause-long cooldown, bounding the
// pathological duty cycle to ~50% instead of ~99.8%.
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

	// Held hot past MaxPause → fail-open resume, cooldown armed.
	now = base.Add(31 * time.Second)
	c.evaluate(600 * time.Millisecond)
	require.False(t, c.paused.Load())
	require.Equal(t, 1, consumer.resumeCount())

	// Still hot but inside the cooldown → must NOT re-pause.
	now = base.Add(41 * time.Second)
	c.evaluate(600 * time.Millisecond)
	require.False(t, c.paused.Load())
	require.Equal(t, 1, consumer.pauseCount(), "must not re-pause during the cooldown")

	// Cooldown (one MaxPause after the resume) elapsed → re-pause allowed.
	now = base.Add(61 * time.Second)
	c.evaluate(600 * time.Millisecond)
	require.True(t, c.paused.Load())
	require.Equal(t, 2, consumer.pauseCount(), "re-pause allowed once the cooldown elapses")
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

	// A genuine drain to the low watermark clears the latch.
	now = base.Add(32 * time.Second)
	c.evaluate(50 * time.Millisecond)
	require.False(t, c.paused.Load())

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
