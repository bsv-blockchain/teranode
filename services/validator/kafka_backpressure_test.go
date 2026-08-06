package validator

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
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

func (f *fakeReader) GetBlockAssemblyQueueStats(_ context.Context) (*blockassembly_api.QueueStatsMessage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.err != nil {
		return nil, f.err
	}

	return &blockassembly_api.QueueStatsMessage{QueueHeadAgeMillis: f.ageMillis}, nil
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
	return newKafkaBackpressureController(ulogger.TestLogger{}, testBackpressureConfig(), reader, consumer)
}

// TestBackpressure_HysteresisPauseThenResume covers the core watermark logic:
// exactly one pause when the age crosses the high watermark, no flapping while
// the age sits between the watermarks, and exactly one resume once it falls to
// the low watermark. This also models the in-flight semantics — a paused
// consumer is not treated as drained; resume waits for the observed age to fall.
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
