package kafka

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// fakeAsyncProducer is a controllable KafkaAsyncProducerI test double. Stop()
// optionally blocks (until block is closed) and returns a configurable error,
// so the helper's quick-return, error-logging and timeout paths can be pinned.
type fakeAsyncProducer struct {
	stopErr   error
	started   chan struct{} // when non-nil, closed once Stop() has begun (after the call count is bumped)
	block     chan struct{} // when non-nil, Stop() blocks until it is closed
	stopCalls int32
}

func (f *fakeAsyncProducer) Start(_ context.Context, _ chan *Message) {}

func (f *fakeAsyncProducer) Stop() error {
	// Bump the counter first, then signal: closing started happens-after the
	// increment (program order) and the test's receive on started happens-before
	// its read of stopCalls, so the read is race-free and deterministic.
	atomic.AddInt32(&f.stopCalls, 1)
	if f.started != nil {
		close(f.started)
	}

	if f.block != nil {
		<-f.block
	}

	return f.stopErr
}

func (f *fakeAsyncProducer) BrokersURL() []string { return nil }

func (f *fakeAsyncProducer) Publish(_ *Message) {}

func (f *fakeAsyncProducer) TryPublish(_ *Message) bool { return false }

// captureLogger records Errorf lines so the test can assert on them, while
// delegating every other method to the no-op TestLogger.
type captureLogger struct {
	ulogger.TestLogger
	mu   sync.Mutex
	msgs []string
}

func (c *captureLogger) Errorf(format string, args ...interface{}) {
	c.mu.Lock()
	c.msgs = append(c.msgs, fmt.Sprintf(format, args...))
	c.mu.Unlock()
}

func (c *captureLogger) contains(sub string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, m := range c.msgs {
		if strings.Contains(m, sub) {
			return true
		}
	}

	return false
}

func (c *captureLogger) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.msgs)
}

func TestStopProducerCtx(t *testing.T) {
	t.Run("quick stop returns and logs nothing", func(t *testing.T) {
		p := &fakeAsyncProducer{}
		lg := &captureLogger{}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		start := time.Now()
		StopProducerCtx(ctx, lg, "quick", p)

		require.Less(t, time.Since(start), 500*time.Millisecond, "helper should return as soon as Stop() completes")
		require.Equal(t, int32(1), atomic.LoadInt32(&p.stopCalls))
		require.Zero(t, lg.count(), "no Errorf expected on the happy path")
	})

	t.Run("stop error is logged", func(t *testing.T) {
		p := &fakeAsyncProducer{stopErr: errors.NewProcessingError("broker rejected flush")}
		lg := &captureLogger{}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		StopProducerCtx(ctx, lg, "erroring", p)

		require.Equal(t, int32(1), atomic.LoadInt32(&p.stopCalls))
		require.True(t, lg.contains("failed to stop erroring kafka producer gracefully"), "Stop() error should be logged")
		require.False(t, lg.contains("exceeded shutdown window"))
	})

	t.Run("wedged stop returns promptly on ctx deadline and logs", func(t *testing.T) {
		started := make(chan struct{})
		block := make(chan struct{})
		p := &fakeAsyncProducer{started: started, block: block}
		// Release the still-running detached Stop() goroutine when the test ends.
		defer close(block)

		lg := &captureLogger{}

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		start := time.Now()
		StopProducerCtx(ctx, lg, "wedged", p)
		elapsed := time.Since(start)

		// Stop() is still blocked (block is only closed on test teardown), so a
		// return here proves the helper raced the deadline rather than waiting on
		// the flush.
		require.Less(t, elapsed, 2*time.Second, "helper must not wait for a wedged Stop()")
		require.True(t, lg.contains("exceeded shutdown window"), "timeout path should log the exceeded-window line")

		// Wait until Stop() has actually begun before reading stopCalls, so the
		// assertion can't observe the detached goroutine before it has run.
		<-started
		require.Equal(t, int32(1), atomic.LoadInt32(&p.stopCalls), "Stop() should have been invoked")
	})

	t.Run("nil producer is a no-op", func(t *testing.T) {
		lg := &captureLogger{}

		require.NotPanics(t, func() {
			StopProducerCtx(context.Background(), lg, "nil", nil)
		})
		require.Zero(t, lg.count())
	})
}
