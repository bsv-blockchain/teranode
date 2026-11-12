package kafka

import (
	"context"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newInMemoryTestProducer(t *testing.T) *KafkaAsyncProducer {
	t.Helper()

	kafkaURL, err := url.Parse("memory://localhost/stop-race-topic")
	require.NoError(t, err)

	producer, err := NewKafkaAsyncProducerFromURL(context.Background(), &mockAsyncLogger{}, kafkaURL, nil)
	require.NoError(t, err)

	return producer
}

// poisonPublishChannel reproduces the state Stop() leaves behind when it wins the race with a
// publish goroutine that has not yet read the channel: the field is nil, while the goroutine
// still holds a live channel of its own.
func poisonPublishChannel(producer *KafkaAsyncProducer) chan *Message {
	producer.channelMu.Lock()
	producer.publishChannel = nil
	producer.channelMu.Unlock()

	ch := make(chan *Message, 1)
	close(ch)

	return ch
}

func requireReturns(t *testing.T, what string, fn func()) {
	t.Helper()

	done := make(chan struct{})

	go func() {
		defer close(done)

		fn()
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s never returned: it is ranging over c.publishChannel (nil) instead of the channel it was given", what)
	}
}

// TestKafkaAsyncProducer_PublishLoopUsesTheChannelItWasGiven pins the invariant behind a
// 10-minute CI hang in services/blockvalidation.
//
// The publish loop used to re-read c.publishChannel instead of ranging over the channel it was
// handed. Stop() nils that field under the write lock, so a Stop that won the race left the loop
// ranging over a nil channel — which blocks forever, so the deferred publishWg.Done() never ran
// and Stop() parked in publishWg.Wait() until the test binary timed out.
func TestKafkaAsyncProducer_PublishLoopUsesTheChannelItWasGiven(t *testing.T) {
	producer := newInMemoryTestProducer(t)
	ch := poisonPublishChannel(producer)

	requireReturns(t, "in-memory publish loop", func() {
		producer.runInMemoryPublishLoop(ch)
	})
}

// TestKafkaAsyncProducer_ProducerWorkerUsesTheChannelItWasGiven is the same pin for the
// real-broker worker, which is the one that actually runs in production.
//
// Scope, so nobody reads more into a green run than is there: this catches a c.publishChannel
// read reintroduced INSIDE runProducerWorker. It does not catch one reintroduced at the call
// site in Start, because it drives the worker directly. Pinning the call site would need a
// synchronisation seam in Start for a window that is a few instructions wide.
func TestKafkaAsyncProducer_ProducerWorkerUsesTheChannelItWasGiven(t *testing.T) {
	initProducerMetrics()

	producer := newInMemoryTestProducer(t)
	ch := poisonPublishChannel(producer)

	requireReturns(t, "producer worker", func() {
		producer.runProducerWorker(context.Background(), ch)
	})
}

// TestKafkaAsyncProducer_StartAfterStopIsRefused covers the resurrection path. Stop() closes the
// underlying producer for good — InMemoryAsyncProducer.Close() closes its input channel under a
// sync.Once — so a Start that reset shuttingDown/closed and spawned a fresh publish loop handed
// that loop a corpse, and the first message through it panicked with "send on closed channel".
func TestKafkaAsyncProducer_StartAfterStopIsRefused(t *testing.T) {
	producer := newInMemoryTestProducer(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	producer.Start(ctx, make(chan *Message, 1))
	require.NoError(t, producer.Stop())

	producer.Start(ctx, make(chan *Message, 1))

	producer.channelMu.RLock()
	republished := producer.publishChannel
	producer.channelMu.RUnlock()

	require.Nil(t, republished, "Start must refuse to resurrect a stopped producer")

	// A refused Start leaves the producer inert rather than panicking on the next message.
	require.NotPanics(t, func() {
		producer.Publish(&Message{Key: []byte("k"), Value: []byte("v")})
	})
}

// TestKafkaAsyncProducer_StopStaysIdempotentWhileAnEarlierStopIsStuck pins the reason Stop()'s
// fast path sits in front of lifecycleMu rather than behind it.
//
// StopProducerCtx abandons a Stop that outruns its deadline, and that orphan keeps running —
// holding lifecycleMu across publishWg.Wait() and an untimed client.Flush(). With the fast path
// behind the mutex, every later Stop() blocked on that orphan forever, which is the same
// package-timeout hang this file exists to prevent, just triggered by a wedged broker.
//
// The stuck orphan is simulated by holding lifecycleMu with shuttingDown already set, which is
// exactly the state it leaves behind.
func TestKafkaAsyncProducer_StopStaysIdempotentWhileAnEarlierStopIsStuck(t *testing.T) {
	producer := newInMemoryTestProducer(t)

	producer.shuttingDown.Store(true)
	producer.lifecycleMu.Lock()

	defer producer.lifecycleMu.Unlock()

	done := make(chan error, 1)

	go func() {
		done <- producer.Stop()
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Stop() blocked on lifecycleMu held by an abandoned Stop: the idempotent fast path must run before the mutex")
	}
}

// TestKafkaAsyncProducer_ConcurrentStartStopDoesNotHang covers the other half of the same
// lifecycle bug: a Stop() that reaches the channel field before Start() has stored it sees nil,
// closes nothing, and then waits on a publish goroutine whose channel nobody will ever close.
// Start and Stop are serialised so neither ordering can wedge shutdown.
func TestKafkaAsyncProducer_ConcurrentStartStopDoesNotHang(t *testing.T) {
	for i := 0; i < 200; i++ {
		producer := newInMemoryTestProducer(t)

		// Cancellable per iteration: startInMemory's outer goroutine parks on its parent ctx
		// until something cancels it, and 200 iterations of Background() would leak 200 of
		// them into the rest of the package's run.
		ctx, cancel := context.WithCancel(context.Background())

		var wg sync.WaitGroup

		wg.Add(2)

		go func() {
			defer wg.Done()

			producer.Start(ctx, make(chan *Message, 1))
		}()

		go func() {
			defer wg.Done()

			_ = producer.Stop()
		}()

		settled := make(chan struct{})

		go func() {
			defer close(settled)

			wg.Wait()
		}()

		select {
		case <-settled:
		case <-time.After(10 * time.Second):
			cancel()
			t.Fatalf("Start/Stop deadlocked on iteration %d", i)
		}

		// Whichever order they landed in, a follow-up Stop must still return.
		stopped := make(chan struct{})

		go func() {
			defer close(stopped)

			_ = producer.Stop()
		}()

		select {
		case <-stopped:
		case <-time.After(10 * time.Second):
			cancel()
			t.Fatalf("second Stop() hung on iteration %d", i)
		}

		cancel()
	}
}
