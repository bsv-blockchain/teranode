package kafka

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	inmemorykafka "github.com/bsv-blockchain/teranode/util/kafka/in_memory_kafka"
	"github.com/stretchr/testify/require"
)

// newLogErrorAndMoveOnWrapper builds the WithLogErrorAndMoveOn wrapper directly, so
// the commit decision can be asserted on the wrapper's RETURN VALUE — which is
// exactly what the per-partition goroutine keys its mark/commit on — without
// standing up a broker.
func newLogErrorAndMoveOnWrapper(ctx context.Context, fn func(*KafkaMessage) error) func(*KafkaMessage) error {
	options := &consumerOptions{}
	WithLogErrorAndMoveOn()(options)

	return wrapConsumerFn(ctx, ulogger.TestLogger{}, "test-topic", fn, options)
}

// TestLogErrorAndMoveOn_ReturnsErrorAfterCancel pins the shutdown carve-out.
//
// Before it, a handler failure at shutdown was logged and swallowed: the wrapper
// returned nil, the per-partition goroutine marked the record for commit and the
// shutdown drain committed it. A transaction whose validation was cancelled
// mid-processing — already spent and created in the UTXO store, never handed to
// block assembly — was therefore never redelivered.
//
// The move-on behaviour for ordinary failures must be preserved exactly: a routine
// bad message still returns nil and the consumer advances past it.
func TestLogErrorAndMoveOn_ReturnsErrorAfterCancel(t *testing.T) {
	handlerErr := errors.NewProcessingError("handler failed")

	t.Run("cancelled context returns the error uncommitted", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		wrapped := newLogErrorAndMoveOnWrapper(ctx, func(*KafkaMessage) error { return handlerErr })

		err := wrapped(&KafkaMessage{})
		require.Error(t, err, "a failure while cancelled must be returned so the offset is not committed")
		require.ErrorIs(t, err, handlerErr)
	})

	t.Run("live context still moves on", func(t *testing.T) {
		wrapped := newLogErrorAndMoveOnWrapper(context.Background(), func(*KafkaMessage) error { return handlerErr })

		require.NoError(t, wrapped(&KafkaMessage{}),
			"a routine failure on a live consumer must still be logged and skipped")
	})

	t.Run("handler context error on a live consumer still moves on", func(t *testing.T) {
		// A per-request deadline that expired inside a handler on a healthy,
		// running consumer must take the ordinary skip path. Returning it would
		// make the per-partition goroutine abandon every remaining record in the
		// fetch — and, because commits are highest-offset-wins per partition, a
		// later successful batch would commit past the abandoned ones. It also
		// terminates the in-memory consumer, which has no restart. The carve-out
		// is keyed on the consumer's own context and nothing else.
		wrapped := newLogErrorAndMoveOnWrapper(context.Background(), func(*KafkaMessage) error {
			return errors.NewProcessingError("giving up", context.DeadlineExceeded)
		})

		require.NoError(t, wrapped(&KafkaMessage{}),
			"a handler-surfaced context error on a live consumer must be skipped, not returned")
	})

	t.Run("success is unaffected", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		wrapped := newLogErrorAndMoveOnWrapper(ctx, func(*KafkaMessage) error { return nil })

		require.NoError(t, wrapped(&KafkaMessage{}), "a successful handler commits as usual, cancelled or not")
	})
}

// TestKafkaConsumer_PauseResumeAfterCloseAreNoOps pins that a closed consumer is
// inert to pause/resume.
//
// closeClient deliberately does NOT nil k.client (that would race the in-flight
// PollFetches), so the nil guard alone let a caller with its own lifecycle — a
// backpressure controller bound to the wrong context, for instance — keep driving
// pause/resume against a closed franz-go client. The closed flag is what makes the
// consumer inert; this asserts both kinds of consumer honour it and neither panics.
func TestKafkaConsumer_PauseResumeAfterCloseAreNoOps(t *testing.T) {
	t.Run("in-memory consumer", func(t *testing.T) {
		kafkaURL, err := url.Parse("memory://localhost/close-noop-topic")
		require.NoError(t, err)

		consumer, err := NewKafkaConsumerGroupFromURL(ulogger.TestLogger{}, kafkaURL, "close-noop-group", true, nil)
		require.NoError(t, err)

		require.False(t, consumer.isClosed(), "precondition: a fresh consumer is not closed")

		// The close outcome itself is not the subject here (an unstarted in-memory
		// consumer may report one); what matters is that the flag is set either way,
		// before the underlying consumer is torn down.
		_ = consumer.Close()
		require.True(t, consumer.isClosed(), "Close must mark the consumer closed on the in-memory path too")

		require.NotPanics(t, func() {
			consumer.PauseAll()
			consumer.ResumeAll()
		}, "pause/resume on a closed consumer must be a no-op, not a panic")
	})

	t.Run("franz-go consumer with no client dialled", func(t *testing.T) {
		// closeClient marks closed whether or not a client was ever dialled, so the
		// guard is what stops the call reaching the client — and it holds even for a
		// consumer that never had one.
		consumer := &KafkaConsumerGroup{
			Config: KafkaConsumerConfig{
				Logger:          ulogger.TestLogger{},
				Topic:           "close-noop-topic",
				ConsumerGroupID: "close-noop-group",
			},
		}

		consumer.closeClient()
		require.True(t, consumer.isClosed())

		require.NotPanics(t, func() {
			consumer.PauseAll()
			consumer.ResumeAll()
		})
	})

	t.Run("in-memory consumer marks itself closed when its context is cancelled", func(t *testing.T) {
		// The lifecycle can end without Close(): the parent context is cancelled while
		// the consumer keeps running. From that moment it must count as inert, or a late
		// pause/resume from a caller with its own lifecycle — the validator's
		// backpressure controller — reaches a consumer that is being torn down.
		//
		// Note that InMemoryConsumerGroup.Consume does NOT return on cancellation, so
		// the flag cannot come from the consume goroutine exiting; startInMemory watches
		// the context for exactly this reason.
		const topic = "close-noop-ctx-cancel-topic"

		broker := inmemorykafka.GetSharedBroker()
		broker.DropTopic(topic)

		kafkaURL, err := url.Parse("memory://localhost/" + topic)
		require.NoError(t, err)

		consumer, err := NewKafkaConsumerGroupFromURL(ulogger.TestLogger{}, kafkaURL, topic+"-group", true, nil)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())

		t.Cleanup(func() {
			cancel()
			_ = consumer.Close()
			broker.DropTopic(topic)
		})

		consumer.Start(ctx, func(*KafkaMessage) error { return nil }, WithLogErrorAndMoveOn())

		require.Eventually(t, func() bool { return broker.HasConsumer(topic) }, 2*time.Second, 5*time.Millisecond)
		require.False(t, consumer.isClosed(), "precondition: a running consumer is not closed")

		cancel()

		require.Eventually(t, func() bool { return consumer.isClosed() }, 2*time.Second, 5*time.Millisecond,
			"a context-cancelled in-memory consumer must mark itself closed even though Close() never ran")

		require.NotPanics(t, func() {
			consumer.PauseAll()
			consumer.ResumeAll()
		})
	})
}

// TestInMemoryConsumer_CloseCancelsTheInternalContext pins the property C10 is about:
// Close() must cancel the context the in-memory arm's watcher and wrapper are bound to,
// even when the parent context is never cancelled.
//
// Before the fix, k.cancel was assigned only in the franz-go branch — which sits after
// Start's isInMemory early return — so on this path Close()'s cancelFn was nil and
// cancelled nothing at all. A consumer started on context.Background() therefore parked
// its watcher for the process lifetime, and the wrapper's shutdown carve-out could never
// fire either.
//
// Asserted through the wrapper's OBSERVABLE behaviour rather than by counting
// goroutines. The carve-out is keyed on ctx.Err(): a handler failure is skipped while the
// context is live and RETURNED once it is done, and a returned error ends ConsumeClaim.
// So "does Close() cancel the internal context" becomes "does a handler failure after
// Close() stop the consumer" — a counter comparison, with none of the noise of a
// process-global goroutine count. (An earlier version of this test did count goroutines
// and could never pass: require.Eventually evaluates its condition in a spawned
// goroutine, so the sample taken inside the condition included that goroutine and
// exactly cancelled out the watcher's exit.)
func TestInMemoryConsumer_CloseCancelsTheInternalContext(t *testing.T) {
	const topic = "close-cancels-internal-ctx-topic"

	broker := inmemorykafka.GetSharedBroker()
	broker.DropTopic(topic)
	t.Cleanup(func() { broker.DropTopic(topic) })

	kafkaURL, err := url.Parse("memory://localhost/" + topic)
	require.NoError(t, err)

	consumer, err := NewKafkaConsumerGroupFromURL(ulogger.TestLogger{}, kafkaURL, topic+"-group", true, nil)
	require.NoError(t, err)

	var (
		mu   sync.Mutex
		seen []string
	)

	seenCount := func() int {
		mu.Lock()
		defer mu.Unlock()

		return len(seen)
	}

	// Deliberately NOT cancellable: Close() is the only shutdown available.
	consumer.Start(context.Background(), func(msg *KafkaMessage) error {
		key := string(msg.Key)

		mu.Lock()
		seen = append(seen, key)
		mu.Unlock()

		if strings.HasPrefix(key, "fail") {
			// A plain failure, carrying no context error of its own: the carve-out must
			// key on the consumer's context, not on the shape of the handler's error.
			return errors.NewProcessingError("handler refused %s", key)
		}

		return nil
	}, WithLogErrorAndMoveOn())

	require.Eventually(t, func() bool { return broker.HasConsumer(topic) }, 2*time.Second, 5*time.Millisecond)
	require.False(t, consumer.isClosed(), "precondition: a running consumer is not closed")

	// While the context is live a failure is skipped and the consumer carries on.
	require.NoError(t, broker.Produce(context.Background(), topic, []byte("fail-1"), []byte("v")))
	require.NoError(t, broker.Produce(context.Background(), topic, []byte("ok-1"), []byte("v")))

	require.Eventually(t, func() bool { return seenCount() == 2 }, 5*time.Second, 5*time.Millisecond,
		"a handler failure on a live consumer is skipped, so the next record still arrives")

	require.NoError(t, consumer.Close())
	require.True(t, consumer.isClosed())

	// The consumer channel is still registered — InMemoryConsumerGroup.Close() never
	// closes it — so records produced now are still delivered to the handler.
	require.NoError(t, broker.Produce(context.Background(), topic, []byte("fail-2"), []byte("v")))

	require.Eventually(t, func() bool { return seenCount() == 3 }, 5*time.Second, 5*time.Millisecond,
		"the record after Close is still delivered")

	// That failure was returned rather than skipped, because the internal context is now
	// done — which ends ConsumeClaim. Nothing produced afterwards can be handled. On the
	// unfixed code Close() cancelled nothing, the failure was skipped, and this record
	// WOULD have been handled.
	require.NoError(t, broker.Produce(context.Background(), topic, []byte("ok-2"), []byte("v")))

	require.Never(t, func() bool { return seenCount() > 3 }, 500*time.Millisecond, 20*time.Millisecond,
		"Close() must cancel the internal context, so a post-Close failure is returned and stops the consumer")
}

// TestInMemoryConsumer_HandlerContextErrorDoesNotSkipRecordsOrKillConsumer drives the
// REAL in-memory consume loop, which is where the cost of a non-nil handler return
// actually lands — the wrapper tests above assert the commit decision in isolation.
//
// Before the carve-out was narrowed to ctx.Err(), a handler error whose chain merely
// contained a context error was returned from the wrapper. In the loop that made
// ConsumeClaim return, which propagates out of Consume, and the consume goroutine
// only logs and exits: one such error permanently killed the consumer that the whole
// test suite and every dev/Docker deployment runs on. It also abandoned the rest of
// the batch.
//
// So: a handler that fails one message with a DeadlineExceeded-wrapped error on a
// healthy consumer must still see every later message, and the consumer must still be
// alive for messages produced afterwards.
func TestInMemoryConsumer_HandlerContextErrorDoesNotSkipRecordsOrKillConsumer(t *testing.T) {
	const (
		topic      = "inmemory-ctxerr-survival-topic"
		firstBatch = 10
		failAt     = 3
	)

	broker := inmemorykafka.GetSharedBroker()
	broker.DropTopic(topic)

	kafkaURL, err := url.Parse("memory://localhost/" + topic)
	require.NoError(t, err)

	consumer, err := NewKafkaConsumerGroupFromURL(ulogger.TestLogger{}, kafkaURL, topic+"-group", true, nil)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		_ = consumer.Close()
		broker.DropTopic(topic)
	})

	var (
		mu   sync.Mutex
		seen []string
	)

	consumer.Start(ctx, func(msg *KafkaMessage) error {
		key := string(msg.Key)

		mu.Lock()
		seen = append(seen, key)
		mu.Unlock()

		if key == fmt.Sprintf("k%d", failAt) {
			// A per-request deadline that expired inside the handler, on a consumer
			// whose own context is perfectly live.
			return errors.NewProcessingError("handler deadline for %s", key, context.DeadlineExceeded)
		}

		return nil
	}, WithLogErrorAndMoveOn())

	require.Eventually(t, func() bool { return broker.HasConsumer(topic) }, 2*time.Second, 5*time.Millisecond)

	seenCount := func() int {
		mu.Lock()
		defer mu.Unlock()

		return len(seen)
	}

	for i := 0; i < firstBatch; i++ {
		require.NoError(t, broker.Produce(ctx, topic, []byte(fmt.Sprintf("k%d", i)), []byte("v")))
	}

	require.Eventually(t, func() bool { return seenCount() == firstBatch }, 5*time.Second, 5*time.Millisecond,
		"every record after the failing one must still be delivered")

	// The consumer must still be alive: a message produced after the failure is
	// consumed too.
	require.NoError(t, broker.Produce(ctx, topic, []byte("after"), []byte("v")))

	require.Eventually(t, func() bool { return seenCount() == firstBatch+1 }, 5*time.Second, 5*time.Millisecond,
		"one handler context error must not terminate the consumer")

	mu.Lock()
	defer mu.Unlock()

	require.Equal(t, "after", seen[len(seen)-1])
	require.Contains(t, seen, fmt.Sprintf("k%d", failAt), "the failing record was delivered to the handler")
}
