package kafka

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
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

	t.Run("context error from the handler returns it even on a live context", func(t *testing.T) {
		// A per-request deadline that expired inside the handler must not be
		// committed past either, even though the consumer's own context is live.
		wrapped := newLogErrorAndMoveOnWrapper(context.Background(), func(*KafkaMessage) error {
			return errors.NewProcessingError("giving up", context.DeadlineExceeded)
		})

		require.Error(t, wrapped(&KafkaMessage{}),
			"a context error surfaced by the handler must be left uncommitted")
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
}
