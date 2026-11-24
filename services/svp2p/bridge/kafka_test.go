package bridge

import (
	"context"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	inmemorykafka "github.com/bsv-blockchain/teranode/util/kafka/in_memory_kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// recordingFinalHandler collects every BlockFinalHandler call so a test can
// assert positively on what arrived. handleBlocksFinalMessage is called
// SYNCHRONOUSLY in the decode tests below, so "zero calls" after it returns
// is a deterministic fact about that one call, not a bounded wait racing an
// async delivery — the D5 trap (a wait for nothing cannot tell
// correctly-suppressed apart from slow) does not apply to those. It DOES
// apply once delivery goes through the real in-memory Kafka consumer
// (StartBlocksFinalConsumer's tests, further down), which is why those use
// require.Eventually against a positive condition rather than a fixed
// sleep-then-check.
type recordingFinalHandler struct {
	mu    sync.Mutex
	calls []finalCall
}

type finalCall struct {
	hash   chainhash.Hash
	header *wire.BlockHeader
}

func (r *recordingFinalHandler) handle(hash chainhash.Hash, header *wire.BlockHeader) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.calls = append(r.calls, finalCall{hash: hash, header: header})
}

func (r *recordingFinalHandler) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.calls)
}

func (r *recordingFinalHandler) at(i int) finalCall {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.calls[i]
}

func mustMarshalBlocksFinal(t *testing.T, header *model.BlockHeader) []byte {
	t.Helper()

	value, err := proto.Marshal(&kafkamessage.KafkaBlocksFinalTopicMessage{Header: header.Bytes()})
	require.NoError(t, err)

	return value
}

// ---------------------------------------------------------------------------
// handleBlocksFinalMessage: decode correctness, unit level.
// ---------------------------------------------------------------------------

func TestHandleBlocksFinalMessage(t *testing.T) {
	logger := ulogger.TestLogger{}

	t.Run("a well-formed message decodes and calls onFinal with the right hash and header", func(t *testing.T) {
		handler := &recordingFinalHandler{}

		header := model.GenesisBlockHeader
		msg := &kafka.KafkaMessage{
			Key:   []byte(header.Hash().String()),
			Value: mustMarshalBlocksFinal(t, header),
		}

		err := handleBlocksFinalMessage(logger, msg, handler.handle)
		require.NoError(t, err)
		require.Equal(t, 1, handler.len())

		call := handler.at(0)
		require.Equal(t, *header.Hash(), call.hash)
		require.Equal(t, header.Hash().String(), call.header.BlockHash().String())
		require.Equal(t, int32(header.Version), call.header.Version) //nolint:gosec // model.BlockHeader.Version is uint32; wire.BlockHeader.Version is int32
	})

	t.Run("a nil key is skipped, not retried, and calls onFinal for nothing", func(t *testing.T) {
		handler := &recordingFinalHandler{}

		err := handleBlocksFinalMessage(logger, &kafka.KafkaMessage{Key: nil, Value: []byte("irrelevant")}, handler.handle)
		require.NoError(t, err, "a parse failure must not be retried (legacy netsync/manager.go:3443-3475)")
		require.Zero(t, handler.len())
	})

	t.Run("a key that is not a hash is skipped", func(t *testing.T) {
		handler := &recordingFinalHandler{}

		err := handleBlocksFinalMessage(logger, &kafka.KafkaMessage{Key: []byte("not-a-hash"), Value: []byte("irrelevant")}, handler.handle)
		require.NoError(t, err)
		require.Zero(t, handler.len())
	})

	t.Run("a value that does not unmarshal as the topic message is skipped", func(t *testing.T) {
		handler := &recordingFinalHandler{}

		header := model.GenesisBlockHeader
		msg := &kafka.KafkaMessage{
			Key:   []byte(header.Hash().String()),
			Value: []byte("not a protobuf message"),
		}

		err := handleBlocksFinalMessage(logger, msg, handler.handle)
		require.NoError(t, err)
		require.Zero(t, handler.len())
	})

	t.Run("a header field that does not decode to 80 bytes is skipped", func(t *testing.T) {
		handler := &recordingFinalHandler{}

		header := model.GenesisBlockHeader
		value, err := proto.Marshal(&kafkamessage.KafkaBlocksFinalTopicMessage{Header: []byte{0x01, 0x02}})
		require.NoError(t, err)

		msg := &kafka.KafkaMessage{Key: []byte(header.Hash().String()), Value: value}

		err = handleBlocksFinalMessage(logger, msg, handler.handle)
		require.NoError(t, err)
		require.Zero(t, handler.len())
	})

	t.Run("a key that does not match the decoded header's own hash is skipped", func(t *testing.T) {
		handler := &recordingFinalHandler{}

		header := model.GenesisBlockHeader
		other := chainhash.Hash{0xAA}
		msg := &kafka.KafkaMessage{
			Key:   []byte(other.String()),
			Value: mustMarshalBlocksFinal(t, header),
		}

		err := handleBlocksFinalMessage(logger, msg, handler.handle)
		require.NoError(t, err)
		require.Zero(t, handler.len(), "a key/header mismatch must never relay the wrong hash")
	})
}

// ---------------------------------------------------------------------------
// StartBlocksFinalConsumer: wiring, over a real in-memory Kafka broker.
// ---------------------------------------------------------------------------

func TestStartBlocksFinalConsumer_NilConfigIsANoOp(t *testing.T) {
	tSettings := settings.NewSettings()
	tSettings.Kafka.BlocksFinalConfig = nil

	handler := &recordingFinalHandler{}

	consumer, err := StartBlocksFinalConsumer(t.Context(), ulogger.TestLogger{}, tSettings, handler.handle)
	require.NoError(t, err)
	require.Nil(t, consumer, "an unconfigured topic must not build a consumer at all")
}

// TestStartBlocksFinalConsumer_DecodesAndRelays is the Kafka leg end to end:
// a message produced onto the configured topic via the in-memory broker
// reaches onFinal decoded. Waiting on HasConsumer before producing (rather
// than a fixed sleep) rules out the flake this style of test is prone to —
// publishing before the in-memory consumer has subscribed, which the broker
// silently drops (see HasConsumer's own doc comment) — and the subsequent
// require.Eventually polls a positive condition (a call arrived) rather than
// a fixed wait, so it cannot pass on a race that just happened to resolve in
// time.
func TestStartBlocksFinalConsumer_DecodesAndRelays(t *testing.T) {
	const topic = "blocks-final-wiring-test"

	tSettings := settings.NewSettings()
	tSettings.ClientName = "svp2p-test"

	kafkaURL, err := url.Parse("memory://localhost:9092/" + topic)
	require.NoError(t, err)
	tSettings.Kafka.BlocksFinalConfig = kafkaURL

	t.Cleanup(func() { inmemorykafka.GetSharedBroker().DropTopic(topic) })

	handler := &recordingFinalHandler{}

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	consumer, err := StartBlocksFinalConsumer(ctx, ulogger.TestLogger{}, tSettings, handler.handle)
	require.NoError(t, err)
	require.NotNil(t, consumer)

	t.Cleanup(func() { require.NoError(t, consumer.Close()) })

	require.Eventually(t, func() bool {
		return inmemorykafka.GetSharedBroker().HasConsumer(topic)
	}, 5*time.Second, 10*time.Millisecond, "the in-memory consumer never subscribed")

	header := model.GenesisBlockHeader
	require.NoError(t, inmemorykafka.GetSharedBroker().Produce(ctx, topic,
		[]byte(header.Hash().String()), mustMarshalBlocksFinal(t, header)))

	require.Eventually(t, func() bool {
		return handler.len() >= 1
	}, 5*time.Second, 10*time.Millisecond, "the produced message never reached onFinal")

	call := handler.at(0)
	require.Equal(t, *header.Hash(), call.hash)
}

// TestStartBlocksFinalConsumer_CloseReturnsPromptlyWithMessageInFlight is
// what this test can actually establish about the D7 lifecycle contract: with
// a message already in flight, ctx cancellation followed by Close returns
// promptly rather than hanging. It does NOT establish that the underlying
// consumer goroutine has exited — fix round 1, review finding I3: against
// util/kafka/in_memory_kafka's own fake, it provably has not (Close's
// internal wg.Wait() is a no-op nothing ever Add()s to, and nothing inside
// the idle ConsumeClaim loop ever checks ctx — see the reasoning recorded in
// Server.go and bridge/kafka.go's own comments). Proving a real drain needs
// that infrastructure fixed first; this test proves the one thing that is
// true today: the call sequence Server.Stop makes does not deadlock.
func TestStartBlocksFinalConsumer_CloseReturnsPromptlyWithMessageInFlight(t *testing.T) {
	const topic = "blocks-final-shutdown-test"

	tSettings := settings.NewSettings()
	tSettings.ClientName = "svp2p-test"

	kafkaURL, err := url.Parse("memory://localhost:9092/" + topic)
	require.NoError(t, err)
	tSettings.Kafka.BlocksFinalConfig = kafkaURL

	t.Cleanup(func() { inmemorykafka.GetSharedBroker().DropTopic(topic) })

	handler := &recordingFinalHandler{}

	ctx, cancel := context.WithCancel(t.Context())

	consumer, err := StartBlocksFinalConsumer(ctx, ulogger.TestLogger{}, tSettings, handler.handle)
	require.NoError(t, err)
	require.NotNil(t, consumer)

	require.Eventually(t, func() bool {
		return inmemorykafka.GetSharedBroker().HasConsumer(topic)
	}, 5*time.Second, 10*time.Millisecond, "the in-memory consumer never subscribed")

	// A message already in flight when the service stops must not wedge
	// shutdown: produce it, then cancel immediately, then Close — mirroring
	// Server.Stop's own order (manager/consumers joined, ctx already
	// cancelled by the daemon).
	header := model.GenesisBlockHeader
	require.NoError(t, inmemorykafka.GetSharedBroker().Produce(ctx, topic,
		[]byte(header.Hash().String()), mustMarshalBlocksFinal(t, header)))

	cancel()

	done := make(chan error, 1)
	go func() { done <- consumer.Close() }()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Close never returned with a message in flight")
	}
}
