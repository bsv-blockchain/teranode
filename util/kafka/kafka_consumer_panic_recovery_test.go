package kafka

import (
	"context"
	"net/url"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	inmemorykafka "github.com/bsv-blockchain/teranode/util/kafka/in_memory_kafka"
	"github.com/stretchr/testify/require"
)

// panicRecoveryTopicSeq gives each run of TestConsumerRecoversFromPanickingHandler
// its own topic name on the process-wide shared in-memory broker. Without
// this, repeated runs in the same test binary (e.g. -count=N) reuse the same
// topic, and InMemoryConsumerGroup.Close does not actually wait for its
// Consume goroutine to deregister (mcg.wg is never Add()'d, so wg.Wait
// returns immediately) - a still-registered channel from the previous run
// can make the readiness check below pass for the wrong consumer.
var panicRecoveryTopicSeq atomic.Int64

// TestConsumerRecoversFromPanickingHandler proves a handler panic on the
// in-memory Kafka path does not take down the process (there is no recover()
// anywhere else in this package - see AGENTS-tracked defect on
// validatortxs) and that the consumer keeps making progress afterwards
// instead of wedging forever on the poison message.
func TestConsumerRecoversFromPanickingHandler(t *testing.T) {
	topic := "panic-recovery-topic-" + strconv.FormatInt(panicRecoveryTopicSeq.Add(1), 10)

	kafkaURL, err := url.Parse("memory://localhost/" + topic)
	require.NoError(t, err)

	logger := &mockLogger{}
	consumer, err := NewKafkaConsumerGroup(KafkaConsumerConfig{
		Logger:            logger,
		URL:               kafkaURL,
		Topic:             topic,
		ConsumerGroupID:   "panic-recovery-group",
		AutoCommitEnabled: true,
	})
	require.NoError(t, err)

	producer, err := NewKafkaAsyncProducer(logger, KafkaProducerConfig{
		Logger:     logger,
		URL:        kafkaURL,
		Topic:      topic,
		BrokersURL: []string{"localhost"},
	})
	require.NoError(t, err)

	producerCtx, producerCancel := context.WithCancel(context.Background())
	defer producerCancel()
	producer.Start(producerCtx, make(chan *Message, 8))

	var goodMessagesHandled atomic.Int32

	handler := func(msg *KafkaMessage) error {
		if string(msg.Key) == "poison" {
			panic("simulated handler panic on malformed message")
		}

		goodMessagesHandled.Add(1)

		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	consumer.Start(ctx, handler)

	// Start() registers the in-memory consumer channel asynchronously (two
	// goroutine hops: the Start goroutine, then InMemoryConsumerGroup.Consume).
	// The broker only broadcasts to channels already registered at Produce
	// time - it does not replay history to late subscribers - so publishing
	// before registration completes silently drops both messages below and
	// the test hangs on the Eventually below until it times out. Wait on the
	// broker's own bookkeeping instead of a sleep so this is deterministic.
	broker := inmemorykafka.GetSharedBroker()
	require.Eventually(t, func() bool {
		return broker.HasConsumer(topic)
	}, 5*time.Second, time.Millisecond, "consumer group never registered with the in-memory broker")

	// Poison message first: if the panic is not recovered, the test binary
	// itself crashes here and no assertion below ever runs.
	producer.Publish(&Message{Key: []byte("poison"), Value: []byte("boom")})

	// A well-formed message published after the poison one must still be
	// processed - proves the consumer goroutine is alive and not wedged
	// retrying the poison message forever.
	producer.Publish(&Message{Key: []byte("good"), Value: []byte("ok")})

	require.Eventually(t, func() bool {
		return goodMessagesHandled.Load() == 1
	}, 5*time.Second, 10*time.Millisecond, "consumer did not process the message following the panic - it is wedged or dead")

	_ = consumer.Close()
	_ = producer.Stop()
}
