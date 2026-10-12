package inmemorykafka

import (
	"context"
	"runtime"
	"testing"
	"time"
	"weak"

	"github.com/stretchr/testify/require"
)

// TestInMemoryBrokerDoesNotRetainProducedMessages pins a production memory leak. The broker kept
// every message it had ever been given, in a history nothing reads, so on a node running Kafka
// in memory each published message stayed on the heap for good. On mainnet, once the sync
// passed checkpoint 945,000 and the validator began publishing transaction metadata, those
// messages reached 6.9 GB, 88% of the live heap, by height 948,797 on 2026-09-30; the garbage
// collector took 68% of the CPU and block validation slowed tenfold.
//
// A message must be collectable once it has been handed to the consumers there are.
func TestInMemoryBrokerDoesNotRetainProducedMessages(t *testing.T) {
	broker := NewInMemoryBroker()

	value := make([]byte, 1<<20)
	held := weak.Make(&value[0])

	require.NoError(t, broker.Produce(context.Background(), "topic", nil, value))

	value = nil //nolint:ineffassign,wastedassign // dropping the test's own reference is the point

	runtime.GC()
	runtime.GC()

	require.Nil(t, held.Value(), "the broker must not keep a produced message alive")

	// The broker itself stays reachable to here, as it does in a running node; without this the
	// collector frees the whole broker and the check above passes whatever the broker retains.
	runtime.KeepAlive(broker)
}

// TestInMemoryBrokerOffsetsKeepCountingWithoutHistory pins that offsets, which were the length of
// the retained history, still start at zero and rise by one per message per topic.
func TestInMemoryBrokerOffsetsKeepCountingWithoutHistory(t *testing.T) {
	broker := NewInMemoryBroker()
	topic := "offsets"

	received := make(chan *Message, 3)
	cg := NewInMemoryConsumerGroup(broker, topic, "group")
	defer cg.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = cg.Consume(ctx, []string{topic}, &captureHandler{received: received}) }()

	require.Eventually(t, func() bool { return broker.HasConsumer(topic) }, 2*time.Second, 5*time.Millisecond)

	for i := 0; i < 3; i++ {
		require.NoError(t, broker.Produce(ctx, topic, nil, []byte{byte(i)}))
	}

	for want := int64(0); want < 3; want++ {
		select {
		case msg := <-received:
			require.Equal(t, want, msg.Offset)
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for offset %d", want)
		}
	}
}
