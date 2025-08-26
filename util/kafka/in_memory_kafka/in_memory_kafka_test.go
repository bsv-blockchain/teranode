package inmemorykafka

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryBrokerProduceConsume(t *testing.T) {
	broker := NewInMemoryBroker()
	topic := "test-topic"
	messageValue := []byte("hello world")

	// Create Producer
	producer := NewInMemorySyncProducer(broker)
	defer func() {
		_ = producer.Close()
	}()

	// Create Consumer
	consumer, err := broker.NewInMemoryConsumer(topic)
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}
	defer func() {
		_ = consumer.Close()
	}()

	// Consume Partition (only partition 0 is supported in this mock)
	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest) // Start from newest for this test
	if err != nil {
		t.Fatalf("Failed to consume partition: %v", err)
	}
	defer func() {
		_ = partitionConsumer.Close()
	}()

	// Use a channel to signal message reception
	messageReceived := make(chan *sarama.ConsumerMessage, 1)
	errCh := make(chan error, 1) // Renamed errs to errCh

	// Start consumer goroutine
	go func() {
		select {
		case msg := <-partitionConsumer.Messages():
			messageReceived <- msg
		case consumerErr := <-partitionConsumer.Errors():
			// This channel is nil in the mock, so this case shouldn't be hit
			if consumerErr != nil { // Check if error is actually non-nil
				errCh <- consumerErr
			}
		case <-time.After(5 * time.Second): // Timeout
			errCh <- errors.NewServiceError("timeout waiting for message")
		}
	}()

	// Produce Message
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(messageValue),
	}

	_, _, err = producer.SendMessage(msg)
	if err != nil {
		t.Fatalf("Failed to send message: %v", err)
	}

	// Wait for message or error
	select {
	case receivedMsg := <-messageReceived:
		if receivedMsg.Topic != topic {
			t.Errorf("Received message with wrong topic: got %s, want %s", receivedMsg.Topic, topic)
		}

		if !bytes.Equal(receivedMsg.Value, messageValue) {
			t.Errorf("Received message with wrong value: got %s, want %s", string(receivedMsg.Value), string(messageValue))
		}

		// Check offset - should be 0 for the first message
		if receivedMsg.Offset != 0 {
			t.Errorf("Received message with wrong offset: got %d, want %d", receivedMsg.Offset, 0)
		}
	case consumerErr := <-errCh:
		t.Fatalf("Consumer error: %v", consumerErr)
	}
}

func TestInMemoryBrokerProduceToNewTopic(t *testing.T) {
	broker := NewInMemoryBroker()
	topic := "new-topic"
	messageValue := []byte("message for new topic")

	producer := NewInMemorySyncProducer(broker)
	defer func() {
		_ = producer.Close()
	}()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(messageValue),
	}

	_, _, err := producer.SendMessage(msg)
	if err != nil {
		t.Fatalf("Failed to send message to new topic: %v", err)
	}

	// Verify topic was created
	broker.mu.RLock()
	_, ok := broker.topics[topic]
	broker.mu.RUnlock()

	if !ok {
		t.Errorf("Topic '%s' was not created automatically after producing", topic)
	}
}

func TestInMemoryBrokerConsumeFromNewTopic(t *testing.T) {
	broker := NewInMemoryBroker()
	topic := "another-new-topic"

	consumer, err := broker.NewInMemoryConsumer(topic)
	if err != nil {
		t.Fatalf("Failed to create consumer for new topic: %v", err)
	}
	defer func() {
		_ = consumer.Close()
	}()

	// Verify topic was created
	broker.mu.RLock()
	_, ok := broker.topics[topic]
	broker.mu.RUnlock()

	if !ok {
		t.Errorf("Topic '%s' was not created automatically after consuming", topic)
	}
}

func TestInMemoryConsumerConsumePartitionError(t *testing.T) {
	broker := NewInMemoryBroker()
	topic := "test-topic-partition"

	// Produce a fake message to ensure a topic exists
	producer := NewInMemorySyncProducer(broker)
	msg := &sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder("dummy")}

	_, _, err := producer.SendMessage(msg) // Assign error from SendMessage
	if err != nil {
		t.Fatalf("Failed to produce setup message: %v", err)
	}

	_ = producer.Close()

	consumer, err := broker.NewInMemoryConsumer(topic)
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}
	defer func() {
		_ = consumer.Close()
	}()

	// Try consuming unsupported partition 1
	_, err = consumer.ConsumePartition(topic, 1, sarama.OffsetOldest)
	if err == nil {
		t.Errorf("Expected error when consuming unsupported partition 1, but got nil")
	} else {
		t.Logf("Got expected error for unsupported partition: %v", err) // Log success
	}
}

func TestInMemorySyncProducerUnimplementedMethods(t *testing.T) {
	broker := NewInMemoryBroker()
	producer := NewInMemorySyncProducer(broker)

	defer func() {
		_ = producer.Close()
	}()

	if err := producer.SendMessages(nil); err == nil {
		t.Error("Expected error for unimplemented SendMessages, got nil")
	}

	if err := producer.BeginTxn(); err == nil {
		t.Error("Expected error for unimplemented BeginTxn, got nil")
	}

	if err := producer.CommitTxn(); err == nil {
		t.Error("Expected error for unimplemented CommitTxn, got nil")
	}
}

func TestInMemoryAsyncProducerProduceSuccess(t *testing.T) {
	broker := NewInMemoryBroker()
	topic := "async-test-success"
	messageValue := "hello async world"
	msgToSend := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(messageValue),
	}

	// 1. Create Consumer FIRST to register its channel with the broker
	consumer, err := broker.NewInMemoryConsumer(topic)
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}

	defer func() {
		_ = consumer.Close()
	}()

	pConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest) // Read from start
	require.NoError(t, err)

	defer func() {
		_ = pConsumer.Close()
	}()

	// 2. Create Producer
	producer := NewInMemoryAsyncProducer(broker, 1) // Buffer size 1
	defer func() {
		_ = producer.Close()
	}()

	// Use WaitGroups to coordinate producer success and consumer reception
	var (
		wgProducer sync.WaitGroup
		wgConsumer sync.WaitGroup
	)

	wgProducer.Add(1)
	wgConsumer.Add(1)

	// Goroutine to wait for producer success
	go func() {
		defer wgProducer.Done()

		select {
		case successMsg := <-producer.Successes():
			assert.Equal(t, msgToSend, successMsg, "Success message should match sent message")
			// Add other assertions if needed
		case err := <-producer.Errors():
			t.Errorf("Expected success, but got error: %v", err)
		case <-time.After(2 * time.Second):
			t.Errorf("Timeout waiting for producer success message") // Use t.Errorf for goroutines
		}
	}()

	// Goroutine to wait for a consumer message
	go func() {
		defer wgConsumer.Done()

		select {
		case msg := <-pConsumer.Messages():
			assert.Equal(t, messageValue, string(msg.Value), "Consumed message value mismatch")
		case <-time.After(2 * time.Second): // Increased timeout slightly
			t.Errorf("Timeout waiting for consumer message") // Use t.Errorf for goroutines
		}
	}()

	// 3. Send the message AFTER consumer is ready
	producer.Input() <- msgToSend

	// 4. Wait for both producer and consumer operations to complete
	wgProducer.Wait()
	wgConsumer.Wait()
}

func TestInMemoryAsyncProducerProduceError(t *testing.T) {
	broker := NewInMemoryBroker()
	producer := NewInMemoryAsyncProducer(broker, 1)

	defer func() {
		_ = producer.Close()
	}()

	topic := "async-test-error"
	msgToSend := &sarama.ProducerMessage{
		Topic: topic,
		Value: FaultyEncoder{}, // Use the faulty encoder
	}

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()

		select {
		case <-producer.Successes():
			t.Error("Expected error, but got success")
		case prodErr := <-producer.Errors():
			assert.Error(t, prodErr.Err, "Expected an encoding error")
			assert.Equal(t, msgToSend, prodErr.Msg, "Error message should match sent message")
			assert.Contains(t, prodErr.Err.Error(), "failed to encode")
		case <-time.After(2 * time.Second):
			t.Error("Timeout waiting for error message")
		}
	}()

	producer.Input() <- msgToSend
	wg.Wait()
}

func TestInMemoryAsyncProducerClose(t *testing.T) {
	broker := NewInMemoryBroker()
	producer := NewInMemoryAsyncProducer(broker, 10) // Larger buffer

	topic := "async-test-close"
	msg1 := &sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder("msg1")}
	msg2 := &sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder("msg2")}

	producer.Input() <- msg1

	producer.Input() <- msg2

	// Give some time for messages to potentially be processed, although Close should handle it
	time.Sleep(50 * time.Millisecond)

	err := producer.Close()
	require.NoError(t, err)

	// Drain channels after close before checking closure state
	for range producer.Successes() {
		// Drain successes
	}

	for range producer.Errors() {
		// Drain errors
	}

	// Now check if channels are closed
	_, okSuccess := <-producer.Successes()
	assert.False(t, okSuccess, "Successes channel should be closed")

	_, okError := <-producer.Errors()
	assert.False(t, okError, "Errors channel should be closed")
}

type FaultyEncoder struct{}

// Encode simulates a faulty encoding process that always returns an error.
func (fe FaultyEncoder) Encode() ([]byte, error) {
	return nil, errors.New(errors.ERR_UNKNOWN, "failed to encode")
}

// Length returns 0, simulating a faulty encoder that does not produce valid output.
func (fe FaultyEncoder) Length() int {
	return 0
}
