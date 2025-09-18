package inmemorykafka

import (
	"bytes"
	"context"
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

// TestBrokerTopics tests the Topics() method
func TestBrokerTopics(t *testing.T) {
	broker := NewInMemoryBroker()

	// Initially no topics
	topics := broker.Topics()
	assert.Empty(t, topics, "Initially broker should have no topics")

	// Add some topics by producing messages
	producer := NewInMemorySyncProducer(broker)
	defer producer.Close()

	msg1 := &sarama.ProducerMessage{Topic: "topic1", Value: sarama.StringEncoder("msg1")}
	msg2 := &sarama.ProducerMessage{Topic: "topic2", Value: sarama.StringEncoder("msg2")}
	msg3 := &sarama.ProducerMessage{Topic: "topic1", Value: sarama.StringEncoder("msg3")} // Same topic

	_, _, err := producer.SendMessage(msg1)
	require.NoError(t, err)
	_, _, err = producer.SendMessage(msg2)
	require.NoError(t, err)
	_, _, err = producer.SendMessage(msg3)
	require.NoError(t, err)

	topics = broker.Topics()
	assert.Len(t, topics, 2, "Should have 2 unique topics")
	assert.Contains(t, topics, "topic1")
	assert.Contains(t, topics, "topic2")
}

// TestSyncProducerSendMessageKeyEncodingError tests key encoding error path
func TestSyncProducerSendMessageKeyEncodingError(t *testing.T) {
	broker := NewInMemoryBroker()
	producer := NewInMemorySyncProducer(broker)
	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: "test-topic",
		Key:   FaultyEncoder{}, // Faulty key encoder
		Value: sarama.StringEncoder("value"),
	}

	_, _, err := producer.SendMessage(msg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to encode key")
}

// TestSyncProducerUnimplementedMethodsCoverage tests all unimplemented methods
func TestSyncProducerUnimplementedMethodsCoverage(t *testing.T) {
	broker := NewInMemoryBroker()
	producer := NewInMemorySyncProducer(broker)
	defer producer.Close()

	// Test all unimplemented transaction methods
	assert.Error(t, producer.AddMessageToTxn(nil, "group", nil))
	assert.Error(t, producer.AddOffsetsToTxn(nil, "group"))
	assert.Error(t, producer.AbortTxn())

	// Test status methods
	assert.Equal(t, sarama.ProducerTxnFlagReady, producer.TxnStatus())
	assert.False(t, producer.IsTransactional())
}

// TestConsumerUnimplementedMethods tests unimplemented consumer methods
func TestConsumerUnimplementedMethods(t *testing.T) {
	broker := NewInMemoryBroker()
	consumer, err := broker.NewInMemoryConsumer("test-topic")
	require.NoError(t, err)
	defer consumer.Close()

	// Test unimplemented methods
	_, err = consumer.Topics()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Topics not implemented")

	_, err = consumer.Partitions("topic")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Partitions not implemented")

	// Test methods that don't return errors (no-op methods)
	assert.Nil(t, consumer.HighWaterMarks())
	consumer.Pause(nil)  // Should not panic
	consumer.PauseAll()  // Should not panic
	consumer.Resume(nil) // Should not panic
	consumer.ResumeAll() // Should not panic
}

// TestPartitionConsumerMethods tests partition consumer methods
func TestPartitionConsumerMethods(t *testing.T) {
	broker := NewInMemoryBroker()
	consumer, err := broker.NewInMemoryConsumer("test-topic")
	require.NoError(t, err)
	defer consumer.Close()

	partConsumer, err := consumer.ConsumePartition("test-topic", 0, sarama.OffsetOldest)
	require.NoError(t, err)
	defer partConsumer.Close()

	// Test Errors() method
	assert.Nil(t, partConsumer.Errors())

	// Test AsyncClose method
	partConsumer.AsyncClose() // Should not panic

	// Test HighWaterMarkOffset with no messages
	assert.Equal(t, int64(0), partConsumer.HighWaterMarkOffset())

	// Add a message and test HighWaterMarkOffset
	producer := NewInMemorySyncProducer(broker)
	msg := &sarama.ProducerMessage{Topic: "test-topic", Value: sarama.StringEncoder("test")}
	_, _, err = producer.SendMessage(msg)
	require.NoError(t, err)
	producer.Close()

	assert.Equal(t, int64(1), partConsumer.HighWaterMarkOffset())

	// Test pause/resume methods
	assert.False(t, partConsumer.IsPaused())
	partConsumer.Pause()  // Should not panic
	partConsumer.Resume() // Should not panic
}

// TestAsyncProducerNewWithZeroBuffer tests buffer size validation
func TestAsyncProducerNewWithZeroBuffer(t *testing.T) {
	broker := NewInMemoryBroker()
	producer := NewInMemoryAsyncProducer(broker, 0) // Zero buffer should default to 100
	defer producer.Close()

	// Test that it doesn't panic and has proper channels
	assert.NotNil(t, producer.Input())
	assert.NotNil(t, producer.Successes())
	assert.NotNil(t, producer.Errors())
}

// TestAsyncProducerMessageHandlerErrorPaths tests error paths in messageHandler
func TestAsyncProducerMessageHandlerErrorPaths(t *testing.T) {
	broker := NewInMemoryBroker()
	producer := NewInMemoryAsyncProducer(broker, 1)
	defer producer.Close()

	// Test with faulty key encoder
	msgWithFaultyKey := &sarama.ProducerMessage{
		Topic: "test-topic",
		Key:   FaultyEncoder{},
		Value: sarama.StringEncoder("value"),
	}

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		select {
		case err := <-producer.Errors():
			assert.Error(t, err.Err)
			assert.Contains(t, err.Err.Error(), "failed to encode key")
			assert.Equal(t, msgWithFaultyKey, err.Msg)
		case <-time.After(2 * time.Second):
			t.Error("Timeout waiting for error")
		}
	}()

	producer.Input() <- msgWithFaultyKey
	wg.Wait()
}

// TestAsyncProducerUnimplementedMethods tests unimplemented async producer methods
func TestAsyncProducerUnimplementedMethods(t *testing.T) {
	broker := NewInMemoryBroker()
	producer := NewInMemoryAsyncProducer(broker, 1)
	defer producer.Close()

	// Test transaction methods
	assert.False(t, producer.IsTransactional())
	assert.Equal(t, sarama.ProducerTxnFlagReady, producer.TxnStatus())
	assert.Error(t, producer.BeginTxn())
	assert.Error(t, producer.CommitTxn())
	assert.Error(t, producer.AbortTxn())
	assert.Error(t, producer.AddMessageToTxn(nil, "group", nil))
	assert.Error(t, producer.AddOffsetsToTxn(nil, "group"))
}

// TestGetSharedBroker tests the singleton broker
func TestGetSharedBroker(t *testing.T) {
	broker1 := GetSharedBroker()
	broker2 := GetSharedBroker()

	// Should return the same instance
	assert.Equal(t, broker1, broker2)
	assert.NotNil(t, broker1)
}

// TestConsumerGroupBasicFunctionality tests consumer group creation and basic methods
func TestConsumerGroupBasicFunctionality(t *testing.T) {
	broker := NewInMemoryBroker()
	cg := NewInMemoryConsumerGroup(broker, "test-topic", "test-group")
	defer cg.Close()

	// Test basic getters
	assert.NotNil(t, cg.Errors())

	// Test pause/resume methods (no-ops)
	cg.PauseAll()
	cg.ResumeAll()
	cg.Pause(nil)
	cg.Resume(nil)
}

// TestConsumerGroupCloseWithoutRunning tests closing a non-running consumer group
func TestConsumerGroupCloseWithoutRunning(t *testing.T) {
	broker := NewInMemoryBroker()
	cg := NewInMemoryConsumerGroup(broker, "test-topic", "test-group")

	// Close without running Consume
	err := cg.Close()
	assert.NoError(t, err)

	// Second close should also not error
	err = cg.Close()
	assert.NoError(t, err)
}

// MockConsumerGroupHandler for testing
type MockConsumerGroupHandler struct {
	setupCalled   bool
	cleanupCalled bool
	consumeError  error
	setupError    error
	cleanupError  error
	messageCount  int
	maxMessages   int
}

func (h *MockConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	h.setupCalled = true
	return h.setupError
}

func (h *MockConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	h.cleanupCalled = true
	return h.cleanupError
}

func (h *MockConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	if h.consumeError != nil {
		return h.consumeError
	}

	for {
		select {
		case msg := <-claim.Messages():
			if msg == nil {
				return nil // Channel closed
			}
			h.messageCount++
			if h.maxMessages > 0 && h.messageCount >= h.maxMessages {
				return nil // Stop after max messages
			}
		case <-session.Context().Done():
			return session.Context().Err()
		}
	}
}

// TestConsumerGroupConsumeMultipleTopics tests error for multiple topics
func TestConsumerGroupConsumeMultipleTopics(t *testing.T) {
	broker := NewInMemoryBroker()
	cg := NewInMemoryConsumerGroup(broker, "test-topic", "test-group")
	defer cg.Close()

	handler := &MockConsumerGroupHandler{}

	err := cg.Consume(context.Background(), []string{"topic1", "topic2"}, handler)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "only supports exactly one topic")
}

// TestConsumerGroupConsumeSetupError tests handler setup error
func TestConsumerGroupConsumeSetupError(t *testing.T) {
	broker := NewInMemoryBroker()
	cg := NewInMemoryConsumerGroup(broker, "test-topic", "test-group")
	defer cg.Close()

	handler := &MockConsumerGroupHandler{
		setupError: errors.New(errors.ERR_UNKNOWN, "setup failed"),
	}

	err := cg.Consume(context.Background(), []string{"test-topic"}, handler)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "handler setup failed")
	assert.True(t, handler.setupCalled)
}

// TestConsumerGroupConsumeCleanupError tests handler cleanup error
func TestConsumerGroupConsumeCleanupError(t *testing.T) {
	broker := NewInMemoryBroker()
	cg := NewInMemoryConsumerGroup(broker, "test-topic", "test-group")
	defer cg.Close()

	handler := &MockConsumerGroupHandler{
		cleanupError: errors.New(errors.ERR_UNKNOWN, "cleanup failed"),
		consumeError: errors.New(errors.ERR_UNKNOWN, "consume done"), // Return immediately
	}

	err := cg.Consume(context.Background(), []string{"test-topic"}, handler)
	// The primary error should be from ConsumeClaim, but cleanup error should be logged
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "consume done") // Primary error from ConsumeClaim
	assert.True(t, handler.setupCalled)
	assert.True(t, handler.cleanupCalled)
}

// TestConsumerGroupConsumeContextCancel tests context cancellation
func TestConsumerGroupConsumeContextCancel(t *testing.T) {
	broker := NewInMemoryBroker()
	cg := NewInMemoryConsumerGroup(broker, "test-topic", "test-group")
	defer cg.Close()

	handler := &MockConsumerGroupHandler{}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err := cg.Consume(ctx, []string{"test-topic"}, handler)
	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

// TestConsumerGroupSessionMethods tests session interface methods
func TestConsumerGroupSessionMethods(t *testing.T) {
	broker := NewInMemoryBroker()

	// Add some topics
	producer := NewInMemorySyncProducer(broker)
	msg1 := &sarama.ProducerMessage{Topic: "topic1", Value: sarama.StringEncoder("test1")}
	msg2 := &sarama.ProducerMessage{Topic: "topic2", Value: sarama.StringEncoder("test2")}
	_, _, err := producer.SendMessage(msg1)
	require.NoError(t, err)
	_, _, err = producer.SendMessage(msg2)
	require.NoError(t, err)
	producer.Close()

	session := &InMemoryConsumerGroupSession{
		ctx:     context.Background(),
		broker:  broker,
		groupID: "test-group",
	}

	// Test Claims()
	claims := session.Claims()
	assert.Len(t, claims, 2)
	assert.Contains(t, claims, "topic1")
	assert.Contains(t, claims, "topic2")
	assert.Equal(t, []int32{0}, claims["topic1"])

	// Test other methods (no-ops)
	assert.Equal(t, "mock-member-test-group", session.MemberID())
	assert.Equal(t, int32(1), session.GenerationID())
	assert.Equal(t, context.Background(), session.Context())

	// Test no-op methods
	session.MarkOffset("topic", 0, 0, "")
	session.Commit()
	session.ResetOffset("topic", 0, 0, "")
	session.MarkMessage(nil, "")
}

// TestConsumerGroupClaimMethods tests claim interface methods
func TestConsumerGroupClaimMethods(t *testing.T) {
	broker := NewInMemoryBroker()
	consumer, err := broker.NewInMemoryConsumer("test-topic")
	require.NoError(t, err)
	defer consumer.Close()

	partConsumer, err := consumer.ConsumePartition("test-topic", 0, sarama.OffsetOldest)
	require.NoError(t, err)
	defer partConsumer.Close()

	claim := &InMemoryConsumerGroupClaim{
		topic:        "test-topic",
		partition:    0,
		partConsumer: partConsumer,
	}

	assert.Equal(t, "test-topic", claim.Topic())
	assert.Equal(t, int32(0), claim.Partition())
	assert.Equal(t, sarama.OffsetOldest, claim.InitialOffset())
	assert.Equal(t, int64(0), claim.HighWaterMarkOffset())
	assert.NotNil(t, claim.Messages())
}

// TestConsumerGroupClaimWithNilPartConsumer tests claim with nil partition consumer
func TestConsumerGroupClaimWithNilPartConsumer(t *testing.T) {
	claim := &InMemoryConsumerGroupClaim{
		topic:        "test-topic",
		partition:    0,
		partConsumer: nil,
	}

	assert.Equal(t, int64(0), claim.HighWaterMarkOffset())

	// Messages should return a closed channel
	msgChan := claim.Messages()
	assert.NotNil(t, msgChan)

	// Channel should be closed (reading should return nil, false)
	msg, ok := <-msgChan
	assert.Nil(t, msg)
	assert.False(t, ok)
}

// TestConsumerCloseBehavior tests consumer close behavior with multiple calls
func TestConsumerCloseBehavior(t *testing.T) {
	broker := NewInMemoryBroker()
	consumer, err := broker.NewInMemoryConsumer("test-topic")
	require.NoError(t, err)

	// First close should succeed
	err = consumer.Close()
	assert.NoError(t, err)

	// Second close should also succeed (sync.Once behavior)
	err = consumer.Close()
	assert.NoError(t, err)
}

// TestSyncProducerSendMessageErrorPaths tests error paths in SendMessage
func TestSyncProducerSendMessageErrorPaths(t *testing.T) {
	broker := NewInMemoryBroker()
	producer := NewInMemorySyncProducer(broker)
	defer producer.Close()

	// Test value encoding error
	msg := &sarama.ProducerMessage{
		Topic: "test-topic",
		Value: FaultyEncoder{}, // Faulty value encoder
	}

	_, _, err := producer.SendMessage(msg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to encode")
}

// TestConsumerGroupSessionNoOpMethods tests no-op session methods for coverage
func TestConsumerGroupSessionNoOpMethods(t *testing.T) {
	broker := NewInMemoryBroker()
	session := &InMemoryConsumerGroupSession{
		ctx:     context.Background(),
		broker:  broker,
		groupID: "test-group",
	}

	// Test no-op methods for coverage
	assert.NotPanics(t, func() {
		session.MarkOffset("topic", 0, 100, "metadata")
		session.Commit()
		session.ResetOffset("topic", 0, 50, "metadata")
		session.MarkMessage(nil, "metadata")
	})
}

// TestConsumerGroupPauseResumeMethods tests pause/resume methods for coverage
func TestConsumerGroupPauseResumeMethods(t *testing.T) {
	broker := NewInMemoryBroker()
	cg := NewInMemoryConsumerGroup(broker, "test-topic", "test-group")
	defer cg.Close()

	// Test pause/resume methods (no-ops)
	partitions := map[string][]int32{"topic1": {0, 1}, "topic2": {0}}
	assert.NotPanics(t, func() {
		cg.PauseAll()
		cg.ResumeAll()
		cg.Pause(partitions)
		cg.Resume(partitions)
	})
}

// TestPartitionConsumerPauseResumeMethods tests partition consumer pause/resume
func TestPartitionConsumerPauseResumeMethods(t *testing.T) {
	broker := NewInMemoryBroker()
	consumer, err := broker.NewInMemoryConsumer("test-topic")
	require.NoError(t, err)
	defer consumer.Close()

	// Test consumer pause/resume methods (no-ops)
	partitions := map[string][]int32{"topic1": {0, 1}}
	assert.NotPanics(t, func() {
		consumer.Pause(partitions)
		consumer.PauseAll()
		consumer.Resume(partitions)
		consumer.ResumeAll()
	})

	// Test partition consumer methods
	partConsumer, err := consumer.ConsumePartition("test-topic", 0, sarama.OffsetOldest)
	require.NoError(t, err)
	defer partConsumer.Close()

	assert.NotPanics(t, func() {
		partConsumer.Pause()
		partConsumer.Resume()
	})
}

// TestHighWaterMarkOffsetWithNonExistentTopic tests HighWaterMarkOffset edge case
func TestHighWaterMarkOffsetWithNonExistentTopic(t *testing.T) {
	broker := NewInMemoryBroker()
	consumer, err := broker.NewInMemoryConsumer("test-topic")
	require.NoError(t, err)
	defer consumer.Close()

	partConsumer, err := consumer.ConsumePartition("test-topic", 0, sarama.OffsetOldest)
	require.NoError(t, err)
	defer partConsumer.Close()

	// Test with non-existent topic in broker's internal state
	// This tests the "topic not found" case in HighWaterMarkOffset
	pc := partConsumer.(*InMemoryPartitionConsumer)
	pc.consumer.topic = "non-existent-topic" // Change to non-existent topic

	offset := partConsumer.HighWaterMarkOffset()
	assert.Equal(t, int64(0), offset) // Should return 0 for non-existent topic
}

// TestAsyncProducerCloseChannelPath tests the close channel in messageHandler
func TestAsyncProducerCloseChannelPath(t *testing.T) {
	broker := NewInMemoryBroker()
	producer := NewInMemoryAsyncProducer(broker, 1)

	// Just test normal close to cover the close paths
	err := producer.Close()
	assert.NoError(t, err)
}

// TestConsumerGroupCloseNotRunning tests Close() when not running
func TestConsumerGroupCloseNotRunning(t *testing.T) {
	broker := NewInMemoryBroker()
	cg := NewInMemoryConsumerGroup(broker, "test-topic", "test-group")

	// Close without ever running Consume
	err := cg.Close()
	assert.NoError(t, err)

	// Second close should also work
	err = cg.Close()
	assert.NoError(t, err)
}
