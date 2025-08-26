package kafka

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewKafkaAsyncProducerMock(t *testing.T) {
	mock := NewKafkaAsyncProducerMock()

	assert.NotNil(t, mock)
	assert.NotNil(t, mock.publishChannel)
	assert.Equal(t, 100, cap(mock.publishChannel)) // Buffer size should be 100
}

func TestKafkaAsyncProducerMockStart(t *testing.T) {
	mock := NewKafkaAsyncProducerMock()
	ctx := context.Background()
	ch := make(chan *Message, 10)

	// Start should not panic and should complete without blocking
	assert.NotPanics(t, func() {
		mock.Start(ctx, ch)
	})
}

func TestKafkaAsyncProducerMockStop(t *testing.T) {
	mock := NewKafkaAsyncProducerMock()

	err := mock.Stop()

	assert.NoError(t, err)
}

func TestKafkaAsyncProducerMockBrokersURL(t *testing.T) {
	mock := NewKafkaAsyncProducerMock()

	brokers := mock.BrokersURL()

	assert.Nil(t, brokers)
}

func TestKafkaAsyncProducerMockPublishChannel(t *testing.T) {
	mock := NewKafkaAsyncProducerMock()

	ch := mock.PublishChannel()

	assert.Equal(t, mock.publishChannel, ch)
	assert.NotNil(t, ch)
	assert.Equal(t, 100, cap(ch))
}

func TestKafkaAsyncProducerMockPublish(t *testing.T) {
	mock := NewKafkaAsyncProducerMock()
	msg := &Message{
		Key:   []byte("test-key"),
		Value: []byte("test-value"),
	}

	// Publish should not block
	mock.Publish(msg)

	// Verify message was published to the channel
	select {
	case receivedMsg := <-mock.publishChannel:
		assert.Equal(t, msg.Key, receivedMsg.Key)
		assert.Equal(t, msg.Value, receivedMsg.Value)
	default:
		t.Fatal("Message was not published to channel")
	}
}

func TestKafkaAsyncProducerMockPublishMultiple(t *testing.T) {
	mock := NewKafkaAsyncProducerMock()

	messages := []*Message{
		{Key: []byte("key1"), Value: []byte("value1")},
		{Key: []byte("key2"), Value: []byte("value2")},
		{Key: []byte("key3"), Value: []byte("value3")},
	}

	// Publish multiple messages
	for _, msg := range messages {
		mock.Publish(msg)
	}

	// Verify all messages were published
	receivedMessages := make([]*Message, 0, len(messages))
	for i := 0; i < len(messages); i++ {
		select {
		case receivedMsg := <-mock.publishChannel:
			receivedMessages = append(receivedMessages, receivedMsg)
		default:
			t.Fatalf("Expected %d messages, only received %d", len(messages), len(receivedMessages))
		}
	}

	// Verify message content
	for i, expectedMsg := range messages {
		assert.Equal(t, expectedMsg.Key, receivedMessages[i].Key)
		assert.Equal(t, expectedMsg.Value, receivedMessages[i].Value)
	}
}

func TestKafkaAsyncProducerMockPublishChannelCapacity(t *testing.T) {
	mock := NewKafkaAsyncProducerMock()

	// Fill the channel to capacity
	for i := 0; i < 100; i++ {
		mock.Publish(&Message{
			Key:   []byte("key"),
			Value: []byte("value"),
		})
	}

	// Channel should now be full
	assert.Equal(t, 100, len(mock.publishChannel))

	// Publishing one more should not block (this is Go's behavior with buffered channels)
	// But the channel will be overfilled if we try to publish more than capacity
}

func TestKafkaAsyncProducerMockPublishNilMessage(t *testing.T) {
	mock := NewKafkaAsyncProducerMock()

	// Publishing nil should not panic
	assert.NotPanics(t, func() {
		mock.Publish(nil)
	})

	// Verify nil message was published
	select {
	case receivedMsg := <-mock.publishChannel:
		assert.Nil(t, receivedMsg)
	default:
		t.Fatal("Nil message was not published to channel")
	}
}

func TestKafkaAsyncProducerMockPublishEmptyMessage(t *testing.T) {
	mock := NewKafkaAsyncProducerMock()
	msg := &Message{
		Key:   []byte{},
		Value: []byte{},
	}

	mock.Publish(msg)

	select {
	case receivedMsg := <-mock.publishChannel:
		assert.Equal(t, []byte{}, receivedMsg.Key)
		assert.Equal(t, []byte{}, receivedMsg.Value)
	default:
		t.Fatal("Empty message was not published to channel")
	}
}

func TestKafkaAsyncProducerMockInterface(t *testing.T) {
	mock := NewKafkaAsyncProducerMock()

	// Verify that mock implements the interface
	var _ KafkaAsyncProducerI = mock

	// Test all interface methods
	assert.NotPanics(t, func() {
		mock.Start(context.Background(), make(chan *Message))
	})

	assert.NotPanics(t, func() {
		err := mock.Stop()
		assert.NoError(t, err)
	})

	assert.NotPanics(t, func() {
		brokers := mock.BrokersURL()
		assert.Nil(t, brokers)
	})

	assert.NotPanics(t, func() {
		mock.Publish(&Message{})
	})
}

func TestKafkaAsyncProducerMockConcurrentPublish(t *testing.T) {
	mock := NewKafkaAsyncProducerMock()
	numGoroutines := 10
	messagesPerGoroutine := 5

	done := make(chan bool, numGoroutines)

	// Start multiple goroutines publishing messages
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			for j := 0; j < messagesPerGoroutine; j++ {
				msg := &Message{
					Key:   []byte("key"),
					Value: []byte("value"),
				}
				mock.Publish(msg)
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines to finish
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Verify all messages were published
	expectedTotal := numGoroutines * messagesPerGoroutine
	actualTotal := len(mock.publishChannel)

	assert.Equal(t, expectedTotal, actualTotal)
}

func TestKafkaAsyncProducerMockStartWithCustomChannel(t *testing.T) {
	mock := NewKafkaAsyncProducerMock()
	customChannel := make(chan *Message, 50)
	ctx := context.Background()

	// Start with custom channel should not affect mock's internal channel
	mock.Start(ctx, customChannel)

	// Mock should still use its own internal channel for Publish
	msg := &Message{Key: []byte("test"), Value: []byte("test")}
	mock.Publish(msg)

	// Message should be in mock's channel, not custom channel
	assert.Equal(t, 1, len(mock.publishChannel))
	assert.Equal(t, 0, len(customChannel))
}

func TestKafkaAsyncProducerMockLargeMessage(t *testing.T) {
	mock := NewKafkaAsyncProducerMock()

	// Create a large message
	largeKey := make([]byte, 10000)
	largeValue := make([]byte, 100000)
	for i := range largeKey {
		largeKey[i] = byte(i % 256)
	}
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	msg := &Message{
		Key:   largeKey,
		Value: largeValue,
	}

	// Should handle large messages without issues
	assert.NotPanics(t, func() {
		mock.Publish(msg)
	})

	// Verify large message was published correctly
	select {
	case receivedMsg := <-mock.publishChannel:
		assert.Equal(t, largeKey, receivedMsg.Key)
		assert.Equal(t, largeValue, receivedMsg.Value)
	default:
		t.Fatal("Large message was not published to channel")
	}
}
