// Package kafka provides Kafka consumer and producer implementations for message handling.
package kafka

import (
	"context"
)

// KafkaAsyncProducerMock provides a mock implementation of KafkaAsyncProducerI for testing.
type KafkaAsyncProducerMock struct {
	publishChannel chan *Message
}

// NewKafkaAsyncProducerMock creates a new mock async producer.
//
// Returns:
//   - *KafkaAsyncProducerMock: Configured mock producer
func NewKafkaAsyncProducerMock() *KafkaAsyncProducerMock {
	client := &KafkaAsyncProducerMock{
		publishChannel: make(chan *Message, 100),
	}

	return client
}

// Start implements the KafkaAsyncProducerI interface for the mock producer.
func (c *KafkaAsyncProducerMock) Start(ctx context.Context, ch chan *Message) {
	// mock implementation
}

// Stop implements the KafkaAsyncProducerI interface for the mock producer.
func (c *KafkaAsyncProducerMock) Stop() error {
	return nil
}

// BrokersURL implements the KafkaAsyncProducerI interface for the mock producer.
func (c *KafkaAsyncProducerMock) BrokersURL() []string {
	return nil
}

// PublishChannel returns the mock's publish channel.
func (c *KafkaAsyncProducerMock) PublishChannel() chan *Message {
	return c.publishChannel
}

// Publish sends a message to the mock's publish channel.
func (c *KafkaAsyncProducerMock) Publish(msg *Message) {
	c.publishChannel <- msg
}

// TryPublish performs a non-blocking send to the mock's publish channel, returning
// false if the buffer is full.
func (c *KafkaAsyncProducerMock) TryPublish(msg *Message) bool {
	select {
	case c.publishChannel <- msg:
		return true
	default:
		return false
	}
}
