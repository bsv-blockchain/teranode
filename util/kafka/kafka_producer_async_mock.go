package kafka

import (
	"context"
)

type KafkaAsyncProducerMock struct {
	publishChannel chan *Message
}

func NewKafkaAsyncProducerMock() *KafkaAsyncProducerMock {
	client := &KafkaAsyncProducerMock{
		publishChannel: make(chan *Message, 100),
	}

	return client
}

func (c *KafkaAsyncProducerMock) Start(ctx context.Context, ch chan *Message) {
}

func (c *KafkaAsyncProducerMock) BrokersURL() []string {
	return nil
}

func (c *KafkaAsyncProducerMock) PublishChannel() chan *Message {
	return c.publishChannel
}

func (c *KafkaAsyncProducerMock) Publish(msg *Message) {
	c.publishChannel <- msg
}
