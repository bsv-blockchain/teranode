//go:build test_all || test_kafka

package kafka

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/stretchr/testify/require"
)

// go test -v -tags test_kafka ./test/...

func TestKafkaProduceConsumeDirect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testContainer, err := RunContainer(ctx)
	require.NoError(t, err)

	const (
		topic   = "test_topic"
		message = "Hello, Kafka!"
	)

	host, err := testContainer.container.Host(ctx)
	require.NoError(t, err)

	kafkaBroker := fmt.Sprintf("%s:%d", host, testContainer.hostPort)
	t.Logf("kafkaBroker: %s", kafkaBroker)

	// Set up the producer configuration
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true

	readyCh := make(chan struct{})
	errCh := make(chan error)

	go func() {
		// Create a consumer
		consumer, err := sarama.NewConsumer([]string{kafkaBroker}, cfg)
		if err != nil {
			errCh <- errors.NewProcessingError("Failed to create consumer", err)
			return
		}

		defer consumer.Close()

		// Consume messages from the topic
		partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
		if err != nil {
			errCh <- errors.NewProcessingError("Failed to start consuming", err)
			return
		}

		defer partitionConsumer.Close()

		// Signal that the consumer is ready
		close(readyCh)

		// Read message
		select {
		case msg := <-partitionConsumer.Messages():
			if string(msg.Value) != message {
				errCh <- errors.NewProcessingError(fmt.Sprintf("Expected message '%s' but got '%s'", message, string(msg.Value)))
				return
			}

		case <-time.After(5 * time.Second):
			errCh <- errors.NewProcessingError("Did not receive message in time")
			return
		}

		errCh <- nil
		close(errCh)
	}()

	// Wait for the consumer to be ready
	<-readyCh

	// Create a producer
	producer, err := sarama.NewSyncProducer([]string{kafkaBroker}, cfg)
	require.NoError(t, err)

	defer producer.Close()

	// Send a message to the topic
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	_, _, err = producer.SendMessage(msg)
	require.NoError(t, err)

	err = <-errCh
	require.NoError(t, err)
}
