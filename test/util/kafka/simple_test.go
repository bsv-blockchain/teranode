//go:build test_all || test_util || test_kafka

package kafka

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// go test -v -tags test_kafka ./test/...

func TestKafkaProduceConsumeDirect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var testContainer *TestContainerWrapper
	var err error
	
	// Retry up to 3 times with random delays to reduce port conflicts
	for attempt := 0; attempt < 3; attempt++ {
		// Add random delay to reduce chance of simultaneous port allocation
		if attempt > 0 {
			delay := time.Duration(100+time.Now().Nanosecond()%500) * time.Millisecond
			t.Logf("Retrying container setup after delay of %v (attempt %d)", delay, attempt+1)
			time.Sleep(delay)
		}
		
		// Try to create and start the container
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Logf("Recovered from panic in container setup (attempt %d): %v", attempt+1, r)
				}
			}()
			
			testContainer, err = RunContainer(ctx)
			if err != nil {
				t.Logf("Failed to create container on attempt %d: %v", attempt+1, err)
				return
			}
		}()
		
		// If successful, break out of retry loop
		if testContainer != nil {
			break
		}
	}
	
	// If all attempts failed, skip the test
	if testContainer == nil {
		t.Skip("Failed to create test container after 3 attempts, likely due to port conflicts")
		return
	}

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

func TestKafkaURLOverride(t *testing.T) {
	t.Run("with replay set", func(t *testing.T) {
		kafkaURLStr := "kafka://localhost:9092/meta?partitions=2&replication=3&retention=60000&flush_bytes=1024&flush_messages=10000&flush_frequency=1s&consumer_ratio=4&replay=1"

		kafkaURL, err := url.Parse(kafkaURLStr)
		require.NoError(t, err)

		assert.Equal(t, "1", kafkaURL.Query().Get("replay"))

		values := kafkaURL.Query()
		values.Set("replay", "0")

		kafkaURL.RawQuery = values.Encode()

		assert.Equal(t, "0", kafkaURL.Query().Get("replay"))
	})

	t.Run("with replay not set", func(t *testing.T) {
		kafkaURLStr := "kafka://localhost:9092/meta?partitions=2&replication=3&retention=60000&flush_bytes=1024&flush_messages=10000&flush_frequency=1s&consumer_ratio=4"

		kafkaURL, err := url.Parse(kafkaURLStr)
		require.NoError(t, err)

		assert.Equal(t, "", kafkaURL.Query().Get("replay"))

		values := kafkaURL.Query()
		values.Set("replay", "0")

		kafkaURL.RawQuery = values.Encode()

		assert.Equal(t, "0", kafkaURL.Query().Get("replay"))
	})
}
