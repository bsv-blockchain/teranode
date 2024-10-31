package kafka

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/docker/go-connections/nat"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	RedpandaImage   = "docker.vectorized.io/vectorized/redpanda"
	RedpandaVersion = "latest"
)

type TestContainerWrapper struct {
	container testcontainers.Container
	hostPort  int
}

func (t *TestContainerWrapper) RunContainer(index int) error {
	assignPort := 9092 + index
	natPort := nat.Port(fmt.Sprintf("%d", assignPort))
	req := testcontainers.ContainerRequest{
		Image: fmt.Sprintf("%s:%s", RedpandaImage, RedpandaVersion),
		ExposedPorts: []string{
			fmt.Sprintf("%d:%d/tcp", assignPort, assignPort), // Map the dynamic port
		},
		Cmd: []string{
			"redpanda", "start",
			"--overprovisioned", "--smp=1",
			fmt.Sprintf("--kafka-addr=PLAINTEXT://0.0.0.0:%d", assignPort),
			fmt.Sprintf("--advertise-kafka-addr=PLAINTEXT://localhost:%d", assignPort),
		},
		WaitingFor: wait.ForLog("Successfully started Redpanda!"),
		AutoRemove: true,
	}

	container, err := testcontainers.GenericContainer(context.Background(), testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return errors.NewProcessingError("could not start the container: %w", err)
	}

	mPort, err := container.MappedPort(context.Background(), natPort)
	if err != nil {
		return errors.NewConfigurationError("could not get the mapped port: %", err)
	}

	t.container = container
	t.hostPort = mPort.Int()

	return nil
}

func (t *TestContainerWrapper) CleanUp() error {
	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFunc()

	if err := t.container.Terminate(ctx); err != nil {
		return errors.NewConfigurationError("could not terminate the container: %w", err)
	}

	return nil
}

func (t *TestContainerWrapper) GetBrokerAddresses() []string {
	return []string{fmt.Sprintf("localhost:%d", t.hostPort)}
}

func Test_KafkaAsyncProducerConsumerAutoCommit_using_tc(t *testing.T) {
	t.Parallel()

	// logger := ulogger.NewZeroLogger("test")
	// logger := ulogger.NewVerboseTestLogger(t)
	logger := ulogger.TestLogger{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testContainer := &TestContainerWrapper{}

	err := testContainer.RunContainer(4)
	require.NoError(t, err)

	defer func() {
		cleanupErr := testContainer.CleanUp()
		if cleanupErr != nil {
			t.Errorf("failed to clean up container: %v", cleanupErr)
		}
	}()

	const (
		kafkaPartitions        = 1
		kafkaReplicationFactor = 1
		kafkaTopic             = "unittest"
	)

	kafkaURL := &url.URL{
		Scheme:   "kafka",
		Host:     testContainer.GetBrokerAddresses()[0],
		Path:     kafkaTopic,
		RawQuery: fmt.Sprintf("partitions=%d&replicationFactor=%d&flush_frequency=1s&replay=1", kafkaPartitions, kafkaReplicationFactor),
	}

	producerClient, err := NewKafkaAsyncProducerFromURL(ctx, logger, kafkaURL)
	require.NoError(t, err)

	producerClient.Start(ctx, make(chan *Message, 100))
	defer producerClient.Close()

	numberOfMessages := 100
	go produceMessages(logger, nil, producerClient, numberOfMessages)

	var wg sync.WaitGroup

	wg.Add(numberOfMessages)

	listenerClient, err := NewKafkaConsumerGroupFromURL(logger, kafkaURL, "kafka_test_consumer", true)
	require.NoError(t, err)

	consumerFn := func(message *KafkaMessage) error {
		msgInt, err := byteArrayToIntFromString(message.Value)
		require.NoError(t, err)

		if message.Offset != int64(msgInt) {
			return err
		}

		logger.Infof("received message: %s, Offset: %d ", string(message.Value), message.Offset)
		wg.Done()

		return nil
	}
	listenerClient.Start(ctx, consumerFn)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Wait group completed successfully
	case <-time.After(5 * time.Second):
		t.Log("!!!!!! test failed - timeout waiting for messages !!!!!!")
		t.FailNow()
	}
}

func Test_KafkaAsyncProducerWithManualCommitParams_using_tc(t *testing.T) {
	t.Parallel()

	// logger := ulogger.NewZeroLogger("test")
	// logger := ulogger.NewVerboseTestLogger(t)
	logger := ulogger.TestLogger{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize your TestContainerWrapper
	testContainer := &TestContainerWrapper{}

	// Start the Kafka container
	err := testContainer.RunContainer(2)
	require.NoError(t, err)

	// Ensure cleanup after the test
	defer func() {
		cleanupErr := testContainer.CleanUp()
		if cleanupErr != nil {
			t.Errorf("failed to clean up container: %v", cleanupErr)
		}
	}()

	const (
		kafkaPartitions        = 1
		kafkaReplicationFactor = 1
		kafkaTopic             = "unittest"
	)

	// Construct the Kafka URL
	kafkaURL := &url.URL{
		Scheme:   "kafka",
		Host:     testContainer.GetBrokerAddresses()[0],
		Path:     kafkaTopic,
		RawQuery: fmt.Sprintf("partitions=%d&replication=%d&retention=6000000&flush_frequency=1s&replay=1", kafkaPartitions, kafkaReplicationFactor),
	}

	logger.Infof("Kafka URL: %v", kafkaURL)

	var wg sync.WaitGroup

	producerClient, err := NewKafkaAsyncProducerFromURL(ctx, logger, kafkaURL)
	require.NoError(t, err)

	producerClient.Start(ctx, make(chan *Message, 10000))
	defer producerClient.Close()

	counter := 0

	// Define test cases
	testCases := []struct {
		name            string
		groupID         string
		consumerClosure func(*KafkaMessage) error
	}{
		{
			name:    "Process messages with all success",
			groupID: "test-group-1",
			consumerClosure: func(message *KafkaMessage) error {
				logger.Infof("Consumer closure#1 received message: %s, Offset: %d", string(message.Value), message.Offset)
				wg.Done()
				return nil
			},
		},
		{
			name:    "Process messages with 2nd time success closure",
			groupID: "test-group-2",
			consumerClosure: func(message *KafkaMessage) error {
				logger.Infof("Consumer closure#2 received message: %s, Offset: %d", string(message.Value), message.Offset)
				counter++
				if counter%2 == 0 {
					wg.Done()
					return nil
				}
				return errors.New(errors.ERR_BLOCK_ERROR, "block error")
			},
		},
		{
			name:    "Process messages with 3rd time success closure",
			groupID: "test-group-3",
			consumerClosure: func(message *KafkaMessage) error {
				logger.Infof("Consumer closure#3 received message: %s, Offset: %d", string(message.Value), message.Offset)
				counter++
				if counter%3 == 0 {
					wg.Done()
					return nil
				}
				return errors.New(errors.ERR_BLOCK_ERROR, "block error")
			},
		},
	}

	wgP := sync.WaitGroup{}
	wgP.Add(10)
	// Send 10 messages
	go produceMessages(logger, &wgP, producerClient, 10)
	wgP.Wait()

	// Run test cases
	for _, tCase := range testCases {
		ctx, cancel := context.WithCancel(context.Background())

		wg.Add(10)
		t.Run(tCase.name, func(t *testing.T) {
			client, err := NewKafkaConsumerGroupFromURL(logger, kafkaURL, tCase.groupID, false)
			require.NoError(t, err)

			client.Start(ctx, tCase.consumerClosure, WithRetryAndMoveOn(3, 1, time.Millisecond))
		})
		wg.Wait()

		// Stop the listener
		cancel()
	}
}

func Test_KafkaAsyncProducerWithManualCommitErrorClosure_using_tc(t *testing.T) {
	t.Parallel()

	// logger := ulogger.NewZeroLogger("test")
	// logger := ulogger.NewVerboseTestLogger(t)
	logger := ulogger.TestLogger{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // shuts down listener

	// Initialize your TestContainerWrapper
	testContainer := &TestContainerWrapper{}

	// Start the Kafka container
	err := testContainer.RunContainer(3)
	require.NoError(t, err)

	// Ensure cleanup after the test
	defer func() {
		cleanupErr := testContainer.CleanUp()
		if cleanupErr != nil {
			t.Errorf("failed to clean up container: %v", cleanupErr)
		}
	}()

	const (
		kafkaPartitions        = 1
		kafkaReplicationFactor = 1
		kafkaTopic             = "unittest"
	)

	// Construct the Kafka URL
	kafkaURL := &url.URL{
		Scheme: "kafka",
		Host:   testContainer.GetBrokerAddresses()[0],
		Path:   kafkaTopic,
		RawQuery: fmt.Sprintf("partitions=%d&replication=%d&retention=600000&flush_frequency=1s&replay=1",
			kafkaPartitions, kafkaReplicationFactor),
	}

	logger.Infof("Kafka URL: %v", kafkaURL)

	producerClient, err := NewKafkaAsyncProducerFromURL(ctx, logger, kafkaURL)
	require.NoError(t, err)

	producerClient.Start(ctx, make(chan *Message, 10000))
	defer producerClient.Close()

	numberOfMessages := 2
	go produceMessages(logger, nil, producerClient, numberOfMessages)

	c := make(chan []byte)
	errClosure := func(message *KafkaMessage) error {
		logger.Infof("Consumer closure received message: %s, Offset: %d, Partition: %d", string(message.Value), message.Offset, message.Partition)
		c <- message.Value

		return errors.New(errors.ERR_BLOCK_ERROR, "block error")
	}

	client, err := NewKafkaConsumerGroupFromURL(logger, kafkaURL, "kafka_test", false)
	require.NoError(t, err)

	// WithRetryAndMoveOn(3, 1, time.Millisecond) is the key here - we're testing that the consumer will retry 3 times before moving on
	client.Start(ctx, errClosure, WithRetryAndMoveOn(3, 1, time.Millisecond))

	messagesReceived := [][]byte{}
	timeout := time.After(10 * time.Second)

	// wait for 4 attempts on each of the 2 original messages
	// or 10 seconds, whichever comes first
	for i := 0; i < numberOfMessages*4; i++ { // each message is processed 4 times before offset commit
		select {
		case <-timeout:
			i = numberOfMessages * 4
		case msg := <-c:
			messagesReceived = append(messagesReceived, msg)
		}
	}

	client.ConsumerGroup.Close()

	// In total we should have 4 messages (1 + 3 retries) for each of the 2 original messages
	require.Equal(t, numberOfMessages*4, len(messagesReceived))

	// Check that first 4 messages are the same first message
	firstValue := 0

	for i := 0; i < 4; i++ {
		value, err := byteArrayToIntFromString(messagesReceived[i])
		require.NoError(t, err)
		require.Equal(t, firstValue, value)
	}

	// Check next 4 messages are the same second message
	secondValue := 1

	for i := 4; i < 8; i++ {
		value, err := byteArrayToIntFromString(messagesReceived[i])
		require.NoError(t, err)
		require.Equal(t, secondValue, value)
	}
}

func byteArrayToIntFromString(message []byte) (int, error) {
	strValue := string(message) // Convert byte array to string

	intValue, err := strconv.Atoi(strValue) // Parse string as integer
	if err != nil {
		return 0, err
	}

	return intValue, nil
}

func produceMessages(logger ulogger.Logger, wg *sync.WaitGroup, client KafkaAsyncProducerI, numberOfMessages int) {
	for i := 0; i < numberOfMessages; i++ {
		msg := []byte(strconv.Itoa(i))
		client.Publish(&Message{
			Value: msg,
		})
		logger.Infof("pushed message: %v", string(msg))

		if wg != nil {
			wg.Done()
		}
	}
}

/*
This test is to ensure that when a consumer is restarted, it will resume from the last committed offset
and not reprocess the same messages again.
*/
func TestKafkaConsumerOffsetContinuation(t *testing.T) {
	t.Parallel()

	logger := ulogger.NewZeroLogger("test")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testContainer := &TestContainerWrapper{}

	err := testContainer.RunContainer(1)
	require.NoError(t, err)

	defer func() {
		cleanupErr := testContainer.CleanUp()
		if cleanupErr != nil {
			t.Errorf("failed to clean up container: %v", cleanupErr)
		}
	}()

	topic := fmt.Sprintf("test-topic-%s", uuid.New().String())
	groupID := fmt.Sprintf("test-group-%s", uuid.New().String())

	kafkaURL := &url.URL{
		Scheme:   "kafka",
		Host:     testContainer.GetBrokerAddresses()[0],
		Path:     topic,
		RawQuery: "partitions=1&replicationFactor=1&flush_frequency=1ms&replay=1",
	}

	producer, err := NewKafkaAsyncProducerFromURL(ctx, logger, kafkaURL)
	require.NoError(t, err)

	producer.Start(ctx, make(chan *Message, 100))
	defer producer.Close()

	t.Log("Publishing first batch of test messages...")

	messages := []string{"msg1", "msg2"}
	for _, msg := range messages {
		producer.Publish(&Message{
			Value: []byte(msg),
		})
	}

	var receivedMessages []string

	consume := func() {
		consumer, err := NewKafkaConsumerGroupFromURL(logger, kafkaURL, groupID, false) // autoCommit = false
		require.NoError(t, err)
		defer consumer.ConsumerGroup.Close()

		consumer.Start(ctx, func(msg *KafkaMessage) error {
			receivedMessages = append(receivedMessages, string(msg.Value))
			return nil
		})

		deadline := time.After(5 * time.Second)

		// wait for 2 messages to be received
		for {
			select {
			case <-deadline:
				break
			default:
				time.Sleep(1 * time.Millisecond)

				if len(receivedMessages) >= 2 {
					return
				}
			}
		}
	}

	consume()

	t.Logf("First batch complete. Received messages: %v", receivedMessages)
	require.Equal(t, []string{"msg1", "msg2"}, receivedMessages)

	t.Log("Publishing second batch of test messages...")

	messages = []string{"msg3", "msg4"}
	for _, msg := range messages {
		producer.Publish(&Message{
			Value: []byte(msg),
		})
	}

	receivedMessages = nil

	consume()

	require.Equal(t, []string{"msg3", "msg4"}, receivedMessages)
}
