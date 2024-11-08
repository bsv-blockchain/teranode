package kafka

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/ulogger"
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

func GetFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}

	defer l.Close()

	return l.Addr().(*net.TCPAddr).Port, nil
}

func RunContainer(ctx context.Context) (*TestContainerWrapper, error) {
	// Get an ephemeral port
	hostPort, err := GetFreePort()
	if err != nil {
		return nil, errors.NewConfigurationError("could not get free port: %w", err)
	}

	// const containerPort = 9092 // The internal port Redpanda uses

	req := testcontainers.ContainerRequest{
		Image: fmt.Sprintf("%s:%s", RedpandaImage, RedpandaVersion),
		ExposedPorts: []string{
			fmt.Sprintf("%d:%d/tcp", hostPort, hostPort),
		},
		Cmd: []string{
			"redpanda", "start",
			"--overprovisioned",
			"--smp=1",
			"--kafka-addr", fmt.Sprintf("PLAINTEXT://0.0.0.0:%d", hostPort),
			"--advertise-kafka-addr", fmt.Sprintf("PLAINTEXT://localhost:%d", hostPort),
		},
		WaitingFor: wait.ForLog("Successfully started Redpanda!"),
		// AutoRemove: true,
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, errors.NewProcessingError("could not start the container: %w", err)
	}

	// mPort, err := container.MappedPort(ctx, nat.Port(fmt.Sprintf("%d/tcp", hostPort, containerPort)))
	// if err != nil {
	// 	return nil, errors.NewConfigurationError("could not get the mapped port: %", err)
	// }

	return &TestContainerWrapper{
		container: container,
		hostPort:  hostPort,
	}, nil
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

func TestRunSimpleKafkaContainer(t *testing.T) {
	// logger := ulogger.NewZeroLogger("test")
	//	logger := ulogger.NewVerboseTestLogger(t)
	logger := ulogger.TestLogger{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testContainer, err := RunContainer(ctx)
	require.NoError(t, err)

	defer func() {
		cleanupErr := testContainer.CleanUp()
		if cleanupErr != nil {
			t.Errorf("failed to clean up container: %v", cleanupErr)
		}
	}()

	host, err := testContainer.container.Host(ctx)
	require.NoError(t, err)

	kafkaURL := &url.URL{
		Scheme:   "kafka",
		Host:     fmt.Sprintf("%s:%d", host, testContainer.hostPort),
		Path:     "unittest",
		RawQuery: "partitions=1&replicationFactor=1&flush_frequency=1s&replay=1",
	}

	producerClient, err := NewKafkaAsyncProducerFromURL(ctx, logger, kafkaURL)
	require.NoError(t, err)

	consumerClient, err := NewKafkaConsumerGroupFromURL(logger, kafkaURL, "kafka_test_consumer", true)
	require.NoError(t, err)

	done := make(chan struct{})

	consumerFn := func(message *KafkaMessage) error {
		logger.Infof("received message: %s = %s, Offset: %d ", message.Key, message.Value, message.Offset)

		defer func() {
			if r := recover(); r != nil {
				logger.Errorf("recovered from panic on channel close: %v", r)
			}
		}()

		close(done)

		return nil
	}

	consumerClient.Start(ctx, consumerFn)

	producerClient.Start(ctx, make(chan *Message, 100))
	defer producerClient.Stop() // nolint:errcheck

	producerClient.Publish(&Message{
		Key:   []byte("key"),
		Value: []byte("value"),
	})

	select {
	case <-done:
		// Wait group completed successfully
	case <-time.After(5 * time.Second):
		t.Log("!!!!!! test failed - timeout waiting for messages !!!!!!")
		t.FailNow()
	}
}

func Test_KafkaAsyncProducerWithManualCommitParams_using_tc(t *testing.T) {
	// t.Parallel()

	// logger := ulogger.NewZeroLogger("test")
	// logger := ulogger.NewVerboseTestLogger(t)
	logger := ulogger.TestLogger{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the Kafka container
	testContainer, err := RunContainer(ctx)
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
	defer producerClient.Stop() // nolint:errcheck

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

	go produceMessages(logger, producerClient, 10)

	// Run test cases
	for _, tCase := range testCases {
		ctx, cancel := context.WithCancel(context.Background())

		wg.Add(10)
		t.Run(tCase.name, func(t *testing.T) {
			client, err := NewKafkaConsumerGroupFromURL(logger, kafkaURL, tCase.groupID, false)
			require.NoError(t, err)

			client.Start(ctx, tCase.consumerClosure, WithRetryAndMoveOn(3, 1, time.Millisecond))
		})

		// Create a channel to signal when the wait group is done
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		// Wait for either completion or timeout
		select {
		case <-done:
			// Wait group completed successfully
		case <-time.After(5 * time.Second):
			t.Error("timeout waiting for messages")
			t.FailNow()
		}

		// Stop the listener
		cancel()
	}
}

func Test_KafkaAsyncProducerWithManualCommitWithRetryAndMoveOnOption_using_tc(t *testing.T) {
	// t.Parallel()

	// logger := ulogger.NewZeroLogger("test")
	// logger := ulogger.NewVerboseTestLogger(t)
	logger := ulogger.TestLogger{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // shuts down listener

	// Start the Kafka container
	testContainer, err := RunContainer(ctx)
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
	defer producerClient.Stop() // nolint:errcheck

	numberOfMessages := 2
	go produceMessages(logger, producerClient, numberOfMessages)

	c := make(chan []byte)
	errClosure := func(message *KafkaMessage) error {
		logger.Infof("Consumer closure received message:	 %s, Offset: %d, Partition: %d", string(message.Value), message.Offset, message.Partition)
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

func Test_KafkaAsyncProducerWithManualCommitWithRetryAndStopOption_using_tc(t *testing.T) {
	// t.Parallel()

	logger := ulogger.TestLogger{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testContainer, err := RunContainer(ctx)
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
	defer producerClient.Stop() // nolint:errcheck

	numberOfMessages := 2
	go produceMessages(logger, producerClient, numberOfMessages)

	messagesChan := make(chan []byte)
	processingDone := make(chan struct{})

	errClosure := func(message *KafkaMessage) error {
		logger.Infof("Consumer closure received message: %s, Offset: %d, Partition: %d",
			string(message.Value), message.Offset, message.Partition)

		select {
		case messagesChan <- message.Value:
		case <-ctx.Done():
			return ctx.Err()
		}

		return errors.New(errors.ERR_BLOCK_ERROR, "block error")
	}

	client, err := NewKafkaConsumerGroupFromURL(logger, kafkaURL, "kafka_test_stop", false)
	require.NoError(t, err)

	stopped := false
	// it should not bother doing a retry and just call the supplied stop func
	client.Start(ctx, errClosure, WithRetryAndStop(0, 1, time.Millisecond, func() {
		c := client.ConsumerGroup
		client.ConsumerGroup = nil
		stopped = true

		c.Close()
	}))

	messagesReceived := [][]byte{}

	go func() {
		defer close(processingDone)

		timeout := time.After(5 * time.Second)

		// consume messages until timeout
		for {
			select {
			case <-timeout:
				return
			case msg := <-messagesChan:
				messagesReceived = append(messagesReceived, msg)
			}
		}
	}()

	<-processingDone

	// Verify first message was attempted
	require.Greater(t, len(messagesReceived), 0)
	value, err := byteArrayToIntFromString(messagesReceived[0])
	require.NoError(t, err)
	require.Equal(t, 0, value)

	// Verify second message was not attempted at all
	require.Equal(t, 1, len(messagesReceived))

	// Verify stop func was called
	require.True(t, stopped)
	require.Nil(t, client.ConsumerGroup)
}

func Test_KafkaAsyncProducerWithManualCommitWithNoOptions_using_tc(t *testing.T) {
	// t.Parallel()

	logger := ulogger.TestLogger{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testContainer, err := RunContainer(ctx)
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
	defer producerClient.Stop() // nolint:errcheck

	numberOfMessages := 2
	go produceMessages(logger, producerClient, numberOfMessages)

	messagesChan := make(chan []byte)
	processingDone := make(chan struct{})

	errClosure := func(message *KafkaMessage) error {
		logger.Infof("Consumer closure received message: %s, Offset: %d, Partition: %d",
			string(message.Value), message.Offset, message.Partition)

		select {
		case messagesChan <- message.Value:
		case <-ctx.Done():
			return ctx.Err()
		}

		return errors.New(errors.ERR_BLOCK_ERROR, "block error")
	}

	client, err := NewKafkaConsumerGroupFromURL(logger, kafkaURL, "kafka_test_stop", false)
	require.NoError(t, err)

	// default Kafka behaviour on message consumption error is to retry forever
	client.Start(ctx, errClosure)

	messagesReceived := [][]byte{}

	go func() {
		defer close(processingDone)

		timeout := time.After(5 * time.Second)

		// consume messages until timeout
		for {
			select {
			case <-timeout:
				return
			case msg := <-messagesChan:
				messagesReceived = append(messagesReceived, msg)
			}
		}
	}()

	<-processingDone
	client.ConsumerGroup.Close() // nolint:errcheck

	// Verify first message was attempted more than once and that every message attempt was first message
	require.Greater(t, len(messagesReceived), 1)

	for _, msg := range messagesReceived {
		value, err := byteArrayToIntFromString(msg)
		require.NoError(t, err)
		require.Equal(t, 0, value)
	}
}

/*
This test is to ensure that when a consumer is restarted, it will resume from the last committed offset
and not reprocess the same messages again.
*/
func TestKafkaConsumerOffsetContinuation(t *testing.T) {
	// t.Parallel()

	logger := ulogger.NewZeroLogger("test")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testContainer, err := RunContainer(ctx)
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
	defer producer.Stop() // nolint:errcheck

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

func byteArrayToIntFromString(message []byte) (int, error) {
	strValue := string(message) // Convert byte array to string

	intValue, err := strconv.Atoi(strValue) // Parse string as integer
	if err != nil {
		return 0, err
	}

	return intValue, nil
}

func produceMessages(logger ulogger.Logger, client KafkaAsyncProducerI, numberOfMessages int) {
	for i := 0; i < numberOfMessages; i++ {
		msg := []byte(strconv.Itoa(i))
		client.Publish(&Message{
			Value: msg,
		})
		logger.Infof("pushed message: %v", string(msg))
	}
}
