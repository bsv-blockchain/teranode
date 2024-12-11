//go:build test_all || test_util || test_kafka

package kafka

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/ulogger"
	ukafka "github.com/bitcoin-sv/ubsv/util/kafka"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// go test -v -tags test_kafka ./test/...

const (
	RedpandaImage   = "redpandadata/redpanda"
	RedpandaVersion = "v24.3.1"
)

type TestContainerWrapper struct {
	container testcontainers.Container
	hostPort  int
}

func GetFreePort() (int, error) {
	var port int

	// Try up to 3 times to get a free port
	for attempts := 0; attempts < 3; attempts++ {
		addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
		if err != nil {
			return 0, err
		}

		l, err := net.ListenTCP("tcp", addr)
		if err != nil {
			// Wait briefly before retry
			time.Sleep(100 * time.Millisecond)
			continue
		}

		port = l.Addr().(*net.TCPAddr).Port

		// Explicitly close the listener
		err = l.Close()
		if err != nil {
			return 0, err
		}

		// Small delay to ensure port is released
		time.Sleep(100 * time.Millisecond)

		// Verify the port is actually free
		testListener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
		if err != nil {
			continue
		}
		testListener.Close()

		return port, nil
	}

	return 0, fmt.Errorf("failed to find a free port after 3 attempts")
}

func RunContainer(ctx context.Context) (*TestContainerWrapper, error) {
	// Get an ephemeral port
	hostPort, err := GetFreePort()
	if err != nil {
		return nil, errors.NewConfigurationError("could not get free port", err)
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
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Reuse:            false,
		Started:          true,
	})
	if err != nil {
		return nil, errors.NewProcessingError("could not start the container", err)
	}

	return &TestContainerWrapper{
		container: container,
		hostPort:  hostPort,
	}, nil
}

func (t *TestContainerWrapper) CleanUp() error {
	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFunc()

	if err := t.container.Terminate(ctx); err != nil {
		return errors.NewConfigurationError("could not terminate the container", err)
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

	producerClient, err := ukafka.NewKafkaAsyncProducerFromURL(ctx, logger, kafkaURL)
	require.NoError(t, err)

	consumerClient, err := ukafka.NewKafkaConsumerGroupFromURL(logger, kafkaURL, "kafka_test_consumer", true)
	require.NoError(t, err)

	done := make(chan struct{})

	consumerFn := func(message *ukafka.KafkaMessage) error {
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

	producerClient.Start(ctx, make(chan *ukafka.Message, 100))
	defer producerClient.Stop() // nolint:errcheck

	producerClient.Publish(&ukafka.Message{
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

func TestKafkaAsyncProducerWithManualCommitParamsUsingTC(t *testing.T) {
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

	t.Logf("Kafka URL: %v", kafkaURL)

	var wg sync.WaitGroup

	producerClient, err := ukafka.NewKafkaAsyncProducerFromURL(ctx, logger, kafkaURL)
	require.NoError(t, err)

	producerClient.Start(ctx, make(chan *ukafka.Message, 10000))
	defer producerClient.Stop() // nolint:errcheck

	counter2 := 0
	counter3 := 0

	// Define test cases
	testCases := []struct {
		name            string
		groupID         string
		consumerClosure func(*ukafka.KafkaMessage) error
	}{
		{
			name:    "Process messages with all success",
			groupID: "test-group-1",
			consumerClosure: func(message *ukafka.KafkaMessage) error {
				logger.Infof("Consumer closure#1 received message: %s, Offset: %d", string(message.Value), message.Offset)
				wg.Done()
				return nil
			},
		},
		{
			name:    "Process messages with 2nd time success closure",
			groupID: "test-group-2",
			consumerClosure: func(message *ukafka.KafkaMessage) error {
				logger.Infof("Consumer closure#2 received message: %s, Offset: %d", string(message.Value), message.Offset)
				counter2++
				if counter2%2 == 0 {
					wg.Done()
					return nil
				}
				return errors.New(errors.ERR_BLOCK_ERROR, "block error")
			},
		},
		{
			name:    "Process messages with 3rd time success closure",
			groupID: "test-group-3",
			consumerClosure: func(message *ukafka.KafkaMessage) error {
				logger.Infof("Consumer closure#3 received message: %s, Offset: %d", string(message.Value), message.Offset)
				counter3++
				if counter3%3 == 0 {
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
		wg.Add(10)

		t.Run(tCase.name, func(t *testing.T) {
			client, err := ukafka.NewKafkaConsumerGroupFromURL(logger, kafkaURL, tCase.groupID, false)
			require.NoError(t, err)

			client.Start(ctx, tCase.consumerClosure, ukafka.WithRetryAndMoveOn(3, 1, time.Millisecond))
		})
	}

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
}

func TestKafkaAsyncProducerWithManualCommitWithRetryAndMoveOnOptionUsingTC(t *testing.T) {
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

	t.Logf("Kafka URL: %v", kafkaURL)

	producerClient, err := ukafka.NewKafkaAsyncProducerFromURL(ctx, logger, kafkaURL)
	require.NoError(t, err)

	producerClient.Start(ctx, make(chan *ukafka.Message, 10000))
	defer producerClient.Stop() // nolint:errcheck

	numberOfMessages := 2
	go produceMessages(logger, producerClient, numberOfMessages)

	c := make(chan []byte, 100) // buffer channel to avoid blocking

	errClosure := func(message *ukafka.KafkaMessage) error {
		logger.Infof("Consumer closure received message:	 %s, Offset: %d, Partition: %d", string(message.Value), message.Offset, message.Partition)
		c <- message.Value

		return errors.New(errors.ERR_BLOCK_ERROR, "block error")
	}

	client, err := ukafka.NewKafkaConsumerGroupFromURL(logger, kafkaURL, "kafka_test", false)
	require.NoError(t, err)

	// WithRetryAndMoveOn(3, 1, time.Millisecond) is the key here - we're testing that the consumer will retry 3 times before moving on
	client.Start(ctx, errClosure, ukafka.WithRetryAndMoveOn(3, 1, time.Millisecond))

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

	_ = client.ConsumerGroup.Close()

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

func TestKafkaAsyncProducerWithManualCommitWithRetryAndStopOptionUsingTC(t *testing.T) {
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

	t.Logf("Kafka URL: %v", kafkaURL)

	producerClient, err := ukafka.NewKafkaAsyncProducerFromURL(ctx, logger, kafkaURL)
	require.NoError(t, err)

	producerClient.Start(ctx, make(chan *ukafka.Message, 10000))
	defer producerClient.Stop() // nolint:errcheck

	numberOfMessages := 2
	go produceMessages(logger, producerClient, numberOfMessages)

	messagesChan := make(chan []byte)
	processingDone := make(chan struct{})

	errClosure := func(message *ukafka.KafkaMessage) error {
		logger.Infof("Consumer closure received message: %s, Offset: %d, Partition: %d",
			string(message.Value), message.Offset, message.Partition)

		select {
		case messagesChan <- message.Value:
		case <-ctx.Done():
			return ctx.Err()
		}

		return errors.New(errors.ERR_BLOCK_ERROR, "block error")
	}

	client, err := ukafka.NewKafkaConsumerGroupFromURL(logger, kafkaURL, "kafka_test_stop", false)
	require.NoError(t, err)

	var stopped atomic.Bool

	// it should not bother doing a retry and just call the supplied stop func
	client.Start(ctx, errClosure, ukafka.WithRetryAndStop(0, 1, time.Millisecond, func() {
		c := client.ConsumerGroup

		stopped.Store(true)

		_ = c.Close()
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
	require.True(t, stopped.Load())
}

func TestKafkaAsyncProducerWithManualCommitWithNoOptionsUsingTC(t *testing.T) {
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

	producerClient, err := ukafka.NewKafkaAsyncProducerFromURL(ctx, logger, kafkaURL)
	require.NoError(t, err)

	producerClient.Start(ctx, make(chan *ukafka.Message, 10000))
	defer producerClient.Stop() // nolint:errcheck

	numberOfMessages := 2
	go produceMessages(logger, producerClient, numberOfMessages)

	messagesChan := make(chan []byte)
	processingDone := make(chan struct{})

	errClosure := func(message *ukafka.KafkaMessage) error {
		logger.Infof("Consumer closure received message: %s, Offset: %d, Partition: %d",
			string(message.Value), message.Offset, message.Partition)

		select {
		case messagesChan <- message.Value:
		case <-ctx.Done():
			return ctx.Err()
		}

		return errors.New(errors.ERR_BLOCK_ERROR, "block error")
	}

	client, err := ukafka.NewKafkaConsumerGroupFromURL(logger, kafkaURL, "kafka_test_stop", false)
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

	_ = client.ConsumerGroup.Close() // nolint:errcheck

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

	producer, err := ukafka.NewKafkaAsyncProducerFromURL(ctx, logger, kafkaURL)
	require.NoError(t, err)

	producer.Start(ctx, make(chan *ukafka.Message, 100))
	defer producer.Stop() // nolint:errcheck

	t.Log("Publishing first batch of test messages...")

	messages := []string{"msg1", "msg2"}
	for _, msg := range messages {
		producer.Publish(&ukafka.Message{
			Value: []byte(msg),
		})
	}

	var (
		receivedMessages []string
		messagesMutex    sync.RWMutex
	)

	consume := func() {
		consumer, err := ukafka.NewKafkaConsumerGroupFromURL(logger, kafkaURL, groupID, false) // autoCommit = false
		require.NoError(t, err)

		defer func() {
			_ = consumer.ConsumerGroup.Close()
		}()

		consumer.Start(ctx, func(msg *ukafka.KafkaMessage) error {
			messagesMutex.Lock()
			receivedMessages = append(receivedMessages, string(msg.Value))
			messagesMutex.Unlock()

			return nil
		})

		deadline := time.After(5 * time.Second)

		// wait for 2 messages to be received
		for {
			select {
			case <-deadline:
				return
			default:
				time.Sleep(1 * time.Millisecond)

				messagesMutex.RLock()
				l := len(receivedMessages)
				messagesMutex.RUnlock()

				if l >= 2 {
					return
				}
			}
		}
	}

	// this relies on replay=1 to replay messages from the beginning
	// because 2 messages have already been published and we want to pick them up
	consume()

	messagesMutex.RLock()
	t.Logf("First batch complete. Received messages: %v", receivedMessages)
	require.Equal(t, []string{"msg1", "msg2"}, receivedMessages)
	messagesMutex.RUnlock()

	t.Log("Publishing second batch of test messages...")

	messages = []string{"msg3", "msg4"}
	for _, msg := range messages {
		producer.Publish(&ukafka.Message{
			Value: []byte(msg),
		})
	}

	messagesMutex.Lock()
	receivedMessages = nil
	messagesMutex.Unlock()

	// this should be ignoring replay=1 because the kafka server knows the previous offset for this consumer group
	// We expect this to find msg3 and msg4
	// If it were to replay, it would find msg1 and msg2 again
	consume()

	messagesMutex.RLock()
	require.Equal(t, []string{"msg3", "msg4"}, receivedMessages)
	messagesMutex.RUnlock()
}

func byteArrayToIntFromString(message []byte) (int, error) {
	strValue := string(message) // Convert byte array to string

	intValue, err := strconv.Atoi(strValue) // Parse string as integer
	if err != nil {
		return 0, err
	}

	return intValue, nil
}

func produceMessages(logger ulogger.Logger, client ukafka.KafkaAsyncProducerI, numberOfMessages int) {
	for i := 0; i < numberOfMessages; i++ {
		msg := []byte(strconv.Itoa(i))
		client.Publish(&ukafka.Message{
			Value: msg,
		})

		logger.Infof("pushed message: %v", string(msg))
	}
}
