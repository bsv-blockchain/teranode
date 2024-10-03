package kafka

import (
	"context"
	"fmt"
	"math/rand"
	"net/url"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/docker/go-connections/nat"
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
		KAFKA_PARTITIONS         = 1
		KAFKA_REPLICATION_FACTOR = 1
		KAFKA_UNITTEST           = "unittest"
	)

	kafkaURL := &url.URL{
		Scheme:   "kafka",
		Host:     testContainer.GetBrokerAddresses()[0],
		Path:     KAFKA_UNITTEST,
		RawQuery: fmt.Sprintf("partitions=%d&replicationFactor=%d", KAFKA_PARTITIONS, KAFKA_REPLICATION_FACTOR),
	}

	// err = waitForKafkaReady(ctx, kafkaURL.Host, 30*time.Second)
	// require.NoError(t, err)

	producerClient, err := NewKafkaAsyncProducer(ulogger.TestLogger{}, kafkaURL, make(chan []byte, 10000))
	require.NoError(t, err)

	go producerClient.Start(ctx)

	numberOfMessages := 100
	go produceMessages(nil, producerClient.PublishChannel, numberOfMessages)

	var wg sync.WaitGroup
	wg.Add(numberOfMessages)

	listenerClient, err := NewKafkaConsumeGroup(ctx, KafkaListenerConfig{
		Logger:            ulogger.TestLogger{},
		URL:               kafkaURL,
		GroupID:           "kafka_test",
		ConsumerCount:     1,
		AutoCommitEnabled: true,
		ConsumerFn: func(message KafkaMessage) error {
			msgInt, err := byteArrayToIntFromString(message.Message.Value)
			require.NoError(t, err)

			if message.Message.Offset != int64(msgInt) {
				return err
			}

			// fmt.Println("received message: ", string(message.Message.Value), ", Offset: ", message.Message.Offset)
			wg.Done()
			return nil
		},
	})
	require.NoError(t, err)
	go listenerClient.Start(ctx)
	wg.Wait()
}

func Test_KafkaAsyncProducerWithManualCommitParams_using_tc(t *testing.T) {
	t.Parallel()
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
		KAFKA_PARTITIONS         = 1
		KAFKA_REPLICATION_FACTOR = 1
		KAFKA_UNITTEST           = "unittest"
	)

	// Construct the Kafka URL
	kafkaURL := &url.URL{
		Scheme:   "kafka",
		Host:     testContainer.GetBrokerAddresses()[0],
		Path:     KAFKA_UNITTEST,
		RawQuery: fmt.Sprintf("partitions=%d&replication=%d&retention=600000&flush_bytes=1024&flush_messages=10000&flush_frequency=1s&replay=1", KAFKA_PARTITIONS, KAFKA_REPLICATION_FACTOR),
	}

	fmt.Println("Kafka URL: ", kafkaURL)
	// (Optional) Uncomment if you want to wait for Kafka readiness
	// err = waitForKafkaReady(ctx, kafkaURL.Host, 30*time.Second)
	// require.NoError(t, err)

	var wg sync.WaitGroup

	producerClient, err := NewKafkaAsyncProducer(ulogger.TestLogger{}, kafkaURL, make(chan []byte, 10000))
	require.NoError(t, err)

	go producerClient.Start(ctx)

	counter := 0

	// Define test cases
	testCases := []struct {
		name            string
		consumerClosure func(KafkaMessage) error
	}{
		{
			name: "Process messages with all success",
			consumerClosure: func(message KafkaMessage) error {
				// fmt.Println("Consumer closure#1 received message: ", string(message.Message.Value), ", Offset: ", message.Message.Offset)
				wg.Done()
				return nil
			},
		},
		{
			name: "Process messages with 2nd time success closure",
			consumerClosure: func(message KafkaMessage) error {
				// fmt.Println("Consumer closure#2 received message: ", string(message.Message.Value), ", Offset: ", message.Message.Offset)
				counter++
				if counter%2 == 0 {
					wg.Done()
					return nil
				}
				return errors.New(errors.ERR_BLOCK_ERROR, "block error")
			},
		},
		{
			name: "Process messages with 3rd time success closure",
			consumerClosure: func(message KafkaMessage) error {
				// fmt.Println("Consumer closure#3 received message: ", string(message.Message.Value), ", Offset: ", message.Message.Offset)
				counter++
				if counter%3 == 0 {
					wg.Done()
					return nil
				}
				return errors.New(errors.ERR_BLOCK_ERROR, "block error")
			},
		},
	}

	// Send 10 messages
	wgP := sync.WaitGroup{}
	wgP.Add(10)
	go produceMessages(&wgP, producerClient.PublishChannel, 10)
	wgP.Wait()

	// Run test cases
	for _, tCase := range testCases {
		t.Run(tCase.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())

			wg.Add(10)

			// Start the Kafka group listener for the current test case
			client, err := NewKafkaConsumeGroup(ctx, KafkaListenerConfig{
				Logger:            ulogger.NewZeroLogger("test"),
				URL:               kafkaURL,
				GroupID:           "kafka_test",
				ConsumerCount:     1,
				AutoCommitEnabled: false,
				ConsumerFn:        tCase.consumerClosure,
			})
			require.NoError(t, err)

			go client.Start(ctx)

			wg.Wait()

			// Stop the listener
			cancel()
		})
	}
}

func Test_KafkaAsyncProducerWithManualCommitErrorClosure_using_tc(t *testing.T) {
	t.Parallel()
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
		KAFKA_PARTITIONS         = 1
		KAFKA_REPLICATION_FACTOR = 1
		KAFKA_UNITTEST           = "unittest"
	)

	// Construct the Kafka URL
	kafkaURL := &url.URL{
		Scheme: "kafka",
		Host:   testContainer.GetBrokerAddresses()[0],
		Path:   KAFKA_UNITTEST,
		RawQuery: fmt.Sprintf("partitions=%d&replication=%d&retention=600000&flush_bytes=1024&flush_messages=10000&flush_frequency=1s&replay=1",
			KAFKA_PARTITIONS, KAFKA_REPLICATION_FACTOR),
	}

	fmt.Println("Kafka URL: ", kafkaURL)

	// (Optional) Uncomment if you want to wait for Kafka readiness
	// err = waitForKafkaReady(ctx, kafkaURL.Host, 30*time.Second)
	// require.NoError(t, err)

	producerClient, err := NewKafkaAsyncProducer(ulogger.TestLogger{}, kafkaURL, make(chan []byte, 10000))
	require.NoError(t, err)

	go producerClient.Start(ctx)

	numberOfMessages := 2
	wg := sync.WaitGroup{}
	wg.Add(numberOfMessages)
	go produceMessages(&wg, producerClient.PublishChannel, numberOfMessages)
	wg.Wait()

	c := make(chan struct{})
	errClosure := func(message KafkaMessage) error {
		fmt.Println("Consumer closure received message: ", string(message.Message.Value), ", Offset: ", message.Message.Offset)
		c <- struct{}{}
		return errors.New(errors.ERR_BLOCK_ERROR, "block error")
	}

	client, err := NewKafkaConsumeGroup(context.Background(), KafkaListenerConfig{
		Logger:            ulogger.TestLogger{},
		URL:               kafkaURL,
		GroupID:           "kafka_test",
		ConsumerCount:     1,
		AutoCommitEnabled: false,
		ConsumerFn:        errClosure,
	})
	require.NoError(t, err)

	go client.Start(context.Background())

	count := 0
	for count < numberOfMessages*4 { // each message is processed 4 times before offset commit
		select {
		case <-time.After(30 * time.Second):
			t.Fatal("Timed out waiting")
		case <-c:
			count++
		}
	}
}

func byteArrayToIntFromString(message []byte) (int, error) {
	strValue := string(message)             // Convert byte array to string
	intValue, err := strconv.Atoi(strValue) // Parse string as integer
	if err != nil {
		return 0, err
	}
	return intValue, nil
}

func produceMessages(wg *sync.WaitGroup, kafkaChan chan []byte, numberOfMessages int) {
	// #nosec G404: ignoring the warning
	r := rand.New(rand.NewSource(time.Now().UnixNano())) // Create a new Rand instance
	for i := 0; i < numberOfMessages; i++ {
		msg := []byte(strconv.Itoa(i))
		kafkaChan <- msg
		fmt.Println("pushed message: ", string(msg))

		randomDelay := time.Duration(r.Intn(200)+1) * time.Millisecond
		time.Sleep(randomDelay)
		if wg != nil {
			wg.Done()
		}
	}
}
