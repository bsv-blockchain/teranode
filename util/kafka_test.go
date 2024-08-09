package util

import (
	"context"
	"fmt"
	"github.com/IBM/sarama"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
	tc "github.com/testcontainers/testcontainers-go/modules/compose"
)

func Test_NewKafkaConsumer(t *testing.T) {
	workerCh := make(chan KafkaMessage)
	consumer := NewKafkaConsumer(workerCh, true)
	if consumer.autoCommitEnabled != true {
		t.Errorf("Expected autoCommitEnabled to be true, got %v", consumer.autoCommitEnabled)
	}
	consumer = NewKafkaConsumer(workerCh, false)
	if consumer.autoCommitEnabled != false {
		t.Errorf("Expected autoCommitEnabled to be false, got %v", consumer.autoCommitEnabled)
	}

}

func Test_KafkaAsyncProducerConsumerAutoCommit(t *testing.T) {
	SkipVeryLongTests(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // shuts down the listener

	workerCh := make(chan KafkaMessage)

	filePaths := "../docker-compose-kafka.yml"
	compose, err := tc.NewDockerCompose(filePaths)
	require.NoError(t, err)

	err = compose.Up(context.Background())
	require.NoError(t, err)

	defer stopKafka(compose, ctx)

	kafkaURL, err, ok := gocore.Config().GetURL("kafka_unitTest")
	require.NoError(t, err)
	require.True(t, ok)

	err = waitForKafkaReady(ctx, kafkaURL.Host, 30*time.Second)
	require.NoError(t, err)

	kafkaChan := make(chan []byte, 10000)
	go func() {
		err = StartAsyncProducer(ulogger.TestLogger{}, kafkaURL, kafkaChan)
		require.NoError(t, err)
	}()

	numberOfMessages := 100
	go produceMessages(nil, kafkaChan, numberOfMessages)

	var wg sync.WaitGroup
	wg.Add(numberOfMessages)

	go func() {
		err = StartKafkaGroupListener(ctx, ulogger.TestLogger{}, kafkaURL, "kafka_test", workerCh, 1, true, func(message KafkaMessage) error {
			msgInt, err := byteArrayToIntFromString(message.Message.Value)
			require.NoError(t, err)

			if message.Message.Offset != int64(msgInt) {
				return err
			}

			fmt.Println("received message: ", string(message.Message.Value), ", Offset: ", message.Message.Offset)
			wg.Done()
			// convert following to int string(message.Message.Value)
			return nil

		})
		require.NoError(t, err)
	}()
	wg.Wait()
}

func Test_KafkaAsyncProducerWithManualCommitParams(t *testing.T) {
	SkipVeryLongTests(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	workerCh := make(chan KafkaMessage)

	filePaths := "../docker-compose-kafka.yml"
	compose, err := tc.NewDockerCompose(filePaths)
	require.NoError(t, err)

	err = compose.Up(ctx)
	require.NoError(t, err)

	defer stopKafka(compose, ctx)

	kafkaURL, err, ok := gocore.Config().GetURL("kafka_unitTest")
	require.NoError(t, err)
	require.True(t, ok)

	err = waitForKafkaReady(ctx, kafkaURL.Host, 30*time.Second)
	require.NoError(t, err)

	var wg sync.WaitGroup

	kafkaChan := make(chan []byte, 10000)
	go func() {
		err = StartAsyncProducer(ulogger.TestLogger{}, kafkaURL, kafkaChan)
		require.NoError(t, err)
	}()

	counter := 0

	// Define test cases
	testCases := []struct {
		name            string
		consumerClosure func(KafkaMessage) error
	}{
		{
			name: "Process messages with all success",
			consumerClosure: func(message KafkaMessage) error {
				fmt.Println("Consumer closure#1 received message: ", string(message.Message.Value), ", Offset: ", message.Message.Offset)
				wg.Done()
				return nil
			},
		},
		{
			name: "Process messages with 2nd time success closure",
			consumerClosure: func(message KafkaMessage) error {
				fmt.Println("Consumer closure#2 received message: ", string(message.Message.Value), ", Offset: ", message.Message.Offset)
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
				fmt.Println("Consumer closure#3 received message: ", string(message.Message.Value), ", Offset: ", message.Message.Offset)
				counter++
				if counter%3 == 0 {
					wg.Done()
					return nil
				}
				return errors.New(errors.ERR_BLOCK_ERROR, "block error")
			},
		},
	}

	// send 10 messages
	wgP := sync.WaitGroup{}
	wgP.Add(10)
	go produceMessages(&wgP, kafkaChan, 10)
	wgP.Wait()

	// Run test cases
	for _, tCase := range testCases {
		t.Run(tCase.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())

			wg.Add(10)

			// because we start a new listener for each test case, it will pick up the previously sent messages - no need to send more messages
			// go produceMessages(nil, kafkaChan, 0, 10)

			go func() {
				err := StartKafkaGroupListener(ctx, ulogger.NewZeroLogger("test"), kafkaURL, "kafka_test", workerCh, 1, false, tCase.consumerClosure)
				require.NoError(t, err)
			}()

			wg.Wait()

			// Stop the listener
			cancel()
		})
	}
}

func Test_KafkaAsyncProducerWithManualCommitErrorClosure(t *testing.T) {
	SkipVeryLongTests(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // shuts down listener

	workerCh := make(chan KafkaMessage)

	filePaths := "../docker-compose-kafka.yml"
	compose, err := tc.NewDockerCompose(filePaths)
	require.NoError(t, err)

	err = compose.Up(context.Background())
	require.NoError(t, err)

	defer stopKafka(compose, ctx)

	kafkaURL, err, ok := gocore.Config().GetURL("kafka_unitTest")
	require.NoError(t, err)
	require.True(t, ok)

	err = waitForKafkaReady(ctx, kafkaURL.Host, 30*time.Second)
	require.NoError(t, err)

	kafkaChan := make(chan []byte, 10000)
	go func() {
		err := StartAsyncProducer(ulogger.TestLogger{}, kafkaURL, kafkaChan)
		require.NoError(t, err)
	}()

	numberOfMessages := 2
	wg := sync.WaitGroup{}
	wg.Add(numberOfMessages)
	go produceMessages(&wg, kafkaChan, numberOfMessages)
	wg.Wait()

	c := make(chan struct{})
	errClosure := func(message KafkaMessage) error {
		fmt.Println("Consumer closure received message: ", string(message.Message.Value), ", Offset: ", message.Message.Offset)
		c <- struct{}{}
		return errors.New(errors.ERR_BLOCK_ERROR, "block error")
	}

	go func() {
		err := StartKafkaGroupListener(ctx, ulogger.TestLogger{}, kafkaURL, "kafka_test", workerCh, 1, false, errClosure)
		require.NoError(t, err)
	}()

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

func Test_KafkaWithCompose(t *testing.T) {
	ctx := context.Background()

	composeFilePaths := "../docker-compose-kafka.yml"
	compose, err := tc.NewDockerCompose(composeFilePaths)
	require.NoError(t, err)

	err = compose.Up(ctx)
	require.NoError(t, err, "Failed to start Docker Compose")
	defer stopKafka(compose, ctx)

	// Kafka client configuration
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	// Kafka address from Docker Compose setup
	kafkaAddress := "localhost:9092"

	err = waitForKafkaReady(ctx, kafkaAddress, 30*time.Second)
	require.NoError(t, err)

	// Test message production
	producer, err := sarama.NewSyncProducer([]string{kafkaAddress}, config)
	require.NoError(t, err, "Failed to create Kafka producer")

	topic := "test_topic"
	message := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder("Hello Kafka"),
	}
	partition, offset, err := producer.SendMessage(message)
	require.NoError(t, err, "Failed to send message to Kafka")
	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)

	// Test message consumption
	consumer, err := sarama.NewConsumer([]string{kafkaAddress}, nil)
	require.NoError(t, err, "Failed to create Kafka consumer")

	partitionConsumer, err := consumer.ConsumePartition(topic, partition, offset)
	require.NoError(t, err, "Failed to consume message from Kafka")
	defer func(partitionConsumer sarama.PartitionConsumer) {
		_ = partitionConsumer.Close()
	}(partitionConsumer)

	select {
	case msg := <-partitionConsumer.Messages():
		fmt.Printf("Received message: %s\n", string(msg.Value))
	case <-time.After(10 * time.Second):
		t.Fatal("Timed out waiting for message")
	}
}

func stopKafka(compose tc.ComposeStack, ctx context.Context) {
	if err := compose.Down(ctx); err != nil {
		panic(err)
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

func waitForKafkaReady(ctx context.Context, kafkaAddr string, timeout time.Duration) error {
	start := time.Now()
	for {
		conn, err := net.DialTimeout("tcp", kafkaAddr, 1*time.Second)
		if err == nil {
			conn.Close()
			return nil
		}
		if time.Since(start) > timeout {
			return errors.NewUnknownError("timed out waiting for Kafka to be ready")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
			// Retry after a short delay
		}
	}
}
