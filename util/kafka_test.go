package util

import (
	"context"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
	tc "github.com/testcontainers/testcontainers-go/modules/compose"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"
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
	ctx := context.Background()
	workerCh := make(chan KafkaMessage)

	filePaths := "../docker-compose-kafka.yml"
	compose, err := tc.NewDockerCompose(filePaths)
	require.NoError(t, err)

	err = compose.Up(context.Background())
	require.NoError(t, err)

	defer stopKafka(compose, ctx)

	// Wait for the services to be ready
	time.Sleep(10 * time.Second)

	kafkaURL, err, ok := gocore.Config().GetURL("kafka_unitTest")
	require.NoError(t, err)
	require.True(t, ok)

	kafkaChan := make(chan []byte, 10000)
	go func() {
		err = StartAsyncProducer(ulogger.TestLogger{}, kafkaURL, kafkaChan)
		require.NoError(t, err)
	}()

	numberOfMessages := 100
	go produceMessages(kafkaChan, numberOfMessages)

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
	ctx := context.Background()
	workerCh := make(chan KafkaMessage)

	filePaths := "../docker-compose-kafka.yml"
	compose, err := tc.NewDockerCompose(filePaths)
	require.NoError(t, err)

	err = compose.Up(ctx)
	require.NoError(t, err)

	defer stopKafka(compose, ctx)

	// Wait for the services to be ready
	time.Sleep(10 * time.Second)

	kafkaURL, err, ok := gocore.Config().GetURL("kafka_unitTest")
	require.NoError(t, err)
	require.True(t, ok)
	var wg sync.WaitGroup

	kafkaChan := make(chan []byte, 10000)
	go func() {
		err = StartAsyncProducer(ulogger.TestLogger{}, kafkaURL, kafkaChan)
		require.NoError(t, err)
	}()

	counter := 0

	// Define test cases
	testCases := []struct {
		name             string
		numberOfMessages int
		consumerClosure  func(KafkaMessage) error
	}{
		{
			name:             "Process messages with all success",
			numberOfMessages: 10,
			consumerClosure: func(message KafkaMessage) error {
				fmt.Println("Consumer closure received message: ", string(message.Message.Value), ", Offset: ", message.Message.Offset)
				wg.Done()
				return nil
			},
		},
		{
			name:             "Process messages with 2nd time success closure",
			numberOfMessages: 10,
			consumerClosure: func(message KafkaMessage) error {
				fmt.Println("Consumer closure received message: ", string(message.Message.Value), ", Offset: ", message.Message.Offset)
				counter++
				if counter%2 == 0 {
					wg.Done()
					return nil
				}
				return errors.New(errors.ERR_BLOCK_ERROR, "block error")
			},
		},
		{
			name:             "Process messages with 3rd time success closure",
			numberOfMessages: 10,
			consumerClosure: func(message KafkaMessage) error {
				fmt.Println("Consumer closure received message: ", string(message.Message.Value), ", Offset: ", message.Message.Offset)
				counter++
				if counter%3 == 0 {
					wg.Done()
					return nil
				}
				return errors.New(errors.ERR_BLOCK_ERROR, "block error")
			},
		},
	}

	// Run test cases
	for _, tCase := range testCases {
		t.Run(tCase.name, func(t *testing.T) {
			wg.Add(tCase.numberOfMessages)

			go produceMessages(kafkaChan, tCase.numberOfMessages)

			go func() {
				err := StartKafkaGroupListener(ctx, ulogger.TestLogger{}, kafkaURL, "kafka_test", workerCh, 1, false, tCase.consumerClosure)
				require.NoError(t, err)
			}()
			wg.Wait()
		})
	}
}

func Test_KafkaAsyncProducerWithManualCommitErrorClosure(t *testing.T) {
	ctx := context.Background()
	workerCh := make(chan KafkaMessage)

	filePaths := "../docker-compose-kafka.yml"
	compose, err := tc.NewDockerCompose(filePaths)
	require.NoError(t, err)

	err = compose.Up(context.Background())
	require.NoError(t, err)

	defer stopKafka(compose, ctx)

	// Wait for the services to be ready
	time.Sleep(10 * time.Second)

	kafkaURL, err, ok := gocore.Config().GetURL("kafka_unitTest")
	require.NoError(t, err)
	require.True(t, ok)

	kafkaChan := make(chan []byte, 10000)
	go func() {
		err = StartAsyncProducer(ulogger.TestLogger{}, kafkaURL, kafkaChan)
		require.NoError(t, err)
	}()

	numberOfMessages := 2
	go produceMessages(kafkaChan, numberOfMessages)

	errClosure := func(message KafkaMessage) error {
		fmt.Println("Consumer closure received message: ", string(message.Message.Value), ", Offset: ", message.Message.Offset)
		return errors.New(errors.ERR_BLOCK_ERROR, "block error")
	}

	go func() {
		err = StartKafkaGroupListener(ctx, ulogger.TestLogger{}, kafkaURL, "kafka_test", workerCh, 1, false, errClosure)
		require.NoError(t, err)

	}()
	// wait to see errors and commits
	time.Sleep(30 * time.Second)
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

func produceMessages(kafkaChan chan []byte, numberOfMessages int) {
	// #nosec G404: ignoring the warning
	r := rand.New(rand.NewSource(time.Now().UnixNano())) // Create a new Rand instance
	for i := 0; i < numberOfMessages; i++ {
		msg := []byte(strconv.Itoa(i))
		kafkaChan <- msg
		fmt.Println("pushed message: ", string(msg))

		randomDelay := time.Duration(r.Intn(200)+1) * time.Millisecond
		time.Sleep(randomDelay)
	}
}

func TestKafkaWithCompose(t *testing.T) {
	ctx := context.Background()

	composeFilePaths := "../docker-compose-kafka.yml"
	compose, err := tc.NewDockerCompose(composeFilePaths)
	require.NoError(t, err)

	err = compose.Up(ctx)
	require.NoError(t, err, "Failed to start Docker Compose")
	//defer compose.Down(ctx)

	// Wait for Kafka to be ready
	time.Sleep(10 * time.Second) // A simple wait for Kafka to start up; adjust as necessary

	// Kafka client configuration
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	// Kafka address from Docker Compose setup
	kafkaAddress := "localhost:9092"

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
	defer partitionConsumer.Close()

	select {
	case msg := <-partitionConsumer.Messages():
		fmt.Printf("Received message: %s\n", string(msg.Value))
	case <-time.After(10 * time.Second):
		t.Fatal("Timed out waiting for message")
	}
}
