package util

import (
	"context"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
	tc "github.com/testcontainers/testcontainers-go/modules/compose"
	"strconv"
	"sync"
	"testing"
	"time"
)

// Test_NewKafkaConsumer consumers with both manual and autocommits
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

// Test_NewKafkaConsumer consumers with both manual and autocommits
func Test_KafkaConsumerWithAutoCommitEnabled(t *testing.T) {
	//workerCh := make(chan KafkaMessage)
	//noErrClosure := func(message KafkaMessage) error {
	//	return nil
	//}

	// half of the messages will return an error, we expect the consumer to not mark it as consumed
	//counter := 0
	//errClosure := func(message KafkaMessage) error {
	//	counter++
	//	if counter%2 == 0 {
	//		return nil
	//	}
	//	return errors.New(errors.ERR_BLOCK_ERROR, "block error")
	//}

	//

	ctx := context.Background()

	filePaths := "../docker-compose-kafka.yml"
	compose, err := tc.NewDockerCompose(filePaths)
	require.NoError(t, err)

	err = compose.Up(context.Background())
	require.NoError(t, err)

	defer stopKafka(compose, ctx)

	// Wait for the services to be ready
	time.Sleep(10 * time.Second)

	// consumerWithErr := NewKafkaConsumer(workerCh, true, errClosure)
	// consumerWithoutErr := NewKafkaConsumer(workerCh, true, noErrClosure)
	// Test with closure

	kafkaURL, err, ok := gocore.Config().GetURL("kafka_unitTest")
	require.NoError(t, err)
	require.True(t, ok)

	fmt.Println("Kafka URL: ", kafkaURL)

	var producer KafkaProducerI
	_, producer, err = ConnectToKafka(kafkaURL)
	require.NoError(t, err)

	fmt.Println("connected to kafka")

	err = producer.Send([]byte("test"), []byte("test"))
	require.NoError(t, err)

	fmt.Println("sent message")

	//consumerCount := 2
	//
	//// Test without error closure, with manual commit
	//err = StartKafkaGroupListener(ctx, ulogger.TestLogger{}, kafkaURL, "kafka_test", workerCh, consumerCount, true, noErrClosure)
	//require.NoError(t, err)
}

func Test_KafkaAsyncProducer(t *testing.T) {
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

	fmt.Println("Kafka URL: ", kafkaURL)

	kafkaChan := make(chan []byte, 10000)
	go func() {
		err = StartAsyncProducer(ulogger.TestLogger{}, kafkaURL, kafkaChan)
		require.NoError(t, err)
	}()

	numberOfMessages := 10
	fmt.Println("starting pushing messages")
	go produceMessages(kafkaChan, numberOfMessages)

	time.Sleep(2 * time.Second)

	var wg sync.WaitGroup
	wg.Add(10)

	fmt.Println("starting consumer")
	go func() {
		err = StartKafkaGroupListener(ctx, ulogger.TestLogger{}, kafkaURL, "kafka_test", workerCh, 1, true, func(message KafkaMessage) error {
			fmt.Println("received message: ", string(message.Message.Value))
			wg.Done()
			return nil

		})
		require.NoError(t, err)
	}()
	wg.Wait()
}

func produceMessages(kafkaChan chan []byte, numberOfMessages int) {
	for i := 0; i < numberOfMessages; i++ {
		msg := []byte("test" + strconv.Itoa(i))
		kafkaChan <- msg
		fmt.Println("pushed message: ", string(msg))
		//time.Sleep(1 * time.Millisecond)
	}

	fmt.Println("done pushing messages")

}

func stopKafka(compose tc.ComposeStack, ctx context.Context) {
	if err := compose.Down(ctx); err != nil {
		panic(err)
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
