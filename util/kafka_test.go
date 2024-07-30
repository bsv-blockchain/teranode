package util

import (
	"context"
	"fmt"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
	tc "github.com/testcontainers/testcontainers-go/modules/compose"
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
	workerCh := make(chan KafkaMessage)
	noErrClosure := func(message KafkaMessage) error {
		return nil
	}

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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	filePaths := "../docker-compose-kafka.yml"
	compose, err := tc.NewDockerCompose(filePaths)
	require.NoError(t, err)

	err = compose.Up(context.Background())
	require.NoError(t, err)

	defer stopKafka(compose, ctx)

	// Wait for the services to be ready
	time.Sleep(30 * time.Second)

	// consumerWithErr := NewKafkaConsumer(workerCh, true, errClosure)
	// consumerWithoutErr := NewKafkaConsumer(workerCh, true, noErrClosure)
	// Test with closure

	kafkaURL, err, ok := gocore.Config().GetURL("kafka_unitTest")
	require.NoError(t, err)
	require.True(t, ok)

	fmt.Println("Kafka URL: ", kafkaURL)

	var blockKafkaProducer KafkaProducerI
	_, blockKafkaProducer, err = ConnectToKafka(kafkaURL)
	require.NoError(t, err)

	err = blockKafkaProducer.Send([]byte("test"), []byte("test"))
	require.NoError(t, err)

	consumerCount := 2

	// Test without error closure, with manual commit
	err = StartKafkaGroupListener(ctx, ulogger.TestLogger{}, kafkaURL, "kafka_test", workerCh, consumerCount, true, noErrClosure)
	require.NoError(t, err)
}

func stopKafka(compose tc.ComposeStack, ctx context.Context) {
	if err := compose.Down(ctx); err != nil {
		panic(err)
	}

}
