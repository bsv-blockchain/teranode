package consumer

import (
	"context"
	"fmt"
	"time"

	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/ordishs/gocore"
)

func NewConsumer() {
	logger := ulogger.TestLogger{}
	bufferSize := 1024 * 1024

	kafkaUrl, err, ok := gocore.Config().GetURL("kafkatest_kafkaBrokers")
	if err != nil || !ok {
		logger.Errorf("unable to parse kafka url: %v", err)
		return
	}
	workers, _ := gocore.Config().GetInt("kafkatest_kafkaWorkers", 100)
	if workers < 1 {
		// no workers, nothing to do
		return
	}
	partitionConsumerRatio, _ := gocore.Config().GetInt("kafkatest_partitionConsumerRation", 8)
	if partitionConsumerRatio < 1 {
		partitionConsumerRatio = 1
	}

	partitions := util.GetQueryParamInt(kafkaUrl, "partitions", 1)

	consumerCount := partitions / partitionConsumerRatio

	fmt.Printf("starting Kafka on address: %s, with %d consumers and %d workers\n", kafkaUrl.String(), consumerCount, workers)

	workerCh := make(chan util.KafkaMessage, bufferSize)
	for i := 0; i < workers; i++ {
		go func(workerNo int) {

			messageCount := 0
			var startTime time.Time
			for msg := range workerCh {
				_ = msg
				if messageCount == 0 {
					startTime = time.Now()
				}
				messageCount++
				if messageCount%100_000 == 0 { // log every n messages
					elapsedTime := time.Since(startTime)
					msgsPerSecond := float64(messageCount) / elapsedTime.Seconds()
					fmt.Printf("worker %d processed %d messages in %.2f seconds. Throughput: %.2f msg/sec\n", workerNo, messageCount, elapsedTime.Seconds(), msgsPerSecond)
				}
			}
		}(i)
	}

	_ = util.StartKafkaGroupListener(context.Background(), logger, kafkaUrl, "kafkatest", workerCh, consumerCount)
	select {}
}
