package producer

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/ordishs/gocore"
)

const rawTx = "0100000001eca36bd38478ea81c4b716dda75b72117605db644bd506b7fe0a9736a39be949010000006b483045022100cdb5ab9fa051af3ea36208d83a9fdfdc11419668b836a42c60f9780d06bb06310220455a2d1953d6620c2c9140b7647e7ebe1ea33cdb00a36f5b96d96b5b12350b964121020b90adc336dcef4d35a7fb8cd7ba0f1d321c46eebc94a3bd1ed4ef63fa4e949bffffffff0204000000000000001976a9148d30f8237c83b899e51f41a27e1317a735749f6d88acf8260000000000001976a9148d30f8237c83b899e51f41a27e1317a735749f6d88ac00000000"

var txsChan chan []byte

func NewProducer(ctx context.Context) {
	logger := ulogger.TestLogger{}

	_, cancel := context.WithCancel(ctx)
	// ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signals
		cancel()
	}()

	kafkaUrl, err, ok := gocore.Config().GetURL("kafkatest_kafkaBrokers")
	if err != nil || !ok {
		logger.Errorf("unable to parse kafka url: %v\n", err)
		return
	}
	txCount, _ := gocore.Config().GetInt("kafkatest_txCount", 500_000)
	// only start the kafka producer if there are workers listening
	// this can be used to disable the kafka producer, by just setting workers to 0

	txsChan = make(chan []byte, 10000)

	go func() {
		if err := util.StartAsyncProducer(logger, kafkaUrl, txsChan); err != nil {
			logger.Errorf("error starting kafka producer: %v", err)
			return
		}
	}()

	if err != nil {
		logger.Errorf("unable to connect to kafka: %v", err)
		return
	}
	time.Sleep(2 * time.Second)
	logger.Infof("connected to kafka at %s", kafkaUrl.Host)

	binaryData, err := hex.DecodeString(rawTx)
	if err != nil {
		logger.Errorf("failed to decode rawTx: %s", err)
		return
	}
	startTime := time.Now()
	messageCount := txCount
	for i := 0; i < txCount; i++ {
		publishToKafka(binaryData)
	}

	elapsedTime := time.Since(startTime)
	msgsPerSecond := float64(messageCount) / elapsedTime.Seconds()
	fmt.Printf("Sent %d messages in %.2f seconds. Throughput: %.2f msgs/sec\n", messageCount, elapsedTime.Seconds(), msgsPerSecond)

	close(txsChan)
	// Wait for a signal to exit
	<-signals
	logger.Debugf("Shutting down producer...")
}

func publishToKafka(bData []byte) {
	txsChan <- bData
}
