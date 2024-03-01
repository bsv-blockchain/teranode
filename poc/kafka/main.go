package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/bitcoin-sv/ubsv/poc/kafka/consumer"
	"github.com/bitcoin-sv/ubsv/poc/kafka/producer"
)

// Transaction represents a transaction with ID and binary data.
type Transaction struct {
	Id   string
	Data []byte
}

const (
	kafkaTopic = "test1"
	kafkaUrl   = "localhost:9092"

	// Your raw transaction hex string
	rawTx             = "0100000001eca36bd38478ea81c4b716dda75b72117605db644bd506b7fe0a9736a39be949010000006b483045022100cdb5ab9fa051af3ea36208d83a9fdfdc11419668b836a42c60f9780d06bb06310220455a2d1953d6620c2c9140b7647e7ebe1ea33cdb00a36f5b96d96b5b12350b964121020b90adc336dcef4d35a7fb8cd7ba0f1d321c46eebc94a3bd1ed4ef63fa4e949bffffffff0204000000000000001976a9148d30f8237c83b899e51f41a27e1317a735749f6d88acf8260000000000001976a9148d30f8237c83b899e51f41a27e1317a735749f6d88ac00000000"
	partitionCount    = 1
	replicationFactor = 1

	subtreeInterval = 1 // interval in seconds
	consumerCount   = 1
)

func main() {

	// mode := flag.String("mode", "consumer", "Run producer or consumer")
	mode := flag.String("mode", "producer", "Run producer or consumer")
	// txCount := flag.Int("txcount", 500_000, "Number of transactions to send to kafka")

	flag.Parse()

	// Validate the mode
	switch *mode {
	case "producer":
		fmt.Println("Running as a producer")
		producer.NewProducer(context.Background())
	case "consumer":
		fmt.Println("Running as a consumer")
		consumer.NewConsumer()
	default:
		fmt.Fprintf(os.Stderr, "Error: Invalid mode specified. Must be 'producer' or 'consumer'\n")
		flag.PrintDefaults()
		os.Exit(1)
	}

}
