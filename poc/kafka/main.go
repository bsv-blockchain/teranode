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

func main() {

	mode := flag.String("mode", "consumer", "Run producer or consumer")
	// mode := flag.String("mode", "producer", "Run producer or consumer")
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
