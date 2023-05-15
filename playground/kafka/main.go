package main

import (
	"crypto/rand"
	"fmt"
	"log"

	"github.com/Shopify/sarama"
)

func main() {
	// to produce messages
	topic := "txs"

	brokersUrl := []string{"localhost:9092"}
	producer, err := ConnectProducer(brokersUrl)
	if err != nil {
		log.Fatalf("unable to connect to kafka: %v", err)
	}
	defer producer.Close()

	n := 0
	for {
		txID := make([]byte, 32)
		_, _ = rand.Read(txID)

		err = PushToQueue(producer, topic, txID)
		if err != nil {
			log.Fatal("failed to write messages:", err)
		}

		n++
	}
}

func ConnectProducer(brokersUrl []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	// NewSyncProducer creates a new SyncProducer using the given broker addresses and configuration.
	conn, err := sarama.NewSyncProducer(brokersUrl, config)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func PushToQueue(producer sarama.SyncProducer, topic string, txID []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(txID),
	}
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return err
	}
	if offset%1000 == 0 {
		fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\r", topic, partition, offset)
	}

	return nil
}
