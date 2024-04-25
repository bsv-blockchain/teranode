package main

import (
	"log"

	"github.com/IBM/sarama"
)

// Define the handler function type
type MessageHandler func(*sarama.ConsumerMessage) error

func main() {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.AutoCommit.Enable = false // Disable automatic offset commit

	// Specify brokers
	brokers := []string{"localhost:9092"}

	// Create a new client
	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create client: %s", err)
	}
	defer client.Close()

	// Create a consumer from the client
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		log.Fatalf("Failed to start consumer: %s", err)
	}
	defer consumer.Close()

	// Create offset manager from the client
	offsetManager, err := sarama.NewOffsetManagerFromClient("my_group", client)
	if err != nil {
		log.Fatalf("Failed to create offset manager: %s", err)
	}
	defer offsetManager.Close()

	topic := "your_topic"
	partition := int32(0)

	// Manage offsets for a specific partition
	partitionOffsetManager, err := offsetManager.ManagePartition(topic, partition)
	if err != nil {
		log.Fatalf("Failed to start partition offset manager: %s", err)
	}
	defer partitionOffsetManager.Close()

	// Consume from the specified topic and partition
	partitionConsumer, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
	if err != nil {
		log.Fatalf("Failed to start consumer for partition: %s", err)
	}
	defer partitionConsumer.Close()

	// Infinite loop to read messages
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			err := handleMessage(msg)
			if err == nil {
				partitionOffsetManager.MarkOffset(msg.Offset+1, "") // Mark message as processed
			}
		case err := <-partitionConsumer.Errors():
			log.Printf("Error occurred: %s", err)
		}
	}
}

// Sample handler that processes messages
func handleMessage(msg *sarama.ConsumerMessage) error {
	// Process message
	log.Printf("Received message: %s", string(msg.Value))
	// Return nil if processing is successful
	return nil
}
