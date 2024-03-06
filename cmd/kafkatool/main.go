package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/IBM/sarama"
	"github.com/ordishs/gocore"
)

func main() {
	if err := resetTopic("block_kafkaBrokers"); err != nil {
		log.Fatal(err)
	}

	if err := resetTopic("blockvalidation_kafkaBrokers"); err != nil {
		log.Fatal(err)
	}

}

func resetTopic(configName string) error {
	url, err, ok := gocore.Config().GetURL(configName)
	if err != nil {
		return fmt.Errorf("error getting Kafka URL (%s): %w", configName, err)
	}

	if !ok {
		return fmt.Errorf("kafka URL not found (%s)", configName)
	}

	hosts := strings.Split(url.Host, ",")

	topic := url.Path[1:]

	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0 // Match this with your Kafka cluster version

	admin, err := sarama.NewClusterAdmin(hosts, config)
	if err != nil {
		return fmt.Errorf("error creating cluster admin: %w", err)
	}
	defer admin.Close()

	// Delete Topic
	err = admin.DeleteTopic(topic)
	if err != nil {
		log.Printf("WARN: Failed to delete topic %s: %v", topic, err)
	}

	queryParams := url.Query()
	partitionsStr := queryParams.Get("partitions")
	replicationStr := queryParams.Get("replication")

	partitions, err := strconv.Atoi(partitionsStr)
	if err != nil {
		return fmt.Errorf("failed to parse partitions: %w", err)
	}

	replication, err := strconv.Atoi(replicationStr)
	if err != nil {
		return fmt.Errorf("failed to parse replication: %w", err)
	}

	// Recreate Topic
	topicDetail := &sarama.TopicDetail{
		NumPartitions:     int32(partitions),
		ReplicationFactor: int16(replication),
	}
	err = admin.CreateTopic(topic, topicDetail, false)
	if err != nil {
		return fmt.Errorf("failed to create topic %s: %w", topic, err)
	}

	return nil
}
