package main

import (
	"fmt"
	"log"
	"strings"

	"github.com/IBM/sarama"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/ordishs/gocore"
)

func main() {
	// log.Println(gocore.Config().Stats())

	if err := resetTopic("kafka_blocksConfig"); err != nil {
		log.Fatal(err)
	}

	if err := resetTopic("kafka_subtreesConfig"); err != nil {
		log.Fatal(err)
	}

	if err := resetTopic("kafka_txsConfig"); err != nil {
		log.Fatal(err)
	}

	if err := resetTopic("kafka_txmetaConfig"); err != nil {
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

	// log.Printf("URL: %s", url.String())

	hosts := strings.Split(url.Host, ",")
	// log.Printf("hosts: %v", hosts)

	topic := url.Path[1:]
	// log.Printf("topic: %s", topic)

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

	partitions := util.GetQueryParamInt(url, "partitions", 1)
	replication := util.GetQueryParamInt(url, "replication", 1)

	// Recreate Topic
	topicDetail := &sarama.TopicDetail{
		NumPartitions:     int32(partitions),
		ReplicationFactor: int16(replication),
	}
	err = admin.CreateTopic(topic, topicDetail, false)
	if err != nil {
		return fmt.Errorf("failed to create topic %s: %w", topic, err)
	}

	log.Printf("%q topic created successfully with %d partitions and a replication factor of %d", topic, partitions, replication)

	return nil
}
