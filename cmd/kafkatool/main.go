package main

import (
	"log"
	"strings"

	"github.com/IBM/sarama"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/ordishs/gocore"
)

func main() {
	log.Println(gocore.Config().Stats())

	topics := []string{
		"kafka_blocksConfig",
		"kafka_blocksFinalConfig",
		"kafka_subtreesConfig",
		"kafka_subtreesFinalConfig",
		"kafka_txsConfig",
		"kafka_txmetaConfig",
	}

	for _, topic := range topics {
		if err := resetTopic(topic); err != nil {
			log.Fatal(err)
		}
	}
}

func resetTopic(configName string) error {
	url, err, ok := gocore.Config().GetURL(configName)
	if err != nil {
		return errors.New(errors.ERR_PROCESSING, "error getting Kafka URL (%s)", configName, err)
	}

	if !ok {
		return errors.New(errors.ERR_PROCESSING, "kafka URL not found (%s)", configName)
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
		return errors.New(errors.ERR_SERVICE_ERROR, "error creating cluster admin", err)
	}
	defer admin.Close()

	// Delete Topic
	err = admin.DeleteTopic(topic)
	if err != nil {
		log.Printf("WARN: Failed to delete topic %s: %v", topic, err)
	}

	partitions := util.GetQueryParamInt(url, "partitions", 1)
	replication := util.GetQueryParamInt(url, "replication", 1)
	retentionPeriod := util.GetQueryParam(url, "retention", "60000") // one minute

	topicDetail := &sarama.TopicDetail{
		NumPartitions:     int32(partitions),
		ReplicationFactor: int16(replication),
		ConfigEntries: map[string]*string{
			"retention.ms": &retentionPeriod, // Set the retention period
		},
	}
	err = admin.CreateTopic(topic, topicDetail, false)
	if err != nil {
		return errors.New(errors.ERR_SERVICE_ERROR, "failed to create topic %s", topic, err)
	}

	log.Printf("%q topic created successfully with %d partitions and a replication factor of %d", topic, partitions, replication)

	return nil
}
