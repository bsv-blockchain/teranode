package main

import (
	"github.com/bitcoin-sv/ubsv/errors"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/IBM/sarama"
	"github.com/ordishs/gocore"
)

func main() {
	log.Println(gocore.Config().Stats())

	// read command line arguments
	if len(os.Args) < 2 {
		log.Fatal("Usage: kafkapartitions config nr_partitions")
	}

	config := os.Args[1]
	partitionsStr := os.Args[2]
	partitions, err := strconv.Atoi(partitionsStr)
	if err != nil {
		log.Fatalf("Invalid number of partitions: %v", err)
	}

	if err := createPartitions(config, partitions); err != nil {
		log.Fatal(err)
	}
}

func createPartitions(configName string, partitions int) error {
	url, err, ok := gocore.Config().GetURL(configName)
	if err != nil {
		return errors.New(errors.ERR_PROCESSING, "error getting Kafka URL (%s)", configName, err)
	}

	if !ok {
		return errors.New(errors.ERR_PROCESSING, "kafka URL not found (%s)", configName)
	}

	hosts := strings.Split(url.Host, ",")
	topic := url.Path[1:]

	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0 // Match this with your Kafka cluster version

	admin, err := sarama.NewClusterAdmin(hosts, config)
	if err != nil {
		return errors.New(errors.ERR_SERVICE_ERROR, "error creating cluster admin", err)
	}
	defer admin.Close()

	err = admin.CreatePartitions(topic, int32(partitions), nil, false)
	if err != nil {
		return errors.New(errors.ERR_PROCESSING, "error changing partitions", err)
	}

	log.Printf("%q changed successfully with %d partitions", topic, partitions)

	return nil
}
