package util

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/Shopify/sarama"
	"github.com/bitcoin-sv/ubsv/ulogger"
)

func ConnectToKafka(kafkaURL *url.URL) (sarama.ClusterAdmin, sarama.SyncProducer, error) {
	brokersUrl := strings.Split(kafkaURL.Host, ",")

	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0

	clusterAdmin, err := sarama.NewClusterAdmin(strings.Split(kafkaURL.Host, ","), config)
	if err != nil {
		return nil, nil, fmt.Errorf("error while creating cluster admin: %v", err)
	}

	partitions := 1
	partitionsStr := kafkaURL.Query().Get("partitions")
	if partitionsStr != "" {
		partitions, err = strconv.Atoi(partitionsStr)
		if err != nil {
			return nil, nil, fmt.Errorf("error while parsing partitions: %v", err)
		}
	}

	replicationFactor := 1
	replicationFactorStr := kafkaURL.Query().Get("replication")
	if replicationFactorStr != "" {
		replicationFactor, err = strconv.Atoi(replicationFactorStr)
		if err != nil {
			return nil, nil, fmt.Errorf("error while parsing replication: %v", err)
		}
	}

	topic := kafkaURL.Path[1:]
	_ = clusterAdmin.CreateTopic(topic, &sarama.TopicDetail{
		NumPartitions:     int32(partitions),
		ReplicationFactor: int16(replicationFactor),
	}, false)

	producer, err := ConnectProducer(brokersUrl)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to connect to kafka: %v", err)
	}

	return clusterAdmin, producer, nil
}

func ConnectProducer(brokersUrl []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Partitioner = sarama.NewManualPartitioner
	// NewSyncProducer creates a new SyncProducer using the given broker addresses and configuration.
	conn, err := sarama.NewSyncProducer(brokersUrl, config)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func StartKafkaGroupListener(ctx context.Context, logger ulogger.Logger, kafkaURL *url.URL, groupID string, workerCh chan []byte) error {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	/**
	 * Set up a new Sarama consumer group
	 */
	consumer := KafkaConsumer{
		ready:    make(chan bool),
		workerCh: workerCh,
	}

	ctx, cancel := context.WithCancel(ctx)
	brokersUrl := strings.Split(kafkaURL.Host, ",")
	client, err := sarama.NewConsumerGroup(brokersUrl, groupID, config)
	if err != nil {
		cancel()
		return fmt.Errorf("error creating consumer group client: %v", err)
	}

	topics := []string{kafkaURL.Path[1:]}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side re-balance happens, the consumer session will need to be
			// recreated to get the new claims
			if err = client.Consume(ctx, topics, &consumer); err != nil {
				logger.Errorf("Error from consumer: %v", err)
			}

			// check if context was cancelled, signalling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()

	<-consumer.ready // Await till the consumer has been set up
	logger.Infof("[Validator] Kafka consumer up and running!...")

	sigusr1 := make(chan os.Signal, 1)
	signal.Notify(sigusr1, syscall.SIGUSR1)

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	keepRunning := true
	for keepRunning {
		select {
		case <-ctx.Done():
			logger.Infof("[Kafka] %s: terminating: context cancelled", groupID)
			keepRunning = false
		case <-sigterm:
			logger.Infof("[Kafka] %s: terminating: via signal", groupID)
			keepRunning = false
		case <-sigusr1:
			//toggleConsumptionFlow(client, &consumptionIsPaused)
		}
	}
	cancel()
	wg.Wait()

	if err = client.Close(); err != nil {
		logger.Errorf("[Kafka] %s: error closing client: %v", groupID, err)
	}

	return nil
}
