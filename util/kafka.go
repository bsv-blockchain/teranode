package util

import (
	"context"
	"encoding/binary"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/ordishs/gocore"
)

/**
kafka-topics.sh --list --bootstrap-server localhost:9092

kafka-topics.sh --describe --bootstrap-server localhost:9092

kafka-console-consumer.sh --topic blocks --bootstrap-server localhost:9092 --from-beginning
*/

type KafkaMessage struct {
	Message *sarama.ConsumerMessage
	Session sarama.ConsumerGroupSession
}

type KafkaProducerI interface {
	GetClient() sarama.ConsumerGroup
	Send(key []byte, data []byte) error
	Close() error
}

type AsyncKafkaProducer struct {
	Producer   sarama.AsyncProducer
	Topic      string
	Partitions int32
	client     sarama.ConsumerGroup
}

func (k *AsyncKafkaProducer) Close() error {
	if err := k.Producer.Close(); err != nil {
		return fmt.Errorf("failed to close Kafka producer: %v", err)
	}

	return nil
}

func (k *AsyncKafkaProducer) GetClient() sarama.ConsumerGroup {
	return k.client
}

func (k *AsyncKafkaProducer) Send(key []byte, data []byte) error {
	partition := binary.LittleEndian.Uint32(key) % uint32(k.Partitions)
	k.Producer.Input() <- &sarama.ProducerMessage{
		Topic:     k.Topic,
		Key:       sarama.ByteEncoder(key),
		Value:     sarama.ByteEncoder(data),
		Partition: int32(partition),
	}

	return nil
}

type SyncKafkaProducer struct {
	Producer   sarama.SyncProducer
	Topic      string
	Partitions int32
	client     sarama.ConsumerGroup
}

func (k *SyncKafkaProducer) Close() error {
	if err := k.Producer.Close(); err != nil {
		return fmt.Errorf("failed to close Kafka producer: %v", err)
	}

	return nil
}

func (k *SyncKafkaProducer) GetClient() sarama.ConsumerGroup {
	return k.client
}

func (k *SyncKafkaProducer) Send(key []byte, data []byte) error {
	partition := binary.LittleEndian.Uint32(key) % uint32(k.Partitions)
	_, _, err := k.Producer.SendMessage(&sarama.ProducerMessage{
		Topic:     k.Topic,
		Key:       sarama.ByteEncoder(key),
		Value:     sarama.ByteEncoder(data),
		Partition: int32(partition),
	})

	return err
}

func ConnectToKafka(kafkaURL *url.URL) (sarama.ClusterAdmin, KafkaProducerI, error) {
	brokersUrl := strings.Split(kafkaURL.Host, ",")

	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0

	clusterAdmin, err := createTopic(kafkaURL, config)
	if err != nil {
		return nil, nil, err
	}

	flushBytes := 16 * 1024
	fb, err := strconv.Atoi(kafkaURL.Query().Get("flush"))
	if err == nil {
		flushBytes = fb
	}

	// DRY it up
	partitions := 1
	partitionsStr := kafkaURL.Query().Get("partitions")
	if partitionsStr != "" {
		partitions, err = strconv.Atoi(partitionsStr)
		if err != nil {
			return nil, nil, fmt.Errorf("error while parsing partitions: %v", err)
		}
	}

	producer, err := ConnectProducer(brokersUrl, kafkaURL.Path[1:], int32(partitions), flushBytes)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to connect to kafka: %v", err)
	}

	return clusterAdmin, producer, nil
}

func createTopic(kafkaURL *url.URL, config *sarama.Config) (sarama.ClusterAdmin, error) {
	clusterAdmin, err := sarama.NewClusterAdmin(strings.Split(kafkaURL.Host, ","), config)
	if err != nil {
		return nil, fmt.Errorf("error while creating cluster admin: %v", err)
	}

	partitions := 1
	partitionsStr := kafkaURL.Query().Get("partitions")
	if partitionsStr != "" {
		partitions, err = strconv.Atoi(partitionsStr)
		if err != nil {
			return nil, fmt.Errorf("error while parsing partitions: %v", err)
		}
	}

	replicationFactor := 1
	replicationFactorStr := kafkaURL.Query().Get("replication")
	if replicationFactorStr != "" {
		replicationFactor, err = strconv.Atoi(replicationFactorStr)
		if err != nil {
			return nil, fmt.Errorf("error while parsing replication: %v", err)
		}
	}

	topic := kafkaURL.Path[1:]
	_ = clusterAdmin.CreateTopic(topic, &sarama.TopicDetail{
		NumPartitions:     int32(partitions),
		ReplicationFactor: int16(replicationFactor),
	}, false)

	return clusterAdmin, nil
}

func ConnectProducer(brokersUrl []string, topic string, partitions int32, flushBytes ...int) (KafkaProducerI, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Partitioner = sarama.NewManualPartitioner

	flush := 16 * 1024
	if len(flushBytes) > 0 {
		flush = flushBytes[0]
	}
	config.Producer.Flush.Bytes = flush

	// NewSyncProducer creates a new SyncProducer using the given broker addresses and configuration.
	conn, err := sarama.NewSyncProducer(brokersUrl, config)
	if err != nil {
		return nil, err
	}

	return &SyncKafkaProducer{
		Producer:   conn,
		Partitions: partitions,
		Topic:      topic,
	}, nil

	//conn, err := sarama.NewAsyncProducer(brokersUrl, config)
	//if err != nil {
	//	return nil, err
	//}

	//return &AsyncKafkaProducer{
	//	Producer:   conn,
	//	Partitions: partitions,
	//	Topic:      topic,
	//}, nil
}

func StartKafkaListener(ctx context.Context, logger ulogger.Logger, kafkaBrokersURL *url.URL, workers int,
	service string, groupID string, workerFn func(ctx context.Context, key []byte, data []byte) error) {

	// create the workers to process all messages
	n := atomic.Uint64{}
	workerCh := make(chan KafkaMessage)
	for i := 0; i < workers; i++ {
		go func() {
			var err error
			for {
				select {
				case <-ctx.Done():
					logger.Infof("[%s] Stopping Kafka worker", service)
					return
				case msg := <-workerCh:
					if err = workerFn(ctx, msg.Message.Key, msg.Message.Value); err != nil {
						// TODO do we need to retry locally?
						logger.Errorf("[%s] Failed to add tx to block assembly: %s", service, err)
					} else {
						// mark the message after no error
						msg.Session.MarkMessage(msg.Message, "")
						// msg.Session.Commit()
						n.Add(1)
					}
				}
			}
		}()
	}

	go func() {
		clusterAdmin, _, err := ConnectToKafka(kafkaBrokersURL)
		if err != nil {
			logger.Fatalf("[%s] unable to connect to kafka: %s", service, err)
		}
		defer func() { _ = clusterAdmin.Close() }()

		topic := kafkaBrokersURL.Path[1:]
		var partitions int
		if partitions, err = strconv.Atoi(kafkaBrokersURL.Query().Get("partitions")); err != nil {
			logger.Fatalf("[%s] unable to parse Kafka partitions: %s", service, err)
		}

		var replicationFactor int
		if replicationFactor, err = strconv.Atoi(kafkaBrokersURL.Query().Get("replication")); err != nil {
			logger.Fatalf("[%s] unable to parse Kafka replication factor: %s", service, err)
		}

		_ = clusterAdmin.CreateTopic(topic, &sarama.TopicDetail{
			NumPartitions:     int32(partitions),
			ReplicationFactor: int16(replicationFactor),
		}, false)

		err = StartKafkaGroupListener(ctx, logger, kafkaBrokersURL, groupID, workerCh, 1)
		if err != nil {
			logger.Errorf("[%s] Kafka listener failed to start: %s", service, err)
		}
	}()
}

func StartKafkaGroupListener(ctx context.Context, logger ulogger.Logger, kafkaURL *url.URL, groupID string, workerCh chan KafkaMessage, consumerCount int,
	consumerClosure ...func(KafkaMessage)) error {

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	// config.Consumer.Offsets.Initial = sarama.OffsetOldest

	ctx, cancel := context.WithCancel(ctx)
	// ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signals
		cancel()
	}()

	var consumerClosureFunc func(KafkaMessage)
	if len(consumerClosure) > 0 {
		consumerClosureFunc = consumerClosure[0]
	} else {
		consumerClosureFunc = nil
	}

	brokersUrl := strings.Split(kafkaURL.Host, ",")
	client, err := sarama.NewConsumerGroup(brokersUrl, groupID, config)
	if err != nil {
		cancel()
		return fmt.Errorf("error creating consumer group client: %v", err)
	}

	topics := []string{kafkaURL.Path[1:]}

	for i := 0; i < consumerCount; i++ {
		go func(consumerIndex int) {
			// defer consumer.Close() // Ensure cleanup, if necessary
			logger.Infof("[kafka] Starting consumer [%d] for group %s on topic %s", consumerIndex, groupID, topics[0])

			for {
				select {
				case <-ctx.Done():
					// Context cancelled, exit goroutine
					return
				default:
					if err := client.Consume(ctx, topics, NewKafkaConsumer(workerCh, consumerClosureFunc)); err != nil {
						logger.Errorf("Error from consumer [%d]: %v", consumerIndex, err)
						// Consider delay before retry or exit based on error type
					}
				}
			}
		}(i)
	}

	// Wait for signal to cancel context
	<-signals
	cancel()
	logger.Infof("[kafka] Shutting down consumers for group %s", groupID)

	<-ctx.Done()
	logger.Infof("[kafka] shutting down consumer for %s", groupID)

	if err = client.Close(); err != nil {
		logger.Errorf("[Kafka] %s: error closing client: %v", groupID, err)
	}

	return nil
}

func StartAsyncProducer(logger ulogger.Logger, kafkaURL *url.URL, ch chan []byte) error {
	logger.Debugf("Starting async producer")
	topic := kafkaURL.Path[1:]
	brokersUrl := strings.Split(kafkaURL.Host, ",")
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	flushBytes, _ := gocore.Config().GetInt("blockassembly_kafka_flushBytes", 1048576)
	flushMessages, _ := gocore.Config().GetInt("blockassembly_kafka_flushMessages", 50000)
	flushFrequency, _ := gocore.Config().GetInt("blockassembly_kafka_flushFrequencyMs", 1000)

	config.Producer.Flush.Bytes = flushBytes
	config.Producer.Flush.Messages = flushMessages
	config.Producer.Flush.Frequency = time.Duration(flushFrequency) * time.Millisecond

	// try turning off acks
	// config.Producer.RequiredAcks = sarama.NoResponse // Equivalent to 'acks=0'
	// config.Producer.Return.Successes = false

	clusterAdmin, err := createTopic(kafkaURL, config)
	if err != nil {
		return err
	}

	defer clusterAdmin.Close()

	producer, err := sarama.NewAsyncProducer(brokersUrl, config)
	if err != nil {
		logger.Fatalf("Failed to start Sarama producer: %v", err)
	}
	defer producer.AsyncClose()

	// Start a goroutine to handle successful message deliveries
	go func() {
		for range producer.Successes() {
			// Handle successful deliveries here, e.g., log them
		}
	}()

	// Start a goroutine to handle errors
	go func() {
		for err := range producer.Errors() {
			logger.Debugf("Failed to deliver message: %v", err)
		}
	}()

	// Sending a batch of 50 messages asynchronously
	go func() {
		for msgBytes := range ch {
			message := &sarama.ProducerMessage{
				Topic: topic,
				Value: sarama.StringEncoder(msgBytes),
			}
			producer.Input() <- message
		}
	}()

	// Wait for a signal to exit
	<-signals
	logger.Debugf("Shutting down producer...")
	return nil
}

// kafka consumer
