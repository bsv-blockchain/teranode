package util

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/ordishs/go-utils"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/Shopify/sarama"
	"github.com/bitcoin-sv/ubsv/ulogger"
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
	fmt.Printf("Sending message to Kafka: %s => %d\n", utils.ReverseAndHexEncodeSlice(key), len(data))

	partition := binary.LittleEndian.Uint32(key) % uint32(k.Partitions)
	p, o, err := k.Producer.SendMessage(&sarama.ProducerMessage{
		Topic:     k.Topic,
		Key:       sarama.ByteEncoder(key),
		Value:     sarama.ByteEncoder(data),
		Partition: int32(partition),
	})

	fmt.Printf("Sent message to Kafka partition: %d, Offset: %d, Error: %v\n", p, o, err)

	return err
}

func ConnectToKafka(kafkaURL *url.URL) (sarama.ClusterAdmin, KafkaProducerI, error) {
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

	flushBytes := 16 * 1024
	fb, err := strconv.Atoi(kafkaURL.Query().Get("flush"))
	if err == nil {
		flushBytes = fb
	}

	producer, err := ConnectProducer(brokersUrl, kafkaURL.Path[1:], int32(partitions), flushBytes)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to connect to kafka: %v", err)
	}

	return clusterAdmin, producer, nil
}

func ConnectProducer(brokersUrl []string, topic string, partitions int32, flushBytes ...int) (KafkaProducerI, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Partitioner = sarama.NewManualPartitioner

	flush := 16 * 1024
	if flushBytes != nil && len(flushBytes) > 0 {
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
						logger.Errorf("[%s] Failed to add tx to block assembly: %s", service, err)
					} else {
						// mark the message after no error
						msg.Session.MarkMessage(msg.Message, "")
						msg.Session.Commit()
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

		err = StartKafkaGroupListener(ctx, logger, kafkaBrokersURL, groupID, workerCh)
		if err != nil {
			logger.Errorf("[%s] Kafka listener failed to start: %s", service, err)
		}
	}()
}

func StartKafkaGroupListener(ctx context.Context, logger ulogger.Logger, kafkaURL *url.URL, groupID string, workerCh chan KafkaMessage) error {
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

	//<-consumer.ready // Await till the consumer has been set up
	logger.Infof("Kafka consumer up and running for %s on topic %s", groupID, topics[0])

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
