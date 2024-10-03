package util

import (
	"context"
	"encoding/binary"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/bitcoin-sv/ubsv/errors"
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
		return errors.NewServiceError("failed to close Kafka producer", err)
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
		return errors.NewServiceError("failed to close Kafka producer", err)
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

	clusterAdmin, err := sarama.NewClusterAdmin(brokersUrl, config)
	if err != nil {
		return nil, nil, errors.NewServiceError("error while creating cluster admin", err)
	}

	partitions := GetQueryParamInt(kafkaURL, "partitions", 1)
	replicationFactor := GetQueryParamInt(kafkaURL, "replication", 1)
	retentionPeriod := GetQueryParam(kafkaURL, "retention", "600000")      // 10 minutes
	segmentBytes := GetQueryParam(kafkaURL, "segment_bytes", "1073741824") // 1GB default

	topic := kafkaURL.Path[1:]

	if err := clusterAdmin.CreateTopic(topic, &sarama.TopicDetail{
		NumPartitions:     int32(partitions),
		ReplicationFactor: int16(replicationFactor),
		ConfigEntries: map[string]*string{
			"retention.ms":        &retentionPeriod, // Set the retention period
			"delete.retention.ms": &retentionPeriod,
			"segment.ms":          &retentionPeriod,
			"segment.bytes":       &segmentBytes,
		},
	}, false); err != nil {
		if !errors.Is(err, sarama.ErrTopicAlreadyExists) {
			return nil, nil, err
		}
	}

	flushBytes := GetQueryParamInt(kafkaURL, "flush_bytes", 1024)

	producer, err := ConnectProducer(brokersUrl, topic, int32(partitions), flushBytes)
	if err != nil {
		return nil, nil, errors.NewServiceError("unable to connect to kafka", err)
	}

	return clusterAdmin, producer, nil
}

func ConnectProducer(brokersURL []string, topic string, partitions int32, flushBytes ...int) (KafkaProducerI, error) {
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
	conn, err := sarama.NewSyncProducer(brokersURL, config)
	if err != nil {
		return nil, err
	}

	return &SyncKafkaProducer{
		Producer:   conn,
		Partitions: partitions,
		Topic:      topic,
	}, nil
}

type MessageStatus struct {
	Success bool
	Error   error
	Time    time.Time
}

type KafkaListenerConfig struct {
	Logger            ulogger.Logger
	URL               *url.URL
	GroupID           string
	ConsumerCount     int
	AutoCommitEnabled bool
	ConsumerFn        func(message KafkaMessage) error
}

type KafkaConsumerClient struct {
	Config            KafkaListenerConfig
	ConsumerGroup     sarama.ConsumerGroup
	LastMessageStatus MessageStatus
}

// txMetaCache : autocommit should be enabled - true, we CAN miss.
// subtree validation : autocommit should be disabled - false.
// block persister : autocommit should be disabled - false.
// block assembly: no kafka setup

// StartKafkaGroupListener Autocommit is enabled/disabled according to the parameter fed in the function.
// We DO NOT read autocommit parameter from the URL.
func NewKafkaGroupListener(ctx context.Context, cfg KafkaListenerConfig) (*KafkaConsumerClient, error) {

	if cfg.URL == nil {
		return nil, errors.NewConfigurationError("kafka URL is not set", nil)
	}

	if cfg.ConsumerCount <= 0 {
		return nil, errors.NewConfigurationError("consumer count must be greater than 0", nil)
	}

	if cfg.ConsumerFn == nil {
		return nil, errors.NewConfigurationError("consumer function is not set", nil)
	}

	if cfg.Logger == nil {
		return nil, errors.NewConfigurationError("logger is not set", nil)
	}

	if cfg.GroupID == "" {
		return nil, errors.NewConfigurationError("group ID is not set", nil)
	}

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// https://github.com/IBM/sarama/issues/1689
	// https://github.com/IBM/sarama/pull/1699
	// Default value for config.Consumer.Offsets.AutoCommit.Enable is true.
	if !cfg.AutoCommitEnabled {
		config.Consumer.Offsets.AutoCommit.Enable = false
	}

	replay := GetQueryParamInt(cfg.URL, "replay", 0)
	if replay == 1 {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	brokersURL := strings.Split(cfg.URL.Host, ",")

	clusterAdmin, err := sarama.NewClusterAdmin(brokersURL, config)
	if err != nil {
		return nil, errors.NewConfigurationError("error while creating cluster admin", err)
	}
	defer func(clusterAdmin sarama.ClusterAdmin) {
		_ = clusterAdmin.Close()
	}(clusterAdmin)

	consumerGroup, err := sarama.NewConsumerGroup(brokersURL, cfg.GroupID, config)
	if err != nil {
		return nil, errors.NewServiceError("error creating consumer group client", err)
	}

	client := &KafkaConsumerClient{
		Config:        cfg,
		ConsumerGroup: consumerGroup,
		LastMessageStatus: MessageStatus{
			Success: true,
			Time:    time.Now(),
			Error:   nil,
		},
	}

	return client, nil
}

func (k *KafkaConsumerClient) Start(ctx context.Context) {
	if k == nil {
		return
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		for err := range k.ConsumerGroup.Errors() {
			k.Config.Logger.Errorf("Kafka consumer error: %v", err)
			k.LastMessageStatus = MessageStatus{
				Success: false,
				Error:   err,
				Time:    time.Now(),
			}
		}
	}()

	consumerFunc := func(message KafkaMessage) error {
		k.LastMessageStatus = MessageStatus{
			Success: true,
			Error:   nil,
			Time:    time.Now(),
		}

		return k.Config.ConsumerFn(message)
	}

	topics := []string{k.Config.URL.Path[1:]}

	for i := 0; i < k.Config.ConsumerCount; i++ {
		go func(consumerIndex int) {
			// defer consumer.Close() // Ensure cleanup, if necessary
			k.Config.Logger.Infof("[kafka] Starting consumer [%d] for group %s on topic %s \n", consumerIndex, k.Config.GroupID, topics[0])

			for {
				select {
				case <-ctx.Done():
					// Context cancelled, exit goroutine
					return
				default:
					if err := k.ConsumerGroup.Consume(ctx, topics, NewKafkaConsumer(k.Config.AutoCommitEnabled, consumerFunc)); err != nil {
						if errors.Is(err, context.Canceled) {
							k.Config.Logger.Infof("[kafka] Consumer [%d] for group %s cancelled", consumerIndex, k.Config.GroupID)
						} else {
							k.Config.Logger.Errorf("Error from consumer [%d]: %v", consumerIndex, err)
							// Consider delay before retry or exit based on error type
						}
					}
				}
			}
		}(i)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-signals
		cancel()
	}()

	select {
	case <-signals:
		k.Config.Logger.Infof("[kafka] Received signal, shutting down consumers for group %s", k.Config.GroupID)
		cancel() // Ensure the context is canceled
	case <-ctx.Done():
		k.Config.Logger.Infof("[kafka] Context done, shutting down consumer for %s", k.Config.GroupID)
	}

	if err := k.ConsumerGroup.Close(); err != nil {
		k.Config.Logger.Errorf("[Kafka] %s: error closing client: %v", k.Config.GroupID, err)
	}
}

type KafkaProducerClient struct {
	Config            KafkaListenerConfig
	Producer          sarama.AsyncProducer
	LastMessageStatus MessageStatus
	PublishChannel    chan []byte
}

func NewAsyncProducer(logger ulogger.Logger, kafkaURL *url.URL, ch chan []byte) (*KafkaProducerClient, error) {
	logger.Debugf("Starting async kafka producer for %v", kafkaURL)
	topic := kafkaURL.Path[1:]
	brokersURL := strings.Split(kafkaURL.Host, ",")

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	config.Producer.Flush.Bytes = GetQueryParamInt(kafkaURL, "flush_bytes", 1024*1024)
	config.Producer.Flush.Messages = GetQueryParamInt(kafkaURL, "flush_messages", 50_000)
	config.Producer.Flush.Frequency = GetQueryParamDuration(kafkaURL, "flush_frequency", 10*time.Second)

	// try turning off acks
	// config.Producer.RequiredAcks = sarama.NoResponse // Equivalent to 'acks=0'
	// config.Producer.Return.Successes = false

	clusterAdmin, err := sarama.NewClusterAdmin(brokersURL, config)
	if err != nil {
		return nil, errors.NewConfigurationError("error while creating cluster admin", err)
	}
	defer func(clusterAdmin sarama.ClusterAdmin) {
		_ = clusterAdmin.Close()
	}(clusterAdmin)

	partitions := GetQueryParamInt(kafkaURL, "partitions", 1)
	replicationFactor := GetQueryParamInt(kafkaURL, "replication", 1)
	retentionPeriod := GetQueryParam(kafkaURL, "retention", "600000")      // 10 minutes
	segmentBytes := GetQueryParam(kafkaURL, "segment_bytes", "1073741824") // 1GB default

	if err := clusterAdmin.CreateTopic(topic, &sarama.TopicDetail{
		NumPartitions:     int32(partitions),
		ReplicationFactor: int16(replicationFactor),
		ConfigEntries: map[string]*string{
			"retention.ms":        &retentionPeriod, // Set the retention period
			"delete.retention.ms": &retentionPeriod,
			"segment.ms":          &retentionPeriod,
			"segment.bytes":       &segmentBytes,
		},
	}, false); err != nil {
		if !errors.Is(err, sarama.ErrTopicAlreadyExists) {
			return nil, errors.NewProcessingError("unable to create topic", err)
		}
	}

	producer, err := sarama.NewAsyncProducer(brokersURL, config)
	if err != nil {
		logger.Fatalf("Failed to start Sarama producer: %v", err)
	}

	client := &KafkaProducerClient{
		Producer: producer,
		Config: KafkaListenerConfig{
			Logger: logger,
			URL:    kafkaURL,
		},
		LastMessageStatus: MessageStatus{
			Success: true,
			Time:    time.Now(),
			Error:   nil,
		},
		PublishChannel: ch,
	}

	return client, nil
}

func (c *KafkaProducerClient) Start(ctx context.Context) {
	if c == nil {
		return
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	topic := c.Config.URL.Path[1:]

	// Start a goroutine to handle successful message deliveries
	go func() {
		for range c.Producer.Successes() {
			c.LastMessageStatus = MessageStatus{
				Success: true,
				Error:   nil,
				Time:    time.Now(),
			}
		}
	}()

	// Start a goroutine to handle errors
	go func() {
		for err := range c.Producer.Errors() {
			c.Config.Logger.Errorf("Failed to deliver message: %v", err)
			c.LastMessageStatus = MessageStatus{
				Success: false,
				Error:   err,
				Time:    time.Now(),
			}
		}
	}()
	// Sending a batch of 50 messages asynchronously
	go func() {
		for msgBytes := range c.PublishChannel {
			message := &sarama.ProducerMessage{
				Topic: topic,
				Value: sarama.StringEncoder(msgBytes),
			}
			c.Producer.Input() <- message
		}
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-signals
		cancel()
	}()

	select {
	case <-signals:
		c.Config.Logger.Infof("[kafka] Received signal, shutting down producer %v ...", c.Config.URL)
		cancel() // Ensure the context is canceled
	case <-ctx.Done():
		c.Config.Logger.Infof("[kafka] Context done, shutting down producer %v ...", c.Config.URL)
	}

	c.Producer.AsyncClose()
}

func (c *KafkaConsumerClient) CheckKafkaHealth(ctx context.Context) (int, string, error) {
	if c == nil {
		return http.StatusOK, "Kafka consumer group not initialized", nil
	}

	if c.LastMessageStatus.Success {
		return http.StatusOK, "OK", nil
	}

	return http.StatusServiceUnavailable, "Last message failed", c.LastMessageStatus.Error
}

func (c *KafkaProducerClient) CheckKafkaHealth(ctx context.Context) (int, string, error) {
	if c == nil {
		return http.StatusOK, "Kafka producer not initialized", nil
	}

	if c.LastMessageStatus.Success {
		return http.StatusOK, "OK", nil
	}

	return http.StatusServiceUnavailable, "Last message failed", c.LastMessageStatus.Error
}
