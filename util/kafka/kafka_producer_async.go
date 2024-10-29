package kafka

import (
	"context"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
)

type KafkaAsyncProducerI interface {
	Start(ctx context.Context)
	BrokersURL() []string
	Publish(msg *Message)
}

type KafkaProducerConfig struct {
	Logger                ulogger.Logger
	URL                   *url.URL
	BrokersURL            []string
	Topic                 string
	Partitions            int32
	ReplicationFactor     int16
	RetentionPeriodMillis string
	SegmentBytes          string
	FlushBytes            int
	FlushMessages         int
	FlushFrequency        time.Duration
}

type MessageStatus struct {
	Success bool
	Error   error
	Time    time.Time
}

type Message struct {
	Key   []byte
	Value []byte
}

type KafkaAsyncProducer struct {
	Config            KafkaProducerConfig
	Producer          sarama.AsyncProducer
	LastMessageStatus MessageStatus
	mu                sync.Mutex
	PublishChannel    chan *Message
}

func NewKafkaAsyncProducerFromURL(logger ulogger.Logger, kafkaURL *url.URL, ch chan *Message) (*KafkaAsyncProducer, error) {
	producerConfig := KafkaProducerConfig{
		Logger:                logger,
		URL:                   kafkaURL,
		BrokersURL:            strings.Split(kafkaURL.Host, ","),
		Topic:                 kafkaURL.Path[1:],
		Partitions:            int32(util.GetQueryParamInt(kafkaURL, "partitions", 1)),     //nolint:gosec
		ReplicationFactor:     int16(util.GetQueryParamInt(kafkaURL, "replication", 1)),    //nolint:gosec
		RetentionPeriodMillis: util.GetQueryParam(kafkaURL, "retention", "600000"),         // 10 minutes
		SegmentBytes:          util.GetQueryParam(kafkaURL, "segment_bytes", "1073741824"), // 1GB default
		FlushBytes:            util.GetQueryParamInt(kafkaURL, "flush_bytes", 1024*1024),
		FlushMessages:         util.GetQueryParamInt(kafkaURL, "flush_messages", 50_000),
		FlushFrequency:        util.GetQueryParamDuration(kafkaURL, "flush_frequency", 10*time.Second),
	}

	return NewKafkaAsyncProducer(logger, producerConfig, ch)
}

func NewKafkaAsyncProducer(logger ulogger.Logger, cfg KafkaProducerConfig, ch chan *Message) (*KafkaAsyncProducer, error) {
	logger.Debugf("Starting async kafka producer for %v", cfg.URL)

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Flush.Bytes = cfg.FlushBytes
	config.Producer.Flush.Messages = cfg.FlushMessages
	config.Producer.Flush.Frequency = cfg.FlushFrequency

	// try turning off acks
	// config.Producer.RequiredAcks = sarama.NoResponse // Equivalent to 'acks=0'
	// config.Producer.Return.Successes = false

	clusterAdmin, err := sarama.NewClusterAdmin(cfg.BrokersURL, config)
	if err != nil {
		return nil, errors.NewConfigurationError("error while creating cluster admin", err)
	}
	defer func(clusterAdmin sarama.ClusterAdmin) {
		_ = clusterAdmin.Close()
	}(clusterAdmin)

	if err := clusterAdmin.CreateTopic(cfg.Topic, &sarama.TopicDetail{
		NumPartitions:     cfg.Partitions,
		ReplicationFactor: cfg.ReplicationFactor,
		ConfigEntries: map[string]*string{
			"retention.ms":        &cfg.RetentionPeriodMillis,
			"delete.retention.ms": &cfg.RetentionPeriodMillis,
			"segment.ms":          &cfg.RetentionPeriodMillis,
			"segment.bytes":       &cfg.SegmentBytes,
		},
	}, false); err != nil {
		if !errors.Is(err, sarama.ErrTopicAlreadyExists) {
			return nil, errors.NewProcessingError("unable to create topic", err)
		}
	}

	producer, err := sarama.NewAsyncProducer(cfg.BrokersURL, config)
	if err != nil {
		return nil, errors.NewServiceError("Failed to create Kafka async producer for %s: %v", cfg.Topic, err)
	}

	client := &KafkaAsyncProducer{
		Producer: producer,
		Config:   cfg,
		LastMessageStatus: MessageStatus{
			Success: true,
			Time:    time.Now(),
			Error:   nil,
		},
		PublishChannel: ch,
	}

	return client, nil
}

func (c *KafkaAsyncProducer) Start(ctx context.Context) {
	if c == nil {
		return
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	topic := c.Config.URL.Path[1:]

	// Start a goroutine to handle successful message deliveries
	go func() {
		for range c.Producer.Successes() {
			c.mu.Lock()
			c.LastMessageStatus = MessageStatus{
				Success: true,
				Error:   nil,
				Time:    time.Now(),
			}
			c.mu.Unlock()
		}
	}()

	// Start a goroutine to handle errors
	go func() {
		for err := range c.Producer.Errors() {
			c.Config.Logger.Errorf("Failed to deliver message: %v", err)
			c.mu.Lock()
			c.LastMessageStatus = MessageStatus{
				Success: false,
				Error:   err,
				Time:    time.Now(),
			}
			c.mu.Unlock()
		}
	}()
	// Sending a batch of 50 messages asynchronously
	go func() {
		for msgBytes := range c.PublishChannel {
			var key sarama.ByteEncoder
			if msgBytes.Key != nil {
				key = sarama.ByteEncoder(msgBytes.Key)
			}

			message := &sarama.ProducerMessage{
				Topic: topic,
				Key:   key,
				Value: sarama.ByteEncoder(msgBytes.Value),
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

func (c *KafkaAsyncProducer) BrokersURL() []string {
	return c.Config.BrokersURL
}

func (c *KafkaAsyncProducer) Publish(msg *Message) {
	c.PublishChannel <- msg
}
