package kafka

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/retry"
)

type KafkaAsyncProducerI interface {
	Start(ctx context.Context, ch chan *Message)
	Stop() error
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
	Config         KafkaProducerConfig
	Producer       sarama.AsyncProducer
	publishChannel chan *Message
	closed         atomic.Bool
}

func NewKafkaAsyncProducerFromURL(ctx context.Context, logger ulogger.Logger, url *url.URL) (*KafkaAsyncProducer, error) {
	producerConfig := KafkaProducerConfig{
		Logger:                logger,
		URL:                   url,
		BrokersURL:            strings.Split(url.Host, ","),
		Topic:                 strings.TrimPrefix(url.Path, "/"),
		Partitions:            int32(util.GetQueryParamInt(url, "partitions", 1)),     // nolint:gosec
		ReplicationFactor:     int16(util.GetQueryParamInt(url, "replication", 1)),    // nolint:gosec
		RetentionPeriodMillis: util.GetQueryParam(url, "retention", "600000"),         // 10 minutes
		SegmentBytes:          util.GetQueryParam(url, "segment_bytes", "1073741824"), // 1GB default
		FlushBytes:            util.GetQueryParamInt(url, "flush_bytes", 1024*1024),
		FlushMessages:         util.GetQueryParamInt(url, "flush_messages", 50_000),
		FlushFrequency:        util.GetQueryParamDuration(url, "flush_frequency", 10*time.Second),
	}

	producer, err := retry.Retry(ctx, logger, func() (*KafkaAsyncProducer, error) {
		return NewKafkaAsyncProducer(logger, producerConfig)
	}, retry.WithMessage(fmt.Sprintf("[P2P] error starting kafka async producer for topic %s", producerConfig.Topic)))
	if err != nil {
		logger.Fatalf("[P2P] failed to start kafka async producer for topic %s: %v", producerConfig.Topic, err)
		return nil, err
	}

	return producer, nil
}

func NewKafkaAsyncProducer(logger ulogger.Logger, cfg KafkaProducerConfig) (*KafkaAsyncProducer, error) {
	logger.Debugf("Starting async kafka producer for %v", cfg.URL)

	config := sarama.NewConfig()
	config.Producer.Flush.Bytes = cfg.FlushBytes
	config.Producer.Flush.Messages = cfg.FlushMessages
	config.Producer.Flush.Frequency = cfg.FlushFrequency

	cfg.Logger.Infof("Starting Kafka async producer for %s topic", cfg.Topic)

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

	if err := createTopic(clusterAdmin, cfg); err != nil {
		return nil, err
	}

	producer, err := sarama.NewAsyncProducer(cfg.BrokersURL, config)
	if err != nil {
		return nil, errors.NewServiceError("Failed to create Kafka async producer for %s", cfg.Topic, err)
	}

	client := &KafkaAsyncProducer{
		Producer: producer,
		Config:   cfg,
	}

	return client, nil
}

func (c *KafkaAsyncProducer) Start(ctx context.Context, ch chan *Message) {
	if c == nil {
		return
	}

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		context, cancel := context.WithCancel(ctx)
		defer cancel()

		c.publishChannel = ch

		go func() {
			for s := range c.Producer.Successes() {
				c.Config.Logger.Infof("Successfully sent message offset: %d", s.Offset)
			}
		}()

		go func() {
			for err := range c.Producer.Errors() {
				c.Config.Logger.Errorf("Failed to deliver message: %v", err)
			}
		}()

		go func() {
			wg.Done()

			for msgBytes := range c.publishChannel {
				var key sarama.ByteEncoder
				if msgBytes.Key != nil {
					key = sarama.ByteEncoder(msgBytes.Key)
				}

				message := &sarama.ProducerMessage{
					Topic: c.Config.Topic,
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
		case <-context.Done():
			c.Config.Logger.Infof("[kafka] Context done, shutting down producer %v ...", c.Config.URL)
		}

		c.Stop() // nolint:errcheck
	}()

	wg.Wait() // don't continue until we know we know the go func has started and is ready to accept messages on the PublishChannel
}

func (c *KafkaAsyncProducer) Stop() error {
	if c == nil {
		return nil
	}

	if c.closed.Load() {
		return nil
	}

	c.closed.Store(true)

	if err := c.Producer.Close(); err != nil {
		c.closed.Store(false)
		return err
	}

	return nil
}

func (c *KafkaAsyncProducer) BrokersURL() []string {
	if c == nil {
		return nil
	}

	return c.Config.BrokersURL
}

func (c *KafkaAsyncProducer) Publish(msg *Message) {
	c.publishChannel <- msg
}

func createTopic(admin sarama.ClusterAdmin, cfg KafkaProducerConfig) error {
	err := admin.CreateTopic(cfg.Topic, &sarama.TopicDetail{
		NumPartitions:     cfg.Partitions,
		ReplicationFactor: cfg.ReplicationFactor,
		ConfigEntries: map[string]*string{
			"retention.ms":        &cfg.RetentionPeriodMillis,
			"delete.retention.ms": &cfg.RetentionPeriodMillis,
			"segment.ms":          &cfg.RetentionPeriodMillis,
			"segment.bytes":       &cfg.SegmentBytes,
		},
	}, false)

	if err != nil {
		if errors.Is(err, sarama.ErrTopicAlreadyExists) {
			return nil
		}

		return errors.NewProcessingError("unable to create topic", err)
	}

	return nil
}
