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

type MessageStatus struct {
	Success bool
	Error   error
	Time    time.Time
}

type KafkaAsyncProducer struct {
	Config            KafkaListenerConfig
	Producer          sarama.AsyncProducer
	LastMessageStatus MessageStatus
	mu                sync.Mutex
	PublishChannel    chan []byte
}

func NewKafkaAsyncProducer(logger ulogger.Logger, kafkaURL *url.URL, ch chan []byte) (*KafkaAsyncProducer, error) {
	logger.Debugf("Starting async kafka producer for %v", kafkaURL)
	topic := kafkaURL.Path[1:]
	brokersURL := strings.Split(kafkaURL.Host, ",")

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	config.Producer.Flush.Bytes = util.GetQueryParamInt(kafkaURL, "flush_bytes", 1024*1024)
	config.Producer.Flush.Messages = util.GetQueryParamInt(kafkaURL, "flush_messages", 50_000)
	config.Producer.Flush.Frequency = util.GetQueryParamDuration(kafkaURL, "flush_frequency", 10*time.Second)

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

	partitions := util.GetQueryParamInt(kafkaURL, "partitions", 1)
	replicationFactor := util.GetQueryParamInt(kafkaURL, "replication", 1)
	retentionPeriod := util.GetQueryParam(kafkaURL, "retention", "600000")      // 10 minutes
	segmentBytes := util.GetQueryParam(kafkaURL, "segment_bytes", "1073741824") // 1GB default

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

	client := &KafkaAsyncProducer{
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
