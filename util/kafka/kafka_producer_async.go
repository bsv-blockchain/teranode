// Package kafka provides Kafka consumer and producer implementations for message handling.
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
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	inmemorykafka "github.com/bsv-blockchain/teranode/util/kafka/in_memory_kafka"
	"github.com/bsv-blockchain/teranode/util/retry"
	"github.com/rcrowley/go-metrics"
)

// init disables go-metrics globally to prevent memory leak from exponential decay sample heap.
// This must be set before any Sarama clients are created.
// See: https://github.com/IBM/sarama/issues/1321
func init() {
	metrics.UseNilMetrics = true
}

// KafkaAsyncProducerI defines the interface for asynchronous Kafka producer operations.
type KafkaAsyncProducerI interface {
	// Start begins the async producer operation with the given message channel
	Start(ctx context.Context, ch chan *Message)

	// Stop gracefully shuts down the async producer
	Stop() error

	// BrokersURL returns the list of Kafka broker URLs
	BrokersURL() []string

	// Publish sends a message to the producer's channel
	Publish(msg *Message)
}

// KafkaProducerConfig holds configuration for the async Kafka producer.
type KafkaProducerConfig struct {
	Logger                ulogger.Logger // Logger instance
	URL                   *url.URL       // Kafka URL
	BrokersURL            []string       // List of broker URLs
	Topic                 string         // Topic to produce to
	Partitions            int32          // Number of partitions
	ReplicationFactor     int16          // Replication factor for topic
	RetentionPeriodMillis string         // Message retention period
	SegmentBytes          string         // Segment size in bytes
	FlushBytes            int            // Flush threshold in bytes
	FlushMessages         int            // Number of messages before flush
	FlushFrequency        time.Duration  // Time between flushes

	// TLS/Authentication configuration
	EnableTLS     bool   // Enable TLS for Kafka connection
	TLSSkipVerify bool   // Skip TLS certificate verification (for testing)
	TLSCAFile     string // Path to CA certificate file
	TLSCertFile   string // Path to client certificate file
	TLSKeyFile    string // Path to client key file

	// Debug logging
	EnableDebugLogging bool // Enable verbose Sarama (Kafka client) debug logging
}

// MessageStatus represents the status of a produced message.
type MessageStatus struct {
	Success bool
	Error   error
	Time    time.Time
}

// Message represents a Kafka message with key and value.
type Message struct {
	Key   []byte
	Value []byte
}

// KafkaAsyncProducer implements asynchronous Kafka producer functionality.
type KafkaAsyncProducer struct {
	Config         KafkaProducerConfig  // Producer configuration
	Producer       sarama.AsyncProducer // Underlying Sarama async producer
	publishChannel chan *Message        // Channel for publishing messages
	closed         atomic.Bool          // Flag indicating if producer is closed
	channelMu      sync.RWMutex         // Mutex to protect publishChannel access
	publishWg      sync.WaitGroup       // WaitGroup to track publish goroutine
}

// NewKafkaAsyncProducerFromURL creates a new async producer from a URL configuration.
// This is a convenience function for production code that extracts settings from kafkaSettings.
// For tests, use NewKafkaAsyncProducer directly with a manually constructed config.
//
// Parameters:
//   - ctx: Context for producer operations
//   - logger: Logger instance
//   - url: URL containing Kafka configuration
//   - kafkaSettings: Kafka settings for TLS and debug logging (can be nil for defaults)
//
// Returns:
//   - *KafkaAsyncProducer: Configured async producer
//   - error: Any error encountered during setup
func NewKafkaAsyncProducerFromURL(ctx context.Context, logger ulogger.Logger, url *url.URL, kafkaSettings *settings.KafkaSettings) (*KafkaAsyncProducer, error) {
	partitionsInt32, err := safeconversion.IntToInt32(util.GetQueryParamInt(url, "partitions", 1))
	if err != nil {
		return nil, err
	}

	replicationFactorInt16, err := safeconversion.IntToInt16(util.GetQueryParamInt(url, "replication", 1))
	if err != nil {
		return nil, err
	}

	// Extract TLS and debug logging settings from kafkaSettings (if provided)
	var enableTLS, tlsSkipVerify, enableDebugLogging bool
	var tlsCAFile, tlsCertFile, tlsKeyFile string
	if kafkaSettings != nil {
		enableTLS = kafkaSettings.EnableTLS
		tlsSkipVerify = kafkaSettings.TLSSkipVerify
		tlsCAFile = kafkaSettings.TLSCAFile
		tlsCertFile = kafkaSettings.TLSCertFile
		tlsKeyFile = kafkaSettings.TLSKeyFile
		enableDebugLogging = kafkaSettings.EnableDebugLogging
	}

	producerConfig := KafkaProducerConfig{
		Logger:                logger,
		URL:                   url,
		BrokersURL:            strings.Split(url.Host, ","),
		Topic:                 strings.TrimPrefix(url.Path, "/"),
		Partitions:            partitionsInt32,
		ReplicationFactor:     replicationFactorInt16,
		RetentionPeriodMillis: util.GetQueryParam(url, "retention", "600000"),         // 10 minutes
		SegmentBytes:          util.GetQueryParam(url, "segment_bytes", "1073741824"), // 1GB default
		FlushBytes:            util.GetQueryParamInt(url, "flush_bytes", 1024*1024),
		FlushMessages:         util.GetQueryParamInt(url, "flush_messages", 50_000),
		FlushFrequency:        util.GetQueryParamDuration(url, "flush_frequency", 10*time.Second),
		// TLS/Auth configuration
		EnableTLS:          enableTLS,
		TLSSkipVerify:      tlsSkipVerify,
		TLSCAFile:          tlsCAFile,
		TLSCertFile:        tlsCertFile,
		TLSKeyFile:         tlsKeyFile,
		EnableDebugLogging: enableDebugLogging,
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

// NewKafkaAsyncProducer creates a new async producer with the given configuration.
//
// Parameters:
//   - logger: Logger instance
//   - cfg: Producer configuration (includes TLS and debug logging settings)
//
// Returns:
//   - *KafkaAsyncProducer: Configured async producer
//   - error: Any error encountered during setup
func NewKafkaAsyncProducer(logger ulogger.Logger, cfg KafkaProducerConfig) (*KafkaAsyncProducer, error) {
	logger.Debugf("Starting async kafka producer for %v", cfg.URL)

	if cfg.URL.Scheme == memoryScheme {
		// --- Use the in-memory implementation ---
		broker := inmemorykafka.GetSharedBroker() // Use alias 'imk'
		// Use a reasonable default buffer size for the mock async producer, or take from config if available
		bufferSize := 256                                                      // Default buffer size for the publish channel
		producer := inmemorykafka.NewInMemoryAsyncProducer(broker, bufferSize) // Use alias 'imk'

		cfg.Logger.Infof("Using in-memory Kafka async producer")
		// No error expected from mock creation

		client := &KafkaAsyncProducer{
			Producer: producer,
			Config:   cfg,
		}

		return client, nil
	}

	// --- Use the real Sarama implementation ---

	config := sarama.NewConfig()
	config.Producer.Flush.Bytes = cfg.FlushBytes
	config.Producer.Flush.Messages = cfg.FlushMessages
	config.Producer.Flush.Frequency = cfg.FlushFrequency
	// config.Producer.Return.Successes = true

	// Enable Sarama debug logging if configured
	if cfg.EnableDebugLogging {
		sarama.Logger = &saramaLoggerAdapter{logger: logger}
		logger.Infof("Kafka debug logging enabled for async producer topic %s", cfg.Topic)
	}

	// Apply authentication settings if TLS is enabled
	if cfg.EnableTLS {
		cfg.Logger.Debugf("Configuring Kafka TLS authentication - EnableTLS: %v, SkipVerify: %v, CA: %s, Cert: %s",
			cfg.EnableTLS, cfg.TLSSkipVerify, cfg.TLSCAFile, cfg.TLSCertFile)

		if err := configureKafkaAuthFromFields(config, cfg.EnableTLS, cfg.TLSSkipVerify, cfg.TLSCAFile, cfg.TLSCertFile, cfg.TLSKeyFile); err != nil {
			return nil, errors.NewConfigurationError("failed to configure Kafka authentication", err)
		}

		cfg.Logger.Debugf("Successfully configured Kafka TLS authentication for async producer topic %s", cfg.Topic)
	}

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

func (c *KafkaAsyncProducer) decodeKeyOrValue(encoder sarama.Encoder) string {
	if encoder == nil {
		return ""
	}

	bytes := encoder.(sarama.ByteEncoder)

	if len(bytes) > 80 {
		return fmt.Sprintf("%x", bytes[:80]) + "... (truncated)"
	}

	return fmt.Sprintf("%x", bytes)
}

// Start begins the async producer operation.
// It sets up message handling and error handling goroutines.
func (c *KafkaAsyncProducer) Start(ctx context.Context, ch chan *Message) {
	if c == nil {
		return
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	c.publishWg.Add(1) // Track the publish goroutine

	go func() {
		context, cancel := context.WithCancel(ctx)

		defer cancel()

		c.channelMu.Lock()
		c.publishChannel = ch
		c.channelMu.Unlock()

		go func() {
			for s := range c.Producer.Successes() {
				key := c.decodeKeyOrValue(s.Key)
				value := c.decodeKeyOrValue(s.Value)

				c.Config.Logger.Debugf("Successfully sent message to topic %s, offset: %d, key: %v, value: %v",
					s.Topic, s.Offset, key, value)
			}
		}()

		go func() {
			for err := range c.Producer.Errors() {
				key := c.decodeKeyOrValue(err.Msg.Key)
				value := c.decodeKeyOrValue(err.Msg.Value)

				c.Config.Logger.Errorf("Failed to deliver message to topic %s: %v, Key: %v, Value: %v",
					err.Msg.Topic, err.Err, key, value)
			}
		}()

		go func() {
			defer c.publishWg.Done()
			wg.Done()

			c.channelMu.RLock()
			ch := c.publishChannel
			c.channelMu.RUnlock()

			for msgBytes := range ch {
				if c.closed.Load() {
					break
				}

				var key sarama.ByteEncoder
				if msgBytes.Key != nil {
					key = sarama.ByteEncoder(msgBytes.Key)
				}

				message := &sarama.ProducerMessage{
					Topic: c.Config.Topic,
					Key:   key,
					Value: sarama.ByteEncoder(msgBytes.Value),
				}

				// Check if closed again right before sending to avoid race condition
				// where Close() is called between the check above and the send below
				if c.closed.Load() {
					break
				}

				// Use a function with recover to safely handle sends to potentially closed channel
				func() {
					defer func() {
						if r := recover(); r != nil {
							// Channel was closed during send, this is expected during shutdown
							c.Config.Logger.Debugf("[kafka] Recovered from send to closed channel during shutdown")
						}
					}()
					c.Producer.Input() <- message
				}()
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

		_ = c.Stop()
	}()

	wg.Wait() // don't continue until we know we know the go func has started and is ready to accept messages on the PublishChannel
}

// Stop gracefully shuts down the async producer.
func (c *KafkaAsyncProducer) Stop() error {
	if c == nil {
		return nil
	}

	if c.closed.Load() {
		return nil
	}

	c.closed.Store(true)

	// Close the publish channel to signal the publish goroutine to exit
	c.channelMu.Lock()
	ch := c.publishChannel
	if ch != nil {
		c.publishChannel = nil // Set to nil BEFORE closing to prevent sends to closed channel
		close(ch)
	}
	c.channelMu.Unlock()

	// Wait for the publish goroutine to finish processing
	c.publishWg.Wait()

	// Now it's safe to close the producer
	if err := c.Producer.Close(); err != nil {
		c.closed.Store(false)
		return err
	}

	return nil
}

// BrokersURL returns the list of configured Kafka broker URLs.
func (c *KafkaAsyncProducer) BrokersURL() []string {
	if c == nil {
		return nil
	}

	return c.Config.BrokersURL
}

// Publish sends a message to the producer's publish channel.
func (c *KafkaAsyncProducer) Publish(msg *Message) {
	if c.closed.Load() {
		return
	}

	c.channelMu.RLock()
	ch := c.publishChannel
	c.channelMu.RUnlock()

	if ch != nil {
		ch <- msg
	}
}

// createTopic creates a new Kafka topic with the specified configuration.
//
// Parameters:
//   - admin: Kafka cluster administrator
//   - cfg: Producer configuration containing topic settings
//
// Returns:
//   - error: Any error encountered during topic creation
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
			err = admin.AlterConfig(sarama.TopicResource, cfg.Topic, map[string]*string{
				"retention.ms":        &cfg.RetentionPeriodMillis,
				"delete.retention.ms": &cfg.RetentionPeriodMillis,
				"segment.ms":          &cfg.RetentionPeriodMillis,
				"segment.bytes":       &cfg.SegmentBytes,
			}, false)

			if err != nil {
				return errors.NewProcessingError("unable to alter topic config", err)
			}

			return nil
		}

		return errors.NewProcessingError("unable to create topic", err)
	}

	return nil
}
