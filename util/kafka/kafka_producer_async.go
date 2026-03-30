// Package kafka provides Kafka consumer and producer implementations for message handling.
package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"math"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	inmemorykafka "github.com/bsv-blockchain/teranode/util/kafka/in_memory_kafka"
	"github.com/bsv-blockchain/teranode/util/retry"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

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
	EnableDebugLogging bool // Enable verbose debug logging
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

// KafkaAsyncProducer implements asynchronous Kafka producer functionality using franz-go.
type KafkaAsyncProducer struct {
	Config         KafkaProducerConfig // Producer configuration
	client         *kgo.Client         // Underlying franz-go client
	publishChannel chan *Message       // Channel for publishing messages
	closed         atomic.Bool         // Flag indicating if producer is closed
	channelMu      sync.RWMutex        // Mutex to protect publishChannel access
	publishWg      sync.WaitGroup      // WaitGroup to track publish goroutine

	// For in-memory support
	inMemoryProducer *inmemorykafka.InMemoryAsyncProducer
	isInMemory       bool
}

// NewKafkaAsyncProducerFromURL creates a new async producer from a URL configuration.
func NewKafkaAsyncProducerFromURL(ctx context.Context, logger ulogger.Logger, url *url.URL, kafkaSettings *settings.KafkaSettings) (*KafkaAsyncProducer, error) {
	partitionsInt32, err := safeconversion.IntToInt32(util.GetQueryParamInt(url, "partitions", 1))
	if err != nil {
		return nil, err
	}

	replicationFactorInt16, err := safeconversion.IntToInt16(util.GetQueryParamInt(url, "replication", 1))
	if err != nil {
		return nil, err
	}

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
		RetentionPeriodMillis: util.GetQueryParam(url, "retention", "600000"),
		SegmentBytes:          util.GetQueryParam(url, "segment_bytes", "1073741824"),
		FlushBytes:            util.GetQueryParamInt(url, "flush_bytes", 1024*1024),
		FlushMessages:         util.GetQueryParamInt(url, "flush_messages", 50_000),
		FlushFrequency:        util.GetQueryParamDuration(url, "flush_frequency", 10*time.Second),
		EnableTLS:             enableTLS,
		TLSSkipVerify:         tlsSkipVerify,
		TLSCAFile:             tlsCAFile,
		TLSCertFile:           tlsCertFile,
		TLSKeyFile:            tlsKeyFile,
		EnableDebugLogging:    enableDebugLogging,
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

// clampBatchMaxBytes clamps the given flush bytes value to the valid range for
// franz-go's ProducerBatchMaxBytes (int32). The minimum is 512 bytes (Kafka
// protocol minimum for a record batch). Small flush_bytes values (e.g. 64) were
// valid with sarama as a flush threshold but are too small for franz-go.
func clampBatchMaxBytes(flushBytes int) int32 {
	const minBatchMaxBytes = 512
	if flushBytes < minBatchMaxBytes {
		flushBytes = minBatchMaxBytes
	}
	if flushBytes > math.MaxInt32 {
		flushBytes = math.MaxInt32
	}
	return int32(flushBytes) //nolint:gosec // bounds checked above
}

// NewKafkaAsyncProducer creates a new async producer with the given configuration using franz-go.
func NewKafkaAsyncProducer(logger ulogger.Logger, cfg KafkaProducerConfig) (*KafkaAsyncProducer, error) {
	logger.Debugf("Starting async kafka producer for %v", cfg.URL)

	if cfg.URL != nil && cfg.URL.Scheme == memoryScheme {
		broker := inmemorykafka.GetSharedBroker()
		bufferSize := 256
		producer := inmemorykafka.NewInMemoryAsyncProducer(broker, bufferSize)

		cfg.Logger.Infof("Using in-memory Kafka async producer")

		client := &KafkaAsyncProducer{
			Config:           cfg,
			inMemoryProducer: producer,
			isInMemory:       true,
		}

		return client, nil
	}

	batchMaxBytes := clampBatchMaxBytes(cfg.FlushBytes)
	if int(batchMaxBytes) != cfg.FlushBytes {
		logger.Warnf("flush_bytes=%d for topic %s clamped to %d for franz-go compatibility", cfg.FlushBytes, cfg.Topic, batchMaxBytes)
	}

	// Build franz-go client options
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.BrokersURL...),
		kgo.DefaultProduceTopic(cfg.Topic),
		kgo.ProducerBatchMaxBytes(batchMaxBytes),
		kgo.ProducerLinger(cfg.FlushFrequency),
		kgo.MaxBufferedRecords(cfg.FlushMessages),
		kgo.DisableIdempotentWrite(),
	}

	// Configure TLS if enabled
	if cfg.EnableTLS {
		tlsConfig, err := buildFranzTLSConfig(cfg.EnableTLS, cfg.TLSSkipVerify, cfg.TLSCAFile, cfg.TLSCertFile, cfg.TLSKeyFile)
		if err != nil {
			return nil, errors.NewConfigurationError("failed to configure TLS for kafka async producer", err)
		}
		opts = append(opts, kgo.DialTLSConfig(tlsConfig))
	}

	// Enable debug logging if configured
	if cfg.EnableDebugLogging {
		opts = append(opts, kgo.WithLogger(&franzLoggerAdapter{logger: logger}))
	}

	// Create the franz-go client
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, errors.NewServiceError("Failed to create Kafka async producer for %s", cfg.Topic, err)
	}

	// Create topic if it doesn't exist (uses Background; constructor is not cancelable)
	if err := createTopicWithFranz(context.Background(), client, cfg); err != nil {
		client.Close()
		return nil, err
	}

	producer := &KafkaAsyncProducer{
		Config: cfg,
		client: client,
	}

	return producer, nil
}

// Start begins the async producer operation.
func (c *KafkaAsyncProducer) Start(ctx context.Context, ch chan *Message) {
	if c == nil {
		return
	}

	// Handle in-memory case
	if c.isInMemory {
		c.startInMemory(ctx, ch)
		return
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	c.publishWg.Add(1)

	go func() {
		internalCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		c.channelMu.Lock()
		c.publishChannel = ch
		c.channelMu.Unlock()

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

				record := &kgo.Record{
					Topic: c.Config.Topic,
					Key:   msgBytes.Key,
					Value: msgBytes.Value,
				}

				if c.closed.Load() {
					break
				}

				// Produce asynchronously with callback
				c.client.Produce(internalCtx, record, func(r *kgo.Record, err error) {
					if err != nil {
						c.Config.Logger.Errorf("Failed to deliver message to topic %s: %v, Key: %x", r.Topic, err, r.Key)
					} else {
						c.Config.Logger.Debugf("Successfully sent message to topic %s, partition: %d, offset: %d", r.Topic, r.Partition, r.Offset)
					}
				})
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
			cancel()
		case <-internalCtx.Done():
			c.Config.Logger.Infof("[kafka] Context done, shutting down producer %v ...", c.Config.URL)
		}

		_ = c.Stop()
	}()

	wg.Wait()
}

// startInMemory handles the in-memory producer case
func (c *KafkaAsyncProducer) startInMemory(ctx context.Context, ch chan *Message) {
	wg := sync.WaitGroup{}
	wg.Add(1)
	c.publishWg.Add(1)

	go func() {
		internalCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		c.channelMu.Lock()
		c.publishChannel = ch
		c.channelMu.Unlock()

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

				c.inMemoryProducer.Produce(c.Config.Topic, msgBytes.Key, msgBytes.Value)
			}
		}()

		// Handle successes
		go func() {
			for range c.inMemoryProducer.Successes() {
				c.Config.Logger.Debugf("Successfully sent message to topic %s", c.Config.Topic)
			}
		}()

		// Handle errors
		go func() {
			for err := range c.inMemoryProducer.Errors() {
				c.Config.Logger.Errorf("Failed to deliver message: %v", err)
			}
		}()

		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

		select {
		case <-signals:
			c.Config.Logger.Infof("[kafka] Received signal, shutting down producer %v ...", c.Config.URL)
			cancel()
		case <-internalCtx.Done():
			c.Config.Logger.Infof("[kafka] Context done, shutting down producer %v ...", c.Config.URL)
		}

		_ = c.Stop()
	}()

	wg.Wait()
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

	c.channelMu.Lock()
	ch := c.publishChannel
	if ch != nil {
		c.publishChannel = nil
		close(ch)
	}
	c.channelMu.Unlock()

	c.publishWg.Wait()

	if c.isInMemory {
		if c.inMemoryProducer != nil {
			if err := c.inMemoryProducer.Close(); err != nil {
				c.closed.Store(false)
				return err
			}
		}
	} else {
		if c.client != nil {
			if err := c.client.Flush(context.Background()); err != nil {
				c.Config.Logger.Warnf("Error flushing kafka producer: %v", err)
			}
			c.client.Close()
		}
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
	c.channelMu.RLock()
	defer c.channelMu.RUnlock()

	if c.closed.Load() || c.publishChannel == nil {
		return
	}

	if c.publishChannel != nil {
		util.SafeSend(c.publishChannel, msg)
	}
}

// createTopicWithFranz creates a new Kafka topic with the specified configuration.
func createTopicWithFranz(ctx context.Context, client *kgo.Client, cfg KafkaProducerConfig) error {
	admin := kadm.NewClient(client)

	retentionMs, err := strconv.ParseInt(cfg.RetentionPeriodMillis, 10, 64)
	if err != nil {
		retentionMs = 600000
	}

	segmentBytes, err := strconv.ParseInt(cfg.SegmentBytes, 10, 64)
	if err != nil {
		segmentBytes = 1073741824
	}

	configs := map[string]*string{
		"retention.ms":        stringPtr(fmt.Sprintf("%d", retentionMs)),
		"delete.retention.ms": stringPtr(fmt.Sprintf("%d", retentionMs)),
		"segment.ms":          stringPtr(fmt.Sprintf("%d", retentionMs)),
		"segment.bytes":       stringPtr(fmt.Sprintf("%d", segmentBytes)),
	}

	topicAlreadyExists := false

	resp, err := admin.CreateTopic(ctx, cfg.Partitions, cfg.ReplicationFactor, configs, cfg.Topic)
	if err != nil {
		if errors.Is(err, kerr.TopicAlreadyExists) {
			topicAlreadyExists = true
		} else {
			return errors.NewProcessingError("unable to create topic", err)
		}
	} else if resp.Err != nil {
		if errors.Is(resp.Err, kerr.TopicAlreadyExists) {
			topicAlreadyExists = true
		} else {
			return errors.NewProcessingError("unable to create topic", resp.Err)
		}
	}

	// If topic already existed, ensure configs are up to date
	if topicAlreadyExists {
		_, alterErr := admin.AlterTopicConfigs(ctx, []kadm.AlterConfig{
			{Name: "retention.ms", Value: stringPtr(fmt.Sprintf("%d", retentionMs))},
			{Name: "delete.retention.ms", Value: stringPtr(fmt.Sprintf("%d", retentionMs))},
			{Name: "segment.ms", Value: stringPtr(fmt.Sprintf("%d", retentionMs))},
			{Name: "segment.bytes", Value: stringPtr(fmt.Sprintf("%d", segmentBytes))},
		}, cfg.Topic)
		if alterErr != nil {
			return errors.NewProcessingError("unable to alter topic config", alterErr)
		}
	}

	return nil
}

// buildFranzTLSConfig builds a TLS configuration for franz-go
func buildFranzTLSConfig(enableTLS bool, tlsSkipVerify bool, tlsCAFile string, tlsCertFile string, tlsKeyFile string) (*tls.Config, error) {
	if !enableTLS {
		return nil, nil
	}

	// #nosec G402 -- InsecureSkipVerify is configurable and may be needed for testing environments
	tlsConfig := &tls.Config{
		InsecureSkipVerify: tlsSkipVerify,
	}

	if tlsCAFile != "" {
		caCert, err := os.ReadFile(tlsCAFile)
		if err != nil {
			return nil, errors.New(errors.ERR_CONFIGURATION, "failed to read TLS CA file: "+tlsCAFile, err)
		}

		if tlsConfig.RootCAs == nil {
			tlsConfig.RootCAs = loadSystemCertPool()
		}

		if !tlsConfig.RootCAs.AppendCertsFromPEM(caCert) {
			return nil, errors.New(errors.ERR_CONFIGURATION, "failed to append CA certificate to RootCAs from file: "+tlsCAFile)
		}
	}

	if tlsCertFile != "" && tlsKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(tlsCertFile, tlsKeyFile)
		if err != nil {
			return nil, errors.New(errors.ERR_CONFIGURATION, "failed to load TLS certificate/key pair", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}

// loadSystemCertPool loads the system certificate pool
func loadSystemCertPool() *x509.CertPool {
	pool, err := x509.SystemCertPool()
	if err != nil {
		return x509.NewCertPool()
	}
	return pool
}

// franzLoggerAdapter adapts ulogger.Logger to franz-go's logger interface
type franzLoggerAdapter struct {
	logger ulogger.Logger
}

func (f *franzLoggerAdapter) Level() kgo.LogLevel {
	return kgo.LogLevelDebug
}

func (f *franzLoggerAdapter) Log(level kgo.LogLevel, msg string, keyvals ...interface{}) {
	formatted := fmt.Sprintf("[FRANZ-GO] %s %v", msg, keyvals)
	switch level {
	case kgo.LogLevelError:
		f.logger.Errorf("%s", formatted)
	case kgo.LogLevelWarn:
		f.logger.Warnf("%s", formatted)
	case kgo.LogLevelInfo:
		f.logger.Infof("%s", formatted)
	case kgo.LogLevelDebug:
		f.logger.Debugf("%s", formatted)
	default:
		f.logger.Infof("%s", formatted)
	}
}

// stringPtr returns a pointer to the string
func stringPtr(s string) *string {
	return &s
}
