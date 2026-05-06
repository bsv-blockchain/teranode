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

	// Transfer rate monitoring
	SlowTransfer SlowTransferConfig // Thresholds for slow-send detection (zero value uses defaults)
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
	shuttingDown   atomic.Bool         // Flag indicating shutdown has started (reject new publishes)
	closed         atomic.Bool         // Flag indicating if producer is closed
	adaptiveSlow   atomic.Bool         // Flag enabling adaptive batching during constrained bandwidth
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

// defaultBatchMaxBytes is the default max batch size for franz-go, matching the
// Kafka broker default for max.message.bytes (1 MiB). This must not be derived
// from flush_bytes, which was a Sarama flush-trigger threshold, not a size limit.
const defaultBatchMaxBytes int32 = 1_048_576

// clampBatchMaxBytes returns a safe ProducerBatchMaxBytes value. The flush_bytes
// config parameter controlled flush timing in Sarama, not max batch size. In
// franz-go, ProducerBatchMaxBytes is a hard limit — setting it too low causes
// Redpanda/Kafka to reject messages with MESSAGE_TOO_LARGE.
//
// When flush_bytes <= defaultBatchMaxBytes (which includes all legacy configs like
// flush_bytes=64 or flush_bytes=1024), we use the 1 MiB default. Only when
// flush_bytes explicitly exceeds 1 MiB do we respect it as a batch size override.
func clampBatchMaxBytes(flushBytes int) int32 {
	if flushBytes <= int(defaultBatchMaxBytes) {
		return defaultBatchMaxBytes
	}
	if flushBytes > math.MaxInt32 {
		return math.MaxInt32
	}
	return int32(flushBytes) //nolint:gosec // bounds checked above
}

// NewKafkaAsyncProducer creates a new async producer with the given configuration using franz-go.
func NewKafkaAsyncProducer(logger ulogger.Logger, cfg KafkaProducerConfig) (*KafkaAsyncProducer, error) {
	logger.Debugf("Starting async kafka producer for %v", cfg.URL)

	producer := &KafkaAsyncProducer{
		Config: cfg,
	}

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

	// Wire transfer-rate monitoring hooks
	slowCfg := cfg.SlowTransfer
	if slowCfg.ThresholdBps == 0 {
		slowCfg = DefaultSlowTransferConfig()
	}
	hook := newProducerMetricsHook(logger, cfg.Topic, slowCfg)
	hook.setSlowStateHandler(func(slow bool, rateBps float64) {
		producer.adaptiveSlow.Store(slow)
		if slow {
			logger.Infof("[kafka] enabling adaptive batching on topic %s (observed %.1f KB/s)", cfg.Topic, rateBps/1024)
			return
		}
		logger.Infof("[kafka] restoring normal batching on topic %s", cfg.Topic)
	})
	opts = append(opts, kgo.WithHooks(hook))

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

	producer.client = client

	return producer, nil
}

func (c *KafkaAsyncProducer) currentBatchLinger() time.Duration {
	if c.adaptiveSlow.Load() {
		linger := c.Config.FlushFrequency * 4
		if linger < 200*time.Millisecond {
			linger = 200 * time.Millisecond
		}
		if linger > 5*time.Second {
			linger = 5 * time.Second
		}
		return linger
	}
	if c.Config.FlushFrequency <= 0 {
		return 10 * time.Second
	}
	return c.Config.FlushFrequency
}

func (c *KafkaAsyncProducer) currentBatchSize() int {
	base := c.Config.FlushMessages
	if base <= 0 {
		base = 1000
	}
	if c.adaptiveSlow.Load() {
		sz := base * 4
		if sz < 100 {
			sz = 100
		}
		if sz > 20000 {
			sz = 20000
		}
		return sz
	}
	if base < 1 {
		return 1
	}
	return base
}

func (c *KafkaAsyncProducer) currentBackpressureThreshold() int {
	threshold := c.currentBatchSize() * 2
	if threshold < 200 {
		return 200
	}
	return threshold
}

func (c *KafkaAsyncProducer) flushBuffered(internalCtx context.Context, buffered []*Message) {
	for _, msgBytes := range buffered {
		if c.closed.Load() || c.shuttingDown.Load() {
			return
		}
		record := &kgo.Record{
			Topic: c.Config.Topic,
			Key:   msgBytes.Key,
			Value: msgBytes.Value,
		}
		c.client.Produce(internalCtx, record, func(r *kgo.Record, err error) {
			if err != nil {
				c.Config.Logger.Errorf("Failed to deliver message to topic %s: %v, Key: %x", r.Topic, err, r.Key)
			} else {
				c.Config.Logger.Debugf("Successfully sent message to topic %s, partition: %d, offset: %d", r.Topic, r.Partition, r.Offset)
			}
		})
	}
}

// Start begins the async producer operation.
func (c *KafkaAsyncProducer) Start(ctx context.Context, ch chan *Message) {
	if c == nil {
		return
	}
	c.shuttingDown.Store(false)
	c.closed.Store(false)

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

			buffered := make([]*Message, 0, 256)
			backpressureLogged := false
			bufferedGauge := prometheusBufferedMessages.WithLabelValues(c.Config.Topic)
			backpressureCounter := prometheusBackpressureSignals.WithLabelValues(c.Config.Topic)
			bufferedGauge.Set(0)

			slowMode := c.adaptiveSlow.Load()
			linger := c.currentBatchLinger()
			maxBatch := c.currentBatchSize()
			backpressureThreshold := c.currentBackpressureThreshold()

			const metricsUpdateInterval = 64
			metricTick := 0

			var lingerTimer *time.Timer
			var lingerCh <-chan time.Time
			defer func() {
				if lingerTimer == nil {
					return
				}
				if !lingerTimer.Stop() {
					select {
					case <-lingerTimer.C:
					default:
					}
				}
			}()

			resetLingerTimer := func(d time.Duration) {
				if lingerTimer == nil {
					lingerTimer = time.NewTimer(d)
				} else {
					if !lingerTimer.Stop() {
						select {
						case <-lingerTimer.C:
						default:
						}
					}
					lingerTimer.Reset(d)
				}
				lingerCh = lingerTimer.C
			}

			flushBufferedFinal := func() {
				if len(buffered) == 0 {
					return
				}
				// Use a fresh context so final drain still runs after parent cancellation.
				flushCtx, flushCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer flushCancel()
				c.flushBuffered(flushCtx, buffered)
				buffered = buffered[:0]
				bufferedGauge.Set(0)
			}

			for {
				if c.closed.Load() || c.shuttingDown.Load() {
					break
				}

				newSlowMode := c.adaptiveSlow.Load()
				if newSlowMode != slowMode {
					slowMode = newSlowMode
					linger = c.currentBatchLinger()
					maxBatch = c.currentBatchSize()
					backpressureThreshold = c.currentBackpressureThreshold()
				}

				metricTick++
				if metricTick >= metricsUpdateInterval {
					bufferedGauge.Set(float64(len(buffered)))
					metricTick = 0
				}

				if len(buffered) > backpressureThreshold {
					if !backpressureLogged {
						backpressureLogged = true
						backpressureCounter.Inc()
						c.Config.Logger.Warnf("[kafka] producer backpressure on topic %s: buffered=%d threshold=%d",
							c.Config.Topic, len(buffered), backpressureThreshold)
					}
				} else {
					backpressureLogged = false
				}

				if len(buffered) == 0 {
					msgBytes, ok := <-ch
					if !ok {
						break
					}
					if msgBytes != nil {
						buffered = append(buffered, msgBytes)
					}
					continue
				}

				if len(buffered) >= maxBatch {
					c.flushBuffered(internalCtx, buffered)
					buffered = buffered[:0]
					bufferedGauge.Set(0)
					continue
				}

				resetLingerTimer(linger)

				select {
				case msgBytes, ok := <-ch:
					if !ok {
						flushBufferedFinal()
						return
					}
					if msgBytes != nil {
						buffered = append(buffered, msgBytes)
					}
				case <-lingerCh:
					lingerCh = nil
					c.flushBuffered(internalCtx, buffered)
					buffered = buffered[:0]
					bufferedGauge.Set(0)
				case <-internalCtx.Done():
					flushBufferedFinal()
					return
				}
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

	if c.shuttingDown.Load() {
		return nil
	}

	c.shuttingDown.Store(true)

	if c.closed.Load() {
		return nil
	}

	c.channelMu.Lock()
	ch := c.publishChannel
	if ch != nil {
		c.publishChannel = nil
		close(ch)
	}
	c.channelMu.Unlock()

	c.publishWg.Wait()
	c.closed.Store(true)

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
// Returns nil for in-memory producers since there are no real brokers.
func (c *KafkaAsyncProducer) BrokersURL() []string {
	if c == nil {
		return nil
	}

	if c.inMemoryProducer != nil {
		return nil
	}

	return c.Config.BrokersURL
}

// Publish sends a message to the producer's publish channel.
func (c *KafkaAsyncProducer) Publish(msg *Message) {
	c.channelMu.RLock()
	defer c.channelMu.RUnlock()

	if c.shuttingDown.Load() || c.closed.Load() || c.publishChannel == nil {
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
