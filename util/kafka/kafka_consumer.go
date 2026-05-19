// Package kafka provides Kafka consumer and producer implementations for message handling.
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

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	inmemorykafka "github.com/bsv-blockchain/teranode/util/kafka/in_memory_kafka"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const memoryScheme = "memory"

// KafkaMessage represents a Kafka message with all necessary fields.
type KafkaMessage struct {
	Key       []byte
	Value     []byte
	Topic     string
	Partition int32
	Offset    int64
	Timestamp time.Time
	// HighWaterMark is the partition's high water mark (the next offset that will be
	// produced) at the time this fetch response was returned. Consumers can compare
	// Offset+1 against HighWaterMark to detect "caught up to the live tail".
	HighWaterMark int64
}

// KafkaConsumerGroupI defines the interface for Kafka consumer group operations.
type KafkaConsumerGroupI interface {
	// Start begins consuming messages using the provided consumer function and options.
	Start(ctx context.Context, consumerFn func(message *KafkaMessage) error, opts ...ConsumerOption)

	// BrokersURL returns the list of Kafka broker URLs.
	BrokersURL() []string

	// Close gracefully shuts down the consumer group.
	Close() error

	// PauseAll suspends fetching from all partitions. Future calls to the broker will not return
	// any records until the partitions have been resumed. This does not trigger a group rebalance.
	PauseAll()

	// ResumeAll resumes all partitions which have been paused. New calls to the broker will return
	// records from these partitions if there are any to be fetched.
	ResumeAll()
}

// KafkaConsumerConfig holds configuration parameters for Kafka consumer.
type KafkaConsumerConfig struct {
	Logger            ulogger.Logger // Logger instance for logging
	URL               *url.URL       // Kafka broker URL
	BrokersURL        []string       // List of Kafka broker URLs
	Topic             string         // Kafka topic to consume from
	Partitions        int            // Number of partitions
	ConsumerGroupID   string         // Consumer group identifier
	AutoCommitEnabled bool           // Whether to auto-commit offsets
	Replay            bool           // Whether to replay messages from the beginning

	// Timeout configuration (query params: maxProcessingTime, sessionTimeout, heartbeatInterval, rebalanceTimeout)
	// Note: MaxProcessingTime configures the Kafka fetch max wait (kgo.FetchMaxWait), i.e., how long the broker
	// may wait before responding to a fetch request when there are no records immediately available.
	MaxProcessingTime time.Duration // Max time broker waits before returning fetch results when no records are available (default: 100ms)
	SessionTimeout    time.Duration // Time broker waits for heartbeat before considering consumer dead (default: 10s)
	HeartbeatInterval time.Duration // Frequency of heartbeats to broker (default: 3s)
	RebalanceTimeout  time.Duration // Max time for all consumers to join rebalance (default: 60s)

	// OffsetReset controls what to do when offset is out of range (query param: offsetReset)
	// Values: "latest" (default, skip to newest), "earliest" (reprocess from oldest), "" (use Replay setting)
	OffsetReset string // Strategy for handling offset out of range errors

	// TLS/Authentication configuration
	EnableTLS     bool   // Enable TLS for Kafka connection
	TLSSkipVerify bool   // Skip TLS certificate verification (for testing)
	TLSCAFile     string // Path to CA certificate file
	TLSCertFile   string // Path to client certificate file
	TLSKeyFile    string // Path to client key file

	// Debug logging
	EnableDebugLogging bool // Enable verbose debug logging
}

// KafkaConsumerGroup implements KafkaConsumerGroupI interface using franz-go.
type KafkaConsumerGroup struct {
	Config   KafkaConsumerConfig
	client   *kgo.Client
	cancelMu sync.Mutex
	cancel   context.CancelFunc
	closeMu  sync.Mutex
	closed   bool

	// For in-memory support
	inMemoryConsumer *inmemorykafka.InMemoryConsumerGroup
	isInMemory       bool
}

// validateTimeoutConfig validates that timeout configuration follows constraints
func validateTimeoutConfig(cfg KafkaConsumerConfig) error {
	if cfg.HeartbeatInterval <= 0 || cfg.SessionTimeout <= 0 {
		return nil // Using defaults, which are already valid
	}

	if cfg.SessionTimeout < 3*cfg.HeartbeatInterval {
		return errors.NewConfigurationError(
			"invalid Kafka consumer timeout configuration for topic %s: sessionTimeout (%v) must be >= 3 * heartbeatInterval (%v). Got ratio: %.2fx",
			cfg.Topic,
			cfg.SessionTimeout,
			cfg.HeartbeatInterval,
			float64(cfg.SessionTimeout)/float64(cfg.HeartbeatInterval),
		)
	}

	return nil
}

// NewKafkaConsumerGroupFromURL creates a new KafkaConsumerGroup from a URL.
func NewKafkaConsumerGroupFromURL(logger ulogger.Logger, url *url.URL, consumerGroupID string, autoCommit bool, kafkaSettings *settings.KafkaSettings) (*KafkaConsumerGroup, error) {
	if url == nil {
		return nil, errors.NewConfigurationError("missing kafka url")
	}

	partitions := util.GetQueryParamInt(url, "partitions", 1)

	// AutoCommitEnabled: whether the consumer commits offsets automatically after processing.
	// Per-topic semantics matter for correctness and at-least-once vs best-effort delivery:
	//   - txMetaCache: true, we CAN miss (best-effort populating cache).
	//   - rejected txs: true, we CAN miss.
	//   - subtree validation: false (at-least-once).
	//   - block persister: false.
	//   - block validation: false.

	// Extract timeout configuration from URL query parameters (in milliseconds).
	// Defaults match common Kafka client defaults; can be overridden per-topic for slow processing (e.g. subtree validation).
	maxProcessingTimeMs := util.GetQueryParamInt(url, "maxProcessingTime", 100)
	sessionTimeoutMs := util.GetQueryParamInt(url, "sessionTimeout", 10000)
	heartbeatIntervalMs := util.GetQueryParamInt(url, "heartbeatInterval", 3000)
	rebalanceTimeoutMs := util.GetQueryParamInt(url, "rebalanceTimeout", 60000)

	// Offset reset strategy: how to handle offset-out-of-range (e.g. "latest", "earliest", or "" for default/Replay).
	offsetReset := url.Query().Get("offsetReset")

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

	consumerConfig := KafkaConsumerConfig{
		Logger:             logger,
		URL:                url,
		BrokersURL:         strings.Split(url.Host, ","),
		Topic:              strings.TrimPrefix(url.Path, "/"),
		Partitions:         partitions,
		ConsumerGroupID:    consumerGroupID,
		AutoCommitEnabled:  autoCommit,
		Replay:             util.GetQueryParamInt(url, "replay", 1) == 1,
		MaxProcessingTime:  time.Duration(maxProcessingTimeMs) * time.Millisecond,
		SessionTimeout:     time.Duration(sessionTimeoutMs) * time.Millisecond,
		HeartbeatInterval:  time.Duration(heartbeatIntervalMs) * time.Millisecond,
		RebalanceTimeout:   time.Duration(rebalanceTimeoutMs) * time.Millisecond,
		OffsetReset:        offsetReset,
		EnableTLS:          enableTLS,
		TLSSkipVerify:      tlsSkipVerify,
		TLSCAFile:          tlsCAFile,
		TLSCertFile:        tlsCertFile,
		TLSKeyFile:         tlsKeyFile,
		EnableDebugLogging: enableDebugLogging,
	}

	if err := validateTimeoutConfig(consumerConfig); err != nil {
		return nil, err
	}

	return NewKafkaConsumerGroup(consumerConfig)
}

// Close gracefully shuts down the Kafka consumer group
func (k *KafkaConsumerGroup) Close() error {
	if k == nil || k.Config.Logger == nil {
		return nil
	}

	k.Config.Logger.Infof("[Kafka] %s: initiating shutdown of consumer group for topic %s", k.Config.ConsumerGroupID, k.Config.Topic)

	k.cancelMu.Lock()
	cancelFn := k.cancel
	k.cancel = nil
	k.cancelMu.Unlock()
	if cancelFn != nil {
		k.Config.Logger.Debugf("[Kafka] %s: canceling context for topic %s", k.Config.ConsumerGroupID, k.Config.Topic)
		cancelFn()
	}

	if k.isInMemory {
		if k.inMemoryConsumer != nil {
			if err := k.inMemoryConsumer.Close(); err != nil {
				k.Config.Logger.Errorf("[Kafka] %s: error closing in-memory consumer for topic %s: %v", k.Config.ConsumerGroupID, k.Config.Topic, err)
				return err
			}
		}
	} else {
		k.closeClient()
	}

	return nil
}

func (k *KafkaConsumerGroup) closeClient() {
	k.closeMu.Lock()
	defer k.closeMu.Unlock()

	if k.closed {
		return
	}

	if k.client != nil {
		k.client.Close()
	}
	k.closed = true
	k.Config.Logger.Infof("[Kafka] %s: successfully closed consumer group for topic %s", k.Config.ConsumerGroupID, k.Config.Topic)
}

// NewKafkaConsumerGroup creates a new Kafka consumer group using franz-go
func NewKafkaConsumerGroup(cfg KafkaConsumerConfig) (*KafkaConsumerGroup, error) {
	if cfg.URL == nil {
		return nil, errors.NewConfigurationError("kafka URL is not set", nil)
	}

	if cfg.Logger == nil {
		return nil, errors.NewConfigurationError("logger is not set", nil)
	}

	if cfg.ConsumerGroupID == "" {
		return nil, errors.NewConfigurationError("group ID is not set", nil)
	}

	cfg.Logger.Infof("Starting Kafka consumer for topic %s in group %s", cfg.Topic, cfg.ConsumerGroupID)

	// Handle in-memory case
	if cfg.URL.Scheme == memoryScheme {
		broker := inmemorykafka.GetSharedBroker()
		consumerGroup := inmemorykafka.NewInMemoryConsumerGroup(broker, cfg.Topic, cfg.ConsumerGroupID)
		cfg.Logger.Infof("Using in-memory Kafka consumer group")

		return &KafkaConsumerGroup{
			Config:           cfg,
			inMemoryConsumer: consumerGroup,
			isInMemory:       true,
		}, nil
	}

	// Apply defaults for non-positive (zero or negative) timeouts. These match the defaults in
	// NewKafkaConsumerGroupFromURL but are needed when callers construct
	// KafkaConsumerConfig directly without going through the URL parser.
	if cfg.MaxProcessingTime <= 0 {
		cfg.MaxProcessingTime = 100 * time.Millisecond
	}
	if cfg.SessionTimeout <= 0 {
		cfg.SessionTimeout = 10 * time.Second
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 3 * time.Second
	}
	if cfg.RebalanceTimeout <= 0 {
		cfg.RebalanceTimeout = 60 * time.Second
	}

	// Validate timeout constraints (also validated in URL parser, but needed for direct callers)
	if err := validateTimeoutConfig(cfg); err != nil {
		return nil, err
	}

	// Build franz-go client options
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.BrokersURL...),
		kgo.ConsumerGroup(cfg.ConsumerGroupID),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.FetchMaxWait(cfg.MaxProcessingTime),
		kgo.SessionTimeout(cfg.SessionTimeout),
		kgo.HeartbeatInterval(cfg.HeartbeatInterval),
		kgo.RebalanceTimeout(cfg.RebalanceTimeout),
	}

	// Configure offset reset behavior
	if cfg.OffsetReset != "" {
		switch strings.ToLower(cfg.OffsetReset) {
		case "latest", "newest":
			opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
			cfg.Logger.Infof("[Kafka] %s: configured to reset to latest offset when out of range", cfg.Topic)
		case "earliest", "oldest":
			opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
			cfg.Logger.Infof("[Kafka] %s: configured to reset to earliest offset when out of range", cfg.Topic)
		default:
			return nil, errors.NewConfigurationError(
				"invalid offsetReset value '%s' for topic %s. Valid values: 'latest', 'earliest'",
				cfg.OffsetReset,
				cfg.Topic,
			)
		}
	} else if cfg.Replay {
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
		cfg.Logger.Infof("[Kafka] %s: replay enabled, configured to consume from earliest offset", cfg.Topic)
	}

	// Configure auto-commit
	if !cfg.AutoCommitEnabled {
		opts = append(opts, kgo.DisableAutoCommit())
	}

	// Configure TLS if enabled
	if cfg.EnableTLS {
		tlsConfig, err := buildFranzTLSConfig(cfg.EnableTLS, cfg.TLSSkipVerify, cfg.TLSCAFile, cfg.TLSCertFile, cfg.TLSKeyFile)
		if err != nil {
			return nil, errors.NewConfigurationError("failed to configure TLS for kafka consumer", err)
		}
		opts = append(opts, kgo.DialTLSConfig(tlsConfig))
	}

	// Enable debug logging if configured
	if cfg.EnableDebugLogging {
		opts = append(opts, kgo.WithLogger(&franzLoggerAdapter{logger: cfg.Logger}))
		cfg.Logger.Infof("Kafka debug logging enabled for consumer group %s", cfg.ConsumerGroupID)
	}

	// Create the franz-go client
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, errors.NewServiceError("failed to create Kafka consumer client for %s", cfg.Topic, err)
	}

	return &KafkaConsumerGroup{
		Config: cfg,
		client: client,
	}, nil
}

// ConsumerOption represents an option for configuring the consumer behavior
type ConsumerOption func(*consumerOptions)

type consumerOptions struct {
	withRetryAndMoveOn    bool
	withRetryAndStop      bool
	withLogErrorAndMoveOn bool
	maxRetries            int
	backoffMultiplier     int
	backoffDurationType   time.Duration
	stopFn                func()
}

// WithRetryAndMoveOn configures error behaviour for the consumer function
func WithRetryAndMoveOn(maxRetries, backoffMultiplier int, backoffDurationType time.Duration) ConsumerOption {
	return func(o *consumerOptions) {
		o.withRetryAndMoveOn = true
		o.withRetryAndStop = false
		o.maxRetries = maxRetries
		o.backoffMultiplier = backoffMultiplier
		o.backoffDurationType = backoffDurationType
	}
}

// WithRetryAndStop configures error behaviour for the consumer function
func WithRetryAndStop(maxRetries, backoffMultiplier int, backoffDurationType time.Duration, stopFn func()) ConsumerOption {
	return func(o *consumerOptions) {
		o.withRetryAndMoveOn = false
		o.withRetryAndStop = true
		o.maxRetries = maxRetries
		o.backoffMultiplier = backoffMultiplier
		o.backoffDurationType = backoffDurationType
		o.stopFn = stopFn
	}
}

// WithLogErrorAndMoveOn configures error behaviour for the consumer function
func WithLogErrorAndMoveOn() ConsumerOption {
	return func(o *consumerOptions) {
		o.withLogErrorAndMoveOn = true
		o.withRetryAndMoveOn = false
		o.withRetryAndStop = false
	}
}

func (k *KafkaConsumerGroup) Start(ctx context.Context, consumerFn func(message *KafkaMessage) error, opts ...ConsumerOption) {
	if k == nil {
		return
	}

	if consumerFn == nil {
		k.Config.Logger.Errorf("kafka consumer %s: consumerFn is nil, cannot start", k.Config.Topic)
		return
	}

	// Handle in-memory case
	if k.isInMemory {
		k.startInMemory(ctx, consumerFn, opts...)
		return
	}

	options := &consumerOptions{
		withRetryAndMoveOn:    false,
		withRetryAndStop:      false,
		withLogErrorAndMoveOn: false,
		maxRetries:            3,
		backoffMultiplier:     2,
		backoffDurationType:   time.Second,
	}
	for _, opt := range opts {
		opt(options)
	}

	// Apply retry/error handling wrappers
	consumerFn = wrapConsumerFn(ctx, k.Config.Logger, k.Config.Topic, consumerFn, options)

	// Create internal context and store cancel func before spawning goroutines.
	// Protected by cancelMu to avoid a data race with Close().
	internalCtx, cancel := context.WithCancel(ctx)
	k.cancelMu.Lock()
	k.cancel = cancel
	k.cancelMu.Unlock()

	go func() {
		defer cancel()

		// Main consume loop
		go func() {
			k.Config.Logger.Debugf("[kafka] starting consumer for group %s on topic %s", k.Config.ConsumerGroupID, k.Config.Topic)

			commitTicker := time.NewTicker(time.Minute)
			defer commitTicker.Stop()

			uncommittedRecords := make([]*kgo.Record, 0)
			var uncommittedMu sync.Mutex

			for {
				select {
				case <-internalCtx.Done():
					k.commitRecords(uncommittedRecords)
					return
				default:
					fetches := k.client.PollFetches(internalCtx)

					if errs := fetches.Errors(); len(errs) > 0 {
						for _, err := range errs {
							if errors.Is(err.Err, context.Canceled) || errors.Is(err.Err, kgo.ErrClientClosed) {
								k.Config.Logger.Debugf("Kafka consumer shutdown: %v", err.Err)
								return
							}
							k.Config.Logger.Errorf("Kafka consumer error on topic %s partition %d: %v", err.Topic, err.Partition, err.Err)
						}
						continue
					}

					fetches.EachPartition(func(p kgo.FetchTopicPartition) {
						hwm := p.HighWatermark
						for _, record := range p.Records {
							kafkaMsg := &KafkaMessage{
								Key:           record.Key,
								Value:         record.Value,
								Topic:         record.Topic,
								Partition:     record.Partition,
								Offset:        record.Offset,
								Timestamp:     record.Timestamp,
								HighWaterMark: hwm,
							}

							if err := consumerFn(kafkaMsg); err != nil {
								k.Config.Logger.Errorf("[kafka_consumer] failed to process message (topic: %s, partition: %d, offset: %d): %v",
									record.Topic, record.Partition, record.Offset, err)
								// Continue to the next record. Skipping the
								// uncommittedRecords append keeps the failed
								// record from being marked done, matching the
								// pre-refactor EachRecord behavior.
								continue
							}

							if !k.Config.AutoCommitEnabled {
								uncommittedMu.Lock()
								uncommittedRecords = append(uncommittedRecords, record)
								uncommittedMu.Unlock()
							}
						}
					})

					select {
					case <-commitTicker.C:
						uncommittedMu.Lock()
						if len(uncommittedRecords) > 0 {
							k.commitRecords(uncommittedRecords)
							uncommittedRecords = uncommittedRecords[:0]
						}
						uncommittedMu.Unlock()
					default:
					}
				}
			}
		}()

		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

		select {
		case <-signals:
			k.Config.Logger.Infof("[kafka] Received signal, shutting down consumers for group %s", k.Config.ConsumerGroupID)
			cancel()
		case <-internalCtx.Done():
			k.Config.Logger.Infof("[kafka] Context done, shutting down consumer for %s", k.Config.ConsumerGroupID)
		}

		k.closeClient()
	}()
}

// startInMemory handles the in-memory consumer case
func (k *KafkaConsumerGroup) startInMemory(ctx context.Context, consumerFn func(message *KafkaMessage) error, opts ...ConsumerOption) {
	options := &consumerOptions{
		maxRetries:          3,
		backoffMultiplier:   2,
		backoffDurationType: time.Second,
	}
	for _, opt := range opts {
		opt(options)
	}

	handler := &inMemoryConsumerHandler{
		logger:     k.Config.Logger,
		consumerFn: consumerFn,
		options:    options,
		topic:      k.Config.Topic,
	}

	go func() {
		err := k.inMemoryConsumer.Consume(ctx, []string{k.Config.Topic}, handler)
		if err != nil && !errors.Is(err, context.Canceled) {
			k.Config.Logger.Errorf("In-memory consumer error: %v", err)
		}
	}()
}

// commitRecords commits the offsets for the given records
func (k *KafkaConsumerGroup) commitRecords(records []*kgo.Record) {
	if len(records) == 0 || k.client == nil {
		return
	}

	offsets := make(map[string]map[int32]kgo.EpochOffset)
	for _, r := range records {
		if _, ok := offsets[r.Topic]; !ok {
			offsets[r.Topic] = make(map[int32]kgo.EpochOffset)
		}
		offsets[r.Topic][r.Partition] = kgo.EpochOffset{
			Epoch:  r.LeaderEpoch,
			Offset: r.Offset + 1,
		}
	}

	k.client.CommitOffsets(context.Background(), offsets, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, _ *kmsg.OffsetCommitResponse, err error) {
		if err != nil {
			k.Config.Logger.Errorf("[kafka] Failed to commit offsets: %v", err)
		}
	})
}

// BrokersURL returns the list of Kafka broker URLs.
// Returns nil for in-memory consumers since there are no real brokers.
func (k *KafkaConsumerGroup) BrokersURL() []string {
	if k.isInMemory {
		return nil
	}

	return k.Config.BrokersURL
}

// PauseAll suspends fetching from all partitions.
func (k *KafkaConsumerGroup) PauseAll() {
	if k.isInMemory {
		k.inMemoryConsumer.PauseAll()
		return
	}
	if k.client != nil {
		k.client.PauseFetchTopics(k.Config.Topic)
		k.Config.Logger.Debugf("[Kafka] %s: paused all partitions for topic %s", k.Config.ConsumerGroupID, k.Config.Topic)
	}
}

// ResumeAll resumes all partitions which have been paused.
func (k *KafkaConsumerGroup) ResumeAll() {
	if k.isInMemory {
		k.inMemoryConsumer.ResumeAll()
		return
	}
	if k.client != nil {
		k.client.ResumeFetchTopics(k.Config.Topic)
		k.Config.Logger.Debugf("[Kafka] %s: resumed all partitions for topic %s", k.Config.ConsumerGroupID, k.Config.Topic)
	}
}

// wrapConsumerFn applies retry/error handling wrappers to consumer function
func wrapConsumerFn(ctx context.Context, logger ulogger.Logger, topic string, consumerFn func(message *KafkaMessage) error, options *consumerOptions) func(message *KafkaMessage) error {
	if options.withRetryAndMoveOn {
		originalFn := consumerFn
		consumerFn = func(msg *KafkaMessage) error {
			var err error
			for i := 0; i < options.maxRetries; i++ {
				err = originalFn(msg)
				if err == nil {
					return nil
				}
				backoff := time.Duration(options.backoffMultiplier*(i+1)) * options.backoffDurationType
				logger.Warnf("[kafka_consumer] retrying processing kafka message... attempt %d/%d, backoff %v", i+1, options.maxRetries, backoff)
				time.Sleep(backoff)
			}

			key := ""
			if msg != nil && msg.Key != nil {
				key = string(msg.Key)
			}
			logger.Errorf("[kafka_consumer] error processing kafka message on topic %s (key: %s), skipping", topic, key)
			return nil
		}
	}

	if options.withRetryAndStop {
		originalFn := consumerFn
		consumerFn = func(msg *KafkaMessage) error {
			var err error
			for i := 0; i < options.maxRetries; i++ {
				err = originalFn(msg)
				if err == nil {
					return nil
				}
				backoff := time.Duration(options.backoffMultiplier*(i+1)) * options.backoffDurationType
				logger.Warnf("[kafka_consumer] retrying processing kafka message... attempt %d/%d, backoff %v", i+1, options.maxRetries, backoff)
				time.Sleep(backoff)
			}

			key := ""
			if msg != nil && msg.Key != nil {
				key = string(msg.Key)
			}
			logger.Errorf("[kafka_consumer] error processing kafka message on topic %s (key: %s), stopping", topic, key)
			if options.stopFn != nil {
				options.stopFn()
			}
			return nil
		}
	}

	if options.withLogErrorAndMoveOn {
		originalFn := consumerFn
		consumerFn = func(msg *KafkaMessage) error {
			err := originalFn(msg)
			if err != nil {
				key := ""
				if msg != nil && msg.Key != nil {
					key = string(msg.Key)
				}
				logger.Errorf("[kafka_consumer] error processing kafka message on topic %s (key: %s), skipping: %v", topic, key, err)
			}
			return nil
		}
	}

	return consumerFn
}

// inMemoryConsumerHandler implements the handler for in-memory consumer
type inMemoryConsumerHandler struct {
	logger     ulogger.Logger
	consumerFn func(message *KafkaMessage) error
	options    *consumerOptions
	topic      string
}

func (h *inMemoryConsumerHandler) Setup(_ inmemorykafka.ConsumerGroupSession) error {
	return nil
}

func (h *inMemoryConsumerHandler) Cleanup(_ inmemorykafka.ConsumerGroupSession) error {
	return nil
}

func (h *inMemoryConsumerHandler) ConsumeClaim(session inmemorykafka.ConsumerGroupSession, claim inmemorykafka.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		kafkaMsg := &KafkaMessage{
			Key:           message.Key,
			Value:         message.Value,
			Topic:         message.Topic,
			Partition:     message.Partition,
			Offset:        message.Offset,
			Timestamp:     message.Timestamp,
			HighWaterMark: claim.HighWaterMarkOffset(),
		}

		var err error
		if h.options.withRetryAndMoveOn {
			for i := 0; i < h.options.maxRetries; i++ {
				err = h.consumerFn(kafkaMsg)
				if err == nil {
					break
				}
				time.Sleep(time.Duration(h.options.backoffMultiplier*(i+1)) * h.options.backoffDurationType)
			}
			if err != nil {
				h.logger.Errorf("[kafka_consumer] error processing message, skipping: %v", err)
			}
			continue
		}

		if h.options.withLogErrorAndMoveOn {
			if err := h.consumerFn(kafkaMsg); err != nil {
				h.logger.Errorf("[kafka_consumer] error processing message, skipping: %v", err)
			}
			continue
		}

		if err := h.consumerFn(kafkaMsg); err != nil {
			if h.options.withRetryAndStop && h.options.stopFn != nil {
				h.options.stopFn()
			}
			return err
		}

		session.MarkMessage(message, "")
	}
	return nil
}
