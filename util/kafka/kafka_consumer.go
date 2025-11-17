// Package kafka provides Kafka consumer and producer implementations for message handling.
package kafka

import (
	"context"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	inmemorykafka "github.com/bsv-blockchain/teranode/util/kafka/in_memory_kafka"
	"github.com/bsv-blockchain/teranode/util/retry"
)

const memoryScheme = "memory"

// saramaLoggerAdapter adapts ulogger.Logger to sarama.StdLogger interface
type saramaLoggerAdapter struct {
	logger ulogger.Logger
}

func (s *saramaLoggerAdapter) Print(v ...interface{}) {
	s.logger.Infof("[SARAMA] %v", v...)
}

func (s *saramaLoggerAdapter) Printf(format string, v ...interface{}) {
	s.logger.Infof("[SARAMA] "+format, v...)
}

func (s *saramaLoggerAdapter) Println(v ...interface{}) {
	s.logger.Infof("[SARAMA] %v", v...)
}

// KafkaMessage wraps sarama.ConsumerMessage to provide additional functionality.
type KafkaMessage struct {
	sarama.ConsumerMessage
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

	// Timeout configuration (query params: maxProcessingTime, sessionTimeout, heartbeatInterval, rebalanceTimeout, channelBufferSize, consumerTimeout)
	MaxProcessingTime time.Duration // Max time to process a message before Sarama stops fetching (Sarama default: 100ms)
	SessionTimeout    time.Duration // Time broker waits for heartbeat before considering consumer dead (Sarama default: 10s)
	HeartbeatInterval time.Duration // Frequency of heartbeats to broker (Sarama default: 3s)
	RebalanceTimeout  time.Duration // Max time for all consumers to join rebalance (Sarama default: 60s)
	ChannelBufferSize int           // Number of messages buffered in internal channels (Sarama default: 256)
	ConsumerTimeout   time.Duration // Max time without messages before watchdog triggers recovery (default: 90s)

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
	EnableDebugLogging bool // Enable verbose Sarama (Kafka client) debug logging
}

// consumeWatchdog monitors Consume() state to detect when stuck in RefreshMetadata and triggers force recovery.
// When stuck is detected (Consume() started but Setup() not called for 90s), it forces recovery by
// closing the consumer group and recreating it. This simulates what happens when Kafka server restarts.
//
// The watchdog tracks two scenarios:
// 1. Consume() called but Setup() never called (stuck in RefreshMetadata before joining group)
// 2. Consume() returns with error, retry loop attempts to call Consume() again, but it hangs
//
// Note: Offset out of range errors are now handled immediately by the error handler, not by the watchdog.
type consumeWatchdog struct {
	consumeStartTime    atomic.Value // time.Time - when Consume() was called
	setupCalledTime     atomic.Value // time.Time - when Setup() was called
	consumeEndTime      atomic.Value // time.Time - when Consume() returned (error or success)
	isAttemptingConsume atomic.Bool  // true between Consume() call and Setup() or error
}

func (w *consumeWatchdog) markConsumeStarted() {
	w.consumeStartTime.Store(time.Now())
	w.setupCalledTime.Store(time.Time{}) // Reset
	w.consumeEndTime.Store(time.Time{})  // Reset
	w.isAttemptingConsume.Store(true)
}

func (w *consumeWatchdog) markSetupCalled() {
	w.setupCalledTime.Store(time.Now())
	w.isAttemptingConsume.Store(false)
}

func (w *consumeWatchdog) markConsumeEnded() {
	w.consumeEndTime.Store(time.Now())
	w.isAttemptingConsume.Store(false)
}

func (w *consumeWatchdog) isStuckInRefreshMetadata(threshold time.Duration) (bool, time.Duration) {
	if !w.isAttemptingConsume.Load() {
		return false, 0
	}

	startTime, ok := w.consumeStartTime.Load().(time.Time)
	if !ok || startTime.IsZero() {
		return false, 0
	}

	setupTime, _ := w.setupCalledTime.Load().(time.Time)
	if !setupTime.IsZero() {
		// Setup was called, not stuck
		return false, 0
	}

	duration := time.Since(startTime)
	return duration > threshold, duration
}

// isStuckAfterError detects when Consume() returned with an error, the retry loop is attempting
// to call Consume() again, but it's been stuck for longer than the threshold without Setup() being called.
// This catches the case where offset errors cause Consume() to hang in RefreshMetadata on retry.
func (w *consumeWatchdog) isStuckAfterError(threshold time.Duration) (bool, time.Duration) {
	// Check if Consume() has ended (returned with error or success)
	endTime, ok := w.consumeEndTime.Load().(time.Time)
	if !ok || endTime.IsZero() {
		// Consume() never ended, use the regular stuck detection
		return false, 0
	}

	// Check if we're currently attempting to consume again
	if !w.isAttemptingConsume.Load() {
		// Not attempting, so can't be stuck
		return false, 0
	}

	// Check when the retry attempt started
	startTime, ok := w.consumeStartTime.Load().(time.Time)
	if !ok || startTime.IsZero() {
		return false, 0
	}

	// If startTime is before endTime, something is wrong with our tracking
	if startTime.Before(endTime) {
		return false, 0
	}

	// Check if Setup() was called after the retry
	setupTime, _ := w.setupCalledTime.Load().(time.Time)
	if !setupTime.IsZero() && setupTime.After(endTime) {
		// Setup was called after the error, so we're not stuck
		return false, 0
	}

	// We've been attempting to consume since the retry started, without Setup() being called
	duration := time.Since(startTime)
	return duration > threshold, duration
}

// KafkaConsumerGroup implements KafkaConsumerGroupI interface.
type KafkaConsumerGroup struct {
	Config        KafkaConsumerConfig
	ConsumerGroup sarama.ConsumerGroup
	cancel        atomic.Value
	watchdog      *consumeWatchdog // Monitors for stuck RefreshMetadata and triggers force recovery

	// For force recovery when consumer is stuck
	consumerMu   sync.Mutex     // Protects consumer recreation
	saramaConfig *sarama.Config // Stored config for recreating consumer
}

// validateTimeoutConfig validates that timeout configuration follows Sarama constraints
func validateTimeoutConfig(cfg KafkaConsumerConfig) error {
	// Only validate if custom timeouts are set (non-zero)
	if cfg.HeartbeatInterval <= 0 || cfg.SessionTimeout <= 0 {
		return nil // Using Sarama defaults, which are already valid
	}

	// Sarama requires: SessionTimeout >= 3 * HeartbeatInterval
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
// This is a convenience function for production code that extracts settings from kafkaSettings.
// For tests, use NewKafkaConsumerGroup directly with a manually constructed config.
func NewKafkaConsumerGroupFromURL(logger ulogger.Logger, url *url.URL, consumerGroupID string, autoCommit bool, kafkaSettings *settings.KafkaSettings) (*KafkaConsumerGroup, error) {
	if url == nil {
		return nil, errors.NewConfigurationError("missing kafka url")
	}

	partitions := util.GetQueryParamInt(url, "partitions", 1)

	// Generate a unique group ID for the txmeta Kafka listener, to ensure that each instance of this service will process all txmeta messages.
	// This is necessary because the txmeta messages are used to populate the txmeta cache, which is shared across all instances of this service.
	// groupID := topic + "-" + uuid.New().String()

	// AutoCommitEnabled:
	// txMetaCache : true, we CAN miss.
	// rejected txs : true, we CAN miss.
	// subtree validation : false.
	// block persister : false.
	// block validation: false.

	// Extract timeout configuration from URL query parameters (in milliseconds)
	// Defaults match Sarama's defaults - can be overridden per-topic for slow processing (e.g., subtree validation)
	maxProcessingTimeMs := util.GetQueryParamInt(url, "maxProcessingTime", 100)  // Sarama default: 100ms
	sessionTimeoutMs := util.GetQueryParamInt(url, "sessionTimeout", 10000)      // Sarama default: 10s
	heartbeatIntervalMs := util.GetQueryParamInt(url, "heartbeatInterval", 3000) // Sarama default: 3s
	rebalanceTimeoutMs := util.GetQueryParamInt(url, "rebalanceTimeout", 60000)  // Sarama default: 60s
	channelBufferSize := util.GetQueryParamInt(url, "channelBufferSize", 256)    // Sarama default: 256
	consumerTimeoutMs := util.GetQueryParamInt(url, "consumerTimeout", 90000)    // Default: 90s (watchdog timeout for no messages)

	// Extract offset reset strategy (how to handle offset out of range errors)
	// Values: "latest" (default), "earliest", or "" (empty uses Replay setting)
	offsetReset := url.Query().Get("offsetReset")

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

	consumerConfig := KafkaConsumerConfig{
		Logger:            logger,
		URL:               url,
		BrokersURL:        strings.Split(url.Host, ","),
		Topic:             strings.TrimPrefix(url.Path, "/"),
		Partitions:        partitions,
		ConsumerGroupID:   consumerGroupID,
		AutoCommitEnabled: autoCommit,
		// default is start from beginning
		// do not ignore everything that is already queued, this is the case where we start a new consumer group for the first time
		// maybe it shouldn't be called replay because it suggests that the consume will always replay messages from the beginning
		Replay:            util.GetQueryParamInt(url, "replay", 1) == 1,
		MaxProcessingTime: time.Duration(maxProcessingTimeMs) * time.Millisecond,
		SessionTimeout:    time.Duration(sessionTimeoutMs) * time.Millisecond,
		HeartbeatInterval: time.Duration(heartbeatIntervalMs) * time.Millisecond,
		RebalanceTimeout:  time.Duration(rebalanceTimeoutMs) * time.Millisecond,
		ChannelBufferSize: channelBufferSize,
		ConsumerTimeout:   time.Duration(consumerTimeoutMs) * time.Millisecond,
		OffsetReset:       offsetReset,
		// TLS/Auth configuration
		EnableTLS:          enableTLS,
		TLSSkipVerify:      tlsSkipVerify,
		TLSCAFile:          tlsCAFile,
		TLSCertFile:        tlsCertFile,
		TLSKeyFile:         tlsKeyFile,
		EnableDebugLogging: enableDebugLogging,
	}

	// Validate timeout configuration
	if err := validateTimeoutConfig(consumerConfig); err != nil {
		return nil, err
	}

	return NewKafkaConsumerGroup(consumerConfig)
}

// Close gracefully shuts down the Kafka consumer group
func (k *KafkaConsumerGroup) Close() error {
	// Check if the consumer group was properly initialized
	if k == nil || k.Config.Logger == nil {
		return nil
	}

	k.Config.Logger.Infof("[Kafka] %s: initiating shutdown of consumer group for topic %s", k.Config.ConsumerGroupID, k.Config.Topic)

	// cancel the context first to signal all consumers to stop
	if k.cancel.Load() != nil {
		k.Config.Logger.Debugf("[Kafka] %s: canceling context for topic %s", k.Config.ConsumerGroupID, k.Config.Topic)
		k.cancel.Load().(context.CancelFunc)()
	}

	// Then close the consumer group
	if k.ConsumerGroup != nil {
		if err := k.ConsumerGroup.Close(); err != nil {
			k.Config.Logger.Errorf("[Kafka] %s: error closing consumer group for topic %s: %v", k.Config.ConsumerGroupID, k.Config.Topic, err)
			return err
		}

		k.Config.Logger.Infof("[Kafka] %s: successfully closed consumer group for topic %s", k.Config.ConsumerGroupID, k.Config.Topic)
	}

	return nil
}

// forceRecovery forces recovery of a stuck consumer by closing and recreating the consumer group.
// This simulates what happens when you restart the Kafka server - the connection closes,
// the stuck Consume() returns with an error, and the retry loop creates a fresh consumer.
//
// This is safe because:
// - Close() unblocks the stuck Consume() call by closing internal connections
// - We use a mutex to prevent concurrent recovery attempts
// - The new consumer group is created with the same configuration
// - The retry loop automatically uses the new consumer on the next iteration
func (k *KafkaConsumerGroup) forceRecovery() error {
	// Lock to prevent concurrent recovery attempts
	k.consumerMu.Lock()
	defer k.consumerMu.Unlock()

	k.Config.Logger.Warnf("[kafka-watchdog] Forcing recovery for topic %s - closing stuck consumer and creating new one", k.Config.Topic)

	// Close the existing consumer group - this will cause stuck Consume() to return
	if k.ConsumerGroup != nil {
		if err := k.ConsumerGroup.Close(); err != nil {
			k.Config.Logger.Errorf("[kafka-watchdog] Error closing stuck consumer group: %v", err)
			// Continue anyway - we'll try to create a new one
		}
	}

	// Create a new consumer group with the same configuration
	newConsumerGroup, err := sarama.NewConsumerGroup(k.Config.BrokersURL, k.Config.ConsumerGroupID, k.saramaConfig)
	if err != nil {
		return errors.NewServiceError("failed to recreate consumer group for %s", k.Config.Topic, err)
	}

	// Replace the consumer group atomically
	k.ConsumerGroup = newConsumerGroup

	k.Config.Logger.Infof("[kafka-watchdog] Successfully recreated consumer group for topic %s", k.Config.Topic)
	return nil
}

// NewKafkaConsumerGroup creates a new Kafka consumer group
// We DO NOT read autocommit parameter from the URL because the handler func has specific error handling logic.
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

	cfg.Logger.Infof("Starting Kafka consumer for topic %s in group %s (concurrency based on partition count)", cfg.Topic, cfg.ConsumerGroupID)

	// Initialize Prometheus metrics (idempotent)
	InitPrometheusMetrics()

	var consumerGroup sarama.ConsumerGroup

	if cfg.URL.Scheme == memoryScheme {
		// --- Use the in-memory implementation ---
		broker := inmemorykafka.GetSharedBroker() // Get the shared broker instance
		// Create the InMemoryConsumerGroup which implements sarama.ConsumerGroup
		consumerGroup = inmemorykafka.NewInMemoryConsumerGroup(broker, cfg.Topic, cfg.ConsumerGroupID)
		cfg.Logger.Infof("Using in-memory Kafka consumer group")
		// No error expected from creation here, unless topic/group were invalid (checked above)

		return &KafkaConsumerGroup{
			Config:        cfg,
			ConsumerGroup: consumerGroup,
			watchdog:      &consumeWatchdog{}, // Initialize watchdog for in-memory consumer
		}, nil
	}

	// --- Use the real Sarama implementation ---
	var err error

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// Enable Sarama debug logging for consumer diagnostics (only if configured)
	// By default, SARAMA logs are too verbose and not needed in production
	if cfg.EnableDebugLogging {
		sarama.Logger = &saramaLoggerAdapter{logger: cfg.Logger}
		cfg.Logger.Infof("Kafka debug logging enabled for consumer group %s", cfg.ConsumerGroupID)
	}

	// Configure consumer group timeouts from URL query parameters to prevent partition abandonment during slow processing

	// Only override Sarama defaults if explicitly set (non-zero values)
	if cfg.MaxProcessingTime > 0 {
		config.Consumer.MaxProcessingTime = cfg.MaxProcessingTime
	}

	if cfg.SessionTimeout > 0 {
		config.Consumer.Group.Session.Timeout = cfg.SessionTimeout
	}

	if cfg.HeartbeatInterval > 0 {
		config.Consumer.Group.Heartbeat.Interval = cfg.HeartbeatInterval
	}

	if cfg.RebalanceTimeout > 0 {
		config.Consumer.Group.Rebalance.Timeout = cfg.RebalanceTimeout
	}

	if cfg.ChannelBufferSize > 0 {
		config.ChannelBufferSize = cfg.ChannelBufferSize
	}

	// Configure network and metadata timeouts to prevent hanging when broker is unavailable
	// See: https://github.com/IBM/sarama/issues/2991 - RefreshMetadata doesn't respect context cancellation
	// These settings ensure metadata fetch fails quickly instead of hanging forever
	config.Net.DialTimeout = 10 * time.Second       // Max time to establish TCP connection
	config.Net.ReadTimeout = 10 * time.Second       // Max time waiting for response from broker
	config.Net.WriteTimeout = 10 * time.Second      // Max time for write operations
	config.Metadata.Timeout = 30 * time.Second      // Overall timeout for metadata operations
	config.Metadata.Retry.Max = 3                   // Retry metadata fetch 3 times
	config.Metadata.Retry.Backoff = 2 * time.Second // Wait 2s between metadata retries

	// Configure authentication if TLS is enabled
	if cfg.EnableTLS {
		cfg.Logger.Debugf("Configuring Kafka TLS authentication - EnableTLS: %v, SkipVerify: %v, CA: %s, Cert: %s",
			cfg.EnableTLS, cfg.TLSSkipVerify, cfg.TLSCAFile, cfg.TLSCertFile)

		if err := configureKafkaAuthFromFields(config, cfg.EnableTLS, cfg.TLSSkipVerify, cfg.TLSCAFile, cfg.TLSCertFile, cfg.TLSKeyFile); err != nil {
			return nil, errors.NewConfigurationError("failed to configure Kafka authentication", err)
		}

		cfg.Logger.Debugf("Successfully configured Kafka TLS authentication for consumer group %s", cfg.ConsumerGroupID)
	}

	// https://github.com/IBM/sarama/issues/1689
	// https://github.com/IBM/sarama/pull/1699
	// Default value for config.Consumer.Offsets.AutoCommit.Enable is true.
	if !cfg.AutoCommitEnabled {
		config.Consumer.Offsets.AutoCommit.Enable = false
	}

	// Configure offset reset behavior
	// This determines what offset to use when:
	// 1. There is no initial offset (new consumer group)
	// 2. Current offset is out of range (offset expired due to retention)
	//
	// NOTE: ResetInvalidOffsets is true by default in Sarama (since v1.38.1)
	// BUT it only works during consumer initialization. If offset becomes invalid
	// during active consumption, the partition consumer will shut down and trigger
	// a rebalance, then restart with Consumer.Offsets.Initial.
	if cfg.OffsetReset != "" {
		switch strings.ToLower(cfg.OffsetReset) {
		case "latest", "newest":
			config.Consumer.Offsets.Initial = sarama.OffsetNewest
			cfg.Logger.Infof("[Kafka] %s: configured to reset to latest offset when out of range", cfg.Topic)
		case "earliest", "oldest":
			config.Consumer.Offsets.Initial = sarama.OffsetOldest
			cfg.Logger.Infof("[Kafka] %s: configured to reset to earliest offset when out of range", cfg.Topic)
		default:
			return nil, errors.NewConfigurationError(
				"invalid offsetReset value '%s' for topic %s. Valid values: 'latest', 'earliest'",
				cfg.OffsetReset,
				cfg.Topic,
			)
		}
	} else if cfg.Replay {
		// Legacy behavior: Replay setting controls initial offset
		// defaults to OffsetNewest
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	clusterAdmin, err := sarama.NewClusterAdmin(cfg.BrokersURL, config)
	if err != nil {
		return nil, errors.NewConfigurationError("error while creating cluster admin", err)
	}
	defer func(clusterAdmin sarama.ClusterAdmin) {
		_ = clusterAdmin.Close()
	}(clusterAdmin)

	consumerGroup, err = sarama.NewConsumerGroup(cfg.BrokersURL, cfg.ConsumerGroupID, config)
	if err != nil {
		return nil, errors.NewServiceError("failed to create Kafka consumer group for %s", cfg.Topic, err)
	}

	return &KafkaConsumerGroup{
		Config:        cfg,
		ConsumerGroup: consumerGroup,
		watchdog:      &consumeWatchdog{}, // Initialize watchdog
		saramaConfig:  config,             // Store config for force recovery
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
// After max retries, the error is logged and the message is skipped
func WithRetryAndMoveOn(maxRetries, backoffMultiplier int, backoffDurationType time.Duration) ConsumerOption {
	return func(o *consumerOptions) {
		o.withRetryAndMoveOn = true
		o.withRetryAndStop = false // can't have both options set
		o.maxRetries = maxRetries
		o.backoffMultiplier = backoffMultiplier
		o.backoffDurationType = backoffDurationType
	}
}

// WithRetryAndStop configures error behaviour for the consumer function
// After max retries, the error is logged and message consumption stops
// Use this when you cannot proceed with the next message in the queue
func WithRetryAndStop(maxRetries, backoffMultiplier int, backoffDurationType time.Duration, stopFn func()) ConsumerOption {
	return func(o *consumerOptions) {
		o.withRetryAndMoveOn = false // can't have both options set
		o.withRetryAndStop = true
		o.maxRetries = maxRetries
		o.backoffMultiplier = backoffMultiplier
		o.backoffDurationType = backoffDurationType
		o.stopFn = stopFn
	}
}

// WithLogErrorAndMoveOn configures error behaviour for the consumer function
// When an error occurs, it is logged and the message is skipped without any retries
// Use this for non-critical messages where you want visibility of failures but don't want to block processing
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

	if options.withRetryAndMoveOn {
		originalFn := consumerFn
		consumerFn = func(msg *KafkaMessage) error {
			_, err := retry.Retry(ctx, k.Config.Logger, func() (any, error) {
				return struct{}{}, originalFn(msg)
			},
				retry.WithRetryCount(options.maxRetries),
				retry.WithBackoffMultiplier(options.backoffMultiplier),
				retry.WithBackoffDurationType(options.backoffDurationType),
				retry.WithMessage("[kafka_consumer] retrying processing kafka message..."))

			// if we can't process the message, log the error and skip to the next message
			if err != nil {
				key := ""
				if msg != nil && msg.Key != nil {
					key = string(msg.Key)
				}

				k.Config.Logger.Errorf("[kafka_consumer] error processing kafka message on topic %s (key: %s), skipping", k.Config.Topic, key)
			}

			return nil // give up and move on
		}
	}

	if options.withRetryAndStop {
		originalFn := consumerFn
		consumerFn = func(msg *KafkaMessage) error {
			_, err := retry.Retry(ctx, k.Config.Logger, func() (any, error) {
				return struct{}{}, originalFn(msg)
			},
				retry.WithRetryCount(options.maxRetries),
				retry.WithBackoffMultiplier(options.backoffMultiplier),
				retry.WithBackoffDurationType(options.backoffDurationType),
				retry.WithMessage("[kafka_consumer] retrying processing kafka message..."))

			// if we can't process the message, log the error and stop consuming any more messages
			if err != nil {
				if options.stopFn != nil {
					key := ""
					if msg != nil && msg.Key != nil {
						key = string(msg.Key)
					}

					k.Config.Logger.Errorf("[kafka_consumer] error processing kafka message on topic %s (key: %s), stopping", k.Config.Topic, key)
					options.stopFn()
				} else {
					c := k.ConsumerGroup
					k.ConsumerGroup = nil

					_ = c.Close()

					panic("error processing kafka message, with no stop function provided")
				}
			}

			return nil
		}
	}

	if options.withLogErrorAndMoveOn {
		originalFn := consumerFn
		consumerFn = func(msg *KafkaMessage) error {
			err := originalFn(msg)

			// if we can't process the message, log the error and skip to the next message
			if err != nil {
				key := ""
				if msg != nil && msg.Key != nil {
					key = string(msg.Key)
				}

				k.Config.Logger.Errorf("[kafka_consumer] error processing kafka message on topic %s (key: %s), skipping: %v", k.Config.Topic, key, err)
			}

			return nil // always move on to the next message
		}
	}

	go func() {
		internalCtx, cancel := context.WithCancel(ctx)
		k.cancel.Store(cancel)
		defer cancel()

		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				default:
					// Safely read current consumer (might be replaced by forceRecovery or offset reset)
					k.consumerMu.Lock()
					currentConsumer := k.ConsumerGroup
					k.consumerMu.Unlock()

					if currentConsumer == nil {
						time.Sleep(100 * time.Millisecond)
						continue
					}

					// Read from current consumer's error channel
					select {
					case err, ok := <-currentConsumer.Errors():
						if !ok {
							// Channel closed (consumer was closed), loop to get new consumer
							time.Sleep(100 * time.Millisecond)
							continue
						}
						if err != nil {
							// Check if this is an offset out of range error
							// This happens when committed offset has been deleted due to retention
							// Sarama's built-in offsetReset=latest will handle the reset when we recreate the consumer
							if errors.Is(err, sarama.ErrOffsetOutOfRange) || strings.Contains(err.Error(), "offset out of range") {
								k.Config.Logger.Errorf("[kafka-consumer-error] Offset out of range error detected: %v. Recreating consumer to trigger Sarama's offset reset...", err)

								// Close current consumer and recreate
								// Sarama will automatically reset to latest offset per offsetReset=latest config
								if recErr := k.forceRecovery(); recErr != nil {
									k.Config.Logger.Errorf("[kafka-consumer-error] Force recovery after offset error failed: %v", recErr)
								} else {
									k.Config.Logger.Infof("[kafka-consumer-error] Successfully recovered from offset out of range error. Sarama will auto-reset to latest offset.")
								}

								continue
							}

							// Don't log context cancellation as an error - it's expected during shutdown
							if errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "context canceled") {
								k.Config.Logger.Debugf("Kafka consumer shutdown: %v", err)
							} else {
								k.Config.Logger.Errorf("Kafka consumer error: %v", err)
							}
						}
					case <-ctx.Done():
						return
					}
				}
			}
		}()

		topics := []string{k.Config.Topic}

		// Watchdog: Active monitoring and recovery for stuck Consume() calls
		// This watchdog detects when Consume() is stuck in RefreshMetadata (Sarama bug #2991)
		// and triggers force recovery by closing and recreating the consumer group.
		// This simulates what happens when you restart the Kafka server in production.
		const watchdogCheckInterval = 30 * time.Second

		// Use configured timeout or default to 90s
		watchdogStuckThreshold := k.Config.ConsumerTimeout
		if watchdogStuckThreshold == 0 {
			watchdogStuckThreshold = 90 * time.Second
		}

		go func() {
			ticker := time.NewTicker(watchdogCheckInterval)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					// Check for initial RefreshMetadata hang (before first successful Setup)
					stuck, duration := k.watchdog.isStuckInRefreshMetadata(watchdogStuckThreshold)
					if stuck {
						// Get watchdog state for logging
						startTime, _ := k.watchdog.consumeStartTime.Load().(time.Time)
						setupTime, _ := k.watchdog.setupCalledTime.Load().(time.Time)

						k.Config.Logger.Errorf(
							"[kafka-consumer-watchdog][topic:%s][group:%s] Consume() stuck for %v (threshold: %v). "+
								"StartTime=%v SetupCalled=%v. Forcing recovery...",
							k.Config.Topic, k.Config.ConsumerGroupID, duration, watchdogStuckThreshold,
							startTime.Format(time.RFC3339), setupTime.IsZero(),
						)

						// Record metrics
						prometheusKafkaWatchdogRecoveryAttempts.WithLabelValues(k.Config.Topic, k.Config.ConsumerGroupID).Inc()
						prometheusKafkaWatchdogStuckDuration.WithLabelValues(k.Config.Topic).Observe(duration.Seconds())

						// Attempt force recovery
						if err := k.forceRecovery(); err != nil {
							k.Config.Logger.Errorf("[kafka-consumer-watchdog][topic:%s] Force recovery failed: %v. Will retry on next watchdog check.", k.Config.Topic, err)
						} else {
							k.Config.Logger.Infof("[kafka-consumer-watchdog][topic:%s] Force recovery successful. Consumer should resume.", k.Config.Topic)
							// Reset watchdog state
							k.watchdog.markConsumeEnded()
						}
						continue
					}

					// Check for hang after error/rebalance (Consume() returned, but retry is stuck)
					stuckAfterError, durationAfterError := k.watchdog.isStuckAfterError(watchdogStuckThreshold)
					if stuckAfterError {
						// Get watchdog state for logging
						startTime, _ := k.watchdog.consumeStartTime.Load().(time.Time)
						setupTime, _ := k.watchdog.setupCalledTime.Load().(time.Time)
						endTime, _ := k.watchdog.consumeEndTime.Load().(time.Time)

						k.Config.Logger.Errorf(
							"[kafka-consumer-watchdog][topic:%s][group:%s] Consume() stuck after error/rebalance for %v (threshold: %v). "+
								"StartTime=%v EndTime=%v SetupCalled=%v. Forcing recovery...",
							k.Config.Topic, k.Config.ConsumerGroupID, durationAfterError, watchdogStuckThreshold,
							startTime.Format(time.RFC3339), endTime.Format(time.RFC3339), setupTime.IsZero(),
						)

						// Record metrics
						prometheusKafkaWatchdogRecoveryAttempts.WithLabelValues(k.Config.Topic, k.Config.ConsumerGroupID).Inc()
						prometheusKafkaWatchdogStuckDuration.WithLabelValues(k.Config.Topic).Observe(durationAfterError.Seconds())

						// Attempt force recovery
						if err := k.forceRecovery(); err != nil {
							k.Config.Logger.Errorf("[kafka-consumer-watchdog][topic:%s] Force recovery failed: %v. Will retry on next watchdog check.", k.Config.Topic, err)
						} else {
							k.Config.Logger.Infof("[kafka-consumer-watchdog][topic:%s] Force recovery successful after error. Consumer should resume.", k.Config.Topic)
							// Reset watchdog state
							k.watchdog.markConsumeEnded()
						}
						continue
					}
				}
			}
		}()

		// Only spawn one consumer goroutine - Sarama handles partition concurrency internally
		go func() {
			k.Config.Logger.Debugf("[kafka] starting consumer for group %s on topic %s (partition-based concurrency)", k.Config.ConsumerGroupID, topics[0])

			for {
				select {
				case <-internalCtx.Done():
					// Context cancelled, exit goroutine
					return
				default:
					// Mark that we're attempting to start Consume() (before RefreshMetadata)
					k.watchdog.markConsumeStarted()

					k.Config.Logger.Debugf("[kafka] Consumer for group %s calling Consume() on topic %s", k.Config.ConsumerGroupID, k.Config.Topic)
					consumeStart := time.Now()

					// Get current consumer group (might be replaced by force recovery or offset reset)
					// Use mutex to ensure we don't read while forceRecovery() is replacing it
					k.consumerMu.Lock()
					currentConsumer := k.ConsumerGroup
					k.consumerMu.Unlock()

					if currentConsumer == nil {
						// Consumer is nil - likely being recreated by error handler after offset error
						// Wait for recovery to create new consumer, then retry
						k.Config.Logger.Debugf("[kafka] Consumer group is nil for topic %s, waiting for recovery to create new consumer...", k.Config.Topic)
						time.Sleep(1 * time.Second)
						continue // Retry with new consumer
					}

					// CRITICAL: Create a NEW context for each Consume() attempt
					// When forceRecovery() closes the consumer, Sarama cancels the context passed to Consume()
					// If we reuse the same context, the next Consume() call will fail immediately
					// We derive from internalCtx so that shutdown still works correctly
					consumeCtx, consumeCancel := context.WithCancel(internalCtx)
					err := currentConsumer.Consume(consumeCtx, topics, NewKafkaConsumer(k.Config, consumerFn, k.watchdog))
					consumeCancel() // Always clean up the context when Consume() returns

					// Consume() returned - mark as no longer attempting
					k.watchdog.markConsumeEnded()
					consumeDuration := time.Since(consumeStart)

					if err != nil {
						k.Config.Logger.Debugf("[kafka] Consumer for group %s Consume() returned after %v", k.Config.ConsumerGroupID, consumeDuration)

						switch {
						case errors.Is(err, sarama.ErrClosedConsumerGroup):
							// Check if context is cancelled - if so, this is a normal shutdown
							select {
							case <-internalCtx.Done():
								k.Config.Logger.Infof("[kafka] Consumer for group %s closed due to context cancellation", k.Config.ConsumerGroupID)
								return
							default:
								// Context still active - this might be force recovery, continue loop to use new consumer
								k.Config.Logger.Infof("[kafka] Consumer for group %s closed but context still active, retrying with new consumer...", k.Config.ConsumerGroupID)
								time.Sleep(1 * time.Second) // Brief pause before retrying
							}
						case errors.Is(err, context.Canceled):
							k.Config.Logger.Infof("[kafka] Consumer for group %s cancelled", k.Config.ConsumerGroupID)
							return
						default:
							// Log error and wait before retrying to prevent tight loop when broker is down
							k.Config.Logger.Errorf("Error from consumer: %v (after %v), retrying in 5s...", err, consumeDuration)
							time.Sleep(5 * time.Second)
						}
					} else {
						// Consume() returned successfully - this is normal (rebalance, coordinator change, etc.)
						// Continue looping to call Consume() again
						k.Config.Logger.Debugf("[kafka] Consumer for group %s Consume() completed successfully after %v", k.Config.ConsumerGroupID, consumeDuration)
					}
				}
			}
		}()

		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

		go func() {
			select {
			case <-signals:
				cancel()
			case <-internalCtx.Done():
				// Context cancelled, exit gracefully without cancelling again
				return
			}
		}()

		select {
		case <-signals:
			k.Config.Logger.Infof("[kafka] Received signal, shutting down consumers for group %s", k.Config.ConsumerGroupID)
			cancel() // Ensure the context is canceled
		case <-internalCtx.Done():
			k.Config.Logger.Infof("[kafka] Context done, shutting down consumer for %s", k.Config.ConsumerGroupID)
		}

		if k.ConsumerGroup != nil {
			if err := k.ConsumerGroup.Close(); err != nil {
				k.Config.Logger.Errorf("[Kafka] %s: error closing client: %v", k.Config.ConsumerGroupID, err)
			}
		}
	}()
}

func (k *KafkaConsumerGroup) BrokersURL() []string {
	return k.Config.BrokersURL
}

// PauseAll suspends fetching from all partitions without triggering a rebalance.
// Heartbeats continue to be sent to the broker, so the consumer remains part of the group.
func (k *KafkaConsumerGroup) PauseAll() {
	if k.ConsumerGroup != nil {
		k.ConsumerGroup.PauseAll()
		k.Config.Logger.Debugf("[Kafka] %s: paused all partitions for topic %s", k.Config.ConsumerGroupID, k.Config.Topic)
	}
}

// ResumeAll resumes all partitions which have been paused.
func (k *KafkaConsumerGroup) ResumeAll() {
	if k.ConsumerGroup != nil {
		k.ConsumerGroup.ResumeAll()
		k.Config.Logger.Debugf("[Kafka] %s: resumed all partitions for topic %s", k.Config.ConsumerGroupID, k.Config.Topic)
	}
}

// KafkaConsumer represents a Sarama consumer group consumer
type KafkaConsumer struct {
	consumerClosure func(*KafkaMessage) error
	cfg             KafkaConsumerConfig
	watchdog        *consumeWatchdog // Monitors for stuck RefreshMetadata and triggers force recovery
}

func NewKafkaConsumer(cfg KafkaConsumerConfig, consumerClosureOrNil func(message *KafkaMessage) error, watchdog *consumeWatchdog) *KafkaConsumer {
	consumer := &KafkaConsumer{
		consumerClosure: consumerClosureOrNil,
		cfg:             cfg,
		watchdog:        watchdog,
	}

	return consumer
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (kc *KafkaConsumer) Setup(sarama.ConsumerGroupSession) error {
	// This is called AFTER RefreshMetadata succeeds and consumer joins group
	if kc.watchdog != nil {
		kc.watchdog.markSetupCalled()
		kc.cfg.Logger.Infof("[kafka] Consumer setup completed for topic %s - successfully joined group after RefreshMetadata", kc.cfg.Topic)
	}
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (kc *KafkaConsumer) Cleanup(session sarama.ConsumerGroupSession) error {
	kc.cfg.Logger.Infof("[kafka-consumer-cleanup][topic:%s] Session ending - committing offsets and releasing partitions. GenerationID: %d, MemberID: %s",
		kc.cfg.Topic, session.GenerationID(), session.MemberID())

	if !kc.cfg.AutoCommitEnabled {
		session.Commit()
	}

	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (kc *KafkaConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	const commitInterval = time.Minute

	messageProcessedSinceLastCommit := false
	messagesProcessed := atomic.Uint64{}

	var mu sync.Mutex // Add mutex to protect messageProcessedSinceLastCommit

	// Start a separate goroutine for commit ticker
	if !kc.cfg.AutoCommitEnabled {
		go func() {
			commitTicker := time.NewTicker(commitInterval)
			defer commitTicker.Stop()

			for {
				select {
				case <-session.Context().Done():
					err := session.Context().Err()
					if err != nil {
						kc.cfg.Logger.Debugf("[kafka_consumer] Context canceled in commit ticker (topic: %s): %v",
							kc.cfg.Topic, err)
					}

					return
				case <-commitTicker.C:
					mu.Lock()
					if messageProcessedSinceLastCommit {
						session.Commit()

						messageProcessedSinceLastCommit = false
					}
					mu.Unlock()
				}
			}
		}()
	}

	// Create a buffered channel for messages to reduce context switching
	// Buffer size is configurable via URL query parameter
	messages := make(chan *sarama.ConsumerMessage, kc.cfg.ChannelBufferSize)

	// Start a separate goroutine to receive messages
	go func() {
		for message := range claim.Messages() {
			select {
			case messages <- message:
			case <-session.Context().Done():
				// Log when context is canceled in the message forwarding goroutine
				err := session.Context().Err()
				if err != nil {
					kc.cfg.Logger.Debugf("[kafka_consumer] Context canceled in message forwarder (topic: %s, partition: %d): %v",
						claim.Topic(), claim.Partition(), err)
				}

				return
			}
		}
	}()

	for {
		select {
		case <-session.Context().Done():
			err := session.Context().Err()
			// Only log detailed information if it's not a normal shutdown
			if err != nil {
				// Get additional context about the consumer state
				partition := claim.Partition()
				topic := claim.Topic()
				highWatermark := claim.HighWaterMarkOffset()

				kc.cfg.Logger.Infof("[kafka_consumer] Context done for consumer (topic: %s, partition: %d, highWatermark: %d): %v. This is normal during shutdown or rebalancing.",
					topic, partition, highWatermark, err)
			}

			return err

		case message := <-messages:
			if message == nil {
				continue
			}

			// Process message
			var err error
			if kc.cfg.AutoCommitEnabled {
				err = kc.handleMessagesWithAutoCommit(message)
			} else {
				err = kc.handleMessageWithManualCommit(session, message)
				if err == nil {
					mu.Lock()
					messageProcessedSinceLastCommit = true
					mu.Unlock()
				}
			}

			if err != nil {
				kc.cfg.Logger.Errorf("[kafka_consumer] failed to process message (topic: %s, partition: %d, offset: %d): %v",
					message.Topic, message.Partition, message.Offset, err)
				return err
			}

			// Increment message counter for heartbeat logging
			messagesProcessed.Add(1)
		}
	}
}

// handleMessageWithManualCommit processes the message and commits the offset only if the processing of the message is successful
func (kc *KafkaConsumer) handleMessageWithManualCommit(session sarama.ConsumerGroupSession, message *sarama.ConsumerMessage) error {
	msg := KafkaMessage{*message}
	// kc.cfg.Logger.Infof("Processing message with offset: %v", message.Offset)

	if err := kc.consumerClosure(&msg); err != nil {
		return err
	}

	// kc.logger.Infof("Committing offset: %v", message.Offset)

	// Update the message offset, processing is successful
	// This doesn't commit the offset to the server, it just marks it as processed in memory on the client
	// The commit is done elsewhere
	session.MarkMessage(message, "")

	return nil
}

func (kc *KafkaConsumer) handleMessagesWithAutoCommit(message *sarama.ConsumerMessage) error {
	return kc.consumerClosure(&KafkaMessage{*message})
}
