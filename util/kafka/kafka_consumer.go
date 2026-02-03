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
	MaxProcessingTime time.Duration // Max time to process a message (default: 100ms)
	SessionTimeout    time.Duration // Time broker waits for heartbeat before considering consumer dead (default: 10s)
	HeartbeatInterval time.Duration // Frequency of heartbeats to broker (default: 3s)
	RebalanceTimeout  time.Duration // Max time for all consumers to join rebalance (default: 60s)
	ChannelBufferSize int           // Number of messages buffered in internal channels (default: 256)
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
	EnableDebugLogging bool // Enable verbose debug logging
}

// consumeWatchdog monitors Consume() state to detect when stuck and triggers force recovery.
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

// KafkaConsumerGroup implements KafkaConsumerGroupI interface using franz-go.
type KafkaConsumerGroup struct {
	Config     KafkaConsumerConfig
	client     *kgo.Client
	cancel     atomic.Value
	watchdog   *consumeWatchdog
	clientMu   sync.Mutex
	clientOpts []kgo.Opt

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
	channelBufferSize := util.GetQueryParamInt(url, "channelBufferSize", 256)
	consumerTimeoutMs := util.GetQueryParamInt(url, "consumerTimeout", 90000)

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
		ChannelBufferSize:  channelBufferSize,
		ConsumerTimeout:    time.Duration(consumerTimeoutMs) * time.Millisecond,
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

	if k.cancel.Load() != nil {
		k.Config.Logger.Debugf("[Kafka] %s: canceling context for topic %s", k.Config.ConsumerGroupID, k.Config.Topic)
		k.cancel.Load().(context.CancelFunc)()
	}

	if k.isInMemory {
		if k.inMemoryConsumer != nil {
			if err := k.inMemoryConsumer.Close(); err != nil {
				k.Config.Logger.Errorf("[Kafka] %s: error closing in-memory consumer for topic %s: %v", k.Config.ConsumerGroupID, k.Config.Topic, err)
				return err
			}
		}
	} else {
		if k.client != nil {
			k.client.Close()
			k.Config.Logger.Infof("[Kafka] %s: successfully closed consumer group for topic %s", k.Config.ConsumerGroupID, k.Config.Topic)
		}
	}

	return nil
}

// forceRecovery forces recovery of a stuck consumer by closing and recreating the client.
func (k *KafkaConsumerGroup) forceRecovery() error {
	k.clientMu.Lock()
	defer k.clientMu.Unlock()

	k.Config.Logger.Warnf("[kafka-watchdog] Forcing recovery for topic %s - closing stuck consumer and creating new one", k.Config.Topic)

	if k.client != nil {
		k.client.Close()
	}

	newClient, err := kgo.NewClient(k.clientOpts...)
	if err != nil {
		return errors.NewServiceError("failed to recreate consumer client for %s", k.Config.Topic, err)
	}

	k.client = newClient
	k.Config.Logger.Infof("[kafka-watchdog] Successfully recreated consumer group for topic %s", k.Config.Topic)

	return nil
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

	InitPrometheusMetrics()

	// Handle in-memory case
	if cfg.URL.Scheme == memoryScheme {
		broker := inmemorykafka.GetSharedBroker()
		consumerGroup := inmemorykafka.NewInMemoryConsumerGroup(broker, cfg.Topic, cfg.ConsumerGroupID)
		cfg.Logger.Infof("Using in-memory Kafka consumer group")

		return &KafkaConsumerGroup{
			Config:           cfg,
			inMemoryConsumer: consumerGroup,
			isInMemory:       true,
			watchdog:         &consumeWatchdog{},
		}, nil
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
		Config:     cfg,
		client:     client,
		watchdog:   &consumeWatchdog{},
		clientOpts: opts,
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

	go func() {
		internalCtx, cancel := context.WithCancel(ctx)
		k.cancel.Store(cancel)
		defer cancel()

		// Watchdog goroutine
		const watchdogCheckInterval = 30 * time.Second
		watchdogStuckThreshold := k.Config.ConsumerTimeout
		if watchdogStuckThreshold == 0 {
			watchdogStuckThreshold = 90 * time.Second
		}

		go k.runWatchdog(internalCtx, watchdogCheckInterval, watchdogStuckThreshold)

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
					k.watchdog.markConsumeStarted()

					k.clientMu.Lock()
					currentClient := k.client
					k.clientMu.Unlock()

					if currentClient == nil {
						time.Sleep(100 * time.Millisecond)
						continue
					}

					fetches := currentClient.PollFetches(internalCtx)
					k.watchdog.markSetupCalled()

					if fetches.IsClientClosed() {
						return
					}

					if errs := fetches.Errors(); len(errs) > 0 {
						for _, err := range errs {
							if errors.Is(err.Err, context.Canceled) {
								k.Config.Logger.Debugf("Kafka consumer shutdown: %v", err.Err)
								return
							}
							k.Config.Logger.Errorf("Kafka consumer error on topic %s partition %d: %v", err.Topic, err.Partition, err.Err)
						}
						k.watchdog.markConsumeEnded()
						continue
					}

					fetches.EachRecord(func(record *kgo.Record) {
						kafkaMsg := &KafkaMessage{
							Key:       record.Key,
							Value:     record.Value,
							Topic:     record.Topic,
							Partition: record.Partition,
							Offset:    record.Offset,
							Timestamp: record.Timestamp,
						}

						if err := consumerFn(kafkaMsg); err != nil {
							k.Config.Logger.Errorf("[kafka_consumer] failed to process message (topic: %s, partition: %d, offset: %d): %v",
								record.Topic, record.Partition, record.Offset, err)
							return
						}

						if !k.Config.AutoCommitEnabled {
							uncommittedMu.Lock()
							uncommittedRecords = append(uncommittedRecords, record)
							uncommittedMu.Unlock()
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

					k.watchdog.markConsumeEnded()
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

		if k.client != nil {
			k.client.Close()
		}
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

// runWatchdog monitors for stuck consumers
func (k *KafkaConsumerGroup) runWatchdog(ctx context.Context, checkInterval, threshold time.Duration) {
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stuck, duration := k.watchdog.isStuckInRefreshMetadata(threshold)
			if stuck {
				k.Config.Logger.Errorf("[kafka-consumer-watchdog][topic:%s][group:%s] Consumer stuck for %v. Forcing recovery...",
					k.Config.Topic, k.Config.ConsumerGroupID, duration)

				prometheusKafkaWatchdogRecoveryAttempts.WithLabelValues(k.Config.Topic, k.Config.ConsumerGroupID).Inc()
				prometheusKafkaWatchdogStuckDuration.WithLabelValues(k.Config.Topic).Observe(duration.Seconds())

				if err := k.forceRecovery(); err != nil {
					k.Config.Logger.Errorf("[kafka-consumer-watchdog][topic:%s] Force recovery failed: %v", k.Config.Topic, err)
				} else {
					k.Config.Logger.Infof("[kafka-consumer-watchdog][topic:%s] Force recovery successful", k.Config.Topic)
					k.watchdog.markConsumeEnded()
				}
			}
		}
	}
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
func (k *KafkaConsumerGroup) BrokersURL() []string {
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
			Key:       message.Key,
			Value:     message.Value,
			Topic:     message.Topic,
			Partition: message.Partition,
			Offset:    message.Offset,
			Timestamp: message.Timestamp,
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
