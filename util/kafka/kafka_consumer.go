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
		// Mark closed before the Close() call and regardless of its outcome, so a
		// late PauseAll/ResumeAll from a caller with its own lifecycle cannot reach
		// a consumer that is being torn down.
		k.markClosed()

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

// markClosed sets the closed flag without touching a franz-go client. It is the
// in-memory arm's counterpart to closeClient, and between them the flag is
// authoritative for both consumer kinds:
//
//	franz-go   closeClient, called by Close and by the consume goroutine when the
//	           internal context ends.
//	in-memory  Close, plus a watcher on the INTERNAL context started by startInMemory,
//	           plus the consume goroutine's own exit.
//
// The in-memory watcher is what covers a parent context cancelled without Close(),
// after which the consumer is inert but no Close ever ran. It cannot be folded into
// the consume goroutine's defer: InMemoryConsumerGroup.Consume does not return on
// context cancellation (see startInMemory). It waits on the internal context rather
// than the caller's so that Close() ends it too — otherwise a consumer started on a
// context that is never cancelled would park it forever.
//
// Idempotent, and called from more than one place on purpose — whichever gets there
// first wins and the rest are no-ops.
func (k *KafkaConsumerGroup) markClosed() {
	k.closeMu.Lock()
	defer k.closeMu.Unlock()

	k.closed = true
}

// isClosed reports whether the consumer has become inert. Note that k.client is
// deliberately NOT nilled on close — that would race the in-flight PollFetches in
// the consume loop — so this flag, not a nil check, is what tells a caller the
// consumer is inert.
//
// Callers that act on the answer must use setFetchPaused rather than this, which
// only reads: releasing closeMu between the check and the act leaves a window in
// which a concurrent Close lands.
func (k *KafkaConsumerGroup) isClosed() bool {
	k.closeMu.Lock()
	defer k.closeMu.Unlock()

	return k.closed
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
	//
	// AutoCommitEnabled=true → AutoCommitMarks: franz-go auto-commits only records that
	// the consume loop has explicitly marked via MarkCommitRecords. Combined with the
	// per-partition consume loop (Start), we mark a record only after consumerFn has
	// returned without error, so a record that caused an error or never reached its
	// handler does not get committed silently by the auto-commit timer.
	//
	// AutoCommitEnabled=false → DisableAutoCommit: callers manage commits via the
	// uncommittedRecords slice + commitTicker in Start.
	if cfg.AutoCommitEnabled {
		opts = append(opts, kgo.AutoCommitMarks())
	} else {
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

	// Create internal context and store cancel func before spawning goroutines.
	// Protected by cancelMu to avoid a data race with Close().
	internalCtx, cancel := context.WithCancel(ctx)
	k.cancelMu.Lock()
	k.cancel = cancel
	k.cancelMu.Unlock()

	// Apply retry/error handling wrappers, bound to internalCtx so both caller
	// cancellation and Close() abort an in-flight retry backoff.
	consumerFn = wrapConsumerFn(internalCtx, k.Config.Logger, k.Config.Topic, consumerFn, options)

	go func() {
		defer cancel()

		// Main consume loop — fire-and-forget per-partition fan-out, no barrier.
		//
		//   [franz-go background fetcher → local record buffer]
		//             ↓
		//   [puller goroutine: tight PollFetches loop]
		//             ↓                ↓                ↓
		//   [partition 0 goroutine] [partition 1 goroutine] [partition N goroutine]
		//             ↓                ↓                ↓
		//                consumerFn (sequential within each goroutine)
		//
		// franz-go's EachPartition is dumb-serial — three nested for-loops
		// calling fn synchronously. To recover the cross-partition parallelism
		// that c2402191f intended, we spawn a goroutine per partition per
		// fetch. The puller does NOT wait for those goroutines (no
		// partitionWg.Wait), so PollFetches is called again immediately and
		// franz-go's local buffer keeps draining.
		//
		// Trade-off: per-partition ordering is preserved within a single
		// fetch's batch, but NOT across fetches — two consecutive fetches for
		// the same partition can dispatch two goroutines that run concurrently.
		// Handlers that depend on strict cross-fetch ordering must enforce it
		// themselves. txmeta entries are independent (and DELETE is sync inside
		// txmetaHandler) so the txmeta hot path is fine.
		//
		// HANDLER CONTRACT — handlers MUST bound their own blocking. Because the
		// puller does not wait, every parked per-partition goroutine keeps holding
		// the []*kgo.Record it was dispatched with while PollFetches keeps
		// returning more. A handler that blocks for an unbounded time therefore
		// reintroduces unbounded goroutine and record retention here, no matter what
		// bound the downstream service enforces on itself: retention becomes
		// proportional to the stall duration rather than to anything this process
		// controls. Nothing in this loop enforces the contract — it is the handler's
		// job (see the validator's bounded block-assembly handoff retry,
		// validator_blockAssemblyShedRetryTimeout, for the shape).
		go func() {
			k.Config.Logger.Debugf("[kafka] starting consumer for group %s on topic %s", k.Config.ConsumerGroupID, k.Config.Topic)

			commitTicker := time.NewTicker(time.Minute)
			defer commitTicker.Stop()

			uncommittedRecords := make([]*kgo.Record, 0)
			var uncommittedMu sync.Mutex

			// partitionWg tracks the per-partition goroutines spawned below.
			// We deliberately do NOT wait on it between fetches (that was the
			// barrier #868bdbb06 removed for steady-state throughput). It is
			// waited on EXACTLY ONCE at shutdown so the final commitRecords
			// captures everything that finished processing — without it, a
			// goroutine mid-consumerFn appends to uncommittedRecords AFTER the
			// final commit has run, leaving its records uncommitted on disk
			// even though they were successfully processed.
			var partitionWg sync.WaitGroup

			// partitionLocks serialises processing within a single partition
			// across consecutive fetches. PollFetches returns immediately and
			// can hand us a second batch from partition P before the goroutine
			// processing the first batch has finished. Without a per-partition
			// lock, batch B could mark/append a later offset while batch A is
			// mid-way; a subsequent commitRecords would then advance the
			// partition past records A had not yet processed, and if A later
			// fails on an earlier offset the broker never re-delivers them.
			// One mutex per partition is allocated lazily on first use; the
			// number of partitions is bounded by the topic config.
			var partitionLocks sync.Map // map[int32]*sync.Mutex

			// shutdownDrain captures everything in flight on every exit path.
			// Must run for context cancellation AND for fetch errors that
			// indicate the client is closing (ErrClientClosed / ctx.Canceled),
			// otherwise records processed after the last ticker commit are
			// lost despite successful processing.
			shutdownDrain := func() {
				partitionWg.Wait()
				uncommittedMu.Lock()
				k.commitRecords(uncommittedRecords)
				uncommittedMu.Unlock()
			}

			for {
				select {
				case <-internalCtx.Done():
					shutdownDrain()
					return
				default:
				}

				fetches := k.client.PollFetches(internalCtx)

				if errs := fetches.Errors(); len(errs) > 0 {
					for _, err := range errs {
						if errors.Is(err.Err, context.Canceled) || errors.Is(err.Err, kgo.ErrClientClosed) {
							k.Config.Logger.Debugf("Kafka consumer shutdown: %v", err.Err)
							shutdownDrain()
							return
						}
						k.Config.Logger.Errorf("Kafka consumer error on topic %s partition %d: %v", err.Topic, err.Partition, err.Err)
					}
					continue
				}

				fetches.EachPartition(func(p kgo.FetchTopicPartition) {
					if len(p.Records) == 0 {
						return
					}

					// Capture HighWatermark in the outer scope: the kgo.FetchTopicPartition
					// value `p` is only valid for the duration of this synchronous callback,
					// and the goroutine below outlives it.
					hwm := p.HighWatermark

					muIface, _ := partitionLocks.LoadOrStore(p.Partition, &sync.Mutex{})
					mu := muIface.(*sync.Mutex)

					partitionWg.Add(1)
					go func(records []*kgo.Record, hwm int64, mu *sync.Mutex) {
						defer partitionWg.Done()
						// Serialise processing within this partition. Different
						// partitions remain parallel; cross-partition ordering is
						// not preserved (and was never claimed).
						mu.Lock()
						defer mu.Unlock()
						for _, record := range records {
							select {
							case <-internalCtx.Done():
								return
							default:
							}

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
								// Don't mark this or any later record in this
								// batch — leave them uncommitted so rebalance/
								// restart re-delivers.
								return
							}

							if k.Config.AutoCommitEnabled {
								// MarkCommitRecords locks internal commit state
								// in franz-go, so concurrent calls from many
								// per-partition goroutines are safe.
								k.client.MarkCommitRecords(record)
							} else {
								uncommittedMu.Lock()
								uncommittedRecords = append(uncommittedRecords, record)
								uncommittedMu.Unlock()
							}
						}
					}(p.Records, hwm, mu)
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

	// Derive an internal context and publish its cancel func to k.cancel, exactly as
	// the franz-go arm does. Two reasons it has to be the internal one and not the
	// caller's:
	//
	//   - Close() cancels whatever is in k.cancel. On the in-memory path that field
	//     used to stay nil, because it is only assigned in the franz-go branch — which
	//     sits AFTER Start's isInMemory early return — so Close() cancelled nothing and
	//     the watcher below could park forever on a context that is never cancelled
	//     (Close() being the only shutdown a caller ever performs).
	//   - It gives the wrapper's shutdown carve-out the same trigger the real consumer
	//     has, so Close() leaves a failing record uncommitted here too.
	internalCtx, cancel := context.WithCancel(ctx)

	k.cancelMu.Lock()
	k.cancel = cancel
	k.cancelMu.Unlock()

	// Reuse the same retry/error wrappers as the real consumer so in-memory
	// (dev/test) semantics match production, including cancellable backoff.
	handler := &inMemoryConsumerHandler{
		consumerFn: wrapConsumerFn(internalCtx, k.Config.Logger, k.Config.Topic, consumerFn, options),
	}

	// Mark the consumer inert when its context ends, mirroring what the franz-go arm
	// already does (the consume goroutine there selects on the internal context and
	// calls closeClient, which sets the same flag). Without this the in-memory arm had
	// no way to reach the flag on a parent context cancelled without Close(), so a
	// caller with its own lifecycle — the validator's backpressure controller — could
	// keep driving pause/resume against a torn-down consumer.
	//
	// It has to be a watcher rather than a defer on the consume goroutine below,
	// because InMemoryConsumerGroup.Consume does NOT return on context cancellation:
	// its handler blocks in `range claim.Messages()`, that channel is fed from the
	// broker's per-consumer channel, and that channel is only closed by Consume's own
	// deferred cleanup — which cannot run until the handler returns.
	//
	// So cancelling the context does not by itself end that goroutine, and neither
	// does Close(), which cancels and nothing more. The goroutine ends on the NEXT
	// message: the wrapper returns its shutdown error for that record, ConsumeClaim
	// returns it, and the loop breaks. Until a message arrives the goroutine stays
	// parked — so after Close() the consumer already reports closed here while that
	// goroutine may still process one more message. That is pre-existing in-memory
	// (test-only) behaviour, not something this flag changes; the watcher exists so
	// the closed flag is reached promptly regardless of when the next message lands.
	//
	// The defer below therefore only covers a Consume that returns for some other
	// reason, such as a setup error, and this watcher covers the cancellation case,
	// which is the common one.
	//
	// Because it waits on internalCtx, BOTH shutdown routes end it: a cancelled parent
	// and Close().
	go func() {
		<-internalCtx.Done()
		k.markClosed()
	}()

	go func() {
		// Releases internalCtx (and therefore the watcher) if Consume ever does
		// return — a setup error, say — so neither outlives the consumer.
		defer cancel()
		defer k.markClosed()

		err := k.inMemoryConsumer.Consume(internalCtx, []string{k.Config.Topic}, handler)
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

// setFetchPaused pauses or resumes fetching for all partitions, and is a no-op once
// the consumer is closed.
//
// A closed consumer is inert: closeClient deliberately does not nil k.client (that
// would race the in-flight PollFetches in the consume loop), so a nil check alone
// would let a pause land on a closed franz-go client. Callers with their own
// lifecycle — the validator's backpressure controller is one — should be bound to
// the consumer's context; this guard is the backstop for when they are not.
//
// The check and the act share ONE critical section. Reading the flag and then acting
// outside the lock would leave a window in which Close lands between them, which is
// the whole scenario the guard exists for. Holding closeMu across the pause is safe:
// closeClient already holds it across client.Close(), and neither
// PauseFetchTopics/ResumeFetchTopics nor the in-memory group's pause/resume (which
// takes only its own lock) calls back into KafkaConsumerGroup.
func (k *KafkaConsumerGroup) setFetchPaused(paused bool) {
	k.closeMu.Lock()
	defer k.closeMu.Unlock()

	action := "ResumeAll"
	if paused {
		action = "PauseAll"
	}

	if k.closed {
		k.Config.Logger.Debugf("[Kafka] %s: ignoring %s on a closed consumer for topic %s", k.Config.ConsumerGroupID, action, k.Config.Topic)
		return
	}

	if k.isInMemory {
		if paused {
			k.inMemoryConsumer.PauseAll()
		} else {
			k.inMemoryConsumer.ResumeAll()
		}

		return
	}

	if k.client == nil {
		return
	}

	if paused {
		k.client.PauseFetchTopics(k.Config.Topic)
		k.Config.Logger.Debugf("[Kafka] %s: paused all partitions for topic %s", k.Config.ConsumerGroupID, k.Config.Topic)

		return
	}

	k.client.ResumeFetchTopics(k.Config.Topic)
	k.Config.Logger.Debugf("[Kafka] %s: resumed all partitions for topic %s", k.Config.ConsumerGroupID, k.Config.Topic)
}

// PauseAll suspends fetching from all partitions. No-op on a closed consumer.
func (k *KafkaConsumerGroup) PauseAll() {
	k.setFetchPaused(true)
}

// ResumeAll resumes all partitions which have been paused. Like PauseAll it is a
// no-op once the consumer is closed.
func (k *KafkaConsumerGroup) ResumeAll() {
	k.setFetchPaused(false)
}

// messageKey returns the message key as a string for logging, empty when absent.
func messageKey(msg *KafkaMessage) string {
	if msg == nil || msg.Key == nil {
		return ""
	}

	return string(msg.Key)
}

// retryWithBackoff runs fn once plus up to maxRetries retries, sleeping with
// linear backoff between attempts (never after the last one). It reports
// cancelled=true when ctx was cancelled mid-backoff, in which case the last
// error is returned without further attempts.
func retryWithBackoff(ctx context.Context, logger ulogger.Logger, options *consumerOptions, msg *KafkaMessage, fn func(message *KafkaMessage) error) (cancelled bool, err error) {
	attempts := max(options.maxRetries, 0) + 1

	for i := range attempts {
		if err = fn(msg); err == nil {
			return false, nil
		}

		if i == attempts-1 {
			break
		}

		backoff := time.Duration(options.backoffMultiplier*(i+1)) * options.backoffDurationType
		logger.Warnf("[kafka_consumer] retrying processing kafka message... attempt %d/%d, backoff %v", i+1, attempts, backoff)

		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return true, err
		}
	}

	return false, err
}

// wrapConsumerFn applies retry/error handling wrappers to consumer function
func wrapConsumerFn(ctx context.Context, logger ulogger.Logger, topic string, consumerFn func(message *KafkaMessage) error, options *consumerOptions) func(message *KafkaMessage) error {
	if options.withRetryAndMoveOn {
		originalFn := consumerFn
		consumerFn = func(msg *KafkaMessage) error {
			cancelled, err := retryWithBackoff(ctx, logger, options, msg, originalFn)
			if err == nil {
				return nil
			}

			if cancelled {
				// Shutdown mid-backoff: return the error without committing so
				// the message is redelivered after restart.
				return err
			}

			logger.Errorf("[kafka_consumer] error processing kafka message on topic %s (key: %s), skipping", topic, messageKey(msg))
			return nil
		}
	}

	if options.withRetryAndStop {
		originalFn := consumerFn
		consumerFn = func(msg *KafkaMessage) error {
			cancelled, err := retryWithBackoff(ctx, logger, options, msg, originalFn)
			if err == nil {
				return nil
			}

			if cancelled {
				// Shutdown mid-backoff: return the error without committing so
				// the message is redelivered after restart.
				return err
			}

			logger.Errorf("[kafka_consumer] error processing kafka message on topic %s (key: %s), stopping", topic, messageKey(msg))
			if options.stopFn != nil {
				options.stopFn()
			}
			return nil
		}
	}

	if options.withLogErrorAndMoveOn {
		originalFn := consumerFn
		consumerFn = func(msg *KafkaMessage) error {
			if err := originalFn(msg); err != nil {
				// Shutdown carve-out, matching the one both retry wrappers above
				// already carry: return the error so the record is NOT committed and
				// is redelivered after a restart. Without it, a handler that failed
				// only because it was cancelled mid-processing had its offset
				// committed by the shutdown drain, permanently losing a record whose
				// side effects were half-applied (e.g. a transaction spent and created
				// in the UTXO store but never handed to block assembly).
				//
				// The condition is ctx.Err() and NOTHING else — specifically not "the
				// handler's error chain contains a context error". A non-nil return
				// here is costly on a RUNNING consumer: the per-partition goroutine
				// abandons every remaining record in the fetch, and because
				// MarkCommitRecords commits the highest marked offset per partition, a
				// later successful batch commits past the abandoned ones — never
				// processed, never redelivered. It also terminates the in-memory
				// consumer, which has no restart. So a per-request deadline expiring
				// inside a handler on a healthy consumer must take the ordinary
				// skip-and-move-on path; the transaction it half-applied is recovered
				// by the unmined reload, exactly as before this carve-out existed.
				//
				// Restricting it to ctx.Err() confines the cost to shutdown, where
				// abandoning the rest of the batch is harmless because those offsets
				// are not committed anyway.
				//
				// Scope, deliberately narrow: "uncommitted" is a guarantee against the
				// shutdown drain, not at-least-once redelivery in a running consumer —
				// MarkCommitRecords commits the highest marked offset per partition, so
				// a later successful record still advances past an earlier failed one.
				if ctx.Err() != nil {
					logger.Errorf("[kafka_consumer] error processing kafka message on topic %s (key: %s) while cancelled, leaving it uncommitted for redelivery: %v", topic, messageKey(msg), err)

					return err
				}

				logger.Errorf("[kafka_consumer] error processing kafka message on topic %s (key: %s), skipping: %v", topic, messageKey(msg), err)
			}
			return nil
		}
	}

	return consumerFn
}

// inMemoryConsumerHandler implements the handler for in-memory consumer
type inMemoryConsumerHandler struct {
	consumerFn func(message *KafkaMessage) error
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

		// consumerFn is pre-wrapped by wrapConsumerFn, so the retry/skip/stop
		// option semantics here are identical to the real consumer's.
		if err := h.consumerFn(kafkaMsg); err != nil {
			return err
		}

		session.MarkMessage(message, "")
	}
	return nil
}
