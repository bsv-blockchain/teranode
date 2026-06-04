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

// defaultOuterBatcherLinger is the default time the outer async batcher
// waits for stragglers to join the buffer before draining into franz-go.
// It is intentionally short — the outer batcher exists only to amortise
// channel reads, NOT to batch records at the broker level (franz-go's
// per-partition batcher does that). See KafkaProducerConfig.OuterBatcherLinger.
const defaultOuterBatcherLinger = 10 * time.Millisecond

// KafkaProducerConfig holds configuration for the async Kafka producer.
//
// The three URL params named after Sarama's Flush.* triggers no longer map
// cleanly onto a single knob each — set them with intent, not by analogy
// to Sarama:
//
//   - FlushFrequency → kgo.ProducerLinger only. This is franz-go's
//     PER-PARTITION linger: how long franz-go waits for a partition's
//     batch to fill before sending it to the broker. It does NOT drive
//     the outer batcher's drain timer any more — see OuterBatcherLinger.
//   - FlushMessages has TWO effects, both keyed on the same value:
//     (1) kgo.MaxBufferedRecords — global back-pressure cap; once
//     exceeded Produce() blocks; (2) outer-batcher flush-size trigger
//     via currentBatchSize(): when the wrapper's pending buffer reaches
//     this length, it drains into franz-go without waiting for the
//     linger. So it IS a (coarse) flush trigger, just not at the
//     broker level.
//   - FlushBytes → kgo.ProducerBatchMaxBytes (per-partition batch hard
//     cap; clamped to ≥1 MiB). Not a flush trigger.
//
// OuterBatcherLinger is the outer drain goroutine's straggler-flush
// timer. It used to be derived from FlushFrequency, which silently
// stacked a second linger on every record and caused the dev-scale-1/2
// txmeta regression at 1.2M TPS. It is now decoupled and defaults to
// defaultOuterBatcherLinger (10ms); operators should rarely change it.
type KafkaProducerConfig struct {
	Logger                ulogger.Logger // Logger instance
	URL                   *url.URL       // Kafka URL
	BrokersURL            []string       // List of broker URLs
	Topic                 string         // Topic to produce to
	Partitions            int32          // Number of partitions
	ReplicationFactor     int16          // Replication factor for topic
	RetentionPeriodMillis string         // Message retention period
	SegmentBytes          string         // Segment size in bytes
	FlushBytes            int            // → kgo.ProducerBatchMaxBytes (per-partition batch hard cap, clamped ≥1 MiB). See doc above.
	FlushMessages         int            // Dual use: kgo.MaxBufferedRecords AND outer-batcher flush-size trigger via currentBatchSize(). See doc above.
	FlushFrequency        time.Duration  // → kgo.ProducerLinger only (per-partition broker-side linger). Outer batcher now uses OuterBatcherLinger. See doc above.
	OuterBatcherLinger    time.Duration  // Straggler-flush timer for the outer drain goroutine; defaults to defaultOuterBatcherLinger when zero/negative

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

	// ManualPartitioning routes every record to Message.Partition without any
	// hashing or sticky-batching by franz-go. Callers MUST set Partition on
	// every Message they publish — there is no fallback. Used by the validator
	// when emitting v2-format txmeta messages so each Kafka message lands on a
	// partition aligned with the receiver's cache bucket range.
	ManualPartitioning bool
}

// MessageStatus represents the status of a produced message.
type MessageStatus struct {
	Success bool
	Error   error
	Time    time.Time
}

// Message represents a Kafka message with key and value.
//
// Partition is honored only when the producer was created with
// KafkaProducerConfig.ManualPartitioning=true. In that mode the producer is
// registered with kgo.ManualPartitioner, which writes the record to exactly
// the partition number specified — there is no fallback, so callers MUST set
// Partition explicitly for every Message. With ManualPartitioning=false the
// field is ignored (franz-go's default StickyKeyPartitioner picks the
// partition from Key).
type Message struct {
	Key       []byte
	Value     []byte
	Partition int32
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

	// produceHook, when non-nil, replaces the real client.Produce call in the
	// flush path. Test-only seam: lets a test capture what the batching loop
	// emits — including the final drain on Stop — without a live broker. It is
	// nil in production.
	produceHook func(*Message)
}

// ProducerOption mutates the KafkaProducerConfig built from a URL before the
// underlying client is constructed. Use with NewKafkaAsyncProducerFromURL to
// override flags that aren't expressible in the URL (e.g. ManualPartitioning).
type ProducerOption func(*KafkaProducerConfig)

// WithManualPartitioning switches the producer to franz-go's ManualPartitioner.
// Every Message.Partition is honored verbatim; there is no fallback for
// records without an explicit partition. Use when the caller wants strict
// control over partition routing (e.g. the validator emitting v2 txmeta
// messages aligned to receiver-side cache bucket ranges).
func WithManualPartitioning() ProducerOption {
	return func(c *KafkaProducerConfig) { c.ManualPartitioning = true }
}

// NewKafkaAsyncProducerFromURL creates a new async producer from a URL configuration.
func NewKafkaAsyncProducerFromURL(ctx context.Context, logger ulogger.Logger, url *url.URL, kafkaSettings *settings.KafkaSettings, opts ...ProducerOption) (*KafkaAsyncProducer, error) {
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
		OuterBatcherLinger:    util.GetQueryParamDuration(url, "outer_batcher_linger", defaultOuterBatcherLinger),
		EnableTLS:             enableTLS,
		TLSSkipVerify:         tlsSkipVerify,
		TLSCAFile:             tlsCAFile,
		TLSCertFile:           tlsCertFile,
		TLSKeyFile:            tlsKeyFile,
		EnableDebugLogging:    enableDebugLogging,
	}

	for _, opt := range opts {
		opt(&producerConfig)
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

	// Build franz-go client options. The mapping between the URL's flush_*
	// query params and franz-go options is documented on KafkaProducerConfig;
	// read those comments before changing any of these settings.
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.BrokersURL...),
		kgo.DefaultProduceTopic(cfg.Topic),
		kgo.ProducerBatchMaxBytes(batchMaxBytes),
		kgo.ProducerLinger(cfg.FlushFrequency),
		kgo.MaxBufferedRecords(cfg.FlushMessages),
		kgo.DisableIdempotentWrite(),
	}

	if cfg.ManualPartitioning {
		opts = append(opts, kgo.RecordPartitioner(kgo.ManualPartitioner()))
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

// currentBatchLinger returns the straggler-flush timeout for the outer
// drain goroutine — how long a non-empty buffer waits for more records
// before being drained into franz-go. This is decoupled from
// FlushFrequency: that field is the franz-go per-partition linger
// (kgo.ProducerLinger), a separate concern.
//
// Note on the adaptive-slow bounds [50ms, 500ms]: when this function
// was driven by FlushFrequency (default 10s, scale-1/2 setting 1s) the
// bounds were [200ms, 5s] — i.e. 10× larger. They were compressed by
// the same factor when the base switched to OuterBatcherLinger
// (default 10ms). The bounds still serve their original purpose
// (don't drop below "a few base lingers", don't sit above "a small
// multiple of base") at the new scale.
func (c *KafkaAsyncProducer) currentBatchLinger() time.Duration {
	base := c.Config.OuterBatcherLinger
	if base <= 0 {
		base = defaultOuterBatcherLinger
	}
	if c.adaptiveSlow.Load() {
		linger := base * 4
		if linger < 50*time.Millisecond {
			linger = 50 * time.Millisecond
		}
		if linger > 500*time.Millisecond {
			linger = 500 * time.Millisecond
		}
		return linger
	}
	return base
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

// flushBuffered produces the buffered messages. It gates only on `closed` (the
// client is gone), deliberately NOT on `shuttingDown`: Stop sets shuttingDown
// before closing the publish channel but closes the client only after the
// worker goroutine has returned (publishWg.Wait precedes client.Close), so
// producing while shutting-down is always safe — and is exactly what the final
// drain on Stop relies on to avoid silently dropping the last buffered batch.
// An earlier shuttingDown gate here defeated that drain and also lost any batch
// cleared after a size/linger flush during shutdown.
func (c *KafkaAsyncProducer) flushBuffered(internalCtx context.Context, buffered []*Message) {
	for _, msgBytes := range buffered {
		if c.closed.Load() {
			return
		}

		// Test seam: capture instead of producing to a real broker.
		if c.produceHook != nil {
			c.produceHook(msgBytes)
			continue
		}

		record := &kgo.Record{
			Topic: c.Config.Topic,
			Key:   msgBytes.Key,
			Value: msgBytes.Value,
		}
		if c.Config.ManualPartitioning {
			record.Partition = msgBytes.Partition
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

// runProducerWorker is the async producer's batching loop: it accumulates
// messages from ch into a local buffer and flushes them on batch-size or linger
// timeout. On shutdown it relies on Stop closing ch: the close drains any
// channel-resident messages into the buffer and the final drain produces them,
// so a graceful Stop does not silently drop buffered messages. (It deliberately
// does NOT break out of the loop merely because shuttingDown is set, which would
// strand messages still queued in ch.)
func (c *KafkaAsyncProducer) runProducerWorker(internalCtx context.Context, ch chan *Message) {
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
		// A fully-closed producer cannot produce, so just exit. Shutdown-in-
		// progress is intentionally NOT an exit condition here: the worker
		// keeps draining until Stop closes ch (handled by the receive paths
		// below), so messages still queued in ch are not stranded.
		if c.closed.Load() {
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

			c.runProducerWorker(internalCtx, ch)
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
