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
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/retry"
	"github.com/ordishs/go-utils"
)

type KafkaMessage struct {
	sarama.ConsumerMessage
}

type KafkaConsumerGroupI interface {
	Start(ctx context.Context, consumerFn func(message *KafkaMessage) error, opts ...ConsumerOption)
	BrokersURL() []string
	Close() error
}

type KafkaConsumerConfig struct {
	Logger            ulogger.Logger
	URL               *url.URL
	BrokersURL        []string
	Topic             string
	Partitions        int
	ConsumerRatio     int
	ConsumerGroupID   string
	ConsumerCount     int
	AutoCommitEnabled bool
	Replay            bool
}

type KafkaConsumerGroup struct {
	Config        KafkaConsumerConfig
	ConsumerGroup sarama.ConsumerGroup
	cancel        context.CancelFunc
}

func NewKafkaConsumerGroupFromURL(logger ulogger.Logger, url *url.URL, consumerGroupID string, autoCommit bool) (*KafkaConsumerGroup, error) {
	if url == nil {
		return nil, errors.NewConfigurationError("missing kafka url")
	}

	partitions := util.GetQueryParamInt(url, "partitions", 1)
	consumerRatio := util.GetQueryParamInt(url, "consumer_ratio", 1)

	if consumerRatio < 1 {
		logger.Warnf("consumer_ratio is less than 1, setting it to 1")

		consumerRatio = 1
	}

	consumerCount := partitions / consumerRatio
	if consumerCount < 1 {
		logger.Warnf("consumer count is less than 0, setting it to 1")

		consumerCount = 1
	}

	// Generate a unique group ID for the txmeta Kafka listener, to ensure that each instance of this service will process all txmeta messages.
	// This is necessary because the txmeta messages are used to populate the txmeta cache, which is shared across all instances of this service.
	// groupID := topic + "-" + uuid.New().String()

	// AutoCommitEnabled:
	// txMetaCache : true, we CAN miss.
	// rejected txs : true, we CAN miss.
	// subtree validation : false.
	// block persister : false.
	// block validation: false.

	consumerConfig := KafkaConsumerConfig{
		Logger:            logger,
		URL:               url,
		BrokersURL:        strings.Split(url.Host, ","),
		Topic:             strings.TrimPrefix(url.Path, "/"),
		Partitions:        partitions,
		ConsumerRatio:     consumerRatio,
		ConsumerGroupID:   consumerGroupID,
		ConsumerCount:     consumerCount,
		AutoCommitEnabled: autoCommit,
		// default is start from beginning
		// do not ignore everything that is already queued, this is the case where we start a new consumer group for the first time
		// maybe it shouldn't be called replay because it suggests that the consume will always replay messages from the beginning
		Replay: util.GetQueryParamInt(url, "replay", 1) == 1,
	}

	return NewKafkaConsumerGroup(consumerConfig)
}

// Close gracefully shuts down the Kafka consumer group
func (k *KafkaConsumerGroup) Close() error {
	if k.ConsumerGroup != nil {
		if err := k.ConsumerGroup.Close(); err != nil {
			k.Config.Logger.Errorf("[Kafka] %s: error closing consumer group: %v", k.Config.ConsumerGroupID, err)
			return err
		}
	}

	// cancel the context
	if k.cancel != nil {
		k.cancel()
	}

	return nil
}

// NewKafkaConsumerGroup creates a new Kafka consumer group
// We DO NOT read autocommit parameter from the URL because the handler func has specific error handling logic.
func NewKafkaConsumerGroup(cfg KafkaConsumerConfig) (*KafkaConsumerGroup, error) {
	if cfg.URL == nil {
		return nil, errors.NewConfigurationError("kafka URL is not set", nil)
	}

	if cfg.ConsumerCount <= 0 {
		return nil, errors.NewConfigurationError("consumer count must be greater than 0", nil)
	}

	if cfg.Logger == nil {
		return nil, errors.NewConfigurationError("logger is not set", nil)
	}

	if cfg.ConsumerGroupID == "" {
		return nil, errors.NewConfigurationError("group ID is not set", nil)
	}

	cfg.Logger.Infof("Starting %d Kafka consumer(s) for %s topic", cfg.ConsumerCount, cfg.Topic)

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// https://github.com/IBM/sarama/issues/1689
	// https://github.com/IBM/sarama/pull/1699
	// Default value for config.Consumer.Offsets.AutoCommit.Enable is true.
	if !cfg.AutoCommitEnabled {
		config.Consumer.Offsets.AutoCommit.Enable = false
	}

	if cfg.Replay {
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

	consumerGroup, err := sarama.NewConsumerGroup(cfg.BrokersURL, cfg.ConsumerGroupID, config)
	if err != nil {
		return nil, errors.NewServiceError("failed to create Kafka consumer group for %s", cfg.Topic, err)
	}

	client := &KafkaConsumerGroup{
		Config:        cfg,
		ConsumerGroup: consumerGroup,
	}

	return client, nil
}

// ConsumerOption represents an option for configuring the consumer behavior
type ConsumerOption func(*consumerOptions)

type consumerOptions struct {
	withRetryAndMoveOn  bool
	withRetryAndStop    bool
	maxRetries          int
	backoffMultiplier   int
	backoffDurationType time.Duration
	stopFn              func()
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

func (k *KafkaConsumerGroup) Start(ctx context.Context, consumerFn func(message *KafkaMessage) error, opts ...ConsumerOption) {
	if k == nil {
		return
	}

	options := &consumerOptions{
		withRetryAndMoveOn:  false,
		withRetryAndStop:    false,
		maxRetries:          3,
		backoffMultiplier:   2,
		backoffDurationType: time.Second,
	}
	for _, opt := range opts {
		opt(options)
	}

	if options.withRetryAndMoveOn {
		originalFn := consumerFn
		consumerFn = func(msg *KafkaMessage) error {
			_, err := retry.Retry(ctx, k.Config.Logger, func() (any, error) { // nolint:errcheck
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
					key = utils.ReverseAndHexEncodeSlice(msg.Key)
				}

				k.Config.Logger.Errorf("[kafka_consumer] error processing kafka message on topic %s (key: %s), skipping", k.Config.Topic, key)
			}

			return nil // give up and move on
		}
	}

	if options.withRetryAndStop {
		originalFn := consumerFn
		consumerFn = func(msg *KafkaMessage) error {
			_, err := retry.Retry(ctx, k.Config.Logger, func() (any, error) { // nolint:errcheck
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
						key = utils.ReverseAndHexEncodeSlice(msg.Key)
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

	wg := sync.WaitGroup{}
	wg.Add(k.Config.ConsumerCount)

	go func() {
		internalCtx, cancel := context.WithCancel(ctx)
		k.cancel = cancel
		defer cancel()

		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case err := <-k.ConsumerGroup.Errors():
					if err != nil {
						k.Config.Logger.Errorf("Kafka consumer error: %v", err)
					}
				}
			}
		}()

		topics := []string{k.Config.Topic}

		for i := 0; i < k.Config.ConsumerCount; i++ {
			go func(consumerIndex int) {
				// defer consumer.Close() // Ensure cleanup, if necessary
				k.Config.Logger.Debugf("[kafka] starting consumer [%d] for group %s on topic %s", consumerIndex, k.Config.ConsumerGroupID, topics[0])
				wg.Done()

				for {
					select {
					case <-internalCtx.Done():
						// Context cancelled, exit goroutine
						return
					default:
						if err := k.ConsumerGroup.Consume(internalCtx, topics, NewKafkaConsumer(k.Config, consumerFn)); err != nil {
							if errors.Is(err, sarama.ErrClosedConsumerGroup) { // nolint:gocritic
								k.Config.Logger.Infof("[kafka] Consumer [%d] for group %s closed", consumerIndex, k.Config.ConsumerGroupID)
								return
							} else if errors.Is(err, context.Canceled) {
								k.Config.Logger.Infof("[kafka] Consumer [%d] for group %s cancelled", consumerIndex, k.Config.ConsumerGroupID)
							} else {
								// Consider delay before retry or exit based on error type
								k.Config.Logger.Errorf("Error from consumer [%d]: %v", consumerIndex, err)
							}
						}
					}
				}
			}(i)
		}

		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

		go func() {
			<-signals
			cancel()
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

	wg.Wait()
}

func (k *KafkaConsumerGroup) BrokersURL() []string {
	return k.Config.BrokersURL
}

// KafkaConsumer represents a Sarama consumer group consumer
type KafkaConsumer struct {
	consumerClosure func(*KafkaMessage) error
	cfg             KafkaConsumerConfig
}

func NewKafkaConsumer(cfg KafkaConsumerConfig, consumerClosureOrNil func(message *KafkaMessage) error) *KafkaConsumer {
	consumer := &KafkaConsumer{
		consumerClosure: consumerClosureOrNil,
		cfg:             cfg,
	}

	return consumer
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (kc *KafkaConsumer) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (kc *KafkaConsumer) Cleanup(session sarama.ConsumerGroupSession) error {
	if !kc.cfg.AutoCommitEnabled {
		session.Commit()
	}

	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (kc *KafkaConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	const (
		batchSize      = 1000
		commitInterval = time.Minute
	)

	messageProcessedSinceLastCommit := false

	var mu sync.Mutex // Add mutex to protect messageProcessedSinceLastCommit

	// Start a separate goroutine for commit ticker
	if !kc.cfg.AutoCommitEnabled {
		go func() {
			commitTicker := time.NewTicker(commitInterval)
			defer commitTicker.Stop()

			for {
				select {
				case <-session.Context().Done():
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
	messages := make(chan *sarama.ConsumerMessage, batchSize)

	// Start a separate goroutine to receive messages
	go func() {
		for message := range claim.Messages() {
			select {
			case messages <- message:
			case <-session.Context().Done():
				return
			}
		}
	}()

	for {
		select {
		case <-session.Context().Done():
			return session.Context().Err()

		case message := <-messages:
			if message == nil {
				continue
			}

			// Process batch of messages
			processed := 0
			for ; processed < batchSize; processed++ {
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

				// Try to get next message without blocking
				select {
				case message = <-messages:
					if message == nil {
						break
					}
				default:
					// No more messages immediately available
					goto BatchComplete
				}
			}
		BatchComplete:
		} // nolint:wsl
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
