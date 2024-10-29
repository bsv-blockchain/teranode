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
	"github.com/bitcoin-sv/ubsv/util/retry"
)

type KafkaConsumerGroupI interface {
	Start(ctx context.Context, consumerFn func(message KafkaMessage) error)
	BrokersURL() []string
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
	Config            KafkaConsumerConfig
	ConsumerGroup     sarama.ConsumerGroup
	LastMessageStatus MessageStatus
	mu                sync.Mutex
}

func NewKafkaConsumerGroupFromURL(ctx context.Context, logger ulogger.Logger, url *url.URL, consumerGroupID string, autoCommit bool) (*KafkaConsumerGroup, error) {
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
	if consumerCount < 0 {
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
		Topic:             url.Path[1:],
		Partitions:        partitions,
		ConsumerRatio:     consumerRatio,
		ConsumerGroupID:   consumerGroupID,
		ConsumerCount:     consumerCount,
		AutoCommitEnabled: autoCommit,
		Replay:            util.GetQueryParamInt(url, "replay", 0) == 1,
	}

	return NewKafkaConsumeGroup(ctx, consumerConfig)
}

// We DO NOT read autocommit parameter from the URL because the handler func has specific error handling logic.
func NewKafkaConsumeGroup(ctx context.Context, cfg KafkaConsumerConfig) (*KafkaConsumerGroup, error) {
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

	cfg.Logger.Infof("Starting %d Kafka consumers for %s messages", cfg.ConsumerCount, cfg.Topic)

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// https://github.com/IBM/sarama/issues/1689
	// https://github.com/IBM/sarama/pull/1699
	// Default value for config.Consumer.Offsets.AutoCommit.Enable is true.
	if !cfg.AutoCommitEnabled {
		config.Consumer.Offsets.AutoCommit.Enable = false
	}

	if cfg.Replay {
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
		return nil, errors.NewServiceError("failed to create Kafka consumer group for %s: %v", cfg.Topic, err)
	}

	client := &KafkaConsumerGroup{
		Config:        cfg,
		ConsumerGroup: consumerGroup,
		LastMessageStatus: MessageStatus{
			Success: true,
			Time:    time.Now(),
			Error:   nil,
		},
	}

	return client, nil
}

func (k *KafkaConsumerGroup) Start(ctx context.Context, consumerFn func(message KafkaMessage) error) {
	if k == nil {
		return
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		for err := range k.ConsumerGroup.Errors() {
			k.Config.Logger.Errorf("Kafka consumer error: %v", err)
			k.mu.Lock()
			k.LastMessageStatus = MessageStatus{
				Success: false,
				Error:   err,
				Time:    time.Now(),
			}
			k.mu.Unlock()
		}
	}()

	consumerFunc := func(message KafkaMessage) error {
		k.mu.Lock()
		k.LastMessageStatus = MessageStatus{
			Success: true,
			Error:   nil,
			Time:    time.Now(),
		}
		k.mu.Unlock()

		return consumerFn(message)
	}

	topics := []string{k.Config.Topic}

	for i := 0; i < k.Config.ConsumerCount; i++ {
		go func(consumerIndex int) {
			// defer consumer.Close() // Ensure cleanup, if necessary
			k.Config.Logger.Infof("[kafka] Starting consumer [%d] for group %s on topic %s \n", consumerIndex, k.Config.ConsumerGroupID, topics[0])

			for {
				select {
				case <-ctx.Done():
					// Context cancelled, exit goroutine
					return
				default:
					if err := k.ConsumerGroup.Consume(ctx, topics, NewKafkaConsumer(k.Config.AutoCommitEnabled, consumerFunc)); err != nil {
						if errors.Is(err, context.Canceled) {
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
	case <-ctx.Done():
		k.Config.Logger.Infof("[kafka] Context done, shutting down consumer for %s", k.Config.ConsumerGroupID)
	}

	if err := k.ConsumerGroup.Close(); err != nil {
		k.Config.Logger.Errorf("[Kafka] %s: error closing client: %v", k.Config.ConsumerGroupID, err)
	}
}

func (k *KafkaConsumerGroup) BrokersURL() []string {
	return k.Config.BrokersURL
}

// KafkaConsumer represents a Sarama consumer group consumer
type KafkaConsumer struct {
	consumerClosure   func(KafkaMessage) error
	autoCommitEnabled bool
	logger            ulogger.Logger
}

func NewKafkaConsumer(autoCommitEnabled bool, consumerClosureOrNil func(message KafkaMessage) error) *KafkaConsumer {
	consumer := &KafkaConsumer{
		consumerClosure:   consumerClosureOrNil,
		autoCommitEnabled: autoCommitEnabled,
		logger:            ulogger.New("kafka_consumer"),
	}

	return consumer
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (kc *KafkaConsumer) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (kc *KafkaConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (kc *KafkaConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/main/consumer_group.go#L27-L29
	ctx := context.Background()

	for {
		select {
		case message := <-claim.Messages():
			if message == nil {
				// Received a nil message, skip
				continue
			}
			// Handle the first message
			// fmt.Printf("Handling first message: topic = %s, partition = %d, offset = %d, key = %s, value = %s\n", message.Topic, message.Partition, message.Offset, string(message.Key), string(message.Value))
			if kc.autoCommitEnabled {
				_ = kc.handleMessagesWithAutoCommit(session, message)
			} else {
				_ = kc.handleMessageWithManualCommit(ctx, session, message)
			}

			messageCount := 1 // Start with 1 message already received.
			// Handle further messages up to a maximum of 1000.
		InnerLoop:
			for messageCount < 1000 {
				select {
				case message := <-claim.Messages():
					if message == nil {
						// Received a nil message, skip
						// TODO: Should we break here? Maybe get rid of break here.
						// Context: If we don't break, in the tests we keep getting nil messages.
						break InnerLoop
					}

					if kc.autoCommitEnabled {
						// No need to check the error here as we are auto committing
						_ = kc.handleMessagesWithAutoCommit(session, message)
					} else {
						_ = kc.handleMessageWithManualCommit(ctx, session, message)
					}
					messageCount++
				default:
					// No more messages, break the inner loop.
					break InnerLoop
				}
			}

		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/Shopify/sarama/issues/1192
		case <-session.Context().Done():
			return session.Context().Err()
		}
	}
}

// handleMessageWithManualCommit processes the message and commits the offset only if the processing of the message is successful
func (kc *KafkaConsumer) handleMessageWithManualCommit(ctx context.Context, session sarama.ConsumerGroupSession, message *sarama.ConsumerMessage) error {
	msg := KafkaMessage{Message: message, Session: session}
	// kc.logger.Infof("Processing message with offset: %v", message.Offset)

	// execute consumer closure
	_, err := retry.Retry(ctx, kc.logger, func() (any, error) {
		return struct{}{}, kc.consumerClosure(msg)
	}, retry.WithRetryCount(3), retry.WithBackoffMultiplier(2),
		retry.WithBackoffDurationType(time.Second), retry.WithMessage("[kafka_consumer] retrying to process message..."))

	// if we can't process the message, log the error and skip to the next message
	if err != nil {
		kc.logger.Errorf("[kafka_consumer] error processing kafka message, skipping: %v", message)
	}

	// kc.logger.Infof("Committing offset: %v", message.Offset)
	// Commit the message offset, processing is successful
	session.MarkMessage(message, "")

	return nil
}

func (kc *KafkaConsumer) handleMessagesWithAutoCommit(session sarama.ConsumerGroupSession, message *sarama.ConsumerMessage) error {
	msg := KafkaMessage{Message: message, Session: session}

	// we don't check the error here as we are auto committing
	_ = kc.consumerClosure(msg)

	// Auto-commit is implied, so we don't need to explicitly mark the message here
	return nil
}
