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
	Logger                            ulogger.Logger
	URL                               *url.URL
	BrokersURL                        []string
	Topic                             string
	Partitions                        int
	ConsumerRatio                     int
	ConsumerGroupID                   string
	ConsumerCount                     int
	AutoCommitEnabled                 bool
	Replay                            bool
	MessageErrorMaxRetries            int
	MessageErrorBackoffMultiplier     int
	MessageErrorBackoffMultiplierType time.Duration
}

type KafkaConsumerGroup struct {
	Config        KafkaConsumerConfig
	ConsumerGroup sarama.ConsumerGroup
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
		Logger:                            logger,
		URL:                               url,
		BrokersURL:                        strings.Split(url.Host, ","),
		Topic:                             strings.TrimPrefix(url.Path, "/"),
		Partitions:                        partitions,
		ConsumerRatio:                     consumerRatio,
		ConsumerGroupID:                   consumerGroupID,
		ConsumerCount:                     consumerCount,
		AutoCommitEnabled:                 autoCommit,
		Replay:                            util.GetQueryParamInt(url, "replay", 0) == 1,
		MessageErrorMaxRetries:            util.GetQueryParamInt(url, "message_error_max_retries", 3),
		MessageErrorBackoffMultiplier:     util.GetQueryParamInt(url, "message_error_backoff_multiplier", 2),
		MessageErrorBackoffMultiplierType: util.GetQueryParamDuration(url, "message_error_backoff_multiplier_type", 1*time.Second),
	}

	return NewKafkaConsumerGroup(consumerConfig)
}

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
		return nil, errors.NewServiceError("failed to create Kafka consumer group for %s: %v", cfg.Topic, err)
	}

	client := &KafkaConsumerGroup{
		Config:        cfg,
		ConsumerGroup: consumerGroup,
	}

	return client, nil
}

func (k *KafkaConsumerGroup) Start(ctx context.Context, consumerFn func(message KafkaMessage) error) {
	if k == nil {
		return
	}

	wg := sync.WaitGroup{}
	wg.Add(k.Config.ConsumerCount)

	go func() {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		go func() {
			for err := range k.ConsumerGroup.Errors() {
				k.Config.Logger.Errorf("Kafka consumer error: %v", err)
			}
		}()

		topics := []string{k.Config.Topic}

		for i := 0; i < k.Config.ConsumerCount; i++ {
			go func(consumerIndex int) {
				// defer consumer.Close() // Ensure cleanup, if necessary
				k.Config.Logger.Infof("[kafka] Starting consumer [%d] for group %s on topic %s \n", consumerIndex, k.Config.ConsumerGroupID, topics[0])
				wg.Done()

				for {
					select {
					case <-ctx.Done():
						// Context cancelled, exit goroutine
						return
					default:
						if err := k.ConsumerGroup.Consume(ctx, topics, NewKafkaConsumer(k.Config, consumerFn)); err != nil {
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
	}()

	wg.Wait()
}

func (k *KafkaConsumerGroup) BrokersURL() []string {
	return k.Config.BrokersURL
}

// KafkaConsumer represents a Sarama consumer group consumer
type KafkaConsumer struct {
	consumerClosure func(KafkaMessage) error
	cfg             KafkaConsumerConfig
}

func NewKafkaConsumer(cfg KafkaConsumerConfig, consumerClosureOrNil func(message KafkaMessage) error) *KafkaConsumer {
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
func (kc *KafkaConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (kc *KafkaConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	const batchSize = 1000

	ctx := context.Background()

	for {
		select {
		case <-session.Context().Done():
			// Should return when `session.Context()` is done.
			// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
			// https://github.com/Shopify/sarama/issues/1192
			return session.Context().Err()

		case message := <-claim.Messages():
			if message == nil {
				continue
			}

			// Process first message
			if kc.cfg.AutoCommitEnabled {
				kc.handleMessagesWithAutoCommit(session, message) //nolint:errcheck
			} else {
				kc.handleMessageWithManualCommit(ctx, session, message) //nolint:errcheck
			}

			// Process any additional messages available (up to batchSize-1)
			processed := 1
			for processed < batchSize {
				select {
				case message := <-claim.Messages():
					if message == nil {
						break
					}

					if kc.cfg.AutoCommitEnabled {
						kc.handleMessagesWithAutoCommit(session, message) //nolint:errcheck
					} else {
						kc.handleMessageWithManualCommit(ctx, session, message) //nolint:errcheck
					}

					processed++
				default:
					// No more messages immediately available
					if !kc.cfg.AutoCommitEnabled {
						// kc.cfg.Logger.Infof("Committing offsets for session %s", session.MemberID())
						// kafka commit concept is confusing as it does two things:
						// - auto commit enabled will 1) update the offset to the latest message received and 2) commit this to the server
						// - auto commit disabled means we do both things ourselves - 1) mark the offset and 2) commit this to the server
						// When it's disabled, we mark the offset manually inside handleMessageWithManualCommit() and
						// we push this to the server (commit) here
						// NOTE: session.Commit() is a blocking call so we don't do it on every message processed, only when the batch is complete
						// In theory this means if the process is terminated before the commit, we could find that when we start up again,
						// we're given the last batch of messages all over again.
						session.Commit()
					}

					break
				}
			}
		}
	}
}

// handleMessageWithManualCommit processes the message and commits the offset only if the processing of the message is successful
func (kc *KafkaConsumer) handleMessageWithManualCommit(ctx context.Context, session sarama.ConsumerGroupSession, message *sarama.ConsumerMessage) error {
	msg := KafkaMessage{Message: message, Session: session}
	kc.cfg.Logger.Infof("Processing message with offset: %v", message.Offset)

	// execute consumer closure
	_, err := retry.Retry(ctx, kc.cfg.Logger, func() (any, error) {
		return struct{}{}, kc.consumerClosure(msg)
	}, retry.WithRetryCount(kc.cfg.MessageErrorMaxRetries), retry.WithBackoffMultiplier(kc.cfg.MessageErrorBackoffMultiplier),
		retry.WithBackoffDurationType(kc.cfg.MessageErrorBackoffMultiplierType), retry.WithMessage("[kafka_consumer] retrying to process message..."))

	// if we can't process the message, log the error and skip to the next message
	if err != nil {
		kc.cfg.Logger.Errorf("[kafka_consumer] error processing kafka message, skipping: %v", message)
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
