package util

import (
	"context"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util/retry"
	"time"

	"github.com/IBM/sarama"
)

// KafkaConsumer represents a Sarama consumer group consumer
type KafkaConsumer struct {
	workerCh          chan KafkaMessage
	consumerClosure   func(KafkaMessage) error
	autoCommitEnabled bool
	logger            ulogger.Logger
}

func NewKafkaConsumer(workerCh chan KafkaMessage, autoCommitEnabled bool, consumerClosure ...func(message KafkaMessage) error) *KafkaConsumer {
	consumer := &KafkaConsumer{
		workerCh:          workerCh,
		autoCommitEnabled: autoCommitEnabled,
		logger:            ulogger.New("kafka_consumer"),
	}

	if len(consumerClosure) > 0 {
		consumer.consumerClosure = consumerClosure[0]
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
			// Handle the first message
			//fmt.Printf("Handling first message: topic = %s, partition = %d, offset = %d, key = %s, value = %s\n", message.Topic, message.Partition, message.Offset, string(message.Key), string(message.Value))
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
	kc.logger.Infof("Processing message with offset: %v", message.Offset)

	if kc.consumerClosure != nil {
		// execute consumer closure
		if err := kc.consumerClosure(msg); err != nil {
			// if the error is not nil, start retry logic
			_, err = retry.Retry(ctx, kc.logger, func() (any, error) {
				return struct{}{}, kc.consumerClosure(msg)
			}, retry.WithRetryCount(3), retry.WithBackoffMultiplier(2),
				retry.WithBackoffDurationType(time.Second), retry.WithMessage("[kafka_consumer] retrying to process message..."))

			// if we still can't process the message, log the error and skip to the next message
			if err != nil {
				kc.logger.Errorf("[kafka_consumer] error processing kafka message, skipping: %v", message)
			}
		}
	} else {
		// if no error, send the message to the worker channel
		kc.workerCh <- msg
	}

	kc.logger.Infof("Committing offset: %v", message.Offset)
	// Commit the message offset, processing is successful
	session.MarkMessage(message, "")
	return nil
}

func (kc *KafkaConsumer) handleMessagesWithAutoCommit(session sarama.ConsumerGroupSession, message *sarama.ConsumerMessage) error {
	msg := KafkaMessage{Message: message, Session: session}

	if kc.consumerClosure != nil {
		// we don't check the error here as we are auto committing
		_ = kc.consumerClosure(msg)
	} else {
		kc.workerCh <- msg
	}

	// Auto-commit is implied, so we don't need to explicitly mark the message here
	return nil
}
