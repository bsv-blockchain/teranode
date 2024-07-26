package util

import (
	"log"

	"github.com/IBM/sarama"
)

// KafkaConsumer represents a Sarama consumer group consumer
type KafkaConsumer struct {
	workerCh          chan KafkaMessage
	consumerClosure   func(KafkaMessage) error
	autoCommitEnabled bool
}

func NewKafkaConsumer(workerCh chan KafkaMessage, autoCommitEnabled bool, consumerClosure ...func(message KafkaMessage) error) *KafkaConsumer {
	consumer := &KafkaConsumer{
		workerCh:          workerCh,
		autoCommitEnabled: autoCommitEnabled,
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

	for {
		select {
		case message := <-claim.Messages():
			// Handle the first message
			if kc.autoCommitEnabled {
				_ = kc.handleMessagesWithAutoCommit(session, message)
			} else {
				if err := kc.handleMessageWithManualCommit(session, message); err != nil {
					// TODO: consider changing logging and/or error handling
					// The message is not marked as consumed, Log the error and continue processing the next message.
					log.Printf("Error processing message: %v", err)
				}
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
						if err := kc.handleMessageWithManualCommit(session, message); err != nil {
							// TODO: consider changing logging and/or error handling
							// The message is not marked as consumed, Log the error and continue processing the next message.
							log.Printf("Error processing message: %v", err)
						}
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
func (kc *KafkaConsumer) handleMessageWithManualCommit(session sarama.ConsumerGroupSession, message *sarama.ConsumerMessage) error {
	msg := KafkaMessage{Message: message, Session: session}

	if kc.consumerClosure != nil {
		// if there is an executing the consumer closure, return it
		if err := kc.consumerClosure(msg); err != nil {
			return err
		}
	} else {
		kc.workerCh <- msg
	}

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
