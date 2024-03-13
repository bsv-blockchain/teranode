package util

import (
	"github.com/IBM/sarama"
)

// KafkaConsumer represents a Sarama consumer group consumer
type KafkaConsumer struct {
	workerCh        chan KafkaMessage
	consumerClosure func(KafkaMessage)
}

func NewKafkaConsumer(workerCh chan KafkaMessage, consumerClosure ...func(message KafkaMessage)) *KafkaConsumer {
	consumer := &KafkaConsumer{
		workerCh: workerCh,
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
			if kc.consumerClosure != nil {
				kc.consumerClosure(KafkaMessage{Message: message, Session: session})
			} else {
				kc.workerCh <- KafkaMessage{Message: message, Session: session}
			}

			// Handle further messages up to a maximum of 1000.
			messageCount := 1 // Start with 1 message already received.
		InnerLoop:
			for messageCount < 1000 {
				select {
				case message := <-claim.Messages():
					if kc.consumerClosure != nil {
						kc.consumerClosure(KafkaMessage{Message: message, Session: session})
					} else {
						kc.workerCh <- KafkaMessage{Message: message, Session: session}
					}
					messageCount++
				default:
					// No more messages, break the inner loop.
					break InnerLoop
				}
			}

			//log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s", string(message.Value), message.Timestamp, message.Topic)
			//session.MarkMessage(message, "")

		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/Shopify/sarama/issues/1192
		case <-session.Context().Done():
			return session.Context().Err()
		}
	}
}
