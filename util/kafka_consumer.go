package util

import (
	"github.com/Shopify/sarama"
)

// KafkaConsumer represents a Sarama consumer group consumer
type KafkaConsumer struct {
	ready    chan bool
	workerCh chan KafkaMessage
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (kc *KafkaConsumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the kc as ready
	close(kc.ready)
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
			kc.workerCh <- KafkaMessage{Message: message, Session: session}
			//log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s", string(message.Value), message.Timestamp, message.Topic)
			//session.MarkMessage(message, "")

		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/Shopify/sarama/issues/1192
		case <-session.Context().Done():
			return nil
		}
	}
}
