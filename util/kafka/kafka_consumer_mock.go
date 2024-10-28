package kafka

func NewMockKafkaConsumerGroup() KafkaConsumerGroupI {
	return &KafkaConsumerGroup{}
}
