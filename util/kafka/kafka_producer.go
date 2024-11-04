package kafka

import (
	"encoding/binary"
	"net/url"
	"strings"

	"github.com/IBM/sarama"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/util"
)

/**
kafka-topics.sh --list --bootstrap-server localhost:9092

kafka-topics.sh --describe --bootstrap-server localhost:9092

kafka-console-consumer.sh --topic blocks --bootstrap-server localhost:9092 --from-beginning
*/

type KafkaProducerI interface {
	GetClient() sarama.ConsumerGroup
	Send(key []byte, data []byte) error
	Close() error
}

type SyncKafkaProducer struct {
	Producer   sarama.SyncProducer
	Topic      string
	Partitions int32
	client     sarama.ConsumerGroup
}

func (k *SyncKafkaProducer) Close() error {
	if err := k.Producer.Close(); err != nil {
		return errors.NewServiceError("failed to close Kafka producer", err)
	}

	return nil
}

func (k *SyncKafkaProducer) GetClient() sarama.ConsumerGroup {
	return k.client
}

func (k *SyncKafkaProducer) Send(key []byte, data []byte) error {
	partition := binary.LittleEndian.Uint32(key) % uint32(k.Partitions)
	_, _, err := k.Producer.SendMessage(&sarama.ProducerMessage{
		Topic:     k.Topic,
		Key:       sarama.ByteEncoder(key),
		Value:     sarama.ByteEncoder(data),
		Partition: int32(partition),
	})

	return err
}

func NewKafkaProducer(kafkaURL *url.URL) (sarama.ClusterAdmin, KafkaProducerI, error) {
	brokersURL := strings.Split(kafkaURL.Host, ",")

	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0

	clusterAdmin, err := sarama.NewClusterAdmin(brokersURL, config)
	if err != nil {
		return nil, nil, errors.NewServiceError("error while creating cluster admin", err)
	}

	partitions := util.GetQueryParamInt(kafkaURL, "partitions", 1)
	replicationFactor := util.GetQueryParamInt(kafkaURL, "replication", 1)
	retentionPeriod := util.GetQueryParam(kafkaURL, "retention", "600000")      // 10 minutes
	segmentBytes := util.GetQueryParam(kafkaURL, "segment_bytes", "1073741824") // 1GB default

	topic := kafkaURL.Path[1:]

	if err := clusterAdmin.CreateTopic(topic, &sarama.TopicDetail{
		NumPartitions:     int32(partitions),
		ReplicationFactor: int16(replicationFactor),
		ConfigEntries: map[string]*string{
			"retention.ms":        &retentionPeriod, // Set the retention period
			"delete.retention.ms": &retentionPeriod,
			"segment.ms":          &retentionPeriod,
			"segment.bytes":       &segmentBytes,
		},
	}, false); err != nil {
		if !errors.Is(err, sarama.ErrTopicAlreadyExists) {
			return nil, nil, err
		}
	}

	flushBytes := util.GetQueryParamInt(kafkaURL, "flush_bytes", 1024)

	producer, err := ConnectProducer(brokersURL, topic, int32(partitions), flushBytes)
	if err != nil {
		return nil, nil, errors.NewServiceError("unable to connect to kafka", err)
	}

	return clusterAdmin, producer, nil
}

func ConnectProducer(brokersURL []string, topic string, partitions int32, flushBytes ...int) (KafkaProducerI, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Partitioner = sarama.NewManualPartitioner

	flush := 16 * 1024
	if len(flushBytes) > 0 {
		flush = flushBytes[0]
	}

	config.Producer.Flush.Bytes = flush

	// NewSyncProducer creates a new SyncProducer using the given broker addresses and configuration.
	conn, err := sarama.NewSyncProducer(brokersURL, config)
	if err != nil {
		return nil, err
	}

	return &SyncKafkaProducer{
		Producer:   conn,
		Partitions: partitions,
		Topic:      topic,
	}, nil
}
