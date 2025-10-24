// Package kafka provides Kafka consumer and producer implementations for message handling.
package kafka

import (
	"encoding/binary"
	"net/url"
	"strings"

	"github.com/IBM/sarama"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/util"
	imk "github.com/bsv-blockchain/teranode/util/kafka/in_memory_kafka"
)

/**
kafka-topics.sh --list --bootstrap-server localhost:9092

kafka-topics.sh --describe --bootstrap-server localhost:9092

kafka-console-consumer.sh --topic blocks --bootstrap-server localhost:9092 --from-beginning
*/

// KafkaProducerI defines the interface for Kafka producer operations.
type KafkaProducerI interface {
	// GetClient returns the underlying consumer group client
	GetClient() sarama.ConsumerGroup

	// Send publishes a message with the given key and data
	Send(key []byte, data []byte) error

	// Close gracefully shuts down the producer
	Close() error
}

// SyncKafkaProducer implements a synchronous Kafka producer.
type SyncKafkaProducer struct {
	Producer   sarama.SyncProducer  // Underlying Sarama sync producer
	Topic      string               // Kafka topic to produce to
	Partitions int32                // Number of partitions
	client     sarama.ConsumerGroup // Associated consumer group client
}

// Close gracefully shuts down the sync producer.
func (k *SyncKafkaProducer) Close() error {
	if err := k.Producer.Close(); err != nil {
		return errors.NewServiceError("failed to close Kafka producer", err)
	}

	return nil
}

// GetClient returns the associated consumer group client.
func (k *SyncKafkaProducer) GetClient() sarama.ConsumerGroup {
	return k.client
}

// Send publishes a message to Kafka with the specified key and data.
// The partition is determined by hashing the key.
func (k *SyncKafkaProducer) Send(key []byte, data []byte) error {
	kPartitionsUint32, err := safeconversion.Int32ToUint32(k.Partitions)
	if err != nil {
		return err
	}

	partition := binary.LittleEndian.Uint32(key) % kPartitionsUint32

	partitionInt32, err := safeconversion.Uint32ToInt32(partition)
	if err != nil {
		return err
	}

	_, _, err = k.Producer.SendMessage(&sarama.ProducerMessage{
		Topic:     k.Topic,
		Key:       sarama.ByteEncoder(key),
		Value:     sarama.ByteEncoder(data),
		Partition: partitionInt32,
	})

	return err
}

// NewKafkaProducer creates a new Kafka producer from the given URL.
// It also creates the topic if it doesn't exist with the specified configuration.
// For "memory" scheme, it uses an in-memory implementation.
//
// Parameters:
//   - kafkaURL: URL containing Kafka configuration including topic and partition settings
//   - kafkaSettings: Kafka settings for TLS and debug logging (can be nil for defaults)
//
// Returns:
//   - ClusterAdmin: Kafka cluster administrator interface (nil for memory scheme)
//   - KafkaProducerI: Configured Kafka producer
//   - error: Any error encountered during setup
func NewKafkaProducer(kafkaURL *url.URL, kafkaSettings *settings.KafkaSettings) (sarama.ClusterAdmin, KafkaProducerI, error) {
	topic := kafkaURL.Path[1:]

	// Handle in-memory producer case
	if kafkaURL.Scheme == memoryScheme {
		// Get the shared broker instance
		broker := imk.GetSharedBroker()
		// Create the in-memory sync producer (implements sarama.SyncProducer)
		inMemSaramaProducer := imk.NewInMemorySyncProducer(broker)
		// No error expected from mock creation

		// Wrap the sarama.SyncProducer in our SyncKafkaProducer to satisfy KafkaProducerI
		producer := &SyncKafkaProducer{
			Producer: inMemSaramaProducer,
			Topic:    topic,
		}

		return nil, producer, nil // Return wrapper type
	}

	// Proceed with real Kafka connection
	brokersURL := strings.Split(kafkaURL.Host, ",")

	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0

	// Disable go-metrics to prevent memory leak from exponential decay sample heap
	// See: https://github.com/IBM/sarama/issues/1321
	config.MetricRegistry = nil

	// Note: Debug logging not supported for sync producer as it doesn't have a logger parameter
	// If needed, add a logger parameter to NewKafkaProducer function

	// Apply authentication settings if kafkaSettings provided and TLS is enabled
	if kafkaSettings != nil && kafkaSettings.EnableTLS {
		if err := configureKafkaAuthFromFields(config, kafkaSettings.EnableTLS, kafkaSettings.TLSSkipVerify,
			kafkaSettings.TLSCAFile, kafkaSettings.TLSCertFile, kafkaSettings.TLSKeyFile); err != nil {
			return nil, nil, errors.NewConfigurationError("failed to configure Kafka authentication", err)
		}
	}

	clusterAdmin, err := sarama.NewClusterAdmin(brokersURL, config)
	if err != nil {
		return nil, nil, errors.NewServiceError("error while creating cluster admin", err)
	}

	partitions := util.GetQueryParamInt(kafkaURL, "partitions", 1)
	replicationFactor := util.GetQueryParamInt(kafkaURL, "replication", 1)
	retentionPeriod := util.GetQueryParam(kafkaURL, "retention", "600000")      // 10 minutes
	segmentBytes := util.GetQueryParam(kafkaURL, "segment_bytes", "1073741824") // 1GB default

	partitionsInt32, err := safeconversion.IntToInt32(partitions)
	if err != nil {
		return nil, nil, err
	}

	replicationFactorInt16, err := safeconversion.IntToInt16(replicationFactor)
	if err != nil {
		// Clean up cluster admin if topic creation prep fails
		_ = clusterAdmin.Close() // Best effort close
		return nil, nil, err
	}

	if err := clusterAdmin.CreateTopic(topic, &sarama.TopicDetail{
		NumPartitions:     partitionsInt32,
		ReplicationFactor: replicationFactorInt16,
		ConfigEntries: map[string]*string{
			"retention.ms":        &retentionPeriod, // Set the retention period
			"delete.retention.ms": &retentionPeriod,
			"segment.ms":          &retentionPeriod,
			"segment.bytes":       &segmentBytes,
		},
	}, false); err != nil {
		if !errors.Is(err, sarama.ErrTopicAlreadyExists) {
			_ = clusterAdmin.Close() // Best effort close
			return nil, nil, err
		}
	}

	flushBytes := util.GetQueryParamInt(kafkaURL, "flush_bytes", 1024)

	producer, err := ConnectProducer(brokersURL, topic, partitionsInt32, kafkaSettings, flushBytes)
	if err != nil {
		_ = clusterAdmin.Close() // Best effort close
		return nil, nil, errors.NewServiceError("unable to connect to kafka", err)
	}

	return clusterAdmin, producer, nil
}

// ConnectProducer establishes a connection to Kafka and creates a new sync producer.
//
// Parameters:
//   - brokersURL: List of Kafka broker URLs
//   - topic: Topic to produce messages to
//   - partitions: Number of partitions for the topic
//   - kafkaSettings: Kafka settings for TLS configuration (can be nil for defaults)
//   - flushBytes: Optional flush size in bytes (defaults to 16KB if not provided)
//
// Returns:
//   - KafkaProducerI: Configured Kafka producer
//   - error: Any error encountered during connection
func ConnectProducer(brokersURL []string, topic string, partitions int32, kafkaSettings *settings.KafkaSettings, flushBytes ...int) (KafkaProducerI, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Partitioner = sarama.NewManualPartitioner

	// Disable go-metrics to prevent memory leak from exponential decay sample heap
	// See: https://github.com/IBM/sarama/issues/1321
	config.MetricRegistry = nil

	// Apply authentication settings if kafkaSettings provided and TLS is enabled
	if kafkaSettings != nil && kafkaSettings.EnableTLS {
		if err := configureKafkaAuthFromFields(config, kafkaSettings.EnableTLS, kafkaSettings.TLSSkipVerify,
			kafkaSettings.TLSCAFile, kafkaSettings.TLSCertFile, kafkaSettings.TLSKeyFile); err != nil {
			return nil, errors.NewConfigurationError("failed to configure Kafka authentication", err)
		}
	}

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
