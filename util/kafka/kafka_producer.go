// Package kafka provides Kafka consumer and producer implementations for message handling.
package kafka

import (
	"context"
	"encoding/binary"
	"net/url"
	"strings"

	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/util"
	imk "github.com/bsv-blockchain/teranode/util/kafka/in_memory_kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

/**
kafka-topics.sh --list --bootstrap-server localhost:9092

kafka-topics.sh --describe --bootstrap-server localhost:9092

kafka-console-consumer.sh --topic blocks --bootstrap-server localhost:9092 --from-beginning
*/

// KafkaProducerI defines the interface for Kafka producer operations.
type KafkaProducerI interface {
	// Send publishes a message with the given key and data
	Send(key []byte, data []byte) error

	// Close gracefully shuts down the producer
	Close() error
}

// SyncKafkaProducer implements a synchronous Kafka producer using franz-go.
type SyncKafkaProducer struct {
	client     *kgo.Client // Underlying franz-go client
	Topic      string      // Kafka topic to produce to
	Partitions int32       // Number of partitions

	// For in-memory support
	inMemoryProducer *imk.InMemorySyncProducer
	isInMemory       bool
}

// Close gracefully shuts down the sync producer.
func (k *SyncKafkaProducer) Close() error {
	if k.isInMemory {
		return k.inMemoryProducer.Close()
	}

	if k.client != nil {
		if err := k.client.Flush(context.Background()); err != nil {
			return errors.NewServiceError("failed to flush Kafka producer", err)
		}
		k.client.Close()
	}

	return nil
}

// Send publishes a message to Kafka with the specified key and data.
// The partition is determined by hashing the key.
func (k *SyncKafkaProducer) Send(key []byte, data []byte) error {
	if k.isInMemory {
		return k.sendInMemory(key, data)
	}

	kPartitionsUint32, err := safeconversion.Int32ToUint32(k.Partitions)
	if err != nil {
		return err
	}

	partition := binary.LittleEndian.Uint32(key) % kPartitionsUint32

	partitionInt32, err := safeconversion.Uint32ToInt32(partition)
	if err != nil {
		return err
	}

	record := &kgo.Record{
		Topic:     k.Topic,
		Key:       key,
		Value:     data,
		Partition: partitionInt32,
	}

	results := k.client.ProduceSync(context.Background(), record)
	if err := results.FirstErr(); err != nil {
		return err
	}

	return nil
}

// sendInMemory handles sending for in-memory producer
func (k *SyncKafkaProducer) sendInMemory(key []byte, data []byte) error {
	return k.inMemoryProducer.Send(k.Topic, key, data)
}

// NewKafkaProducer creates a new Kafka producer from the given URL using franz-go.
// It also creates the topic if it doesn't exist with the specified configuration.
// For "memory" scheme, it uses an in-memory implementation.
//
// Parameters:
//   - kafkaURL: URL containing Kafka configuration including topic and partition settings
//   - kafkaSettings: Kafka settings for TLS and debug logging (can be nil for defaults)
//
// Returns:
//   - KafkaProducerI: Configured Kafka producer
//   - error: Any error encountered during setup
func NewKafkaProducer(kafkaURL *url.URL, kafkaSettings *settings.KafkaSettings) (KafkaProducerI, error) {
	return NewKafkaProducerWithContext(context.Background(), kafkaURL, kafkaSettings)
}

// NewKafkaProducerWithContext creates a new Kafka producer from the given URL using franz-go with context.
func NewKafkaProducerWithContext(ctx context.Context, kafkaURL *url.URL, kafkaSettings *settings.KafkaSettings) (KafkaProducerI, error) {
	topic := kafkaURL.Path[1:]

	// Handle in-memory producer case
	if kafkaURL.Scheme == memoryScheme {
		broker := imk.GetSharedBroker()
		inMemProducer := imk.NewInMemorySyncProducer(broker)

		producer := &SyncKafkaProducer{
			Topic:            topic,
			inMemoryProducer: inMemProducer,
			isInMemory:       true,
		}

		return producer, nil
	}

	// Proceed with real franz-go connection
	brokersURL := strings.Split(kafkaURL.Host, ",")

	partitions := util.GetQueryParamInt(kafkaURL, "partitions", 1)
	replicationFactor := util.GetQueryParamInt(kafkaURL, "replication", 1)
	retentionPeriod := util.GetQueryParam(kafkaURL, "retention", "600000")
	segmentBytes := util.GetQueryParam(kafkaURL, "segment_bytes", "1073741824")
	flushBytes := util.GetQueryParamInt(kafkaURL, "flush_bytes", 16*1024)

	partitionsInt32, err := safeconversion.IntToInt32(partitions)
	if err != nil {
		return nil, err
	}

	replicationFactorInt16, err := safeconversion.IntToInt16(replicationFactor)
	if err != nil {
		return nil, err
	}

	// Build franz-go client options
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokersURL...),
		kgo.DefaultProduceTopic(topic),
		kgo.ProducerBatchMaxBytes(int32(flushBytes)),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.RecordRetries(5),
	}

	// Configure TLS if enabled
	if kafkaSettings != nil && kafkaSettings.EnableTLS {
		tlsConfig, err := buildFranzTLSConfig(kafkaSettings.EnableTLS, kafkaSettings.TLSSkipVerify,
			kafkaSettings.TLSCAFile, kafkaSettings.TLSCertFile, kafkaSettings.TLSKeyFile)
		if err != nil {
			return nil, errors.NewConfigurationError("failed to configure TLS for kafka producer", err)
		}
		opts = append(opts, kgo.DialTLSConfig(tlsConfig))
	}

	// Create the franz-go client
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, errors.NewServiceError("error while creating kafka client", err)
	}

	// Create topic configuration
	cfg := KafkaProducerConfig{
		Topic:                 topic,
		Partitions:            partitionsInt32,
		ReplicationFactor:     replicationFactorInt16,
		RetentionPeriodMillis: retentionPeriod,
		SegmentBytes:          segmentBytes,
	}

	// Create topic if it doesn't exist
	if err := createTopicWithFranz(ctx, client, cfg); err != nil {
		client.Close()
		return nil, err
	}

	return &SyncKafkaProducer{
		client:     client,
		Partitions: partitionsInt32,
		Topic:      topic,
	}, nil
}

// ConnectProducer establishes a connection to Kafka and creates a new sync producer using franz-go.
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
	flush := 16 * 1024
	if len(flushBytes) > 0 {
		flush = flushBytes[0]
	}

	// Build franz-go client options
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokersURL...),
		kgo.DefaultProduceTopic(topic),
		kgo.ProducerBatchMaxBytes(int32(flush)),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.RecordRetries(5),
	}

	// Configure TLS if enabled
	if kafkaSettings != nil && kafkaSettings.EnableTLS {
		tlsConfig, err := buildFranzTLSConfig(kafkaSettings.EnableTLS, kafkaSettings.TLSSkipVerify,
			kafkaSettings.TLSCAFile, kafkaSettings.TLSCertFile, kafkaSettings.TLSKeyFile)
		if err != nil {
			return nil, errors.NewConfigurationError("failed to configure TLS for kafka producer", err)
		}
		opts = append(opts, kgo.DialTLSConfig(tlsConfig))
	}

	// Create the franz-go client
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}

	return &SyncKafkaProducer{
		client:     client,
		Partitions: partitions,
		Topic:      topic,
	}, nil
}
