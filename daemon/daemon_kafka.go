package daemon

import (
	"context"
	"net/url"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/urlutil"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/labstack/gommon/random"
	"github.com/ordishs/gocore"
)

// getKafkaAsyncProducer creates a new Kafka async producer from the provided URL.
func getKafkaAsyncProducer(ctx context.Context, logger ulogger.Logger, url *url.URL, kafkaSettings *settings.KafkaSettings) (*kafka.KafkaAsyncProducer, error) {
	producer, err := kafka.NewKafkaAsyncProducerFromURL(ctx, logger, url, kafkaSettings)
	if err != nil {
		return nil, err
	}

	return producer, nil
}

// getKafkaBlocksAsyncProducer creates a new Kafka async producer for blocks using the configuration from settings.
func getKafkaBlocksAsyncProducer(ctx context.Context, logger ulogger.Logger,
	settings *settings.Settings) (*kafka.KafkaAsyncProducer, error) {
	kafkaBlocksConfig := settings.Kafka.BlocksConfig
	if kafkaBlocksConfig == nil {
		return nil, errors.NewConfigurationError("missing Kafka URL for blocks producer - blocksConfig")
	}

	return getKafkaAsyncProducer(ctx, logger, kafkaBlocksConfig, &settings.Kafka)
}

// getKafkaBlocksFinalAsyncProducer creates a new Kafka async producer for blocks final using the configuration from settings.
func getKafkaBlocksFinalAsyncProducer(ctx context.Context, logger ulogger.Logger,
	settings *settings.Settings) (*kafka.KafkaAsyncProducer, error) {
	kafkaBlocksFinalConfig := settings.Kafka.BlocksFinalConfig
	if kafkaBlocksFinalConfig == nil {
		return nil, errors.NewConfigurationError("missing Kafka URL for blocks final producer - blocksFinalConfig")
	}

	return getKafkaAsyncProducer(ctx, logger, kafkaBlocksFinalConfig, &settings.Kafka)
}

// getKafkaRejectedTxAsyncProducer creates a new Kafka async producer for rejected transactions using the configuration from settings.
func getKafkaRejectedTxAsyncProducer(ctx context.Context, logger ulogger.Logger,
	settings *settings.Settings) (*kafka.KafkaAsyncProducer, error) {
	kafkaRejectedTxConfig := settings.Kafka.RejectedTxConfig
	if kafkaRejectedTxConfig == nil {
		return nil, errors.NewConfigurationError("missing Kafka URL for rejected tx producer - rejectedTxConfig")
	}

	return getKafkaAsyncProducer(ctx, logger, kafkaRejectedTxConfig, &settings.Kafka)
}

// getKafkaSubtreesAsyncProducer creates a new Kafka async producer for subtrees using the configuration from settings.
func getKafkaSubtreesAsyncProducer(ctx context.Context, logger ulogger.Logger,
	settings *settings.Settings) (*kafka.KafkaAsyncProducer, error) {
	kafkaSubtreesConfig := settings.Kafka.SubtreesConfig
	if kafkaSubtreesConfig == nil {
		return nil, errors.NewConfigurationError("missing Kafka URL for subtrees producer - subtreesConfig")
	}

	return getKafkaAsyncProducer(ctx, logger, kafkaSubtreesConfig, &settings.Kafka)
}

// getKafkaTxmetaAsyncProducer creates a new Kafka async producer for txmeta using the configuration from settings.
func getKafkaTxmetaAsyncProducer(ctx context.Context, logger ulogger.Logger,
	settings *settings.Settings) (*kafka.KafkaAsyncProducer, error) {
	kafkaTxmetaConfig := settings.Kafka.TxMetaConfig
	if kafkaTxmetaConfig == nil {
		return nil, errors.NewConfigurationError("missing Kafka URL for txmeta producer - txmetaConfig")
	}

	return getKafkaAsyncProducer(ctx, logger, kafkaTxmetaConfig, &settings.Kafka)
}

// getKafkaTxAsyncProducer creates a new Kafka async producer for validator transactions using the configuration from gocore.
func getKafkaTxAsyncProducer(ctx context.Context, logger ulogger.Logger, settings *settings.Settings) (kafka.KafkaAsyncProducerI, error) {
	value, found := gocore.Config().Get("kafka_validatortxsConfig")
	if !found || value == "" {
		return nil, nil
	}

	kafkaURL, err := urlutil.ParseMultiHostURL(value)
	if err != nil {
		return nil, errors.NewConfigurationError("failed to get Kafka URL for validatortxs producer - kafka_validatortxsConfig", err)
	}

	var producer kafka.KafkaAsyncProducerI

	producer, err = kafka.NewKafkaAsyncProducerFromURL(ctx, logger, kafkaURL, &settings.Kafka)
	if err != nil {
		return nil, errors.NewServiceError("could not create validatortxs kafka producer for local validator", err)
	}

	return producer, nil
}

// getKafkaConsumerGroup creates a new Kafka consumer group from the provided URL and consumer group ID.
func getKafkaConsumerGroup(logger ulogger.Logger, url *url.URL, consumerGroupID string,
	autoCommit bool, kafkaSettings *settings.KafkaSettings) (*kafka.KafkaConsumerGroup, error) {
	consumer, err := kafka.NewKafkaConsumerGroupFromURL(logger, url, consumerGroupID, autoCommit, kafkaSettings)
	if err != nil {
		return nil, errors.NewConfigurationError("missing Kafka URL for "+url.String(), err)
	}

	return consumer, nil
}

// getKafkaBlocksConsumerGroup creates a new Kafka consumer group for blocks using the configuration from settings.
func getKafkaBlocksConsumerGroup(logger ulogger.Logger, settings *settings.Settings,
	consumerGroupID string) (*kafka.KafkaConsumerGroup, error) {
	kafkaBlocksConfig := settings.Kafka.BlocksConfig
	if kafkaBlocksConfig == nil {
		return nil, errors.NewConfigurationError("missing Kafka URL for blocks consumer - blocksConfig")
	}

	return getKafkaConsumerGroup(logger, kafkaBlocksConfig, consumerGroupID, false, &settings.Kafka)
}

/*
	func getKafkaBlocksFinalConsumerGroup(logger ulogger.Logger, settings *settings.Settings, consumerGroupID string) (*kafka.KafkaConsumerGroup, error) {
		kafkaBlocksFinalConfig := settings.Kafka.BlocksFinalConfig
		return getKafkaConsumerGroup(logger, kafkaBlocksFinalConfig, consumerGroupID, false)
	}
*/

// getKafkaRejectedTxConsumerGroup creates a new Kafka consumer group for rejected transactions using the configuration from settings.
func getKafkaRejectedTxConsumerGroup(logger ulogger.Logger, settings *settings.Settings,
	consumerGroupID string) (*kafka.KafkaConsumerGroup, error) {
	kafkaRejectedTxConfig := settings.Kafka.RejectedTxConfig
	if kafkaRejectedTxConfig == nil {
		return nil, errors.NewConfigurationError("missing Kafka URL for rejected tx consumer - rejectedTxConfig")
	}

	return getKafkaConsumerGroup(logger, kafkaRejectedTxConfig, consumerGroupID, true, &settings.Kafka)
}

// getKafkaSubtreesConsumerGroup creates a new Kafka consumer group for subtrees using the configuration from settings.
func getKafkaSubtreesConsumerGroup(logger ulogger.Logger, settings *settings.Settings,
	consumerGroupID string) (*kafka.KafkaConsumerGroup, error) {
	kafkaSubtreesConfig := settings.Kafka.SubtreesConfig
	if kafkaSubtreesConfig == nil {
		return nil, errors.NewConfigurationError("missing Kafka URL for subtrees consumer - subtreesConfig")
	}

	return getKafkaConsumerGroup(logger, kafkaSubtreesConfig, consumerGroupID, true, &settings.Kafka)
}

// getKafkaTxConsumerGroup creates a new Kafka consumer group for validator transactions using the configuration from gocore.
func getKafkaTxConsumerGroup(logger ulogger.Logger, settings *settings.Settings,
	consumerGroupID string) (kafka.KafkaConsumerGroupI, error) {
	kafkaTxConfig := settings.Kafka.ValidatorTxsConfig
	if kafkaTxConfig == nil {
		return nil, nil
	}

	return getKafkaConsumerGroup(logger, kafkaTxConfig, consumerGroupID, true, &settings.Kafka)
}

// getKafkaTxmetaConsumerGroup creates a new Kafka consumer group for txmeta using the configuration from settings.
func getKafkaTxmetaConsumerGroup(logger ulogger.Logger, settings *settings.Settings,
	consumerGroupID string) (*kafka.KafkaConsumerGroup, error) {
	kafkaTxmetaConfig := settings.Kafka.TxMetaConfig
	if kafkaTxmetaConfig == nil {
		return nil, errors.NewConfigurationError("missing Kafka URL for txmeta consumer - txmetaConfig")
	}

	// add a random postfix to the consumer group ID to allow multiple instances to run
	// concurrently and consuming the same messages independently
	consumerGroupID = consumerGroupID + "." + random.String(16, random.Alphanumeric)

	return getKafkaConsumerGroup(logger, kafkaTxmetaConfig, consumerGroupID, true, &settings.Kafka)
}

func getKafkaInvalidBlocksConsumerGroup(logger ulogger.Logger, settings *settings.Settings, consumerGroupID string) (*kafka.KafkaConsumerGroup, error) {
	kafkaInvalidBlocksConfig := settings.Kafka.InvalidBlocksConfig
	if kafkaInvalidBlocksConfig == nil {
		return nil, nil // Optional, return nil if not configured
	}

	return getKafkaConsumerGroup(logger, kafkaInvalidBlocksConfig, consumerGroupID, true, &settings.Kafka)
}

func getKafkaInvalidSubtreeConsumerGroup(logger ulogger.Logger, settings *settings.Settings, consumerGroupID string) (*kafka.KafkaConsumerGroup, error) {
	kafkaInvalidSubtreeConfig := settings.Kafka.InvalidSubtreesConfig
	if kafkaInvalidSubtreeConfig == nil {
		return nil, nil // Optional, return nil if not configured
	}

	return getKafkaConsumerGroup(logger, kafkaInvalidSubtreeConfig, consumerGroupID, true, &settings.Kafka)
}
