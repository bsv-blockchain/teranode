package daemon

import (
	"context"
	"net/url"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/kafka"
	"github.com/ordishs/gocore"
)

// Producer functions
func getKafkaAsyncProducer(ctx context.Context, logger ulogger.Logger, url *url.URL) (*kafka.KafkaAsyncProducer, error) {
	producer, err := kafka.NewKafkaAsyncProducerFromURL(ctx, logger, url)
	if err != nil {
		return nil, err
	}

	return producer, nil
}
func getKafkaBlocksAsyncProducer(ctx context.Context, logger ulogger.Logger, settings *settings.Settings) (*kafka.KafkaAsyncProducer, error) {
	kafkaBlocksConfig := settings.Kafka.BlocksConfig
	if kafkaBlocksConfig == nil {
		return nil, errors.NewConfigurationError("missing Kafka URL for blocks producer - blocksConfig")
	}

	return getKafkaAsyncProducer(ctx, logger, kafkaBlocksConfig)
}

func getKafkaBlocksFinalAsyncProducer(ctx context.Context, logger ulogger.Logger, settings *settings.Settings) (*kafka.KafkaAsyncProducer, error) {
	kafkaBlocksFinalConfig := settings.Kafka.BlocksFinalConfig
	if kafkaBlocksFinalConfig == nil {
		return nil, errors.NewConfigurationError("missing Kafka URL for blocks final producer - blocksFinalConfig")
	}

	return getKafkaAsyncProducer(ctx, logger, kafkaBlocksFinalConfig)
}

func getKafkaRejectedTxAsyncProducer(ctx context.Context, logger ulogger.Logger, settings *settings.Settings) (*kafka.KafkaAsyncProducer, error) {
	kafkaRejectedTxConfig := settings.Kafka.RejectedTxConfig
	if kafkaRejectedTxConfig == nil {
		return nil, errors.NewConfigurationError("missing Kafka URL for rejected tx producer - rejectedTxConfig")
	}

	return getKafkaAsyncProducer(ctx, logger, kafkaRejectedTxConfig)
}

func getKafkaSubtreesAsyncProducer(ctx context.Context, logger ulogger.Logger, settings *settings.Settings) (*kafka.KafkaAsyncProducer, error) {
	kafkaSubtreesConfig := settings.Kafka.SubtreesConfig
	if kafkaSubtreesConfig == nil {
		return nil, errors.NewConfigurationError("missing Kafka URL for subtrees producer - subtreesConfig")
	}

	return getKafkaAsyncProducer(ctx, logger, kafkaSubtreesConfig)
}

func getKafkaTxmetaAsyncProducer(ctx context.Context, logger ulogger.Logger, settings *settings.Settings) (*kafka.KafkaAsyncProducer, error) {
	kafkaTxmetaConfig := settings.Kafka.TxMetaConfig
	if kafkaTxmetaConfig == nil {
		return nil, errors.NewConfigurationError("missing Kafka URL for txmeta producer - txmetaConfig")
	}

	return getKafkaAsyncProducer(ctx, logger, kafkaTxmetaConfig)
}

func getKafkaTxAsyncProducer(ctx context.Context, logger ulogger.Logger) (kafka.KafkaAsyncProducerI, error) {
	value, found := gocore.Config().Get("kafka_validatortxsConfig")
	if !found || value == "" {
		return nil, nil
	}

	url, err := url.ParseRequestURI(value)
	if err != nil {
		return nil, errors.NewConfigurationError("failed to get Kafka URL for validatortxs producer - kafka_validatortxsConfig", err)
	}

	producer, err := kafka.NewKafkaAsyncProducerFromURL(ctx, logger, url)
	if err != nil {
		return nil, errors.NewServiceError("could not create validatortxs kafka producer for local validator", err)
	}

	return producer, nil
}

// Consumer functions
func getKafkaConsumerGroup(logger ulogger.Logger, url *url.URL, consumerGroupID string, autoCommit bool) (*kafka.KafkaConsumerGroup, error) {
	consumer, err := kafka.NewKafkaConsumerGroupFromURL(logger, url, consumerGroupID, autoCommit)
	if err != nil {
		return nil, errors.NewConfigurationError("missing Kafka URL for "+url.String(), err)
	}

	return consumer, nil
}

func getKafkaBlocksConsumerGroup(logger ulogger.Logger, settings *settings.Settings, consumerGroupID string) (*kafka.KafkaConsumerGroup, error) {
	kafkaBlocksConfig := settings.Kafka.BlocksConfig
	if kafkaBlocksConfig == nil {
		return nil, errors.NewConfigurationError("missing Kafka URL for blocks consumer - blocksConfig")
	}

	return getKafkaConsumerGroup(logger, kafkaBlocksConfig, consumerGroupID, false)
}

// func getKafkaBlocksFinalConsumerGroup(logger ulogger.Logger, settings *settings.Settings, consumerGroupID string) (*kafka.KafkaConsumerGroup, error) {
// 	kafkaBlocksFinalConfig := settings.Kafka.BlocksFinalConfig
// 	return getKafkaConsumerGroup(logger, kafkaBlocksFinalConfig, consumerGroupID, false)
// }

func getKafkaRejectedTxConsumerGroup(logger ulogger.Logger, settings *settings.Settings, consumerGroupID string) (*kafka.KafkaConsumerGroup, error) {
	kafkaRejectedTxConfig := settings.Kafka.RejectedTxConfig
	if kafkaRejectedTxConfig == nil {
		return nil, errors.NewConfigurationError("missing Kafka URL for rejected tx consumer - rejectedTxConfig")
	}

	return getKafkaConsumerGroup(logger, kafkaRejectedTxConfig, consumerGroupID, true)
}

func getKafkaSubtreesConsumerGroup(logger ulogger.Logger, settings *settings.Settings, consumerGroupID string) (*kafka.KafkaConsumerGroup, error) {
	kafkaSubtreesConfig := settings.Kafka.SubtreesConfig
	if kafkaSubtreesConfig == nil {
		return nil, errors.NewConfigurationError("missing Kafka URL for subtrees consumer - subtreesConfig")
	}

	return getKafkaConsumerGroup(logger, kafkaSubtreesConfig, consumerGroupID, true)
}

func getKafkaTxConsumerGroup(logger ulogger.Logger, settings *settings.Settings, consumerGroupID string) (kafka.KafkaConsumerGroupI, error) {
	kafkaTxConfig := settings.Kafka.ValidatorTxsConfig
	if kafkaTxConfig == nil {
		return nil, nil
	}

	return getKafkaConsumerGroup(logger, kafkaTxConfig, consumerGroupID, true)
}

func getKafkaTxmetaConsumerGroup(logger ulogger.Logger, settings *settings.Settings, consumerGroupID string) (*kafka.KafkaConsumerGroup, error) {
	kafkaTxmetaConfig := settings.Kafka.TxMetaConfig
	if kafkaTxmetaConfig == nil {
		return nil, errors.NewConfigurationError("missing Kafka URL for txmeta consumer - txmetaConfig")
	}

	return getKafkaConsumerGroup(logger, kafkaTxmetaConfig, consumerGroupID, true)
}
