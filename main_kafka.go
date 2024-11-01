package main

import (
	"context"
	"net/url"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util/kafka"
	"github.com/ordishs/gocore"
)

// Producer functions
func getKafkaAsyncProducer(ctx context.Context, logger ulogger.Logger, setting string) (*kafka.KafkaAsyncProducer, error) {
	url, err, _ := gocore.Config().GetURL(setting)
	if err != nil {
		return nil, errors.NewConfigurationError("failed to get Kafka URL for "+setting, err)
	}

	producer, err := kafka.NewKafkaAsyncProducerFromURL(ctx, logger, url)
	if err != nil {
		return nil, err
	}

	return producer, nil
}
func getKafkaBlocksAsyncProducer(ctx context.Context, logger ulogger.Logger) (*kafka.KafkaAsyncProducer, error) {
	return getKafkaAsyncProducer(ctx, logger, "kafka_blocksConfig")
}

func getKafkaBlocksFinalAsyncProducer(ctx context.Context, logger ulogger.Logger) (*kafka.KafkaAsyncProducer, error) {
	return getKafkaAsyncProducer(ctx, logger, "kafka_blocksFinalConfig")
}

func getKafkaRejectedTxAsyncProducer(ctx context.Context, logger ulogger.Logger) (*kafka.KafkaAsyncProducer, error) {
	return getKafkaAsyncProducer(ctx, logger, "kafka_rejectedTxConfig")
}

func getKafkaSubtreesAsyncProducer(ctx context.Context, logger ulogger.Logger) (*kafka.KafkaAsyncProducer, error) {
	return getKafkaAsyncProducer(ctx, logger, "kafka_subtreesConfig")
}

func getKafkaTxmetaAsyncProducer(ctx context.Context, logger ulogger.Logger) (*kafka.KafkaAsyncProducer, error) {
	return getKafkaAsyncProducer(ctx, logger, "kafka_txmetaConfig")
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
func getKafkaConsumerGroup(logger ulogger.Logger, setting string, consumerGroupID string, autoCommit bool) (*kafka.KafkaConsumerGroup, error) {
	url, err, _ := gocore.Config().GetURL(setting)
	if err != nil {
		return nil, errors.NewConfigurationError("failed to get Kafka URL for "+setting, err)
	}

	consumer, err := kafka.NewKafkaConsumerGroupFromURL(logger, url, consumerGroupID, autoCommit)
	if err != nil {
		return nil, errors.NewConfigurationError("missing Kafka URL for "+setting, err)
	}

	return consumer, nil
}

func getKafkaBlocksConsumerGroup(logger ulogger.Logger, consumerGroupID string) (*kafka.KafkaConsumerGroup, error) {
	return getKafkaConsumerGroup(logger, "kafka_blocksConfig", consumerGroupID, false)
}

func getKafkaBlocksFinalConsumerGroup(logger ulogger.Logger, consumerGroupID string) (*kafka.KafkaConsumerGroup, error) {
	return getKafkaConsumerGroup(logger, "kafka_blocksFinalConfig", consumerGroupID, false)
}

func getKafkaRejectedTxConsumerGroup(logger ulogger.Logger, consumerGroupID string) (*kafka.KafkaConsumerGroup, error) {
	return getKafkaConsumerGroup(logger, "kafka_rejectedTxConfig", consumerGroupID, true)
}

func getKafkaSubtreesConsumerGroup(logger ulogger.Logger, consumerGroupID string) (*kafka.KafkaConsumerGroup, error) {
	return getKafkaConsumerGroup(logger, "kafka_subtreesConfig", consumerGroupID, true)
}

func getKafkaTxConsumerGroup(logger ulogger.Logger, consumerGroupID string) (kafka.KafkaConsumerGroupI, error) {

	value, found := gocore.Config().Get("kafka_validatortxsConfig")
	if !found || value == "" {
		return nil, nil
	}

	url, err := url.ParseRequestURI(value)
	if err != nil {
		return nil, errors.NewConfigurationError("failed to get Kafka URL for validatortxs consumer - kafka_validatortxsConfig", err)
	}

	consumer, err := kafka.NewKafkaConsumerGroupFromURL(logger, url, consumerGroupID, true)

	if err != nil {
		return nil, errors.NewConfigurationError("failed to create new Kafka listener for kafka_validatortxsConfig", err)
	}

	return consumer, nil
}

func getKafkaTxmetaConsumerGroup(logger ulogger.Logger, consumerGroupID string) (*kafka.KafkaConsumerGroup, error) {
	return getKafkaConsumerGroup(logger, "kafka_txmetaConfig", consumerGroupID, true)
}
