package daemon

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

func TestNonNilConsumerGroup(t *testing.T) {
	// A typed nil must map to a true nil interface so downstream nil checks
	// (e.g. p2p's startInvalidBlocksConsumer) behave correctly.
	require.Nil(t, nonNilConsumerGroup(nil))

	group := &kafka.KafkaConsumerGroup{}
	require.NotNil(t, nonNilConsumerGroup(group))
	require.Same(t, group, nonNilConsumerGroup(group))
}

// TestValidatorTxsConfigContextDivergence documents why the producer must not read
// the raw gocore key: with an alternative settings context the raw value in the
// ambient context and the value parsed for that context differ.
func TestValidatorTxsConfigContextDivergence(t *testing.T) {
	rawAmbient, _ := gocore.Config().Get("kafka_validatortxsConfig")
	rawOperator, _ := gocore.Config("operator").Get("kafka_validatortxsConfig")
	require.NotEqual(t, rawAmbient, rawOperator, "settings.conf sets kafka_validatortxsConfig.operator only")

	parsed := settings.NewSettings("operator").Kafka.ValidatorTxsConfig
	require.NotNil(t, parsed, "operator context sets kafka_validatortxsConfig")
}

// TestGetKafkaTxAsyncProducerUsesParsedSetting pins the producer to the same parsed
// setting the consumer group uses, so both sides of the Propagation -> Validator
// transport agree under an alternative settings context.
func TestGetKafkaTxAsyncProducerUsesParsedSetting(t *testing.T) {
	kafkaURL, err := url.Parse("memory://in-memory/validator-txs")
	require.NoError(t, err)

	tSettings := &settings.Settings{
		Kafka: settings.KafkaSettings{
			ValidatorTxsConfig: kafkaURL,
		},
	}

	producer, err := getKafkaTxAsyncProducer(context.Background(), ulogger.TestLogger{}, tSettings)
	require.NoError(t, err)
	require.NotNil(t, producer, "producer must be created from the parsed ValidatorTxsConfig setting")

	consumer, err := getKafkaTxConsumerGroup(ulogger.TestLogger{}, tSettings, "test-group")
	require.NoError(t, err)
	require.NotNil(t, consumer, "consumer must be created from the same parsed setting")
}
