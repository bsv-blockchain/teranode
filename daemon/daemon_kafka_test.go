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

// TestGetKafkaTxmetaEvictionAsyncProducer pins that block assembly's txmeta producer is
// optional (nil, nil without a txmeta topic, so the interface really is nil) and is
// created without manual partitioning even when the validator ships v2.
func TestGetKafkaTxmetaEvictionAsyncProducer(t *testing.T) {
	t.Run("no txmeta topic configured gives a nil interface and no error", func(t *testing.T) {
		producer, err := getKafkaTxmetaEvictionAsyncProducer(context.Background(), ulogger.TestLogger{}, &settings.Settings{})
		require.NoError(t, err)
		require.Nil(t, producer, "must be a true nil interface so block assembly skips publishing")
	})

	t.Run("txmeta topic configured gives a producer, also under the v2 wire format", func(t *testing.T) {
		kafkaURL, err := url.Parse("memory://in-memory/txmeta")
		require.NoError(t, err)

		tSettings := &settings.Settings{Kafka: settings.KafkaSettings{TxMetaConfig: kafkaURL}}
		tSettings.Validator.TxMetaWireFormat = "v2"

		producer, err := getKafkaTxmetaEvictionAsyncProducer(context.Background(), ulogger.TestLogger{}, tSettings)
		require.NoError(t, err)
		require.NotNil(t, producer)
	})
}
