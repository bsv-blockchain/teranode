package validator

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// newPolicyRejectedTestTx builds a minimal valid transaction carrying an
// OP_RETURN payload of the given size, so tests can control tx.Size().
func newPolicyRejectedTestTx(t *testing.T, payloadSize int) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	require.NoError(t, tx.From(
		"0000000000000000000000000000000000000000000000000000000000000001",
		0,
		"76a914000000000000000000000000000000000000000088ac",
		1000,
	))
	require.NoError(t, tx.AddOpReturnOutput(make([]byte, payloadSize)))

	return tx
}

func TestPublishPolicyRejectedTx(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	policyErr := errors.NewTxPolicyError("test policy rejection")

	t.Run("publishes tx within size limit", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		producer := kafka.NewKafkaAsyncProducerMock()

		v := &Validator{
			logger:                              logger,
			settings:                            tSettings,
			policyRejectedTxKafkaProducerClient: producer,
		}

		tx := newPolicyRejectedTestTx(t, 32)
		v.publishPolicyRejectedTx(ctx, logger, tx, policyErr)

		select {
		case msg := <-producer.PublishChannel():
			var m kafkamessage.KafkaTxPolicyRejectedTopicMessage
			require.NoError(t, proto.Unmarshal(msg.Value, &m))
			require.Equal(t, tx.TxIDChainHash().CloneBytes(), m.TxHash)
			require.Equal(t, tx.SerializeBytes(), m.RawTx)
			require.Contains(t, m.Reason, "test policy rejection")
		default:
			t.Fatal("expected a message to be published")
		}
	})

	t.Run("skips tx exceeding kafka max message bytes", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.Validator.KafkaMaxMessageBytes = 256
		producer := kafka.NewKafkaAsyncProducerMock()

		v := &Validator{
			logger:                              logger,
			settings:                            tSettings,
			policyRejectedTxKafkaProducerClient: producer,
		}

		tx := newPolicyRejectedTestTx(t, 1024)
		require.Greater(t, tx.Size(), tSettings.Validator.KafkaMaxMessageBytes)

		v.publishPolicyRejectedTx(ctx, logger, tx, policyErr)

		select {
		case msg := <-producer.PublishChannel():
			t.Fatalf("expected no message to be published, got %d bytes", len(msg.Value))
		default:
			// expected: oversized tx is skipped, consumers fall back to HTTP fetch
		}
	})

	t.Run("skips tx exceeding default 1MB limit", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)
		producer := kafka.NewKafkaAsyncProducerMock()

		v := &Validator{
			logger:                              logger,
			settings:                            tSettings,
			policyRejectedTxKafkaProducerClient: producer,
		}

		tx := newPolicyRejectedTestTx(t, 2*1024*1024)
		require.Greater(t, tx.Size(), tSettings.Validator.KafkaMaxMessageBytes)

		v.publishPolicyRejectedTx(ctx, logger, tx, policyErr)

		select {
		case msg := <-producer.PublishChannel():
			t.Fatalf("expected no message to be published, got %d bytes", len(msg.Value))
		default:
		}
	})

	t.Run("nil producer is a no-op", func(t *testing.T) {
		tSettings := test.CreateBaseTestSettings(t)

		v := &Validator{
			logger:   logger,
			settings: tSettings,
		}

		tx := newPolicyRejectedTestTx(t, 32)
		v.publishPolicyRejectedTx(ctx, logger, tx, policyErr) // must not panic
	})
}
