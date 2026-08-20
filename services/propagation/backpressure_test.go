package propagation

// Tests for the ingress back-pressure gate.
//
// Ingress is the only point where every submitter path still gets a
// synchronous, retryable answer: on the Kafka route the caller is told 200 as
// soon as the message is published, so a shed decided later — in the validator
// consumer — can no longer be reported to anyone. Rejecting here also means a
// shed transaction is never stored and never published, so nothing has to be
// retried or reclaimed on the node's side.
//
// Ordering is load-bearing in both directions: the gate sits AFTER the terminal
// structural checks (a coinbase or input-less transaction must get its terminal
// verdict, not a retryable "busy" a broadcaster would replay forever) and
// BEFORE storeTransaction/publish.

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/stretchr/testify/require"
)

// staticIngestGate drives the back-pressure verdict directly, so the gate can
// be exercised without standing up a queue poller.
type staticIngestGate bool

func (g staticIngestGate) Overloaded() bool { return bool(g) }

func newBackpressureServer(t *testing.T, gate IngestGate) (*PropagationServer, *memory.Memory, *MockKafkaProducer) {
	t.Helper()

	txStore := memory.New()
	producer := &MockKafkaProducer{PublishedMessages: make([]*kafka.Message, 0)}

	ps := &PropagationServer{
		logger:    ulogger.TestLogger{},
		validator: &validator.MockValidator{},
		txStore:   txStore,
		settings: &settings.Settings{
			Validator: settings.ValidatorSettings{
				KafkaMaxMessageBytes: 1024 * 1024,
			},
		},
		validatorKafkaProducerClient: producer,
		ingestGate:                   gate,
	}

	return ps, txStore, producer
}

func backpressureTestTx(t *testing.T) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	tx.Inputs = []*bt.Input{{PreviousTxSatoshis: 1000, PreviousTxOutIndex: 1, SequenceNumber: 1}}
	require.NoError(t, tx.Inputs[0].PreviousTxIDAdd(&chainhash.Hash{}))
	require.NoError(t, tx.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))

	return tx
}

func TestIngressBackpressureGate(t *testing.T) {
	ctx := context.Background()

	t.Run("shed is synchronous and stores nothing", func(t *testing.T) {
		ps, txStore, producer := newBackpressureServer(t, staticIngestGate(true))

		tx := backpressureTestTx(t)

		err := ps.processTransactionInternal(ctx, tx)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrThresholdExceeded), "submitter must get a retryable THRESHOLD_EXCEEDED, got: %v", err)

		exists, existsErr := txStore.Exists(ctx, tx.TxIDChainHash()[:], fileformat.FileTypeTx)
		require.NoError(t, existsErr)
		require.False(t, exists, "a shed transaction must not be stored")

		require.Empty(t, producer.PublishedMessages, "a shed transaction must not be published to Kafka")
	})

	t.Run("terminal verdicts beat the busy signal", func(t *testing.T) {
		ps, _, _ := newBackpressureServer(t, staticIngestGate(true))

		// Structurally unacceptable: no inputs. Must get its terminal reject
		// rather than a retryable "busy" that would be replayed forever.
		tx := bt.NewTx()
		require.NoError(t, tx.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))

		err := ps.processTransactionInternal(ctx, tx)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrTxInvalid), "expected the terminal invalid verdict, got: %v", err)
		require.False(t, errors.Is(err, errors.ErrThresholdExceeded))
	})

	t.Run("under the limit the transaction flows through", func(t *testing.T) {
		ps, txStore, producer := newBackpressureServer(t, staticIngestGate(false))

		tx := backpressureTestTx(t)

		require.NoError(t, ps.processTransactionInternal(ctx, tx))

		exists, existsErr := txStore.Exists(ctx, tx.TxIDChainHash()[:], fileformat.FileTypeTx)
		require.NoError(t, existsErr)
		require.True(t, exists, "an accepted transaction must be stored")

		require.Len(t, producer.PublishedMessages, 1, "an accepted transaction must be published")
	})

	// The validator can still shed a transaction that got past ingress (the
	// verdict is up to a poll interval stale, and direct submissions never see
	// the ingress gate). WrapGRPC builds the status from the OUTERMOST code, so
	// propagation must keep THRESHOLD_EXCEEDED there: re-wrapping as
	// ProcessingError turns a retryable "busy" into Internal, which ARC-style
	// broadcasters treat as a hard reject.
	t.Run("a shed from the validator stays retryable through propagation", func(t *testing.T) {
		ps, _, _ := newBackpressureServer(t, nil)
		ps.validatorKafkaProducerClient = nil
		ps.validator = &validator.MockValidator{
			Errors: []error{errors.NewThresholdExceededError("block assembly ingest queue is over the limit")},
		}

		err := ps.processTransactionInternal(ctx, backpressureTestTx(t))
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrThresholdExceeded), "the shed code must survive propagation's error funnel, got: %v", err)
	})

	t.Run("an invalid transaction stays terminal through propagation", func(t *testing.T) {
		ps, _, _ := newBackpressureServer(t, nil)
		ps.validatorKafkaProducerClient = nil
		ps.validator = &validator.MockValidator{
			Errors: []error{errors.NewTxInvalidError("bad script")},
		}

		err := ps.processTransactionInternal(ctx, backpressureTestTx(t))
		require.Error(t, err)
		require.False(t, errors.Is(err, errors.ErrThresholdExceeded), "a genuine validation failure must not be reported as back-pressure, got: %v", err)
	})

	t.Run("no gate configured never sheds", func(t *testing.T) {
		ps, _, producer := newBackpressureServer(t, nil)

		require.NoError(t, ps.processTransactionInternal(ctx, backpressureTestTx(t)))
		require.Len(t, producer.PublishedMessages, 1)
	})
}
