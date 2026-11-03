package validator

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/validator/validator_api"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/labstack/echo/v4"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func newUnlockFailedServer(t *testing.T, validateErr error) *Server {
	t.Helper()

	initPrometheusMetrics()

	server := NewServer(ulogger.TestLogger{}, test.CreateBaseTestSettings(t), nil, nil, nil, nil, nil, nil, nil)
	server.validator = &MockValidator{
		ValidateFunc: func(context.Context, *bt.Tx) (*meta.Data, error) {
			return &meta.Data{Locked: true}, validateErr
		},
	}

	return server
}

func unlockFailedErr() error {
	return errors.NewTxUnlockFailedError("[Validate] error marking tx as spendable", errors.NewProcessingError("context deadline exceeded"))
}

func kafkaTxMessage(t *testing.T) *kafka.KafkaMessage {
	t.Helper()

	value, err := proto.Marshal(&kafkamessage.KafkaTxValidationTopicMessage{Tx: sampleTx})
	require.NoError(t, err)

	return &kafka.KafkaMessage{Value: value}
}

// An accepted tx whose unlock failed must not count as an invalid tx. That counter is the signal
// for malformed or attack traffic.
func TestHandleKafkaTxMessage_UnlockFailureIsNotInvalid(t *testing.T) {
	server := newUnlockFailedServer(t, unlockFailedErr())

	before := testutil.ToFloat64(prometheusInvalidTransactions)

	require.NoError(t, server.handleKafkaTxMessage(context.Background(), kafkaTxMessage(t)))
	require.Equal(t, before, testutil.ToFloat64(prometheusInvalidTransactions))
}

func TestHandleKafkaTxMessage_RealFailureStillCounts(t *testing.T) {
	server := newUnlockFailedServer(t, errors.NewTxInvalidError("bad tx"))

	before := testutil.ToFloat64(prometheusInvalidTransactions)

	require.Error(t, server.handleKafkaTxMessage(context.Background(), kafkaTxMessage(t)))
	require.Equal(t, before+1, testutil.ToFloat64(prometheusInvalidTransactions))
}

// The gRPC handler keeps returning the typed error, so callers such as subtree validation can tell
// "accepted, still locked" apart from a real failure, but it must not count an invalid tx.
func TestValidateTransaction_UnlockFailureIsNotInvalid(t *testing.T) {
	server := newUnlockFailedServer(t, unlockFailedErr())

	before := testutil.ToFloat64(prometheusInvalidTransactions)

	_, err := server.ValidateTransaction(context.Background(), &validator_api.ValidateTransactionRequest{TransactionData: sampleTx, BlockHeight: 100})
	require.ErrorIs(t, errors.UnwrapGRPC(err), errors.ErrTxUnlockFailed, "the typed error must survive the gRPC hop")
	require.Equal(t, before, testutil.ToFloat64(prometheusInvalidTransactions))
}

func TestHandleSingleTx_UnlockFailureIsOK(t *testing.T) {
	server := newUnlockFailedServer(t, unlockFailedErr())

	rec := httptest.NewRecorder()
	c := echo.New().NewContext(httptest.NewRequest(http.MethodPost, "/tx", strings.NewReader(string(sampleTx))), rec)

	require.NoError(t, server.handleSingleTx(context.Background())(c))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "OK", rec.Body.String())
}

func TestHandleMultipleTx_UnlockFailureIsOK(t *testing.T) {
	server := newUnlockFailedServer(t, unlockFailedErr())

	rec := httptest.NewRecorder()
	c := echo.New().NewContext(httptest.NewRequest(http.MethodPost, "/txs", strings.NewReader(string(sampleTx)+string(sampleTx))), rec)

	require.NoError(t, server.handleMultipleTx(context.Background())(c))
	require.Equal(t, http.StatusOK, rec.Code)
}
