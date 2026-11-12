package rpc

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/rpc/bsvjson"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/stretchr/testify/require"
)

type erroringValidator struct {
	validator.Interface
	err error
}

func (e erroringValidator) Validate(context.Context, *bt.Tx, uint32, ...validator.Option) (*meta.Data, error) {
	return nil, e.err
}

// The validator accepted the tx and only its unlock failed, so sendrawtransaction must not report a rejection.
func TestHandleSendRawTransaction_UnlockFailureIsAccepted(t *testing.T) {
	initPrometheusMetrics()

	s := newRPCServerForAbsurdFeeTest(t, 10_000_000, 100_000_000, erroringValidator{err: errors.NewTxUnlockFailedError("[Validate] error marking tx as spendable", errors.NewProcessingError("context deadline exceeded"))})

	cmd := buildSendRawTxCmd(t, 99_999_000, nil)

	txid, err := handleSendRawTransaction(context.Background(), s, cmd, nil)
	require.NoError(t, err)
	require.NotEmpty(t, txid)
}

func TestHandleSendRawTransaction_RealValidationFailureStillRejects(t *testing.T) {
	initPrometheusMetrics()

	s := newRPCServerForAbsurdFeeTest(t, 10_000_000, 100_000_000, erroringValidator{err: errors.NewTxInvalidError("bad tx")})

	cmd := buildSendRawTxCmd(t, 99_999_000, nil)

	_, err := handleSendRawTransaction(context.Background(), s, cmd, nil)
	require.Error(t, err)

	rpcErr, ok := err.(*bsvjson.RPCError)
	require.True(t, ok)
	require.Contains(t, rpcErr.Message, txRejectedPrefix)
}
