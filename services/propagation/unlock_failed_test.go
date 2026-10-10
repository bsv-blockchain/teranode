package propagation

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/propagation/propagation_api"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/blob/null"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/stretchr/testify/require"
)

func processWithValidatorError(t *testing.T, validateErr error) error {
	t.Helper()

	tracing.SetupMockTracer()

	txStore, err := null.New(ulogger.TestLogger{})
	require.NoError(t, err)

	ps := &PropagationServer{
		logger:   ulogger.TestLogger{},
		settings: test.CreateBaseTestSettings(t),
		validator: &validator.MockValidator{
			ValidateFunc: func(context.Context, *bt.Tx) (*meta.Data, error) {
				return nil, validateErr
			},
		},
		txStore: txStore,
	}

	txs := transactions.CreateTestTransactionChainWithCount(t, 3)

	_, err = ps.ProcessTransaction(context.Background(), &propagation_api.ProcessTransactionRequest{Tx: txs[1].ExtendedBytes()})

	return err
}

// The validator accepted the tx and only its unlock failed, so the submitter must not be told it failed.
func TestProcessTransaction_UnlockFailureIsAccepted(t *testing.T) {
	err := processWithValidatorError(t, errors.NewTxUnlockFailedError("[Validate] error marking tx as spendable", errors.NewProcessingError("context deadline exceeded")))
	require.NoError(t, err)
}

func TestProcessTransaction_RealValidationFailureStillRejects(t *testing.T) {
	err := processWithValidatorError(t, errors.NewTxInvalidError("bad tx"))
	require.Error(t, err)
}
