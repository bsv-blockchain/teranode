package propagation

import (
	"net/http"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// TestHTTPStatusForTxError_ThresholdExceeded pins the propagation HTTP surface's
// mapping for a block-assembly shed: ErrThresholdExceeded is a retryable overload
// (503 Service Unavailable), not a generic 500. Every other unmapped error still
// falls through to 500.
func TestHTTPStatusForTxError_ThresholdExceeded(t *testing.T) {
	require.Equal(t, http.StatusServiceUnavailable, httpStatusForTxError(errors.ErrThresholdExceeded))
	require.Equal(t, http.StatusServiceUnavailable, httpStatusForTxError(errors.NewThresholdExceededError("wrapped")))
	require.Equal(t, http.StatusInternalServerError, httpStatusForTxError(errors.NewProcessingError("other")))
}

// TestStatusForPrunedSpendingTx pins ERR_UTXO_SPENDING_TX_PRUNED on the HTTP
// classifier. It is answered with HTTP 409, the same status as ErrSpent beside
// it. It is this node's record, not a consensus verdict: usually the spender was
// mined, fully spent and pruned here, but the same code also answers a spend an
// attempt left behind without creating its transaction, and an Aerospike
// deletedChildren marker. Unclassified it fell through to 500, telling a
// submitter that the answer was a server fault.
func TestStatusForPrunedSpendingTx(t *testing.T) {
	require.Equal(t, http.StatusConflict, httpStatusForTxError(errors.ErrUtxoSpendingTxPruned))
	require.Equal(t, http.StatusConflict,
		httpStatusForTxError(errors.NewProcessingError("wrapped", errors.NewUtxoSpendingTxPrunedError("pruned"))))
}

// TestHTTPStatusForTxError_DAHGraceFault pins the arm order for a DAH-grace
// fault: a missing-parent error wrapping a node fault keeps its 422, also after
// the gRPC hop from the validator, while a bare node fault still maps to 503.
func TestHTTPStatusForTxError_DAHGraceFault(t *testing.T) {
	faults := map[string]error{
		"chain check": errors.NewServiceUnavailableError("current-chain check failed"),
		"meta read":   errors.NewStorageError("meta read failed"),
	}

	for name, fault := range faults {
		t.Run(name, func(t *testing.T) {
			graceErr := errors.NewTxMissingParentError("parent missing and DAH grace undecided", fault)

			require.Equal(t, http.StatusUnprocessableEntity, httpStatusForTxError(graceErr))
			require.Equal(t, http.StatusUnprocessableEntity, httpStatusForTxError(errors.UnwrapGRPC(errors.WrapGRPC(graceErr))))
		})
	}

	require.Equal(t, http.StatusServiceUnavailable, httpStatusForTxError(errors.NewServiceUnavailableError("bare fault")))
}
