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
// classifier. The spending transaction was mined, fully spent and pruned, so the
// rejection is permanent and about chain state, exactly like ErrSpent beside it.
// Unclassified it fell through to 500, telling a submitter their settled,
// deterministic rejection was a server fault.
func TestStatusForPrunedSpendingTx(t *testing.T) {
	require.Equal(t, http.StatusConflict, httpStatusForTxError(errors.ErrUtxoSpendingTxPruned))
	require.Equal(t, http.StatusConflict,
		httpStatusForTxError(errors.NewProcessingError("wrapped", errors.NewUtxoSpendingTxPrunedError("pruned"))))
}
