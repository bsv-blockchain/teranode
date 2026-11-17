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
