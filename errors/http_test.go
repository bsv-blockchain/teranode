package errors

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestHTTPErrorRoundTrip pins the projection: an internal HTTP hop must carry
// enough for the receiver to classify the failure, and nothing more.
func TestHTTPErrorRoundTrip(t *testing.T) {
	t.Run("verdict survives with its code", func(t *testing.T) {
		h := http.Header{}
		AttachHTTPError(h, NewProcessingError("[Validate][abc] failed",
			NewTxPolicyError("insufficient-fee")))

		got := HTTPErrorFrom(h)
		require.NotNil(t, got)
		require.Equal(t, ERR_TX_POLICY, got.Code())
		require.Equal(t, "insufficient-fee", got.Message())
	})

	t.Run("node fault keeps the outermost code and does not leak the cause", func(t *testing.T) {
		h := http.Header{}
		AttachHTTPError(h, NewProcessingError("could not reach the utxo store",
			NewStorageError("aerospike: connection refused at 10.0.0.4:3000")))

		got := HTTPErrorFrom(h)
		require.NotNil(t, got)
		require.Equal(t, ERR_PROCESSING, got.Code())
		require.NotContains(t, got.Message(), "10.0.0.4")
	})

	t.Run("absent, empty and malformed headers yield nil", func(t *testing.T) {
		require.Nil(t, HTTPErrorFrom(nil))
		require.Nil(t, HTTPErrorFrom(http.Header{}))

		h := http.Header{}
		h.Set(HTTPErrorHeader, "not base64!!")
		require.Nil(t, HTTPErrorFrom(h))

		h.Set(HTTPErrorHeader, "//////8=") // valid base64, not a TError
		require.Nil(t, HTTPErrorFrom(h))
	})

	t.Run("nil error and nil header are no-ops", func(t *testing.T) {
		h := http.Header{}
		AttachHTTPError(h, nil)
		require.Empty(t, h.Get(HTTPErrorHeader))
		require.NotPanics(t, func() { AttachHTTPError(nil, NewTxInvalidError("x")) })
	})

	t.Run("an oversized message is truncated, never dropped", func(t *testing.T) {
		h := http.Header{}
		AttachHTTPError(h, NewTxInvalidError("%s", strings.Repeat("é", 8*1024)))

		value := h.Get(HTTPErrorHeader)
		require.NotEmpty(t, value, "the code must survive even when the message will not")
		require.LessOrEqual(t, len(value), maxHTTPErrorHeaderBytes)

		got := HTTPErrorFrom(h)
		require.NotNil(t, got, "a truncated message must still decode")
		require.Equal(t, ERR_TX_INVALID, got.Code())
		require.Contains(t, got.Message(), truncationSuffix)
	})

	t.Run("invalid UTF-8 is stripped rather than failing the marshal", func(t *testing.T) {
		h := http.Header{}
		AttachHTTPError(h, NewTxInvalidError("%s", "bad-txns\xff-oversize"))

		got := HTTPErrorFrom(h)
		require.NotNil(t, got)
		require.Equal(t, "bad-txns-oversize", got.Message())
	})
}
