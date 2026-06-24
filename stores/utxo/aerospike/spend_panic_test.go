package aerospike

import (
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestHandleSpendPanic verifies the deferred recover helper used by Spend
// surfaces a recovered panic as an error. Without this, Spend's unnamed
// returns silently produced (nil, nil) on panic — masking UTXO state
// corruption from callers.
func TestHandleSpendPanic(t *testing.T) {
	InitPrometheusMetrics()

	logger := ulogger.TestLogger{}

	t.Run("nil recovered leaves err untouched", func(t *testing.T) {
		var err error
		handleSpendPanic(nil, &err, logger)
		require.NoError(t, err)
	})

	t.Run("recovered value sets err when nil", func(t *testing.T) {
		var err error
		handleSpendPanic("boom", &err, logger)
		require.Error(t, err)
		require.Contains(t, err.Error(), "panic")
		require.Contains(t, err.Error(), "boom")
	})

	t.Run("existing err is preserved", func(t *testing.T) {
		var (
			original error = errors.NewUnknownError("original failure")
			err            = original
		)
		handleSpendPanic("late panic", &err, logger)
		require.Same(t, original, err)
	})
}
