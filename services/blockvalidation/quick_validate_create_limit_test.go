package blockvalidation

import (
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/require"
)

// TestQuickValidateCreateLimit covers quickValidateCreateLimit's three cases:
// an explicit setting always wins, the derived value is batch size times batcher
// workers, and the derived value floors at 8x the batch size when the store has
// no batcher worker cap (BatcherMaxConcurrent <= 0).
func TestQuickValidateCreateLimit(t *testing.T) {
	t.Run("explicit setting wins over derivation", func(t *testing.T) {
		bv := &settings.BlockValidationSettings{QuickValidateCreateConcurrency: 250}
		store := &settings.UtxoStoreSettings{StoreBatcherSize: 50, BatcherMaxConcurrent: 64}

		require.Equal(t, 250, quickValidateCreateLimit(bv, store))
	})

	t.Run("derives batch size times batcher workers", func(t *testing.T) {
		bv := &settings.BlockValidationSettings{QuickValidateCreateConcurrency: 0}
		store := &settings.UtxoStoreSettings{StoreBatcherSize: 50, BatcherMaxConcurrent: 64}

		require.Equal(t, 50*64, quickValidateCreateLimit(bv, store))
	})

	t.Run("floors at 8x batch size when batcher workers unbounded", func(t *testing.T) {
		bv := &settings.BlockValidationSettings{QuickValidateCreateConcurrency: 0}
		store := &settings.UtxoStoreSettings{StoreBatcherSize: 50, BatcherMaxConcurrent: 0}

		require.Equal(t, 50*8, quickValidateCreateLimit(bv, store))
	})
}
