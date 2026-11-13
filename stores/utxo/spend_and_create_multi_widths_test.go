package utxo

import (
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/require"
)

// TestBelowCheckpointCreateWidth covers belowCheckpointCreateWidth's cases: an
// explicit setting always wins, the derived value is batch size times batcher
// workers, capped at the ceiling, and floored at 8x the batch size when the
// store has no batcher worker cap (BatcherMaxConcurrent <= 0).
func TestBelowCheckpointCreateWidth(t *testing.T) {
	t.Run("explicit setting wins over derivation", func(t *testing.T) {
		bv := &settings.BlockValidationSettings{QuickValidateCreateConcurrency: 250}
		store := &settings.UtxoStoreSettings{StoreBatcherSize: 50, BatcherMaxConcurrent: 64}

		require.Equal(t, 250, belowCheckpointCreateWidth(bv, store))
	})

	t.Run("derives batch size times batcher workers", func(t *testing.T) {
		bv := &settings.BlockValidationSettings{QuickValidateCreateConcurrency: 0}
		store := &settings.UtxoStoreSettings{StoreBatcherSize: 50, BatcherMaxConcurrent: 64}

		require.Equal(t, 50*64, belowCheckpointCreateWidth(bv, store))
	})

	t.Run("floors at 8x batch size when batcher workers unbounded", func(t *testing.T) {
		bv := &settings.BlockValidationSettings{QuickValidateCreateConcurrency: 0}
		store := &settings.UtxoStoreSettings{StoreBatcherSize: 50, BatcherMaxConcurrent: 0}

		require.Equal(t, 50*8, belowCheckpointCreateWidth(bv, store))
	})

	// At the default batch size of 2048 and 64 batcher workers the product is
	// 131,072 callers, each a goroutine holding a transaction. The derived value
	// stops at the ceiling.
	t.Run("derived value is capped at the ceiling", func(t *testing.T) {
		bv := &settings.BlockValidationSettings{QuickValidateCreateConcurrency: 0}
		store := &settings.UtxoStoreSettings{StoreBatcherSize: 2048, BatcherMaxConcurrent: 64}

		require.Equal(t, belowCheckpointDerivedCreateCeiling, belowCheckpointCreateWidth(bv, store))
	})

	t.Run("the ceiling never goes below the 8x floor", func(t *testing.T) {
		bv := &settings.BlockValidationSettings{QuickValidateCreateConcurrency: 0}
		store := &settings.UtxoStoreSettings{StoreBatcherSize: 4096, BatcherMaxConcurrent: 64}

		require.Equal(t, 4096*8, belowCheckpointCreateWidth(bv, store))
	})

	t.Run("an explicit setting is not capped", func(t *testing.T) {
		bv := &settings.BlockValidationSettings{QuickValidateCreateConcurrency: 100_000}
		store := &settings.UtxoStoreSettings{StoreBatcherSize: 2048, BatcherMaxConcurrent: 64}

		require.Equal(t, 100_000, belowCheckpointCreateWidth(bv, store))
	})

	t.Run("SpendAndCreateMultiWidthsFor uses it for the first wave", func(t *testing.T) {
		tSettings := &settings.Settings{
			BlockValidation: settings.BlockValidationSettings{QuickValidateCreateConcurrency: 0},
			UtxoStore:       settings.UtxoStoreSettings{StoreBatcherSize: 50, BatcherMaxConcurrent: 64},
		}

		require.Equal(t, 50*64, SpendAndCreateMultiWidthsFor(tSettings).Create)
	})
}
