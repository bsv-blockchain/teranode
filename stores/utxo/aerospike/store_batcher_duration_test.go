package aerospike

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/require"
)

func TestStoreBatcherDuration(t *testing.T) {
	t.Run("uses utxostore_storeBatcherDurationMillis when the deprecated key is unset", func(t *testing.T) {
		tSettings := &settings.Settings{}
		tSettings.UtxoStore.StoreBatcherDurationMillis = 3

		d, deprecated := storeBatcherDuration(tSettings)

		require.Equal(t, 3*time.Millisecond, d)
		require.False(t, deprecated)
	})

	t.Run("deprecated aerospike_storeBatcherDuration overrides when set", func(t *testing.T) {
		tSettings := &settings.Settings{}
		tSettings.UtxoStore.StoreBatcherDurationMillis = 3
		tSettings.Aerospike.StoreBatcherDuration = 7 * time.Millisecond

		d, deprecated := storeBatcherDuration(tSettings)

		require.Equal(t, 7*time.Millisecond, d)
		require.True(t, deprecated)
	})
}

func TestStoreBatcherDurationDefaults(t *testing.T) {
	tSettings := settings.NewSettings()

	// With neither key set, the Create batcher keeps its historical 10ms timeout.
	d, deprecated := storeBatcherDuration(tSettings)

	require.Equal(t, 10*time.Millisecond, d)
	require.False(t, deprecated)
}
