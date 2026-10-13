//go:build teraslab

package teraslab_test

import (
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo"
	teraslabstore "github.com/bsv-blockchain/teranode/stores/utxo/teraslab"
	"github.com/stretchr/testify/require"
)

func TestBlockHeight(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()

	t.Run("initial block height is 1 (set by initTeraSlab)", func(t *testing.T) {
		require.Equal(t, uint32(1), store.GetBlockHeight())
	})

	t.Run("SetBlockHeight zero returns error", func(t *testing.T) {
		err := store.SetBlockHeight(0)
		require.Error(t, err)
	})

	t.Run("SetBlockHeight stores value", func(t *testing.T) {
		err := store.SetBlockHeight(12345)
		require.NoError(t, err)
		require.Equal(t, uint32(12345), store.GetBlockHeight())
	})
}

func TestMedianBlockTime(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()

	t.Run("initial median time is 0", func(t *testing.T) {
		require.Equal(t, uint32(0), store.GetMedianBlockTime())
	})

	t.Run("SetMedianBlockTime stores value", func(t *testing.T) {
		err := store.SetMedianBlockTime(1640995200)
		require.NoError(t, err)
		require.Equal(t, uint32(1640995200), store.GetMedianBlockTime())
	})
}

func TestGetBlockState(t *testing.T) {
	store, _, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()

	err := store.SetBlockHeight(500)
	require.NoError(t, err)
	err = store.SetMedianBlockTime(1640995200)
	require.NoError(t, err)

	state := store.GetBlockState()
	require.Equal(t, uint32(500), state.Height)
	require.Equal(t, uint32(1640995200), state.MedianTime)
}

func TestHealth(t *testing.T) {
	store, ctx, deferFn := initTeraSlabWithDefaults(t)
	defer deferFn()

	// Shallow check: server Health only, no liveness Ping.
	status, details, err := store.Health(ctx, false)
	require.NoError(t, err)
	require.Equal(t, 200, status)
	require.Contains(t, details, "TeraSlab")

	// Liveness check: server Health plus a Ping round trip.
	status, details, err = store.Health(ctx, true)
	require.NoError(t, err)
	require.Equal(t, 200, status)
	require.Contains(t, details, "TeraSlab")
}

func TestInterfaceCompliance(t *testing.T) {
	var _ utxo.Store = (*teraslabstore.Store)(nil)
}
