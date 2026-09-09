package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// TestBlockStateIsOneSnapshot pins the reason this store embeds
// utxo.BlockStateFields instead of carrying its own height and median-time
// atomics: the pair GetBlockState returns has to be the same memory the
// single-field getters read, or a caller making two calls can be handed a
// height from one chain tip and a median time from another.
//
// The stub this replaced returned an empty utxo.BlockState regardless of what
// had been set, so every finality decision reading through GetBlockState saw
// height 0 and median time 0 on a store that knew better. That is what the
// first assertion here would have caught.
func TestBlockStateIsOneSnapshot(t *testing.T) {
	var s Store

	require.NoError(t, s.SetBlockState(800_000, 1_700_000_000))
	require.Equal(t, utxo.BlockState{Height: 800_000, MedianTime: 1_700_000_000}, s.GetBlockState())
	require.Equal(t, uint32(800_000), s.GetBlockHeight())
	require.Equal(t, uint32(1_700_000_000), s.GetMedianBlockTime())

	// A height-only write must carry the median time forward rather than
	// clearing it, so a height-only bootstrap cannot erase a known tip time.
	require.NoError(t, s.SetBlockHeight(800_001))
	require.Equal(t, utxo.BlockState{Height: 800_001, MedianTime: 1_700_000_000}, s.GetBlockState())

	// And the reverse: a median-time-only write keeps the height.
	require.NoError(t, s.SetMedianBlockTime(1_700_000_600))
	require.Equal(t, utxo.BlockState{Height: 800_001, MedianTime: 1_700_000_600}, s.GetBlockState())
}

// TestSetBlockHeightRejectsZero holds this store to the same rule as the rest:
// height zero cannot be told apart from a store that was never written, so
// accepting it would let a caller silently reset the tip.
func TestSetBlockHeightRejectsZero(t *testing.T) {
	var s Store

	require.NoError(t, s.SetBlockHeight(700_000))
	require.Error(t, s.SetBlockHeight(0))
	require.Error(t, s.SetBlockState(0, 123))
	require.Equal(t, uint32(700_000), s.GetBlockHeight(), "a rejected write must not disturb the stored tip")
}
