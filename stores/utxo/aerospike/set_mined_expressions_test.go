package aerospike

import (
	"testing"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/stretchr/testify/require"
)

func TestParseSetMinedState(t *testing.T) {
	store := &Store{}

	bins := map[string]interface{}{
		"blockIDs":       []interface{}{100, 101, 102},
		"totalExtraRecs": 5,
		"external":       true,
	}

	state, err := store.parseSetMinedState(bins)
	require.NoError(t, err)

	require.Equal(t, []uint32{100, 101, 102}, state.BlockIDs)
	require.NotNil(t, state.TotalExtraRecs)
	require.Equal(t, 5, *state.TotalExtraRecs)
	require.True(t, state.External)
}

func TestParseSetMinedState_EmptyBins(t *testing.T) {
	store := &Store{}

	bins := map[string]interface{}{}

	state, err := store.parseSetMinedState(bins)
	require.NoError(t, err)

	require.Empty(t, state.BlockIDs)
	require.Nil(t, state.TotalExtraRecs)
	require.False(t, state.External)
}

func TestParseSetMinedState_OpResults(t *testing.T) {
	store := &Store{}

	// OpResults is a []interface{} alias, simulate the compound result format
	// from ListAppend followed by GetBinOp: [count, [values]]
	bins := map[string]interface{}{
		"blockIDs": aerospike.OpResults{1, []interface{}{100, 101}},
	}

	state, err := store.parseSetMinedState(bins)
	require.NoError(t, err)

	require.Equal(t, []uint32{100, 101}, state.BlockIDs)
}

func TestParseSetMinedState_IntSlice(t *testing.T) {
	store := &Store{}

	bins := map[string]interface{}{
		"blockIDs": []uint32{100, 101, 102},
	}

	state, err := store.parseSetMinedState(bins)
	require.NoError(t, err)

	require.Equal(t, []uint32{100, 101, 102}, state.BlockIDs)
}
