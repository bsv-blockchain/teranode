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

// TestParseSetMinedState_ExplicitEmptyList covers the input that
// processBatchResultsForSetMinedExpressions would see if the Aerospike record
// returned the blockIDs bin as an empty list. The caller gates entry into the
// returned map on `len(state.BlockIDs) > 0`, so an empty list here means the
// hash is silently dropped from the result map — and the model-layer coverage
// check is the only remaining guard (see Test_updateTxMinedStatus_Internal/
// "should error when SetMinedMulti partially omits hashes").
func TestParseSetMinedState_ExplicitEmptyList(t *testing.T) {
	store := &Store{}

	bins := map[string]interface{}{
		"blockIDs": []interface{}{},
	}

	state, err := store.parseSetMinedState(bins)
	require.NoError(t, err)
	require.Empty(t, state.BlockIDs, "empty bin list must produce zero-length BlockIDs so the caller omits the hash from the result map")
}
