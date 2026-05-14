package aerospike

import (
	"context"
	"testing"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/assert"
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

// newTestStoreForSetMinedExpressions builds the minimum Store fields
// processBatchResultsForSetMinedExpressions touches when iterating purely
// error/FILTERED_OUT batch records (no DAH follow-ups fire on this path).
func newTestStoreForSetMinedExpressions(t *testing.T) *Store {
	t.Helper()

	InitPrometheusMetrics()

	tSettings := &settings.Settings{}
	tSettings.Aerospike.UseDefaultPolicies = true

	return &Store{
		ctx:       context.Background(),
		namespace: "test-ns",
		setName:   "test-set",
		logger:    ulogger.TestLogger{},
		settings:  tSettings,
	}
}

// newFakeBatchWrite produces a real *aerospike.BatchWrite (which satisfies
// aerospike.BatchRecordIfc) so a test can mutate its per-record Err without
// going through the real client.
func newFakeBatchWrite(t *testing.T, hash *chainhash.Hash) aerospike.BatchRecordIfc {
	t.Helper()
	key, err := aerospike.NewKey("test-ns", "test-set", hash[:])
	require.NoError(t, err)
	// Minimal op set; the production code only reads BatchRec().Err and
	// BatchRec().Record on this path.
	return aerospike.NewBatchWrite(nil, key, aerospike.PutOp(aerospike.NewBin("dummy", 0)))
}

// TestProcessBatchResultsForSetMinedExpressions_FilteredOutSynthesizesMapEntry
// pins the soundness comment in processBatchResultsForSetMinedExpressions:
// FILTERED_OUT proves the current blockID is already present in the durable
// list (filter expression: count(blockID in blockIDs) == 0 -> run), so the
// function MUST synthesize blockIDs[hash] = [minedBlockInfo.BlockID] and count
// the record as a successful update. Any regression that drops the synthesis
// (or returns an error) would silently break batch-coverage at the model
// layer.
func TestProcessBatchResultsForSetMinedExpressions_FilteredOutSynthesizesMapEntry(t *testing.T) {
	s := newTestStoreForSetMinedExpressions(t)

	hash := &chainhash.Hash{0xAA}
	rec := newFakeBatchWrite(t, hash)
	rec.BatchRec().Err = &aerospike.AerospikeError{ResultCode: types.FILTERED_OUT}

	const blockID uint32 = 1234
	got, err := s.processBatchResultsForSetMinedExpressions(
		context.Background(),
		[]aerospike.BatchRecordIfc{rec},
		[]*chainhash.Hash{hash},
		blockID+1, // thisBlockHeight; unused on the FILTERED_OUT path
		utxo.MinedBlockInfo{BlockID: blockID},
	)

	require.NoError(t, err, "FILTERED_OUT must be treated as a successful idempotent retry, not an error")
	require.Contains(t, got, *hash, "hash must be present in the returned map so the model-layer coverage check passes")
	assert.Equal(t, []uint32{blockID}, got[*hash], "synthesized slice must contain exactly the current blockID")
}

// TestProcessBatchResultsForSetMinedExpressions_KeyNotFoundIsHardError pairs
// with the synthesis test above: an unrelated AerospikeError (here
// KEY_NOT_FOUND_ERROR) must NOT be silently dropped — it returns an error and
// leaves the hash absent from the result map.
func TestProcessBatchResultsForSetMinedExpressions_KeyNotFoundIsHardError(t *testing.T) {
	s := newTestStoreForSetMinedExpressions(t)

	hash := &chainhash.Hash{0xBB}
	rec := newFakeBatchWrite(t, hash)
	rec.BatchRec().Err = &aerospike.AerospikeError{ResultCode: types.KEY_NOT_FOUND_ERROR}

	got, err := s.processBatchResultsForSetMinedExpressions(
		context.Background(),
		[]aerospike.BatchRecordIfc{rec},
		[]*chainhash.Hash{hash},
		100,
		utxo.MinedBlockInfo{BlockID: 99},
	)

	require.Error(t, err)
	assert.NotContains(t, got, *hash, "KEY_NOT_FOUND must not synthesize a result-map entry")
}
