package pruner

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// makeInputBytes builds a minimal Aerospike input-bin entry consisting of
// a 32-byte previous TXID plus a 4-byte little-endian previous output index.
// This matches the wire format consumed by extractInputReference.
func makeInputBytes(t *testing.T, parentTxID chainhash.Hash, prevIndex uint32) []byte {
	t.Helper()
	buf := make([]byte, 36)
	copy(buf[0:32], parentTxID[:])
	binary.LittleEndian.PutUint32(buf[32:36], prevIndex)
	return buf
}

// makeChildResult constructs an aerospike.Result that processRecordChunk will
// treat as a non-external, non-defensive, deletable record with the supplied
// parent inputs. The child txid is also synthesised from the index seed.
func makeChildResult(t *testing.T, s *Service, childSeed byte, parents []chainhash.Hash) *aerospike.Result {
	t.Helper()

	var childTxID chainhash.Hash
	for i := range childTxID {
		childTxID[i] = childSeed
	}

	inputs := make([]interface{}, 0, len(parents))
	for i, p := range parents {
		inputs = append(inputs, makeInputBytes(t, p, uint32(i)))
	}

	key, err := aerospike.NewKey(s.namespace, s.set, childTxID[:])
	require.NoError(t, err)

	bins := aerospike.BinMap{
		s.fieldTxID:     childTxID.CloneBytes(),
		s.fieldInputs:   inputs,
		s.fieldExternal: false,
	}

	return &aerospike.Result{
		Record: &aerospike.Record{
			Key:  key,
			Bins: bins,
		},
	}
}

// newTestServiceForSkip builds a Service configured for direct unit testing of
// processRecordChunk's prunedSet skip path. Defensive mode is off and
// SkipDeletions is on so the deletion path stays gated; the test only
// exercises chunks where ALL inputs reference parents already in the set,
// so flushCleanupBatches never attempts a real Aerospike call.
func newTestServiceForSkip(t *testing.T) *Service {
	t.Helper()
	ensurePrometheusMetrics()

	return &Service{
		logger: ulogger.NewVerboseTestLogger(t),
		settings: &settings.Settings{
			Pruner: settings.PrunerSettings{
				SkipDeletions: true,
			},
		},
		namespace:            "test",
		set:                  "test",
		utxoBatchSize:        128,
		defensiveEnabled:     false,
		fieldTxID:            fields.TxID.String(),
		fieldUtxos:           fields.Utxos.String(),
		fieldInputs:          fields.Inputs.String(),
		fieldDeletedChildren: fields.DeletedChildren.String(),
		fieldExternal:        fields.External.String(),
		fieldDeleteAtHeight:  fields.DeleteAtHeight.String(),
		fieldTotalExtraRecs:  fields.TotalExtraRecs.String(),
		fieldUnminedSince:    fields.UnminedSince.String(),
		fieldBlockHeights:    fields.BlockHeights.String(),
	}
}

// TestProcessRecordChunk_SkipsParentsInPrunedSet verifies that when every
// parent TXID referenced by a chunk is registered in the shared PrunedTxSet,
// processRecordChunk:
//   - omits all parent-update accumulation
//   - increments utxo_pruner_parents_skipped_pruned_total once per input
//   - reports the child record as processed (skipped count = 0)
//
// Because all parent updates are skipped, flushCleanupBatches receives an
// empty parentUpdates map and never touches the Aerospike client — making
// the test deterministic without a real cluster.
func TestProcessRecordChunk_SkipsParentsInPrunedSet(t *testing.T) {
	ctx := context.Background()
	svc := newTestServiceForSkip(t)

	// Two distinct parents to confirm the metric increments per input.
	// PrunedTxSet.CheckAndRemove is destructive, but each parent appears
	// only once in the chunk, so the destructive semantic is fine here.
	var parentA chainhash.Hash
	for i := range parentA {
		parentA[i] = 0xAA
	}
	var parentB chainhash.Hash
	for i := range parentB {
		parentB[i] = 0xBB
	}

	prunedSet := NewPrunedTxSet(4, 0)
	prunedSet.Add(parentA)
	prunedSet.Add(parentB)
	require.Equal(t, 2, prunedSet.Len())

	chunk := []*aerospike.Result{
		makeChildResult(t, svc, 0x11, []chainhash.Hash{parentA, parentB}),
	}

	before := testutil.ToFloat64(prometheusUtxoParentsSkippedPruned)

	processed, skipped, err := svc.processRecordChunk(ctx, 1000, chunk, prunedSet)
	require.NoError(t, err)
	require.Equal(t, 1, processed, "the record itself is still processed for deletion")
	require.Equal(t, 0, skipped, "no defensive skip when defensive mode is off")

	after := testutil.ToFloat64(prometheusUtxoParentsSkippedPruned)
	require.Equal(t, float64(2), after-before,
		"each input whose parent is in the set must increment the skipped-pruned metric")

	require.Equal(t, 0, prunedSet.Len(), "both parents should have been removed by CheckAndRemove")
}

// TestProcessRecordChunk_EmptyPrunedSetIsNoOp verifies that an empty
// PrunedTxSet does not cause any skipped-pruned increments. The chunk is
// crafted with zero inputs so no parent updates accumulate and the
// flushCleanupBatches deletion path stays gated by SkipDeletions.
func TestProcessRecordChunk_EmptyPrunedSetIsNoOp(t *testing.T) {
	ctx := context.Background()
	svc := newTestServiceForSkip(t)

	var childTxID chainhash.Hash
	for i := range childTxID {
		childTxID[i] = 0x77
	}

	key, keyErr := aerospike.NewKey(svc.namespace, svc.set, childTxID[:])
	require.NoError(t, keyErr)

	// Empty inputs (e.g. coinbase) — getTxInputsFromBins returns an empty
	// slice, so no parent loop runs.
	chunk := []*aerospike.Result{
		{
			Record: &aerospike.Record{
				Key: key,
				Bins: aerospike.BinMap{
					svc.fieldTxID:     childTxID.CloneBytes(),
					svc.fieldInputs:   []interface{}{},
					svc.fieldExternal: false,
				},
			},
		},
	}

	prunedSet := NewPrunedTxSet(4, 0)

	before := testutil.ToFloat64(prometheusUtxoParentsSkippedPruned)

	processed, skipped, err := svc.processRecordChunk(ctx, 1000, chunk, prunedSet)
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	require.Equal(t, 0, skipped)

	after := testutil.ToFloat64(prometheusUtxoParentsSkippedPruned)
	require.Equal(t, float64(0), after-before)
}
