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

// newTestService builds a Service configured for direct unit testing of
// processRecordChunk. Defensive mode is off and SkipDeletions is on so the
// deletion path stays gated and flushCleanupBatches never attempts a real
// Aerospike call.
func newTestService(t *testing.T) *Service {
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

// TestProcessRecordChunk_NilPrunedSetIsNoOp verifies that the nil prunedSet
// production always passes (the cuckoo skip is disabled, see #1701) causes no
// skipped-pruned increments. The chunk is crafted with zero inputs so no
// parent updates accumulate and the flushCleanupBatches deletion path stays
// gated by SkipDeletions.
func TestProcessRecordChunk_NilPrunedSetIsNoOp(t *testing.T) {
	ctx := context.Background()
	svc := newTestService(t)

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

	before := testutil.ToFloat64(prometheusUtxoParentsSkippedPruned)

	processed, skipped, err := svc.processRecordChunk(ctx, 1000, chunk, nil)
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	require.Equal(t, 0, skipped)

	after := testutil.ToFloat64(prometheusUtxoParentsSkippedPruned)
	require.Equal(t, float64(0), after-before)
}
