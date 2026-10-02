package pruner

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func newChunkTestService(t *testing.T) *Service {
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

func TestProcessRecordChunk_EmptyInputs(t *testing.T) {
	ctx := context.Background()
	svc := newChunkTestService(t)

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

	processed, skipped, err := svc.processRecordChunk(ctx, 1000, chunk)
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	require.Equal(t, 0, skipped)

}

// TestProcessRecordChunk_MissingExternalTxIsSkippedNotFatal proves that a record
// whose external blob has vanished is retained on its own without aborting the
// chunk. Before the fix this returned a ProcessingError, which PruneWithPartitions
// classifies as a non-timeout error and never retries, so one bad record stopped
// all pruning permanently.
func TestProcessRecordChunk_MissingExternalTxIsSkippedNotFatal(t *testing.T) {
	ctx := context.Background()
	svc := newChunkTestService(t)
	svc.external = memory.New()

	var missingTxID, healthyTxID chainhash.Hash
	for i := range missingTxID {
		missingTxID[i] = 0x11
		healthyTxID[i] = 0x22
	}

	missingKey, keyErr := aerospike.NewKey(svc.namespace, svc.set, missingTxID[:])
	require.NoError(t, keyErr)

	healthyKey, keyErr := aerospike.NewKey(svc.namespace, svc.set, healthyTxID[:])
	require.NoError(t, keyErr)

	// Neither the .tx nor the .outputs blob exists for missingTxID, so its input
	// references cannot be recovered and its parents cannot be marked.
	exists, err := svc.external.Exists(ctx, missingTxID[:], fileformat.FileTypeOutputs)
	require.NoError(t, err)
	require.False(t, exists)

	chunk := []*aerospike.Result{
		{
			Record: &aerospike.Record{
				Key: missingKey,
				Bins: aerospike.BinMap{
					svc.fieldTxID:     missingTxID.CloneBytes(),
					svc.fieldExternal: true,
				},
			},
		},
		{
			Record: &aerospike.Record{
				Key: healthyKey,
				Bins: aerospike.BinMap{
					svc.fieldTxID:     healthyTxID.CloneBytes(),
					svc.fieldInputs:   []interface{}{},
					svc.fieldExternal: false,
				},
			},
		},
	}

	before := testutil.ToFloat64(prometheusUtxoInputResolutionErrors)

	processed, skipped, err := svc.processRecordChunk(ctx, 1000, chunk)
	require.NoError(t, err, "an unresolvable record must not fail the whole chunk")
	require.Equal(t, 1, processed, "the healthy record in the same chunk must still be pruned")

	// Deliberately NOT counted as skipped: skippedCount feeds the pre-existing
	// utxo_pruner_records_deleted_skipped_total, which has always meant "the
	// defensive check refused to delete this". An unresolvable record is a
	// different condition and gets its own counter, so the old one keeps its
	// meaning.
	require.Equal(t, 0, skipped, "an input-resolution retention must not be reported as a defensive deletion skip")
	require.Equal(t, float64(1), testutil.ToFloat64(prometheusUtxoInputResolutionErrors)-before,
		"the retained record must be reported on utxo_pruner_input_resolution_errors_total")
}
