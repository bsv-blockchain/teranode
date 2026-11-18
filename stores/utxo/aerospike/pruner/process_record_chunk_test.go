package pruner

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

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

// processRecordChunk processes one chunk on its own: its deletes are sent and
// drained before it returns. processed counts the records confirmed deleted.
func (s *Service) processRecordChunk(ctx context.Context, blockHeight uint32, chunk []*aerospike.Result) (int, int, error) {
	carry := &deleteCarry{blockHeight: blockHeight}

	processed, skipped, err := s.processRecordChunkCarry(ctx, blockHeight, chunk, carry)
	if err != nil {
		return processed, skipped, err
	}

	deleted, heldBack, err := s.drainCarry(ctx, carry)

	return processed + deleted, skipped + heldBack, err
}

// TestProcessRecordChunk_RecordWithoutInputs verifies a record with no inputs
// (e.g. coinbase) is processed without a parent update. The chunk is crafted
// with zero inputs so no parent updates accumulate and the flushCleanupBatches
// deletion path stays gated by SkipDeletions.
func TestProcessRecordChunk_RecordWithoutInputs(t *testing.T) {
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

	processed, skipped, err := svc.processRecordChunk(ctx, 1000, chunk)
	require.NoError(t, err)
	require.Equal(t, 1, processed)
	require.Equal(t, 0, skipped)
}

// TestProcessRecordChunk_UnresolvableInputs covers a record whose input references cannot be
// read. A record whose external blobs are both gone is deleted, as on main: it was left behind
// by pruner_skipDeletions or an unanswered delete, after its parents were marked, and retaining
// it would rescan it on every block for good. Any other failure retains the record on its own,
// without aborting the chunk, and counts it. Before the retention fix such a failure returned a
// ProcessingError, which PruneWithPartitions classifies as a non-timeout error and never
// retries, so one bad record stopped all pruning permanently.
func TestProcessRecordChunk_UnresolvableInputs(t *testing.T) {
	ctx := context.Background()

	var externalTxID, healthyTxID chainhash.Hash
	for i := range externalTxID {
		externalTxID[i] = 0x11
		healthyTxID[i] = 0x22
	}

	chunkOf := func(t *testing.T, svc *Service, bad aerospike.BinMap) []*aerospike.Result {
		t.Helper()

		badKey, keyErr := aerospike.NewKey(svc.namespace, svc.set, externalTxID[:])
		require.NoError(t, keyErr)

		healthyKey, keyErr := aerospike.NewKey(svc.namespace, svc.set, healthyTxID[:])
		require.NoError(t, keyErr)

		return []*aerospike.Result{
			{Record: &aerospike.Record{Key: badKey, Bins: bad}},
			{Record: &aerospike.Record{Key: healthyKey, Bins: aerospike.BinMap{
				svc.fieldTxID:     healthyTxID.CloneBytes(),
				svc.fieldInputs:   []interface{}{},
				svc.fieldExternal: false,
			}}},
		}
	}

	t.Run("a record whose external blobs are both gone is deleted", func(t *testing.T) {
		svc := newTestService(t)
		svc.external = memory.New()

		exists, err := svc.external.Exists(ctx, externalTxID[:], fileformat.FileTypeOutputs)
		require.NoError(t, err)
		require.False(t, exists)

		exists, err = svc.external.Exists(ctx, externalTxID[:], fileformat.FileTypeTx)
		require.NoError(t, err)
		require.False(t, exists)

		before := testutil.ToFloat64(prometheusUtxoInputResolutionErrors)

		processed, skipped, err := svc.processRecordChunk(ctx, 1000, chunkOf(t, svc, aerospike.BinMap{
			svc.fieldTxID:     externalTxID.CloneBytes(),
			svc.fieldExternal: true,
		}))
		require.NoError(t, err)
		require.Equal(t, 2, processed, "the record with no blobs is pruned with the healthy one")
		require.Equal(t, 0, skipped)
		require.Equal(t, before, testutil.ToFloat64(prometheusUtxoInputResolutionErrors), "nothing is retained, so nothing is counted")
	})

	t.Run("a blob that will not parse retains its record and not the chunk", func(t *testing.T) {
		svc := newTestService(t)
		svc.external = memory.New()
		require.NoError(t, svc.external.Set(ctx, externalTxID[:], fileformat.FileTypeTx, []byte{0x01, 0x02, 0x03}))

		before := testutil.ToFloat64(prometheusUtxoInputResolutionErrors)

		processed, skipped, err := svc.processRecordChunk(ctx, 1000, chunkOf(t, svc, aerospike.BinMap{
			svc.fieldTxID:     externalTxID.CloneBytes(),
			svc.fieldExternal: true,
		}))
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
	})
}

func TestProcessRecordChunk_ClearsParentRefsOfConflictingRecords(t *testing.T) {
	ctx := context.Background()

	var childTxID chainhash.Hash
	for i := range childTxID {
		childTxID[i] = 0x55
	}

	build := func(svc *Service, conflicting interface{}) []*aerospike.Result {
		key, err := aerospike.NewKey(svc.namespace, svc.set, childTxID[:])
		require.NoError(t, err)

		bins := aerospike.BinMap{
			svc.fieldTxID:     childTxID.CloneBytes(),
			svc.fieldInputs:   []interface{}{},
			svc.fieldExternal: false,
		}

		if conflicting != nil {
			bins[svc.fieldConflicting] = conflicting
		}

		return []*aerospike.Result{{Record: &aerospike.Record{Key: key, Bins: bins}}}
	}

	newSvc := func(t *testing.T, defensive bool, fn func(context.Context, *chainhash.Hash) error) *Service {
		svc := newTestService(t)
		svc.defensiveEnabled = defensive
		svc.fieldConflicting = fields.Conflicting.String()
		svc.clearParentRefs = fn

		return svc
	}

	t.Run("conflicting record has its parent references cleared before deletion", func(t *testing.T) {
		var calls []chainhash.Hash

		svc := newSvc(t, true, func(_ context.Context, h *chainhash.Hash) error {
			calls = append(calls, *h)
			return nil
		})

		processed, skipped, err := svc.processRecordChunk(ctx, 1000, build(svc, true))
		require.NoError(t, err)
		require.Equal(t, 1, processed)
		require.Equal(t, 0, skipped)
		require.Equal(t, []chainhash.Hash{childTxID}, calls)
	})

	t.Run("record is kept when clearing parent references fails", func(t *testing.T) {
		calls := 0

		svc := newSvc(t, true, func(context.Context, *chainhash.Hash) error {
			calls++
			return errors.NewServiceError("boom")
		})

		processed, skipped, err := svc.processRecordChunk(ctx, 1000, build(svc, true))
		require.NoError(t, err)
		require.Equal(t, 0, processed)
		require.Equal(t, 1, skipped)
		require.Equal(t, 1, calls)
	})

	t.Run("non-conflicting record is not cleared", func(t *testing.T) {
		for _, conflicting := range []interface{}{nil, false} {
			calls := 0

			svc := newSvc(t, true, func(context.Context, *chainhash.Hash) error {
				calls++
				return nil
			})

			processed, skipped, err := svc.processRecordChunk(ctx, 1000, build(svc, conflicting))
			require.NoError(t, err)
			require.Equal(t, 1, processed)
			require.Equal(t, 0, skipped)
			require.Equal(t, 0, calls)
		}
	})

	t.Run("non-defensive mode never clears", func(t *testing.T) {
		calls := 0

		svc := newSvc(t, false, func(context.Context, *chainhash.Hash) error {
			calls++
			return nil
		})

		processed, _, err := svc.processRecordChunk(ctx, 1000, build(svc, true))
		require.NoError(t, err)
		require.Equal(t, 1, processed)
		require.Equal(t, 0, calls)
	})

	t.Run("defensive mode without a clear function deletes as before", func(t *testing.T) {
		svc := newSvc(t, true, nil)

		processed, skipped, err := svc.processRecordChunk(ctx, 1000, build(svc, true))
		require.NoError(t, err)
		require.Equal(t, 1, processed)
		require.Equal(t, 0, skipped)
	})
}

// TestProcessRecordChunk_PageMasterIsReadOnce: a pagination record scanned on
// its own has its master read by dropUnminedRecords. When that read finds the
// master present, the page goes with the master's own deletion and flushChunk
// drops it without reading the master again, so the chunk pays one master
// read, not two. Only a master that the first read found missing is read again,
// because it may have been recreated since.
func TestProcessRecordChunk_PageMasterIsReadOnce(t *testing.T) {
	notFound := &aerospike.AerospikeError{ResultCode: types.KEY_NOT_FOUND_ERROR}

	for _, tc := range []struct {
		name         string
		first        masterRead
		wantRereads  int
		wantDeletion bool
	}{
		{name: "master present at the first read", first: masterRead{record: &aerospike.Record{Bins: aerospike.BinMap{}}}},
		{name: "master missing at the first read", first: masterRead{err: notFound}, wantRereads: 1, wantDeletion: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := newTestService(t)
			svc.settings.Pruner.SkipDeletions = false
			svc.fieldConflicting = fields.Conflicting.String()

			txID := chainhash.HashH([]byte("paged"))

			pageKey, err := aerospike.NewKey(svc.namespace, svc.set, uaerospike.CalculateKeySourceInternal(&txID, 1))
			require.NoError(t, err)

			firstReads := 0
			svc.readMastersFn = func([]*aerospike.Key) ([]masterRead, error) {
				firstReads++

				return []masterRead{tc.first}, nil
			}

			rereads := 0
			pageDeletes := 0
			svc.batchOperateFn = func(_ *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) error {
				for _, rec := range records {
					switch rec.(type) {
					case *aerospike.BatchRead:
						rereads++
						rec.BatchRec().Err = notFound
						rec.BatchRec().ResultCode = types.KEY_NOT_FOUND_ERROR
					case *aerospike.BatchDelete:
						pageDeletes++
						rec.BatchRec().ResultCode = types.OK
					}
				}

				return nil
			}

			chunk := []*aerospike.Result{{Record: &aerospike.Record{Key: pageKey, Bins: aerospike.BinMap{
				svc.fieldTxID:   txID.CloneBytes(),
				svc.fieldInputs: []interface{}{},
			}}}}

			_, _, chunkErr := svc.processRecordChunk(context.Background(), testPruneHeight, chunk)
			require.NoError(t, chunkErr)
			require.Equal(t, 1, firstReads, "dropUnminedRecords reads the page's master once")
			require.Equal(t, tc.wantRereads, rereads, "flushChunk re-reads only a master the first read found missing")

			if tc.wantDeletion {
				require.Equal(t, 1, pageDeletes, "an orphan page is deleted")
			} else {
				require.Zero(t, pageDeletes, "a page of a present master goes with the master's own deletion")
			}
		})
	}
}
