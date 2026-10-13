package pruner

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestClearUnminedDAH(t *testing.T) {
	newSvc := func(t *testing.T, fn func(records []aerospike.BatchRecordIfc, call int) error) (*Service, *int) {
		svc := newTestService(t)
		svc.settings.Pruner.SkipDeletions = false
		svc.fieldConflicting = "conflicting"

		calls := 0
		svc.batchOperateFn = func(_ *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) error {
			calls++
			return fn(records, calls)
		}

		return svc, &calls
	}

	key := func(t *testing.T, name string) *aerospike.Key {
		k, err := aerospike.NewKey("test", "test", name)
		require.NoError(t, err)

		return k
	}

	// markHad makes a write report that it found a DAH to clear
	markHad := func(svc *Service, r aerospike.BatchRecordIfc) {
		r.BatchRec().Record = &aerospike.Record{Bins: aerospike.BinMap{svc.fieldDeleteAtHeight: 7}}
	}

	clears := func(t *testing.T) []*dahClear {
		return []*dahClear{{masterKey: key(t, "m1"), pageKeys: []*aerospike.Key{key(t, "p1")}}}
	}

	t.Run("clears masters and pages and counts them", func(t *testing.T) {
		var svc *Service

		svc, calls := newSvc(t, func(records []aerospike.BatchRecordIfc, _ int) error {
			for _, r := range records {
				markHad(svc, r)
			}

			return nil
		})

		before := testutil.ToFloat64(prometheusUtxoDAHClearedUnmined)

		require.Equal(t, 2, svc.clearUnminedDAH(context.Background(), clears(t)))
		require.Equal(t, 2, *calls)
		require.Equal(t, float64(2), testutil.ToFloat64(prometheusUtxoDAHClearedUnmined)-before)
	})

	t.Run("records without a DAH are not counted but pages are still written", func(t *testing.T) {
		svc, calls := newSvc(t, func([]aerospike.BatchRecordIfc, int) error { return nil })

		require.Zero(t, svc.clearUnminedDAH(context.Background(), clears(t)))
		require.Equal(t, 2, *calls)
	})

	t.Run("master batch error writes no pages", func(t *testing.T) {
		svc, calls := newSvc(t, func([]aerospike.BatchRecordIfc, int) error { return errors.NewProcessingError("boom") })

		before := testutil.ToFloat64(prometheusUtxoDAHClearedUnmined)

		require.Zero(t, svc.clearUnminedDAH(context.Background(), clears(t)))
		require.Equal(t, 1, *calls)
		require.Zero(t, testutil.ToFloat64(prometheusUtxoDAHClearedUnmined)-before)
	})

	for name, code := range map[string]types.ResultCode{"per-record error": types.TIMEOUT, "key not found": types.KEY_NOT_FOUND_ERROR, "filtered out": types.FILTERED_OUT} {
		t.Run("master "+name+" skips its pages", func(t *testing.T) {
			svc, calls := newSvc(t, func(records []aerospike.BatchRecordIfc, _ int) error {
				for _, r := range records {
					r.BatchRec().Err = &aerospike.AerospikeError{ResultCode: code}
				}

				return nil
			})

			require.Zero(t, svc.clearUnminedDAH(context.Background(), clears(t)))
			require.Equal(t, 1, *calls)
		})
	}

	t.Run("page batch error keeps the master count", func(t *testing.T) {
		var svc *Service

		svc, calls := newSvc(t, func(records []aerospike.BatchRecordIfc, call int) error {
			if call == 2 {
				return errors.NewProcessingError("boom")
			}

			for _, r := range records {
				markHad(svc, r)
			}

			return nil
		})

		require.Equal(t, 1, svc.clearUnminedDAH(context.Background(), clears(t)))
		require.Equal(t, 2, *calls)
	})

	t.Run("SkipDeletions writes nothing", func(t *testing.T) {
		svc, calls := newSvc(t, func([]aerospike.BatchRecordIfc, int) error { return nil })
		svc.settings.Pruner.SkipDeletions = true

		require.Zero(t, svc.clearUnminedDAH(context.Background(), clears(t)))
		require.Zero(t, *calls)
	})

	t.Run("cancelled context writes nothing", func(t *testing.T) {
		svc, calls := newSvc(t, func([]aerospike.BatchRecordIfc, int) error { return nil })

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		require.Zero(t, svc.clearUnminedDAH(ctx, clears(t)))
		require.Zero(t, *calls)
	})
}

func TestDropUnminedRecords_DedupesPageKeys(t *testing.T) {
	svc := newTestService(t)
	svc.fieldConflicting = "conflicting"

	master := unminedRecord(t, svc, 0, aerospike.BinMap{svc.fieldUnminedSince: 7, svc.fieldTotalExtraRecs: 1})
	page := unminedRecord(t, svc, 1, aerospike.BinMap{})

	svc.readMastersFn = func([]*aerospike.Key) ([]masterRead, error) {
		return []masterRead{{record: &aerospike.Record{Bins: aerospike.BinMap{svc.fieldUnminedSince: 7}}}}, nil
	}

	_, dropped, clears := svc.dropUnminedRecords([]*aerospike.Result{master, page})
	require.Equal(t, 2, dropped)
	require.Len(t, clears, 1)
	require.Len(t, clears[0].pageKeys, 1)
}

func unminedRecord(t *testing.T, svc *Service, num uint32, bins aerospike.BinMap) *aerospike.Result {
	t.Helper()

	txID := chainhash.HashH([]byte("dedupe"))

	key, err := aerospike.NewKey(svc.namespace, svc.set, uaerospike.CalculateKeySourceInternal(&txID, num))
	require.NoError(t, err)

	bins[svc.fieldTxID] = txID.CloneBytes()

	return &aerospike.Result{Record: &aerospike.Record{Key: key, Bins: bins}}
}
