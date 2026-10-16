package aerospike

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// capturingLogger records Warnf and Errorf lines (formatted) on top of the silent test logger.
type capturingLogger struct {
	ulogger.TestLogger
	mu    sync.Mutex
	warns []string
	errs  []string
}

func (l *capturingLogger) Warnf(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.warns = append(l.warns, fmt.Sprintf(format, args...))
}

func (l *capturingLogger) Errorf(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.errs = append(l.errs, fmt.Sprintf(format, args...))
}

func expiredBatch(t *testing.T, n int) ([]aerospike.BatchRecordIfc, []chainhash.Hash, []int) {
	t.Helper()

	batch := make([]aerospike.BatchRecordIfc, n)
	txIDs := make([]chainhash.Hash, n)
	preserved := make([]int, n)

	for i := range batch {
		txIDs[i] = chainhash.Hash{byte(i + 1)}
		key, err := aerospike.NewKey("test-ns", "test-set", txIDs[i][:])
		require.NoError(t, err)

		batch[i] = aerospike.NewBatchWrite(nil, key, aerospike.PutOp(aerospike.NewBin(fields.PreserveUntil.String(), nil)))
		preserved[i] = 190
	}

	return batch, txIDs, preserved
}

// writtenExternalMaster marks a batch record as written: an external master with 2 pagination
// records whose DAH was cleared by the expiry (5 -> none), so a child unset follow-up is due.
func writtenExternalMaster(rec aerospike.BatchRecordIfc) {
	rec.BatchRec().Record = &aerospike.Record{Bins: aerospike.BinMap{
		fields.External.String():       true,
		fields.TotalExtraRecs.String(): 2,
		fields.DeleteAtHeight.String(): []interface{}{5, nil},
	}}
}

// A top-level BatchOperate error (one node timing out) leaves some records written, with
// preserveUntil already cleared. Their child follow-ups must still run: nothing revisits them.
func TestProcessBatchExpiredPreservations_PartialBatchErrorStillFollowsUp(t *testing.T) {
	prev := childDAHRetryBackoff
	childDAHRetryBackoff = time.Millisecond

	t.Cleanup(func() { childDAHRetryBackoff = prev })

	s := newTestStoreForGet(t)
	batch, txIDs, preserved := expiredBatch(t, 2)

	var sizes []int

	s.batchOperateFn = func(_ *aerospike.BatchPolicy, recs []aerospike.BatchRecordIfc) aerospike.Error {
		sizes = append(sizes, len(recs))

		if len(sizes) == 1 {
			writtenExternalMaster(recs[0])
			recs[1].BatchRec().Err = aerospike.ErrNetwork

			return aerospike.ErrNetwork
		}

		return nil
	}

	err := s.processBatchExpiredPreservations(context.Background(), batch, txIDs, preserved)
	require.Error(t, err, "the top-level error must still be returned")
	require.Equal(t, []int{2, 2}, sizes, "the written record's 2 pagination records must be unset")
}

func TestApplyChildDAHFollowUps_ErrorLineNamesFailedTxIDs(t *testing.T) {
	prev := childDAHRetryBackoff
	childDAHRetryBackoff = time.Millisecond

	t.Cleanup(func() { childDAHRetryBackoff = prev })

	s := newTestStoreForGet(t)
	logger := &capturingLogger{}
	s.logger = logger
	s.batchOperateFn = func(*aerospike.BatchPolicy, []aerospike.BatchRecordIfc) aerospike.Error {
		return aerospike.ErrNetwork
	}

	var f childDAHFollowUps

	ids := make([]chainhash.Hash, 15)
	for i := range ids {
		ids[i] = chainhash.Hash{byte(i + 1)}
		f.addUnset(&ids[i], 1)
	}

	res := s.applyChildDAHFollowUps(context.Background(), &f)
	require.Len(t, res.failedUnsets, 15)
	require.NotEmpty(t, logger.errs)

	line := logger.errs[0]
	require.Contains(t, line, ids[0].String())
	require.Contains(t, line, ids[maxLoggedChildDAHTxIDs-1].String())
	require.NotContains(t, line, ids[maxLoggedChildDAHTxIDs].String(), "the txid list is capped")
	require.NotContains(t, line, "\n")
	require.True(t, strings.Contains(line, "15 txs"))
}

func TestRestoreDAHBatch_CountsFailuresInTheCallersCounter(t *testing.T) {
	prev := childDAHRetryBackoff
	childDAHRetryBackoff = time.Millisecond

	t.Cleanup(func() { childDAHRetryBackoff = prev })

	s := newTestStoreForGet(t)
	s.batchOperateFn = func(*aerospike.BatchPolicy, []aerospike.BatchRecordIfc) aerospike.Error {
		return aerospike.ErrNetwork
	}

	counter := prometheus.NewCounter(prometheus.CounterOpts{Name: "test_restore_failed"})
	shared := testutil.ToFloat64(prometheusTxMetaAerospikeExpiredPreservationRestoreFailed)

	require.Error(t, s.restoreDAHBatch(context.Background(), restoreItems(t, 3), counter))
	require.InDelta(t, 3, testutil.ToFloat64(counter), 0)
	require.InDelta(t, shared, testutil.ToFloat64(prometheusTxMetaAerospikeExpiredPreservationRestoreFailed), 0,
		"another caller's failures must not be counted as expired-preservation failures")
}

// TestSetDAHForChildRecordsMulti_MissingPageWarnNamesMasters pins that a page missing under a
// master is not silent: it is lost data, so the (rate limited) Warn names the masters, capped.
func TestSetDAHForChildRecordsMulti_MissingPageWarnNamesMasters(t *testing.T) {
	lastMissingPageWarn.Store(0)

	s := newTestStoreForGet(t)
	logger := &capturingLogger{}
	s.logger = logger
	s.batchOperateFn = func(_ *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
		for _, r := range records {
			r.BatchRec().Err = aerospike.ErrKeyNotFound
		}

		return nil
	}

	items := make([]childDAHItem, 15)
	ids := make([]chainhash.Hash, 15)

	for i := range items {
		ids[i] = chainhash.Hash{byte(i + 1)}
		items[i] = childDAHItem{TxID: &ids[i], ChildCount: 2, DeleteAtHeight: 10}
	}

	require.NoError(t, s.SetDAHForChildRecordsMulti(items), "a missing page is benign")
	require.Len(t, logger.warns, 1)

	line := logger.warns[0]
	require.Contains(t, line, ids[0].String())
	require.Contains(t, line, ids[maxLoggedChildDAHTxIDs-1].String())
	require.NotContains(t, line, ids[maxLoggedChildDAHTxIDs].String(), "the txid list is capped")
	require.Equal(t, 1, strings.Count(line, ids[0].String()), "one entry per master, not per page")
	require.NotContains(t, line, "\n")
}
