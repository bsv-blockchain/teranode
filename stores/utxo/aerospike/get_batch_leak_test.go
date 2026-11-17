package aerospike

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-batcher/v2/completion"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// newTestStoreForGet builds the minimum Store fields the get path touches. It
// leaves s.client nil — tests install batchOperateFn so BatchOperate is never
// called through the real client.
func newTestStoreForGet(t *testing.T) *Store {
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

// TestSendGetBatch_PanicSignalsAllWaiters reproduces the production leak: a
// panic inside the dispatch fn (here simulated at BatchOperate; in production it
// was an unchecked type assertion in getTxFromBins) must not orphan the waiting
// submitters. go-batcher recovers the panic, so without our own recover-defer
// every done channel in the batch would be orphaned and its goroutine leaked.
func TestSendGetBatch_PanicSignalsAllWaiters(t *testing.T) {
	s := newTestStoreForGet(t)
	s.batchOperateFn = func(*aerospike.BatchPolicy, []aerospike.BatchRecordIfc) aerospike.Error {
		panic("simulated batch panic")
	}

	const n = 8

	group := completion.NewGroup(n)
	results := make([]batchGetItemData, n)
	batch := make([]*batchGetItem, n)

	for i := range batch {
		batch[i] = &batchGetItem{
			hash:   chainhash.Hash{byte(i)},
			fields: []fields.FieldName{fields.Fee},
			group:  group,
			result: &results[i],
		}
	}

	// Our recover-defer must swallow the panic (after completing every item), so
	// calling it directly does not propagate.
	require.NotPanics(t, func() { s.sendGetBatch(batch) })

	// A single group.Wait stands in for "every item received a completion signal,
	// none was orphaned": the group's counter only reaches zero once every item's
	// complete() has run.
	requireGroupCompleted(t, group, 2*time.Second)

	for i := range results {
		require.Error(t, results[i].Err, "item %d was orphaned", i)
	}
}

// TestSendOutpointBatch_NeverOrphans drives sendOutpointBatch through its panic /
// BatchOperate-error / result paths and asserts the whole group completes — no
// waiting submitter is orphaned on any path.
func TestSendOutpointBatch_NeverOrphans(t *testing.T) {
	const n = 4

	mk := func(group *completion.Group) []*batchOutpoint {
		b := make([]*batchOutpoint, n)
		for i := range b {
			in := &bt.Input{PreviousTxOutIndex: 0}
			h := chainhash.Hash{byte(i + 1)}
			_ = in.PreviousTxIDAdd(&h)
			b[i] = &batchOutpoint{outpoint: in, group: group}
		}
		return b
	}

	// "ok (nil records)" drives the success path with mocked records that carry
	// no Record; the resulting nil-deref is caught by the panic guard, which must
	// still complete every item rather than orphan the submitters.
	for name, fn := range map[string]func() func(*aerospike.BatchPolicy, []aerospike.BatchRecordIfc) aerospike.Error{
		"panic": panicOperate, "batchOperate error": errorOperate, "ok (nil records)": okOperate,
	} {
		t.Run(name, func(t *testing.T) {
			s := newTestStoreForGet(t)
			s.batchOperateFn = fn()

			group := completion.NewGroup(n)
			b := mk(group)

			require.NotPanics(t, func() { s.sendOutpointBatch(b) })

			requireGroupCompleted(t, group, 2*time.Second)
		})
	}
}

// TestGet_BoundedWaitWhenBatcherWedged verifies the keystone guarantee: when the
// batch op wedges and the caller context has no deadline (as in legacy sync /
// validation), get() still returns after batcherWait instead of parking forever.
func TestGet_BoundedWaitWhenBatcherWedged(t *testing.T) {
	s := newTestStoreForGet(t)
	s.batcherWait = 150 * time.Millisecond

	release := make(chan struct{})
	defer close(release)

	s.batchOperateFn = func(*aerospike.BatchPolicy, []aerospike.BatchRecordIfc) aerospike.Error {
		<-release // wedge the batch op until the test ends
		return &aerospike.AerospikeError{ResultCode: types.TIMEOUT}
	}

	// getBatcher is nil, so get() dispatches sendGetBatch on a goroutine that
	// wedges inside batchOperate; done never fires and ctx has no deadline.
	start := time.Now()

	_, err := s.get(context.Background(), &chainhash.Hash{0x01}, []fields.FieldName{fields.Fee})

	require.Error(t, err)
	require.Contains(t, err.Error(), "did not complete within")
	require.Less(t, time.Since(start), time.Second, "get() should return at ~batcherWait, not block")
}
