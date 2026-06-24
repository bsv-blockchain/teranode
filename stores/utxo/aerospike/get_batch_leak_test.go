package aerospike

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
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

	batch := make([]*batchGetItem, n)
	for i := range batch {
		batch[i] = &batchGetItem{
			hash:   chainhash.Hash{byte(i)},
			fields: []fields.FieldName{fields.Fee},
			done:   make(chan batchGetItemData, 1),
		}
	}

	// Our recover-defer must swallow the panic (after signalling), so calling it
	// directly does not propagate.
	require.NotPanics(t, func() { s.sendGetBatch(batch) })

	for i, it := range batch {
		select {
		case data := <-it.done:
			require.Error(t, data.Err, "item %d was orphaned", i)
		case <-time.After(2 * time.Second):
			t.Fatalf("item %d orphaned: no done signal after panic", i)
		}
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
