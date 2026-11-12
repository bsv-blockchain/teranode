package aerospike

import (
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// TestSignalBatchPanic_BumpsStoreMetric covers the one thing the store's thin
// wrapper adds over util.SignalBatchPanic: the PanicRecovered counter, which is
// package-private here. The fan-out contract itself (complete every item
// exactly once, never clobber an already-completed item) is tested against the
// shared helper in util/batch_panic_test.go.
func TestSignalBatchPanic_BumpsStoreMetric(t *testing.T) {
	InitPrometheusMetrics()

	logger := ulogger.TestLogger{}

	type item struct {
		completed atomic.Bool
		result    error
	}

	signal := func(it *item, err error) {
		if it.completed.CompareAndSwap(false, true) {
			it.result = err
		}
	}

	counter := prometheusUtxoMapErrors.WithLabelValues("Batch", "PanicRecovered")

	before := testutil.ToFloat64(counter)

	require.False(t, signalBatchPanic(nil, []*item{{}}, "test", logger, signal), "nil recovered is a no-op")
	require.Equal(t, before, testutil.ToFloat64(counter), "a no-op must not bump the counter")

	batch := []*item{{}, {}}
	require.True(t, signalBatchPanic("boom", batch, "sendGetBatch", logger, signal))
	require.Equal(t, before+1, testutil.ToFloat64(counter), "a recovered panic must bump the counter exactly once")

	for i, it := range batch {
		require.True(t, it.completed.Load(), "item %d must be completed after the panic sweep", i)
		require.Contains(t, it.result.Error(), "panic in sendGetBatch")
	}
}
