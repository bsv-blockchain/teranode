//go:build testtxmetacache

package txmetacache

import (
	"context"
	"math"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

const (
	bytesTestCacheMB     = 1
	bytesTestFillEntries = 60_000
	bytesTestSampleEvery = 500
)

func newBytesTestCache(t *testing.T, bucket BucketType) (*TxMetaCache, *settings.Settings) {
	t.Helper()

	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	tSettings := settings.NewSettings()

	c, err := NewTxMetaCache(ctx, tSettings, ulogger.TestLogger{}, ns, bucket, bytesTestCacheMB)
	require.NoError(t, err)

	return c.(*TxMetaCache), tSettings
}

func publishedBytes(t *testing.T, c *TxMetaCache) (uint64, uint64) {
	t.Helper()

	txMetaCacheMetricsRegistry.mu.Lock()
	saved := txMetaCacheMetricsRegistry.caches
	txMetaCacheMetricsRegistry.caches = map[*TxMetaCache]struct{}{c: {}}
	txMetaCacheMetricsRegistry.mu.Unlock()

	defer func() {
		txMetaCacheMetricsRegistry.mu.Lock()
		txMetaCacheMetricsRegistry.caches = saved
		txMetaCacheMetricsRegistry.mu.Unlock()
	}()

	updateTxMetaCachePrometheusMetrics()

	bytes := testutil.ToFloat64(prometheusBlockValidationTxMetaCacheBytes)
	capacity := testutil.ToFloat64(prometheusBlockValidationTxMetaCacheCapacityBytes)

	stats := c.GetCacheStats()
	require.InDelta(t, float64(stats.Bytes), bytes, 0)
	require.InDelta(t, float64(stats.CapacityBytes), capacity, 0)

	return uint64(bytes), uint64(capacity)
}

func insertBytesTestEntries(t *testing.T, c *TxMetaCache, from, to int) {
	t.Helper()

	for i := from; i < to; i++ {
		hash := mixedKey(uint64(i))
		data := &meta.Data{
			Fee:         uint64(i),
			SizeInBytes: 250,
			TxInpoints:  subtree.NewTxInpointsFromPacked([]chainhash.Hash{mixedKey(uint64(i) + 1<<40)}, []uint32{1, 0}),
		}

		require.NoError(t, c.SetCache(&hash, data))
	}
}

func fillAndTrackBytes(t *testing.T, c *TxMetaCache) (uint64, uint64) {
	t.Helper()

	bytes, capacity := publishedBytes(t, c)
	require.Zero(t, bytes)
	require.Positive(t, capacity)
	require.LessOrEqual(t, capacity, uint64(bytesTestCacheMB*1024*1024))

	insertBytesTestEntries(t, c, 0, 100)

	first, _ := publishedBytes(t, c)
	require.Positive(t, first)

	insertBytesTestEntries(t, c, 100, 1000)

	second, _ := publishedBytes(t, c)
	require.Greater(t, second, first)

	for from := 1000; from < bytesTestFillEntries; from += bytesTestSampleEvery {
		insertBytesTestEntries(t, c, from, from+bytesTestSampleEvery)

		bytes, sampleCapacity := publishedBytes(t, c)
		require.Equal(t, capacity, sampleCapacity)
		require.LessOrEqual(t, bytes, capacity)
	}

	final, _ := publishedBytes(t, c)

	return final, capacity
}

func requireRingCacheFull(t *testing.T, bucket BucketType) {
	t.Helper()

	c, _ := newBytesTestCache(t, bucket)

	final, capacity := fillAndTrackBytes(t, c)
	require.Equal(t, uint64(bytesTestCacheMB*1024*1024), capacity)
	require.Equal(t, capacity, final)
}

func TestCacheBytes_Unallocated(t *testing.T) {
	requireRingCacheFull(t, Unallocated)
}

func TestCacheBytes_Trimmed(t *testing.T) {
	requireRingCacheFull(t, Trimmed)
}

func TestCacheBytes_Native(t *testing.T) {
	requireRingCacheFull(t, Native)
}

func TestCacheBytes_Pointer(t *testing.T) {
	c, _ := newBytesTestCache(t, Pointer)

	final, capacity := fillAndTrackBytes(t, c)

	perBucket := uint64(bytesTestCacheMB * 1024 * 1024 / BucketsCount / pointerCacheAvgEntryBytes)
	require.Equal(t, perBucket*BucketsCount*pointerCacheAvgEntryBytes, capacity)
	require.Equal(t, capacity, final)
}

func TestCacheBytes_Lockfree(t *testing.T) {
	c, _ := newBytesTestCache(t, Lockfree)

	final, capacity := fillAndTrackBytes(t, c)

	lc := c.cache.(*lockFreeBackend).cache
	require.Equal(t, uint64(len(lc.lanes))*lc.ringSize, capacity)
	require.Equal(t, capacity, final)

	lc.Reset()

	bytes, _ := publishedBytes(t, c)
	require.Zero(t, bytes)
}

func TestCacheBytes_PreallocatedFullHoldsKeptSpanBelowCapacity(t *testing.T) {
	c, tSettings := newBytesTestCache(t, Preallocated)

	final, capacity := fillAndTrackBytes(t, c)
	require.Equal(t, uint64(bytesTestCacheMB*1024*1024), capacity)

	chunks := bytesTestCacheMB * 1024 * 1024 / BucketsCount / ChunkSize
	removed := int(math.Ceil(float64(chunks*tSettings.SubtreeValidation.TxMetaCacheTrimRatio) / 100))
	keptSpan := uint64((chunks-removed)*ChunkSize) * BucketsCount

	require.Positive(t, c.GetCacheStats().TrimCount)
	require.GreaterOrEqual(t, final, keptSpan)
	require.Less(t, final, capacity)
}

func TestCacheBytes_ResetEmptiesRingCache(t *testing.T) {
	c, _ := newBytesTestCache(t, Unallocated)

	insertBytesTestEntries(t, c, 0, 1000)

	bytes, _ := publishedBytes(t, c)
	require.Positive(t, bytes)

	c.cache.(*improvedCacheBackend).cache.Reset()

	bytes, _ = publishedBytes(t, c)
	require.Zero(t, bytes)
}
