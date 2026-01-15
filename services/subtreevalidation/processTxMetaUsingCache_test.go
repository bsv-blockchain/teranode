package subtreevalidation

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/txmetacache"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

func setupTestCache(t testing.TB) *txmetacache.TxMetaCache {
	t.Helper()
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	c, err := txmetacache.NewTxMetaCache(ctx, tSettings, logger, utxoStore, txmetacache.Unallocated)
	require.NoError(t, err)

	return c.(*txmetacache.TxMetaCache)
}

func setupCacheTestServer(t testing.TB, cache *txmetacache.TxMetaCache, batchSize, concurrency, threshold int) *Server {
	t.Helper()
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.SubtreeValidation.ProcessTxMetaUsingCacheBatchSize = batchSize
	tSettings.SubtreeValidation.ProcessTxMetaUsingCacheConcurrency = concurrency
	tSettings.SubtreeValidation.ProcessTxMetaUsingCacheMissingTxThreshold = threshold

	return &Server{
		logger:    ulogger.TestLogger{},
		settings:  tSettings,
		utxoStore: cache,
	}
}

func generateTestHashes(count int) []chainhash.Hash {
	hashes := make([]chainhash.Hash, count)
	for i := 0; i < count; i++ {
		hashes[i] = chainhash.HashH([]byte{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)})
	}
	return hashes
}

func populateCache(t testing.TB, cache *txmetacache.TxMetaCache, hashes []chainhash.Hash) {
	t.Helper()
	testMeta := &meta.Data{
		Fee:         100,
		SizeInBytes: 250,
		TxInpoints:  subtree.TxInpoints{ParentTxHashes: []chainhash.Hash{}},
		BlockIDs:    []uint32{},
	}

	for i := range hashes {
		err := cache.SetCache(&hashes[i], testMeta)
		require.NoError(t, err)
	}
}

func TestProcessTxMetaUsingCache_MismatchedSliceLengths(t *testing.T) {
	cache := setupTestCache(t)
	server := setupCacheTestServer(t, cache, 1024, 4, 100)
	ctx := context.Background()

	txHashes := generateTestHashes(10)
	txMetaSlice := make([]metaSliceItem, 5) // Different length

	_, err := server.processTxMetaUsingCache(ctx, txHashes, txMetaSlice, false)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrProcessing))
}

func TestProcessTxMetaUsingCache_NonCachedStore(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.SubtreeValidation.ProcessTxMetaUsingCacheBatchSize = 1024
	tSettings.SubtreeValidation.ProcessTxMetaUsingCacheConcurrency = 4
	tSettings.SubtreeValidation.ProcessTxMetaUsingCacheMissingTxThreshold = 100

	ns, err := nullstore.NewNullStore()
	require.NoError(t, err)

	server := &Server{
		logger:    ulogger.TestLogger{},
		settings:  tSettings,
		utxoStore: ns, // Not a TxMetaCache
	}

	ctx := context.Background()
	txHashes := generateTestHashes(10)
	txMetaSlice := make([]metaSliceItem, 10)

	missed, err := server.processTxMetaUsingCache(ctx, txHashes, txMetaSlice, false)
	require.NoError(t, err)
	require.Equal(t, 10, missed, "should report all txHashes as missed when not using cache")
}

func TestProcessTxMetaUsingCache_AllFound(t *testing.T) {
	cache := setupTestCache(t)
	server := setupCacheTestServer(t, cache, 1024, 4, 100)
	ctx := context.Background()

	txHashes := generateTestHashes(100)
	populateCache(t, cache, txHashes)

	txMetaSlice := make([]metaSliceItem, len(txHashes))

	missed, err := server.processTxMetaUsingCache(ctx, txHashes, txMetaSlice, false)
	require.NoError(t, err)
	require.Equal(t, 0, missed, "should have 0 misses when all txs are in cache")

	// Verify all metadata was populated
	for i, m := range txMetaSlice {
		require.NotNil(t, m, "txMetaSlice[%d] should not be nil", i)
	}
}

func TestProcessTxMetaUsingCache_AllMissing(t *testing.T) {
	cache := setupTestCache(t)
	server := setupCacheTestServer(t, cache, 1024, 4, 1000) // High threshold to avoid fail-fast
	ctx := context.Background()

	txHashes := generateTestHashes(100)
	txMetaSlice := make([]metaSliceItem, len(txHashes))

	missed, err := server.processTxMetaUsingCache(ctx, txHashes, txMetaSlice, false)
	require.NoError(t, err)
	require.Equal(t, 100, missed, "should have all misses when cache is empty")

	// Verify no metadata was populated
	for i, m := range txMetaSlice {
		require.False(t, m.isSet, "txMetaSlice[%d] should be nil", i)
	}
}

func TestProcessTxMetaUsingCache_PartialHits(t *testing.T) {
	cache := setupTestCache(t)
	server := setupCacheTestServer(t, cache, 1024, 4, 1000)
	ctx := context.Background()

	txHashes := generateTestHashes(100)
	// Populate only first 50 in cache
	populateCache(t, cache, txHashes[:50])

	txMetaSlice := make([]metaSliceItem, len(txHashes))

	missed, err := server.processTxMetaUsingCache(ctx, txHashes, txMetaSlice, false)
	require.NoError(t, err)
	require.Equal(t, 50, missed, "should have 50 misses when half are in cache")

	// Verify first 50 are populated, rest are nil
	for i := 0; i < 50; i++ {
		require.True(t, txMetaSlice[i].isSet, "txMetaSlice[%d] should not be nil", i)
	}
	for i := 50; i < 100; i++ {
		require.False(t, txMetaSlice[i].isSet, "txMetaSlice[%d] should be nil", i)
	}
}

func TestProcessTxMetaUsingCache_SkipCoinbasePlaceholder(t *testing.T) {
	cache := setupTestCache(t)
	server := setupCacheTestServer(t, cache, 1024, 4, 1000)
	ctx := context.Background()

	txHashes := make([]chainhash.Hash, 10)
	for i := 0; i < 10; i++ {
		if i == 5 {
			txHashes[i] = *subtree.CoinbasePlaceholderHash
		} else {
			txHashes[i] = chainhash.HashH([]byte{byte(i)})
		}
	}

	txMetaSlice := make([]metaSliceItem, len(txHashes))

	missed, err := server.processTxMetaUsingCache(ctx, txHashes, txMetaSlice, false)
	require.NoError(t, err)
	// Should be 9 misses, not 10, because coinbase placeholder is skipped
	require.Equal(t, 9, missed, "coinbase placeholder should be skipped")
}

func TestProcessTxMetaUsingCache_SkipPrefilledEntries(t *testing.T) {
	cache := setupTestCache(t)
	server := setupCacheTestServer(t, cache, 1024, 4, 1000)
	ctx := context.Background()

	txHashes := generateTestHashes(10)
	txMetaSlice := make([]metaSliceItem, len(txHashes))

	// Pre-fill some entries
	for i := 0; i < 5; i++ {
		txMetaSlice[i] = metaSliceItem{
			fee:   uint64(100 + i),
			isSet: true,
		}
	}

	missed, err := server.processTxMetaUsingCache(ctx, txHashes, txMetaSlice, false)
	require.NoError(t, err)
	// Should only miss the 5 that weren't prefilled
	require.Equal(t, 5, missed, "should skip prefilled entries")

	// Verify prefilled entries are unchanged
	for i := 0; i < 5; i++ {
		require.Equal(t, uint64(100+i), txMetaSlice[i].fee)
	}
}

func TestProcessTxMetaUsingCache_FailFastThresholdExceeded(t *testing.T) {
	cache := setupTestCache(t)
	server := setupCacheTestServer(t, cache, 1024, 4, 5) // Low threshold of 5
	ctx := context.Background()

	txHashes := generateTestHashes(100)
	txMetaSlice := make([]metaSliceItem, len(txHashes))

	_, err := server.processTxMetaUsingCache(ctx, txHashes, txMetaSlice, true)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrProcessing))
}

func TestProcessTxMetaUsingCache_FailFastNotTriggered(t *testing.T) {
	cache := setupTestCache(t)
	server := setupCacheTestServer(t, cache, 1024, 4, 1000) // High threshold
	ctx := context.Background()

	txHashes := generateTestHashes(10)
	txMetaSlice := make([]metaSliceItem, len(txHashes))

	missed, err := server.processTxMetaUsingCache(ctx, txHashes, txMetaSlice, true)
	require.NoError(t, err)
	require.Equal(t, 10, missed)
}

func TestProcessTxMetaUsingCache_ContextCancellation(t *testing.T) {
	cache := setupTestCache(t)
	server := setupCacheTestServer(t, cache, 10, 1, 100000) // Small batch, single goroutine
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	txHashes := generateTestHashes(100)
	txMetaSlice := make([]metaSliceItem, len(txHashes))

	_, err := server.processTxMetaUsingCache(ctx, txHashes, txMetaSlice, false)
	require.Error(t, err)
}

func TestProcessTxMetaUsingCache_MultipleBatches(t *testing.T) {
	cache := setupTestCache(t)
	server := setupCacheTestServer(t, cache, 10, 4, 10000) // Small batch size
	ctx := context.Background()

	txHashes := generateTestHashes(100) // Will require 10 batches
	populateCache(t, cache, txHashes)

	txMetaSlice := make([]metaSliceItem, len(txHashes))

	missed, err := server.processTxMetaUsingCache(ctx, txHashes, txMetaSlice, false)
	require.NoError(t, err)
	require.Equal(t, 0, missed)

	for i, m := range txMetaSlice {
		require.NotNil(t, m, "txMetaSlice[%d] should not be nil", i)
	}
}

func TestProcessTxMetaUsingCache_EmptyInput(t *testing.T) {
	cache := setupTestCache(t)
	server := setupCacheTestServer(t, cache, 1024, 4, 100)
	ctx := context.Background()

	txHashes := make([]chainhash.Hash, 0)
	txMetaSlice := make([]metaSliceItem, 0)

	missed, err := server.processTxMetaUsingCache(ctx, txHashes, txMetaSlice, false)
	require.NoError(t, err)
	require.Equal(t, 0, missed)
}

// Benchmarks

func BenchmarkProcessTxMetaUsingCache_AllHits(b *testing.B) {
	testCases := []struct {
		name    string
		txCount int
	}{
		{"1k_tx", 1_000},
		{"10k_tx", 10_000},
		{"100k_tx", 100_000},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			cache := setupTestCache(b)
			server := setupCacheTestServer(b, cache, 1024, 32, 1000000)
			ctx := context.Background()

			txHashes := generateTestHashes(tc.txCount)
			populateCache(b, cache, txHashes)

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				txMetaSlice := make([]metaSliceItem, len(txHashes))
				_, _ = server.processTxMetaUsingCache(ctx, txHashes, txMetaSlice, false)
			}

			b.ReportMetric(float64(tc.txCount), "tx/op")
		})
	}
}

func BenchmarkProcessTxMetaUsingCache_AllMisses(b *testing.B) {
	testCases := []struct {
		name    string
		txCount int
	}{
		{"1k_tx", 1_000},
		{"10k_tx", 10_000},
		{"100k_tx", 100_000},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			cache := setupTestCache(b)
			server := setupCacheTestServer(b, cache, 1024, 32, 1000000)
			ctx := context.Background()

			txHashes := generateTestHashes(tc.txCount)
			// Don't populate cache - all misses

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				txMetaSlice := make([]metaSliceItem, len(txHashes))
				_, _ = server.processTxMetaUsingCache(ctx, txHashes, txMetaSlice, false)
			}

			b.ReportMetric(float64(tc.txCount), "tx/op")
		})
	}
}

func BenchmarkProcessTxMetaUsingCache_50PercentHitRate(b *testing.B) {
	testCases := []struct {
		name    string
		txCount int
	}{
		{"1k_tx", 1_000},
		{"10k_tx", 10_000},
		{"100k_tx", 100_000},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			cache := setupTestCache(b)
			server := setupCacheTestServer(b, cache, 1024, 32, 1000000)
			ctx := context.Background()

			txHashes := generateTestHashes(tc.txCount)
			// Populate only first half
			populateCache(b, cache, txHashes[:tc.txCount/2])

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				txMetaSlice := make([]metaSliceItem, len(txHashes))
				_, _ = server.processTxMetaUsingCache(ctx, txHashes, txMetaSlice, false)
			}

			b.ReportMetric(float64(tc.txCount), "tx/op")
		})
	}
}

func BenchmarkProcessTxMetaUsingCache_VariableConcurrency(b *testing.B) {
	concurrencyLevels := []int{1, 4, 8, 16, 32}
	const txCount = 10_000

	for _, concurrency := range concurrencyLevels {
		b.Run("concurrency_"+string(rune('0'+concurrency/10))+string(rune('0'+concurrency%10)), func(b *testing.B) {
			cache := setupTestCache(b)
			server := setupCacheTestServer(b, cache, 1024, concurrency, 1000000)
			ctx := context.Background()

			txHashes := generateTestHashes(txCount)
			populateCache(b, cache, txHashes)

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				txMetaSlice := make([]metaSliceItem, len(txHashes))
				_, _ = server.processTxMetaUsingCache(ctx, txHashes, txMetaSlice, false)
			}

			b.ReportMetric(float64(txCount), "tx/op")
		})
	}
}

func BenchmarkProcessTxMetaUsingCache_VariableBatchSize(b *testing.B) {
	const txCount = 1024 * 1024
	const batchSize = 1024

	b.Run("batch_"+formatNumber(batchSize), func(b *testing.B) {
		cache := setupTestCache(b)
		server := setupCacheTestServer(b, cache, batchSize, 32, 1000000)
		ctx := context.Background()

		txHashes := generateTestHashes(txCount)
		populateCache(b, cache, txHashes)

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			txMetaSlice := make([]metaSliceItem, len(txHashes))
			_, _ = server.processTxMetaUsingCache(ctx, txHashes, txMetaSlice, false)
		}

		b.ReportMetric(float64(txCount), "tx/op")
	})
}

func formatNumber(n int) string {
	switch {
	case n >= 1000:
		return string(rune('0'+n/1000)) + "k"
	default:
		result := ""
		for n > 0 {
			result = string(rune('0'+n%10)) + result
			n /= 10
		}
		return result
	}
}
