package subtreevalidation

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
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

// testCacheBucket is the backend setupTestCache builds. The probe tests run
// against the deployed default; TestProcessTxMetaUsingCache_Lockfree reruns
// them against the lock-free backend.
var testCacheBucket = txmetacache.Unallocated

func setupTestCache(t testing.TB) *txmetacache.TxMetaCache {
	t.Helper()
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	c, err := txmetacache.NewTxMetaCache(ctx, tSettings, logger, utxoStore, testCacheBucket)
	require.NoError(t, err)

	return c.(*txmetacache.TxMetaCache)
}

func TestProcessTxMetaUsingCache_Lockfree(t *testing.T) {
	testCacheBucket = txmetacache.Lockfree

	t.Cleanup(func() { testCacheBucket = txmetacache.Unallocated })

	for name, fn := range map[string]func(*testing.T){
		"PartialEntryTreatedAsMiss":       TestProcessTxMetaUsingCache_PartialEntryTreatedAsMiss,
		"CoinbaseEntryWithNoParentsIsHit": TestProcessTxMetaUsingCache_CoinbaseEntryWithNoParentsIsHit,
		"AllFound":                        TestProcessTxMetaUsingCache_AllFound,
		"AllMissing":                      TestProcessTxMetaUsingCache_AllMissing,
		"PartialHits":                     TestProcessTxMetaUsingCache_PartialHits,
		"SkipCoinbasePlaceholder":         TestProcessTxMetaUsingCache_SkipCoinbasePlaceholder,
		"SkipPrefilledEntries":            TestProcessTxMetaUsingCache_SkipPrefilledEntries,
		"FailFastThresholdExceeded":       TestProcessTxMetaUsingCache_FailFastThresholdExceeded,
		"MultipleBatches":                 TestProcessTxMetaUsingCache_MultipleBatches,
		"FieldsMatchCachedMetaEndToEnd":   testProcessTxMetaUsingCacheFields,
		"LargeMixedSubtree":               TestProcessTxMetaUsingCache_LargeMixedSubtree,
	} {
		t.Run(name, fn)
	}
}

// testProcessTxMetaUsingCacheFields checks every field the probe copies out of
// the cache, including the parent hashes and vouts, survive the round trip.
func testProcessTxMetaUsingCacheFields(t *testing.T) {
	cache := setupTestCache(t)
	server := setupCacheTestServer(t, cache, 4, 1000)

	hashes := generateTestHashes(3000)
	populateCache(t, cache, hashes)

	txMetaSlice := make([]metaSliceItem, len(hashes))

	missed, err := server.processTxMetaUsingCache(context.Background(), hashes, txMetaSlice, false)
	require.NoError(t, err)
	require.Zero(t, missed)

	ref, ok := cache.GetMetaCached(context.Background(), hashes[0])
	require.True(t, ok)

	refInpoints, err := ref.TxInpoints.Serialize()
	require.NoError(t, err)

	for i := range txMetaSlice {
		require.True(t, txMetaSlice[i].isSet)
		require.Equal(t, uint64(100), txMetaSlice[i].fee)
		require.Equal(t, uint64(250), txMetaSlice[i].sizeInBytes)

		got, err := txMetaSlice[i].txInpoints.Serialize()
		require.NoError(t, err)
		require.Equal(t, refInpoints, got)
	}
}

func setupCacheTestServer(t testing.TB, cache *txmetacache.TxMetaCache, concurrency, threshold int) *Server {
	t.Helper()
	tSettings := test.CreateBaseTestSettings(t)
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
	// Seed complete entries: a non-coinbase tx always has at least one parent, so
	// a real cache entry carries a non-empty ParentTxHashes. (An empty one is a
	// partial entry that the cache read-path now correctly treats as a miss.)
	parent := chainhash.HashH([]byte("test-parent"))
	in := &bt.Input{PreviousTxOutIndex: 0}
	require.NoError(t, in.PreviousTxIDAdd(&parent))
	ti, err := subtree.NewTxInpointsFromInputs([]*bt.Input{in})
	require.NoError(t, err)

	testMeta := &meta.Data{
		Fee:         100,
		SizeInBytes: 250,
		TxInpoints:  ti,
		BlockIDs:    []uint32{},
	}

	for i := range hashes {
		err := cache.SetCache(&hashes[i], testMeta)
		require.NoError(t, err)
	}
}

func TestProcessTxMetaUsingCache_PartialEntryTreatedAsMiss(t *testing.T) {
	cache := setupTestCache(t)
	server := setupCacheTestServer(t, cache, 4, 1000)
	ctx := context.Background()

	// Simulate a poisoned cache entry: a non-coinbase tx cached with empty
	// ParentTxHashes (as a reduced-field BatchDecorate path would write). This
	// is the entry that wedges block validation when trusted verbatim.
	hash := chainhash.HashH([]byte("partial-cache-entry"))
	partial := &meta.Data{
		Fee:         100,
		SizeInBytes: 250,
		TxInpoints:  subtree.TxInpoints{ParentTxHashes: []chainhash.Hash{}},
		BlockIDs:    []uint32{},
	}
	require.NoError(t, cache.SetCache(&hash, partial))

	txHashes := []chainhash.Hash{hash}
	txMetaSlice := make([]metaSliceItem, 1)

	missed, err := server.processTxMetaUsingCache(ctx, txHashes, txMetaSlice, false)
	require.NoError(t, err)
	require.Equal(t, 1, missed, "partial entry (non-coinbase, empty ParentTxHashes) must be treated as a miss so the store fallback can self-heal it")
	require.False(t, txMetaSlice[0].isSet, "partial entry must not be marked as set")
}

func TestProcessTxMetaUsingCache_CoinbaseEntryWithNoParentsIsHit(t *testing.T) {
	cache := setupTestCache(t)
	server := setupCacheTestServer(t, cache, 4, 1000)
	ctx := context.Background()

	// A coinbase tx legitimately has no parents; an empty-ParentTxHashes entry
	// flagged IsCoinbase is complete and must still be served from the cache.
	hash := chainhash.HashH([]byte("coinbase-cache-entry"))
	coinbaseMeta := &meta.Data{
		Fee:         0,
		SizeInBytes: 100,
		IsCoinbase:  true,
		TxInpoints:  subtree.TxInpoints{ParentTxHashes: []chainhash.Hash{}},
		BlockIDs:    []uint32{},
	}
	require.NoError(t, cache.SetCache(&hash, coinbaseMeta))

	txHashes := []chainhash.Hash{hash}
	txMetaSlice := make([]metaSliceItem, 1)

	missed, err := server.processTxMetaUsingCache(ctx, txHashes, txMetaSlice, false)
	require.NoError(t, err)
	require.Equal(t, 0, missed, "a coinbase entry with no parents is complete and must be a hit")
	require.True(t, txMetaSlice[0].isSet)
	require.True(t, txMetaSlice[0].coinbase)
}

func TestProcessTxMetaUsingCache_MismatchedSliceLengths(t *testing.T) {
	cache := setupTestCache(t)
	server := setupCacheTestServer(t, cache, 4, 100)
	ctx := context.Background()

	txHashes := generateTestHashes(10)
	txMetaSlice := make([]metaSliceItem, 5) // Different length

	_, err := server.processTxMetaUsingCache(ctx, txHashes, txMetaSlice, false)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrProcessing))
}

func TestProcessTxMetaUsingCache_NonCachedStore(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
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
	server := setupCacheTestServer(t, cache, 4, 100)
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
	server := setupCacheTestServer(t, cache, 4, 1000) // High threshold to avoid fail-fast
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
	server := setupCacheTestServer(t, cache, 4, 1000)
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
	server := setupCacheTestServer(t, cache, 4, 1000)
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
	server := setupCacheTestServer(t, cache, 4, 1000)
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
	server := setupCacheTestServer(t, cache, 4, 5) // Low threshold of 5
	ctx := context.Background()

	txHashes := generateTestHashes(100)
	txMetaSlice := make([]metaSliceItem, len(txHashes))

	_, err := server.processTxMetaUsingCache(ctx, txHashes, txMetaSlice, true)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrProcessing))
}

func TestProcessTxMetaUsingCache_FailFastNotTriggered(t *testing.T) {
	cache := setupTestCache(t)
	server := setupCacheTestServer(t, cache, 4, 1000) // High threshold
	ctx := context.Background()

	txHashes := generateTestHashes(10)
	txMetaSlice := make([]metaSliceItem, len(txHashes))

	missed, err := server.processTxMetaUsingCache(ctx, txHashes, txMetaSlice, true)
	require.NoError(t, err)
	require.Equal(t, 10, missed)
}

func TestProcessTxMetaUsingCache_ContextCancellation(t *testing.T) {
	cache := setupTestCache(t)
	server := setupCacheTestServer(t, cache, 1, 100000) // single probe worker
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	txHashes := generateTestHashes(100)
	txMetaSlice := make([]metaSliceItem, len(txHashes))

	_, err := server.processTxMetaUsingCache(ctx, txHashes, txMetaSlice, false)
	require.Error(t, err)
}

func TestProcessTxMetaUsingCache_MultipleBatches(t *testing.T) {
	cache := setupTestCache(t)
	server := setupCacheTestServer(t, cache, 4, 10000)
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

// TestProcessTxMetaUsingCache_LargeMixedSubtree probes a subtree large enough
// to be split into many tasks, with a coinbase placeholder, prefilled entries,
// hits and misses mixed through it, and checks every slot ends up as expected.
func TestProcessTxMetaUsingCache_LargeMixedSubtree(t *testing.T) {
	cache := setupTestCache(t)
	server := setupCacheTestServer(t, cache, 4, 1_000_000)

	const n = 50_000

	txHashes := generateTestHashes(n)
	txHashes[0] = *subtree.CoinbasePlaceholderHash

	var cached []chainhash.Hash

	for i := 1; i < n; i++ {
		if i%3 != 0 {
			cached = append(cached, txHashes[i])
		}
	}

	populateCache(t, cache, cached)

	txMetaSlice := make([]metaSliceItem, n)
	for i := 5; i < n; i += 7 {
		txMetaSlice[i] = metaSliceItem{isSet: true, fee: 9}
	}

	missed, err := server.processTxMetaUsingCache(context.Background(), txHashes, txMetaSlice, false)
	require.NoError(t, err)

	wantMissed := 0

	for i := 0; i < n; i++ {
		prefilled := i >= 5 && (i-5)%7 == 0

		switch {
		case i == 0:
			require.False(t, txMetaSlice[i].isSet, "coinbase placeholder is never looked up")
		case prefilled:
			require.Equal(t, uint64(9), txMetaSlice[i].fee, "slot %d was prefilled and must be left alone", i)
		case i%3 != 0:
			require.True(t, txMetaSlice[i].isSet, "slot %d is cached", i)
			require.Equal(t, uint64(100), txMetaSlice[i].fee, "slot %d", i)
		default:
			require.False(t, txMetaSlice[i].isSet, "slot %d is not cached", i)

			wantMissed++
		}
	}

	require.Equal(t, wantMissed, missed)
}

func TestProbeSpans(t *testing.T) {
	for _, tc := range []struct{ n, size int }{{0, 4}, {1, 4}, {4, 4}, {5, 4}, {4097, 4096}, {1_000_000, 4096}} {
		spans := probeSpans(tc.n, tc.size)

		next := 0
		for _, sp := range spans {
			require.Equal(t, next, sp[0], "n=%d: spans must be contiguous", tc.n)
			require.Greater(t, sp[1], sp[0], "n=%d: no empty span", tc.n)
			require.LessOrEqual(t, sp[1]-sp[0], tc.size, "n=%d", tc.n)

			next = sp[1]
		}

		require.Equal(t, tc.n, next, "n=%d: spans must cover every index", tc.n)
	}
}

// BenchmarkProbeTxMetaCache_FullSubtree probes a full-size subtree, all hits,
// with a fresh txMetaSlice per call as ValidateSubtreeInternal makes one.
func BenchmarkProbeTxMetaCache_FullSubtree(b *testing.B) {
	for name, bucket := range map[string]txmetacache.BucketType{"unallocated": txmetacache.Unallocated, "lockfree": txmetacache.Lockfree} {
		b.Run(name, func(b *testing.B) {
			testCacheBucket = bucket

			b.Cleanup(func() { testCacheBucket = txmetacache.Unallocated })

			cache := setupTestCache(b)
			server := setupCacheTestServer(b, cache, 32, 1_000_000)

			txHashes := generateTestHashes(1 << 20)
			populateCache(b, cache, txHashes)

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				txMetaSlice := make([]metaSliceItem, len(txHashes))
				_, _ = server.processTxMetaUsingCache(context.Background(), txHashes, txMetaSlice, false)
			}

			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(len(txHashes)), "ns/tx")
		})
	}
}

func TestProcessTxMetaUsingCache_EmptyInput(t *testing.T) {
	cache := setupTestCache(t)
	server := setupCacheTestServer(t, cache, 4, 100)
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
			server := setupCacheTestServer(b, cache, 32, 1000000)
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
			server := setupCacheTestServer(b, cache, 32, 1000000)
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
			server := setupCacheTestServer(b, cache, 32, 1000000)
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
			server := setupCacheTestServer(b, cache, concurrency, 1000000)
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
