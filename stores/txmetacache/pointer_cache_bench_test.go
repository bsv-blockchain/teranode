package txmetacache

import (
	"context"
	"math/rand"
	"runtime"
	"sort"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// benchEntry is a pre-built triple used to feed both cache implementations
// identical data: the same key, the same *meta.Data, and the same serialised
// bytes that the Kafka ingest path produces.
type benchEntry struct {
	hash  chainhash.Hash
	data  *meta.Data
	bytes []byte
}

// buildBenchEntries allocates n synthetic test entries with deterministic
// hashes and two parent inputs each. Two parents is a reasonable median for
// Bitcoin transactions and matches the size assumption baked into
// pointerCacheAvgEntryBytes.
func buildBenchEntries(b *testing.B, n int) []benchEntry {
	b.Helper()

	entries := make([]benchEntry, n)
	rng := rand.New(rand.NewSource(0x6caf3d7))

	for i := 0; i < n; i++ {
		var h chainhash.Hash
		_, _ = rng.Read(h[:])

		parents := make([]chainhash.Hash, 2)
		_, _ = rng.Read(parents[0][:])
		_, _ = rng.Read(parents[1][:])

		// voutIdxs packed layout: [count_p0, voutIdx_p0_0, count_p1, voutIdx_p1_0]
		// One vout per parent, each index 0 — keeps the synthetic data shape stable
		// across go-subtree versions.
		d := &meta.Data{
			Fee:         uint64(1000 + i),
			SizeInBytes: uint64(250 + i%500),
			IsCoinbase:  i%4096 == 0,
			TxInpoints:  subtree.NewTxInpointsFromPacked(parents, []uint32{1, 0, 1, 0}),
		}

		bts, err := d.MetaBytes()
		require.NoError(b, err)

		entries[i] = benchEntry{hash: h, data: d, bytes: bts}
	}

	return entries
}

// newBenchTxMetaCache constructs a TxMetaCache of the given size in MB using
// the requested bucket type. The wrapped utxo.Store is a nullstore so cache
// misses don't fan out to a real backend.
func newBenchTxMetaCache(b *testing.B, sizeMB int, bucket BucketType) *TxMetaCache {
	b.Helper()

	ctx := context.Background()

	ns, err := nullstore.NewNullStore()
	require.NoError(b, err)
	require.NoError(b, ns.SetBlockHeight(100))

	c, err := NewTxMetaCache(ctx, settings.NewSettings(), ulogger.NewErrorTestLogger(b), ns, bucket, sizeMB)
	require.NoError(b, err)

	return c.(*TxMetaCache)
}

const (
	benchCacheSizeMB = 64
	benchEntryCount  = 8 * 1024 // matches one entry per shard at production BucketsCount; small enough to fit
)

// BenchmarkSet_Existing measures the byte-path Set cost on the legacy
// (ImprovedCache) backend.
func BenchmarkSet_Existing(b *testing.B) {
	benchSetBytes(b, Unallocated)
}

// BenchmarkSet_PointerWithDeserialize measures the byte-path Set cost on the
// pointer backend: deserialise bytes into a *meta.Data and insert. This is
// the realistic Kafka ingest cost when pointer mode is on.
func BenchmarkSet_PointerWithDeserialize(b *testing.B) {
	benchSetBytes(b, Pointer)
}

func benchSetBytes(b *testing.B, bucket BucketType) {
	c := newBenchTxMetaCache(b, benchCacheSizeMB, bucket)
	entries := buildBenchEntries(b, benchEntryCount)
	keys := make([][]byte, len(entries))

	for i := range entries {
		keys[i] = entries[i].hash[:]
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		e := &entries[i%len(entries)]
		if err := c.SetCacheFromBytes(keys[i%len(entries)], e.bytes); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSet_PointerNative measures the pointer-cache insert when the
// caller already has a *meta.Data (Phase D target — Kafka deserialises once
// in the writer and hands the pointer to the cache).
func BenchmarkSet_PointerNative(b *testing.B) {
	c := newBenchTxMetaCache(b, benchCacheSizeMB, Pointer)
	entries := buildBenchEntries(b, benchEntryCount)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		e := &entries[i%len(entries)]
		if err := c.SetCache(&e.hash, e.data); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkGet_Existing measures the byte-path Get cost on the legacy
// backend. The new GetMetaCached returns a *meta.Data — ImprovedCache
// allocates and deserialises one per call.
func BenchmarkGet_Existing(b *testing.B) {
	benchGet(b, Unallocated)
}

// BenchmarkGet_Pointer measures the same operation on the pointer backend.
// PointerCache.Get returns the stored pointer with zero allocations.
func BenchmarkGet_Pointer(b *testing.B) {
	benchGet(b, Pointer)
}

func benchGet(b *testing.B, bucket BucketType) {
	c := newBenchTxMetaCache(b, benchCacheSizeMB, bucket)
	entries := buildBenchEntries(b, benchEntryCount)

	for i := range entries {
		require.NoError(b, c.SetCacheFromBytes(entries[i].hash[:], entries[i].bytes))
	}

	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, ok := c.GetMetaCached(ctx, entries[i%len(entries)].hash)
		if !ok {
			b.Fatal("expected hit")
		}
	}
}

// BenchmarkConcurrentReadHeavy_Existing simulates the production read pattern
// (32:1 read:write) on the legacy backend.
func BenchmarkConcurrentReadHeavy_Existing(b *testing.B) {
	benchConcurrentReadHeavy(b, Unallocated)
}

// BenchmarkConcurrentReadHeavy_Pointer is the same workload on the pointer
// backend.
func BenchmarkConcurrentReadHeavy_Pointer(b *testing.B) {
	benchConcurrentReadHeavy(b, Pointer)
}

func benchConcurrentReadHeavy(b *testing.B, bucket BucketType) {
	// Use a generously sized cache so eviction is not the dominant signal.
	c := newBenchTxMetaCache(b, benchCacheSizeMB*4, bucket)
	entries := buildBenchEntries(b, benchEntryCount)

	for i := range entries {
		require.NoError(b, c.SetCacheFromBytes(entries[i].hash[:], entries[i].bytes))
	}

	ctx := context.Background()

	var misses uint64

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := uint64(0)

		for pb.Next() {
			idx := atomic.AddUint64(&i, 1)
			e := &entries[idx%uint64(len(entries))]

			// 1 write per 32 reads.
			if idx%32 == 0 {
				if err := c.SetCacheFromBytes(e.hash[:], e.bytes); err != nil {
					b.Fatal(err)
				}

				continue
			}

			if _, ok := c.GetMetaCached(ctx, e.hash); !ok {
				atomic.AddUint64(&misses, 1)
			}
		}
	})

	b.StopTimer()

	if b.N > 0 {
		b.ReportMetric(float64(misses)*100/float64(b.N), "miss_pct")
	}
}

// BenchmarkGCPause_Existing populates the legacy cache near saturation and
// reports GC pause distribution under mixed Set/Get traffic.
func BenchmarkGCPause_Existing(b *testing.B) {
	benchGCPause(b, Unallocated)
}

// BenchmarkGCPause_Pointer is the same workload on the pointer backend.
func BenchmarkGCPause_Pointer(b *testing.B) {
	benchGCPause(b, Pointer)
}

func benchGCPause(b *testing.B, bucket BucketType) {
	c := newBenchTxMetaCache(b, benchCacheSizeMB, bucket)
	entries := buildBenchEntries(b, 100_000)

	for i := range entries {
		if err := c.SetCacheFromBytes(entries[i].hash[:], entries[i].bytes); err != nil {
			b.Fatal(err)
		}
	}

	runtime.GC()

	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		e := &entries[i%len(entries)]

		if i%32 == 0 {
			if err := c.SetCacheFromBytes(e.hash[:], e.bytes); err != nil {
				b.Fatal(err)
			}

			continue
		}

		// Misses are expected at capacity; the GC signal is what we want.
		_, _ = c.GetMetaCached(ctx, e.hash)
	}

	b.StopTimer()

	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	pauses := collectRecentPauses(&before, &after)
	if len(pauses) == 0 {
		b.ReportMetric(0, "gc_pause_p99_ms")
		b.ReportMetric(0, "gc_count")
		return
	}

	sort.Slice(pauses, func(i, j int) bool { return pauses[i] < pauses[j] })

	p50 := pauses[len(pauses)/2]
	p99 := pauses[(len(pauses)*99)/100]
	if (len(pauses)*99)/100 >= len(pauses) {
		p99 = pauses[len(pauses)-1]
	}

	b.ReportMetric(float64(p50)/1e6, "gc_pause_p50_ms")
	b.ReportMetric(float64(p99)/1e6, "gc_pause_p99_ms")
	b.ReportMetric(float64(after.NumGC-before.NumGC), "gc_count")
	b.ReportMetric(float64(after.HeapAlloc)/(1024*1024), "heap_alloc_mb")
}

// collectRecentPauses returns the slice of GC pause durations (ns) that fired
// between two MemStats snapshots, using runtime's circular PauseNs buffer.
func collectRecentPauses(before, after *runtime.MemStats) []uint64 {
	n := after.NumGC - before.NumGC
	if n == 0 {
		return nil
	}

	if n > uint32(len(after.PauseNs)) {
		n = uint32(len(after.PauseNs))
	}

	out := make([]uint64, 0, n)

	for i := uint32(0); i < n; i++ {
		idx := (after.NumGC + 255 - i) % 256
		out = append(out, after.PauseNs[idx])
	}

	return out
}

// BenchmarkMemoryEfficiency_Existing fills the legacy cache and reports
// HeapInuse growth plus live-entry count.
func BenchmarkMemoryEfficiency_Existing(b *testing.B) {
	benchMemoryEfficiency(b, Unallocated)
}

// BenchmarkMemoryEfficiency_Pointer is the same measurement on the pointer
// backend.
func BenchmarkMemoryEfficiency_Pointer(b *testing.B) {
	benchMemoryEfficiency(b, Pointer)
}

func benchMemoryEfficiency(b *testing.B, bucket BucketType) {
	runtime.GC()
	runtime.GC()

	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	c := newBenchTxMetaCache(b, benchCacheSizeMB, bucket)
	entries := buildBenchEntries(b, 200_000)

	for i := range entries {
		if err := c.SetCacheFromBytes(entries[i].hash[:], entries[i].bytes); err != nil {
			b.Fatal(err)
		}
	}

	runtime.GC()
	runtime.GC()

	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	stats := c.GetCacheStats()

	heapDelta := int64(after.HeapInuse) - int64(before.HeapInuse)
	if heapDelta < 0 {
		heapDelta = 0
	}

	heapDeltaMB := float64(heapDelta) / (1024 * 1024)

	b.ReportMetric(heapDeltaMB, "heap_inuse_growth_mb")
	b.ReportMetric(float64(stats.ValidEntriesCount), "live_entries")

	if heapDeltaMB > 0 {
		b.ReportMetric(float64(stats.ValidEntriesCount)/heapDeltaMB, "entries_per_mb")
	}

	_ = entries[0].hash
}
