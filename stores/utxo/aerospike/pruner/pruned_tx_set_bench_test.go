package pruner

import (
	"crypto/rand"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// makeHashes pre-generates n random chainhash.Hash values to avoid mixing
// RNG cost into the measurement loop.
func makeHashes(b *testing.B, n int) []chainhash.Hash {
	b.Helper()
	hashes := make([]chainhash.Hash, n)
	for i := range hashes {
		if _, err := rand.Read(hashes[i][:]); err != nil {
			b.Fatalf("makeHashes: rand.Read: %v", err)
		}
	}
	return hashes
}

// --- Cuckoo (sharded, the deployed implementation) ---
//
// Tests construct NewPrunedTxSet(256, 64_000_000) — 256 shards × ~250K
// entries/shard. After 4 slots/bucket and power-of-two rounding that
// yields ~65,536 buckets per shard, totalling ~64 MiB of fingerprint
// storage — small enough to run on a developer machine. The per-op cost
// we're measuring (hash extract, bucket access, atomic CAS) is
// independent of total capacity.

// BenchmarkCuckoo_Add measures inserts into a lightly-loaded filter.
// We pre-generate a bounded pool of random hashes and cycle through it
// — keeping memory bounded regardless of b.N. Once the pool is consumed
// (typically after the first b.N=1M iterations), subsequent Adds will
// hit fingerprints already present and the cuckoo's idempotent fast
// path applies; that still exercises the same hash-extract → bucket
// access → atomic-CAS code path we care about, just without an
// eviction. Capping the pool avoids the OOM risk of pre-generating
// b.N × 32 B hashes on a CI runner when go test bumps b.N very high.
func BenchmarkCuckoo_Add(b *testing.B) {
	const cap_ = 64_000_000
	const poolSize = 1 << 20 // 1M hashes ≈ 32 MiB pool
	hashes := makeHashes(b, poolSize)
	set := NewPrunedTxSet(256, cap_)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		set.Add(hashes[i&(poolSize-1)])
	}
}

func BenchmarkCuckoo_CheckAndRemove_Hit(b *testing.B) {
	set := NewPrunedTxSet(256, 64_000_000)
	hashes := makeHashes(b, 1<<20)
	for _, h := range hashes {
		set.Add(h)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		set.CheckAndRemove(hashes[i&((1<<20)-1)])
	}
}

func BenchmarkCuckoo_CheckAndRemove_Miss(b *testing.B) {
	set := NewPrunedTxSet(256, 64_000_000)
	misses := makeHashes(b, 1<<20)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		set.CheckAndRemove(misses[i&((1<<20)-1)])
	}
}

func BenchmarkCuckoo_Parallel_AddPlusCheck(b *testing.B) {
	set := NewPrunedTxSet(256, 64_000_000)
	hashes := makeHashes(b, 1<<20)
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			set.Add(hashes[i&((1<<20)-1)])
			set.CheckAndRemove(hashes[(i+1)&((1<<20)-1)])
			i++
		}
	})
}

// --- Map (the pre-cuckoo implementation, recreated here for comparison) ---

type mapShard struct {
	mu sync.Mutex
	m  map[chainhash.Hash]struct{}
}

type mapSet struct {
	shards     []mapShard
	mask       uint8
	maxEntries int64
	count      atomic.Int64
}

func newMapSet(shardCount int, maxEntries int) *mapSet {
	n := 1
	for n < shardCount {
		n <<= 1
	}
	if n > 256 {
		n = 256
	}
	s := &mapSet{shards: make([]mapShard, n), mask: uint8(n - 1), maxEntries: int64(maxEntries)}
	for i := range s.shards {
		s.shards[i].m = make(map[chainhash.Hash]struct{}, 64)
	}
	return s
}

// AddSaturated mirrors the per-PR-628 fast path: once count >= cap, Add becomes
// an atomic-load early-return — no lock, no map touch. This is the path the
// saturated 10M-entry map followed in the original code, and the one we
// believe defined the 1.7M/sec baseline.
func (s *mapSet) AddSaturated(h chainhash.Hash) {
	if s.maxEntries > 0 && s.count.Load() >= s.maxEntries {
		return
	}
	sh := &s.shards[h[0]&s.mask]
	sh.mu.Lock()
	_, exists := sh.m[h]
	if !exists {
		sh.m[h] = struct{}{}
		s.count.Add(1)
	}
	sh.mu.Unlock()
}

func (s *mapSet) CheckAndRemove(h chainhash.Hash) bool {
	sh := &s.shards[h[0]&s.mask]
	sh.mu.Lock()
	_, ok := sh.m[h]
	if ok {
		delete(sh.m, h)
		s.count.Add(-1)
	}
	sh.mu.Unlock()
	return ok
}

// BenchmarkMap_Add_Saturated measures the saturated fast path: cap=0 means
// already-full, every Add returns immediately. This was the dominant
// behaviour in PR #628 once the 10M cap was hit.
func BenchmarkMap_Add_Saturated(b *testing.B) {
	set := newMapSet(256, 1)           // cap=1, immediately saturated after first add
	set.AddSaturated(chainhash.Hash{}) // fill the one slot
	hashes := makeHashes(b, 1<<20)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		set.AddSaturated(hashes[i&((1<<20)-1)])
	}
}

// BenchmarkMap_Add_Active measures Add while still below cap — every call
// takes the lock and writes to the map.
func BenchmarkMap_Add_Active(b *testing.B) {
	set := newMapSet(256, 1_000_000_000) // effectively unlimited for the bench
	hashes := makeHashes(b, 1<<20)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		set.AddSaturated(hashes[i&((1<<20)-1)])
	}
}

func BenchmarkMap_CheckAndRemove_Hit(b *testing.B) {
	set := newMapSet(256, 1_000_000_000)
	hashes := makeHashes(b, 1<<20)
	for _, h := range hashes {
		set.AddSaturated(h)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		set.CheckAndRemove(hashes[i&((1<<20)-1)])
	}
}

func BenchmarkMap_CheckAndRemove_Miss(b *testing.B) {
	set := newMapSet(256, 1_000_000_000)
	misses := makeHashes(b, 1<<20)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		set.CheckAndRemove(misses[i&((1<<20)-1)])
	}
}

func BenchmarkMap_Parallel_AddPlusCheck_Saturated(b *testing.B) {
	set := newMapSet(256, 1)
	set.AddSaturated(chainhash.Hash{})
	hashes := makeHashes(b, 1<<20)
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			set.AddSaturated(hashes[i&((1<<20)-1)])
			set.CheckAndRemove(hashes[(i+1)&((1<<20)-1)])
			i++
		}
	})
}
