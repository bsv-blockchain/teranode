package mmaphash

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
)

func BenchmarkUpsert(b *testing.B) {
	tbl, err := New(Options{Dir: b.TempDir(), Prefix: "bench", KeySize: 36, ValueSize: 0, Expected: uint64(b.N)})
	if err != nil {
		b.Fatal(err)
	}
	defer tbl.Close()

	key := make([]byte, 36)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binary.LittleEndian.PutUint64(key[0:8], uint64(i))
		binary.LittleEndian.PutUint64(key[8:16], uint64(i)*0x9e3779b97f4a7c15)
		binary.LittleEndian.PutUint64(key[16:24], uint64(i)*2654435761)
		if _, _, err := tbl.Upsert(key, 0); err != nil {
			b.Fatal(err)
		}
	}
}

// growBenchKey writes the j-th key for a grow benchmark. When clustered, key[0:8]
// (bucket) and key[8:16] (segment) are fixed so every entry lands in one segment
// at one start bucket — the single-parent-consolidation shape this feature exists
// for; only key[24:32] distinguishes entries. When distributed, all windows vary.
func growBenchKey(key []byte, j uint64, clustered bool) {
	if clustered {
		binary.LittleEndian.PutUint64(key[0:8], 0xAAAAAAAA)
		binary.LittleEndian.PutUint64(key[8:16], 0xBBBBBBBB)
		binary.LittleEndian.PutUint64(key[16:24], 0)
		binary.LittleEndian.PutUint64(key[24:32], j)
		return
	}
	putBenchKey(key[:32], j)
}

// BenchmarkTableGrow characterizes the cost of a single grow() — allocate a new
// mmap at 2x and rehash every live entry — at a range of table sizes. grow holds
// every segment lock, so this is also the stop-the-world window during which all
// Upsert/Lookup block. Population is excluded from the timing; only grow() runs
// in the timed region.
//
// Two key shapes are measured, and the difference is the point:
//   - distributed: keys spread across buckets, so each rehash placement is ~O(1)
//     and a grow is O(N).
//   - clustered: every key shares bucket+segment (one consolidated parent's
//     outputs), so the rehash placement probe scans the whole chain — O(N^2) for
//     the hot segment. This is the workload the feature actually targets, so its
//     cost is the one that matters for the block_diskMapDirs default decision.
//
// Clustered sizes are kept small precisely because the cost is O(N^2): measured
// ~64ms@16K and ~1.0s@65K (15.7x per 4x), so 2^22 would run for minutes — that
// blow-up is itself the finding, and it extrapolates to many minutes of
// stop-the-world at large-block scale.
func BenchmarkTableGrow(b *testing.B) {
	cases := []struct {
		clustered bool
		sizes     []uint64
	}{
		{clustered: false, sizes: []uint64{1 << 16, 1 << 18, 1 << 20, 1 << 22}},
		{clustered: true, sizes: []uint64{1 << 12, 1 << 13, 1 << 14}},
	}
	for _, c := range cases {
		shape := "distributed"
		if c.clustered {
			shape = "clustered"
		}
		for _, n := range c.sizes {
			b.Run(fmt.Sprintf("%s/entries=%d", shape, n), func(b *testing.B) {
				key := make([]byte, 36)
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					tbl, err := New(Options{Dir: b.TempDir(), Prefix: "growbench", KeySize: 36, ValueSize: 0, Expected: n})
					if err != nil {
						b.Fatal(err)
					}
					for j := uint64(0); j < n; j++ {
						growBenchKey(key, j, c.clustered)
						if _, _, err := tbl.Upsert(key, 0); err != nil {
							b.Fatal(err)
						}
					}
					b.StartTimer()

					if err := tbl.grow(tbl.gen.Load()); err != nil {
						b.Fatal(err)
					}

					b.StopTimer()
					_ = tbl.Close()
				}
			})
		}
	}
}

// parallelBenchKeys inserts n keys into a fresh table in dir and returns both.
func parallelBenchKeys(b *testing.B, dir string, n uint64) (*Table, [][]byte) {
	b.Helper()

	tbl, err := New(Options{Dir: dir, Prefix: "parbench", KeySize: 32, ValueSize: 8, Expected: n})
	if err != nil {
		b.Fatal(err)
	}

	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = mkBenchKey(uint64(i))
		if _, _, err := tbl.Upsert(keys[i], uint64(i)); err != nil {
			b.Fatal(err)
		}
	}

	return tbl, keys
}

// mkBenchKey spreads i over the bucket, segment and disk-router windows.
func mkBenchKey(i uint64) []byte {
	k := make([]byte, 32)
	putBenchKey(k, i)

	return k
}

// putBenchKey writes a hash-like 32-byte key for i: each 8-byte window is an
// independent splitmix64 output, as the bucket, segment and disk windows of a
// real txid are. A plain i*odd per window does not do: the low bits of i*odd
// depend only on the low bits of i, so the segment window would fix i's low
// bits and leave each segment's keys only a few distinct buckets.
func putBenchKey(k []byte, i uint64) {
	for w := uint64(0); w < 4; w++ {
		binary.LittleEndian.PutUint64(k[w*8:w*8+8], splitmix64(i*4+w))
	}
}

func splitmix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb

	return x ^ (x >> 31)
}

// BenchmarkParallelLookup measures how Lookup scales with cores (-cpu); ns/op
// is wall time per lookup across all goroutines, so it should fall as cores
// are added. Hits only, on a page-cache-resident table. The 100k size is a
// small block, which gets the minimum segment count.
func BenchmarkParallelLookup(b *testing.B) {
	for _, n := range []uint64{100_000, 1 << 20, 1 << 24} {
		// Built once per size, not on every b.N ramp step.
		tbl, keys := parallelBenchKeys(b, b.TempDir(), n)

		b.Run(fmt.Sprintf("entries=%d", n), func(b *testing.B) {
			var next atomic.Uint64

			b.RunParallel(func(pb *testing.PB) {
				i := next.Add(1) * 104729
				for pb.Next() {
					if _, found, _ := tbl.Lookup(keys[i%n]); !found {
						b.Error("miss")
						return
					}
					i++
				}
			})
		})

		_ = tbl.Close()
	}
}

// BenchmarkParallelUpsert measures how Upsert scales with cores (-cpu). Each
// goroutine inserts its own key range through a reused buffer, so the only
// shared state is the table. Every b.N ramp step gets a fresh table of the same
// fixed size, so each sees the same layout; a step of more than expected
// inserts would include grows. The table is prefaulted first, so this measures
// inserts rather than first-touch page faults (which the model memory-vs-disk
// benchmarks include, as block validation pays them).
func BenchmarkParallelUpsert(b *testing.B) {
	const expected = 1 << 24

	tbl, err := New(Options{Dir: b.TempDir(), Prefix: "parbench", KeySize: 32, ValueSize: 8, Expected: expected})
	if err != nil {
		b.Fatal(err)
	}
	defer tbl.Close()

	prefault(tbl)

	// Each goroutine takes a disjoint 2^40-key range, so no key is inserted twice.
	var nextRange atomic.Uint64

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		key := make([]byte, 32)
		i := nextRange.Add(1) << 40
		for pb.Next() {
			putBenchKey(key, i)
			if _, _, err := tbl.Upsert(key, 1); err != nil {
				b.Error(err)
				return
			}
			i++
		}
	})
}

// prefault touches every page of the table's mapping. Storing 0 over a slot's
// state byte, which is already 0 (empty), allocates the page without changing
// any slot.
func prefault(t *Table) {
	page := os.Getpagesize()
	for off := 0; off < len(t.data); off += page {
		t.data[off] = 0
	}
}
