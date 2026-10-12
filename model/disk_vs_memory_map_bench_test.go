package model

import (
	"fmt"
	"math/rand/v2"
	"runtime"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"golang.org/x/sync/errgroup"
)

// Benchmarks comparing the in-memory maps block validation uses by default
// with the disk-backed ones block_diskMapDirs selects, driven the way
// Block.Valid drives them: GOMAXPROCS goroutines, each working a contiguous
// share of the keys. Vary the core count with -cpu:
//
//	go test -run '^$' -bench 'MemoryVsDisk' -cpu 1,4,16 -benchtime 3x ./model/
//
// ns/key is the wall time per key across all workers, so it falls as cores
// are added only if the map scales. heapMB is the Go heap the filled map
// holds; the disk maps hold theirs in the page cache instead. At these sizes
// the disk tables stay page-cache resident, so the disk variants measure the
// table code and its locking, not device I/O.
//
// Known bias: each memory iteration builds a fresh map, so its first-touch page
// faults are timed, while production reuses pooled, already-faulted maps. The
// disk variants' Close (munmap) runs outside the timer. Both favour disk.

// mapBenchSizes are the block sizes to run. 100k is a small block, which gets
// the minimum segment count on disk. -short, as the CI benchmark comparison
// runs, keeps only that one: the larger sizes take minutes per iteration once
// a runner's page cache cannot hold the disk tables.
func mapBenchSizes() []int {
	if testing.Short() {
		return []int{100_000}
	}

	return []int{100_000, 1 << 20, 1 << 24}
}

var mapBenchImpls = []struct {
	name  string
	disks int // 0 = in-memory
}{
	{"memory", 0},
	{"disk_1", 1},
	{"disk_2", 2},
}

// mapBenchHashes returns n distinct pseudo-random hashes, deterministic per seed.
func mapBenchHashes(n int, seed uint64) []chainhash.Hash {
	r := rand.New(rand.NewChaCha8([32]byte{byte(seed), byte(seed >> 8)}))
	hashes := make([]chainhash.Hash, n)

	for i := range hashes {
		for j := 0; j < chainhash.HashSize; j += 8 {
			v := r.Uint64()
			for k := 0; k < 8; k++ {
				hashes[i][j+k] = byte(v >> (8 * k))
			}
		}
	}

	return hashes
}

// runShares splits [0, n) into one contiguous share per GOMAXPROCS and runs fn
// on each share concurrently.
func runShares(n int, fn func(lo, hi int) error) error {
	workers := runtime.GOMAXPROCS(0)
	share := (n + workers - 1) / workers

	var g errgroup.Group

	for lo := 0; lo < n; lo += share {
		lo, hi := lo, min(lo+share, n)
		g.Go(func() error { return fn(lo, hi) })
	}

	return g.Wait()
}

func newBenchTxMap(b *testing.B, n, disks int) (txmap.TxMap, func()) {
	b.Helper()

	if disks == 0 {
		return txmap.NewSplitSwissMapUint64(uint32(n), txMapBuckets), func() {} //nolint:gosec // bench sizes fit
	}

	dirs := make([]string, disks)
	for i := range dirs {
		dirs[i] = b.TempDir()
	}

	m, err := NewDiskTxMapUint64(DiskTxMapUint64Options{BasePaths: dirs, Prefix: "bench-txmap", FilterCapacity: uint(n)})
	if err != nil {
		b.Fatal(err)
	}

	return m, func() { _ = m.Close() }
}

func newBenchParentSpendsMap(b *testing.B, n, disks int) (ParentSpendsMap, func()) {
	b.Helper()

	if disks == 0 {
		return NewSplitSyncedParentMap(parentSpendsBuckets, uint64(n)), func() {}
	}

	dirs := make([]string, disks)
	for i := range dirs {
		dirs[i] = b.TempDir()
	}

	m, err := NewDiskParentSpendsMap(DiskParentSpendsMapOptions{BasePaths: dirs, Prefix: "bench-parentspends", FilterCapacity: uint(n)})
	if err != nil {
		b.Fatal(err)
	}

	return m, func() { _ = m.Close() }
}

func fillBenchTxMap(m txmap.TxMap, hashes []chainhash.Hash) error {
	return runShares(len(hashes), func(lo, hi int) error {
		for i := lo; i < hi; i++ {
			if err := m.Put(hashes[i], uint64(i)); err != nil { //nolint:gosec // i >= 0
				return err
			}
		}

		return nil
	})
}

func heapInUse() uint64 {
	runtime.GC()

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	return ms.HeapAlloc
}

// heapGrowthMB is the heap growth since before, in MiB, floored at zero.
func heapGrowthMB(before uint64) float64 {
	after := heapInUse()
	if after < before {
		return 0
	}

	return float64(after-before) / (1 << 20)
}

func reportPerKey(b *testing.B, keysPerOp int) {
	perKey := float64(b.Elapsed().Nanoseconds()) / float64(b.N*keysPerOp)
	b.ReportMetric(perKey, "ns/key")
	b.ReportMetric(1e3/perKey, "Mkeys/s")
}

// BenchmarkTxMapPut_MemoryVsDisk is the checkDuplicateTransactions write
// phase: one Put per transaction into a fresh map.
func BenchmarkTxMapPut_MemoryVsDisk(b *testing.B) {
	for _, n := range mapBenchSizes() {
		hashes := mapBenchHashes(n, 1)

		for _, impl := range mapBenchImpls {
			b.Run(fmt.Sprintf("txs=%d/%s", n, impl.name), func(b *testing.B) {
				var heapMB float64

				for i := 0; i < b.N; i++ {
					b.StopTimer()

					// heapInUse forces a GC; measuring once keeps the
					// untimed cost of a run bounded.
					var before uint64
					if i == 0 {
						before = heapInUse()
					}
					m, closeFn := newBenchTxMap(b, n, impl.disks)

					b.StartTimer()

					if err := fillBenchTxMap(m, hashes); err != nil {
						b.Fatal(err)
					}

					b.StopTimer()

					if i == 0 {
						heapMB = heapGrowthMB(before)
					}

					runtime.KeepAlive(m)
					closeFn()
					b.StartTimer()
				}

				reportPerKey(b, n)
				b.ReportMetric(heapMB, "heapMB")
			})
		}
	}
}

// BenchmarkTxMapGetFrozen_MemoryVsDisk is the validOrderAndBlessed read phase
// on the frozen txMap: "hit" is the per-transaction lookup, "miss" the lookup
// of a parent that is not in the block.
func BenchmarkTxMapGetFrozen_MemoryVsDisk(b *testing.B) {
	for _, n := range mapBenchSizes() {
		hashes := mapBenchHashes(n, 1)
		misses := mapBenchHashes(n, 2)

		for _, impl := range mapBenchImpls {
			for _, lookup := range []struct {
				name  string
				keys  []chainhash.Hash
				found bool
			}{{"hit", hashes, true}, {"miss", misses, false}} {
				b.Run(fmt.Sprintf("txs=%d/%s/%s", n, impl.name, lookup.name), func(b *testing.B) {
					m, closeFn := newBenchTxMap(b, n, impl.disks)
					defer closeFn()

					if err := fillBenchTxMap(m, hashes); err != nil {
						b.Fatal(err)
					}

					m.Freeze()

					b.ResetTimer()

					for i := 0; i < b.N; i++ {
						err := runShares(n, func(lo, hi int) error {
							for j := lo; j < hi; j++ {
								if _, ok := m.Get(lookup.keys[j]); ok != lookup.found {
									return errors.NewProcessingError("key %d: found=%v, want %v", j, ok, lookup.found)
								}
							}

							return nil
						})
						if err != nil {
							b.Fatal(err)
						}
					}

					reportPerKey(b, n)
				})
			}
		}
	}
}

// BenchmarkParentSpendsSetIfNotExists_MemoryVsDisk is the duplicate-input
// check in validOrderAndBlessed: one SetIfNotExists per input into a fresh
// map, at two inputs per transaction (the block_parentSpendsCapacityMultiplier
// default the map is sized with).
func BenchmarkParentSpendsSetIfNotExists_MemoryVsDisk(b *testing.B) {
	for _, txs := range mapBenchSizes() {
		n := 2 * txs
		parents := mapBenchHashes(txs, 3)

		inpoints := make([]subtreepkg.Inpoint, n)
		for i := range inpoints {
			inpoints[i] = subtreepkg.Inpoint{Hash: parents[i/2], Index: uint32(i % 2)} //nolint:gosec // 0 or 1
		}

		for _, impl := range mapBenchImpls {
			b.Run(fmt.Sprintf("inputs=%d/%s", n, impl.name), func(b *testing.B) {
				var heapMB float64

				for i := 0; i < b.N; i++ {
					b.StopTimer()

					// heapInUse forces a GC; measuring once keeps the
					// untimed cost of a run bounded.
					var before uint64
					if i == 0 {
						before = heapInUse()
					}
					m, closeFn := newBenchParentSpendsMap(b, n, impl.disks)

					b.StartTimer()

					err := runShares(n, func(lo, hi int) error {
						for j := lo; j < hi; j++ {
							inserted, err := m.SetIfNotExists(inpoints[j])
							if err != nil {
								return err
							}

							if !inserted {
								return errors.NewProcessingError("inpoint %d reported as duplicate", j)
							}
						}

						return nil
					})
					if err != nil {
						b.Fatal(err)
					}

					b.StopTimer()

					if i == 0 {
						heapMB = heapGrowthMB(before)
					}

					runtime.KeepAlive(m)
					closeFn()
					b.StartTimer()
				}

				reportPerKey(b, n)
				b.ReportMetric(heapMB, "heapMB")
			})
		}
	}
}
