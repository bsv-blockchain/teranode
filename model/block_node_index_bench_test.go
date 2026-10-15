package model

import (
	"fmt"
	"hash/maphash"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// BenchmarkNodeIndexLarge measures the node index at a size where neither the index nor the node
// hashes fit in cache, as on a block of hundreds of millions of txs: a concurrent fill, then
// frozen lookups of one parent per tx, of which missRate are not in the block.
func BenchmarkNodeIndexLarge(b *testing.B) {
	if testing.Short() {
		b.Skip("holds about 4GB")
	}

	const n = 32 << 20

	nodes := newTestNodeHashes(n)
	outside := newTestNodeHashes(2 * n)[n:] // hashes that are not in the index

	workers := runtime.GOMAXPROCS(0)
	seed := maphash.MakeSeed()
	hash := func(h chainhash.Hash) uint64 { return maphash.Comparable(seed, h) }

	fill := func() *nodeIndex {
		x := newNodeIndex(n, nodes.hashAt, hash)
		parallelRange(n, workers, func(i int) {
			if err := x.Put(nodes[i], uint32(i)); err != nil { // nolint: gosec
				panic(err)
			}
		})

		return x
	}

	b.Run("put", func(b *testing.B) {
		for it := 0; it < b.N; it++ {
			fill()
		}

		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/n, "ns/put")
	})

	// built here, not in put, so the get benchmarks also run on their own (-bench .../get)
	x := fill()
	x.Freeze()

	for _, missRate := range []float64{0, 0.1, 1} {
		// the parent each tx looks up, fixed so every run does the same lookups
		rng := rand.New(rand.NewSource(1))
		parents := make([]*chainhash.Hash, n)
		misses := 0

		for i := range parents {
			if rng.Float64() < missRate {
				parents[i] = &outside[rng.Intn(n)]
				misses++
			} else {
				parents[i] = &nodes[rng.Intn(n)]
			}
		}

		b.Run(fmt.Sprintf("get/miss=%v", missRate), func(b *testing.B) {
			for it := 0; it < b.N; it++ {
				var found atomic.Int64

				parallelChunks(n, workers, func(lo, hi int) {
					c := 0

					for i := lo; i < hi; i++ {
						if _, ok := x.Get(*parents[i]); ok {
							c++
						}
					}

					found.Add(int64(c))
				})

				if int(found.Load()) != n-misses {
					b.Fatalf("found %d, want %d", found.Load(), n-misses)
				}
			}

			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/n, "ns/get")
		})
	}
}

// parallelRange runs f over [0,n) in contiguous chunks on workers goroutines.
func parallelRange(n, workers int, f func(i int)) {
	parallelChunks(n, workers, func(lo, hi int) {
		for i := lo; i < hi; i++ {
			f(i)
		}
	})
}

// parallelChunks runs f on contiguous chunks of [0,n), one per worker goroutine.
func parallelChunks(n, workers int, f func(lo, hi int)) {
	var wg sync.WaitGroup

	chunk := (n + workers - 1) / workers

	for w := 0; w < workers; w++ {
		lo, hi := w*chunk, min((w+1)*chunk, n)

		wg.Add(1)

		go func() {
			defer wg.Done()

			f(lo, hi)
		}()
	}

	wg.Wait()
}
