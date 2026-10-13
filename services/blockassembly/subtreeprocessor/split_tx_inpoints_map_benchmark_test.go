package subtreeprocessor

import (
	"encoding/binary"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
)

// heapStats returns the live heap bytes and objects after a full GC.
func heapStats() (bytes, objects uint64) {
	runtime.GC()

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	return ms.HeapAlloc, ms.HeapObjects
}

func benchTxHash(i int) chainhash.Hash {
	var h chainhash.Hash

	binary.LittleEndian.PutUint64(h[:], 0x9e3779b97f4a7c15*uint64(i+1)) //nolint:gosec // test data
	binary.LittleEndian.PutUint64(h[8:], uint64(i)+1)                   //nolint:gosec // test data

	return h
}

// benchTxInpoints builds the inpoints of tx i afresh, as the queue hands them
// to the map: the map owns what it is given, or encodes it and lets it go.
func benchTxInpoints(i int) *subtreepkg.TxInpoints {
	var parent chainhash.Hash

	binary.LittleEndian.PutUint64(parent[:], ^uint64(i)) //nolint:gosec // test data

	in := subtreepkg.NewTxInpointsFromPacked([]chainhash.Hash{parent}, []uint32{1, uint32(i % 3)}) //nolint:gosec // test data

	return &in
}

// fillSplitMap inserts txs [0, total) with one writer per core, SetIfNotExists,
// as the dequeue does.
func fillSplitMap(m *SplitTxInpointsMap, total int) {
	workers := runtime.GOMAXPROCS(0)
	perWorker := (total + workers - 1) / workers

	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		lo, hi := min(w*perWorker, total), min((w+1)*perWorker, total)

		wg.Go(func() {
			for i := lo; i < hi; i++ {
				m.SetIfNotExists(benchTxHash(i), benchTxInpoints(i))
			}
		})
	}

	wg.Wait()
}

// BenchmarkSplitTxInpointsMapFootprint fills a map with total txs and reports
// what it costs to keep: heap bytes and objects per tx, how long a full GC takes
// with the map live, and the ingest rate.
//
// Run with -benchtime=1x.
func BenchmarkSplitTxInpointsMapFootprint(b *testing.B) {
	for _, total := range []int{4 << 20} {
		b.Run(fmt.Sprintf("%dM", total>>20), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()

				baseBytes, baseObjects := heapStats()

				b.StartTimer()

				start := time.Now()
				m := NewSplitTxInpointsMap(16384)
				fillSplitMap(m, total)
				fill := time.Since(start)

				b.StopTimer()

				heapStats() // drops the garbage of the fill

				beforeGC := time.Now()
				bytes, objects := heapStats()
				gc := time.Since(beforeGC)

				b.ReportMetric(float64(bytes-baseBytes)/float64(total), "heapB/tx")
				b.ReportMetric(float64(objects-baseObjects)/float64(total), "heapObjects/tx")
				b.ReportMetric(float64(total)/fill.Seconds()/1e6, "Mtx/s-ingest")
				b.ReportMetric(float64(gc.Milliseconds()), "gc-ms")

				m.Clear()
				runtime.KeepAlive(m)
			}
		})
	}
}

// BenchmarkSplitTxInpointsMapEmpty reports what a map costs when it holds few
// or no txs, which is what a small node pays for each half of the double buffer.
func BenchmarkSplitTxInpointsMapEmpty(b *testing.B) {
	for _, txs := range []int{0, 1000, 100_000} {
		b.Run(fmt.Sprintf("%dtxs", txs), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				baseBytes, _ := heapStats()

				m := NewSplitTxInpointsMap(16384)
				fillSplitMap(m, txs)

				bytes, _ := heapStats()

				b.ReportMetric(float64(bytes-baseBytes)/(1<<20), "MiB")
				runtime.KeepAlive(m)
			}
		})
	}
}

// BenchmarkSplitTxInpointsMapGet is the cost of one Get, the call the hot paths
// avoid where they can.
func BenchmarkSplitTxInpointsMapGet(b *testing.B) {
	const total = 1 << 20

	m := NewSplitTxInpointsMap(16384)
	fillSplitMap(m, total)

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0

		for pb.Next() {
			if _, ok := m.Get(benchTxHash(i % total)); !ok {
				b.Error("missing")
			}

			i++
		}
	})
}

// BenchmarkSplitTxInpointsMapSetIfNotExists is the ingest call, into a map
// that is cleared every total txs as a block would.
func BenchmarkSplitTxInpointsMapSetIfNotExists(b *testing.B) {
	const total = 1 << 20

	m := NewSplitTxInpointsMap(16384)

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0

		for pb.Next() {
			m.SetIfNotExists(benchTxHash(i%total), benchTxInpoints(i%total))

			i++
		}
	})
}
