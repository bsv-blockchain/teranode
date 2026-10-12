package subtreeprocessor

import (
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
)

// BenchmarkRemainderMove measures the tx map work moveForwardBlock does for
// every tx that stays queued after a block: move its inpoints from the
// previous half into the fresh half, split over GOMAXPROCS workers as
// legacyParallelGetAndSetIfNotExists does (Get + SetIfNotExists in memory,
// one MoveFrom per worker between disk maps).
//
// Run with -benchtime=1x; each iteration moves total transactions.
func BenchmarkRemainderMove(b *testing.B) {
	const batchSize = 1024

	for _, total := range []int{8 << 20} {
		for _, v := range []struct {
			dirs int
			bulk bool
		}{{0, false}, {1, true}, {2, true}} {
			dirs, bulk := v.dirs, v.bulk
			disk := dirs > 0

			name := "memTxMap"
			if disk {
				name = fmt.Sprintf("diskTxMap_%ddir", dirs)
			}

			if bulk {
				name += "_bulk"
			}

			for _, warm := range []bool{false, true} {
				variant := "cold"
				if warm {
					variant = "warm" // the fresh half was filled and cleared before, as after the first block
				}

				b.Run(fmt.Sprintf("%s/%s/%dM", name, variant, total>>20), func(b *testing.B) {
					nodes, inpoints := makeDequeueBenchBatches(total, batchSize)

					newMap := func() TxInpointsMap {
						if !disk {
							return NewSplitTxInpointsMap(4096)
						}

						paths := make([]string, dirs)
						for d := range paths {
							paths[d] = b.TempDir()
						}

						m, err := NewDiskTxMap(DiskTxMapOptions{BasePaths: paths})
						if err != nil {
							b.Fatal(err)
						}

						b.Cleanup(func() { _ = m.Close() })

						return m
					}

					b.ReportAllocs()
					b.ResetTimer()

					for i := 0; i < b.N; i++ {
						b.StopTimer()

						from, to := newMap(), newMap()

						for j := range nodes {
							for k := range nodes[j] {
								from.SetIfNotExists(nodes[j][k].Hash, inpoints[j][k])
							}
						}

						if dm, ok := from.(*DiskTxMap); ok {
							_ = dm.Flush() // the previous block's map is on disk by now
						}

						if warm {
							moveRemainder(b, nodes, from, to, disk, bulk)
							to.Clear()
						}

						b.StartTimer()

						moveRemainder(b, nodes, from, to, disk, bulk)
					}

					b.ReportMetric(float64(b.N*total)/b.Elapsed().Seconds(), "txs/s")
				})
			}
		}
	}
}

// moveRemainder moves every node's entry from one map to the other over
// GOMAXPROCS workers: per hash in memory, or with one MoveFrom per worker
// between disk maps (bulk).
func moveRemainder(b *testing.B, nodes [][]subtreepkg.Node, from, to TxInpointsMap, disk, bulk bool) {
	workers := runtime.GOMAXPROCS(0)
	perWorker := (len(nodes) + workers - 1) / workers

	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		part := nodes[min(w*perWorker, len(nodes)):min((w+1)*perWorker, len(nodes))]

		wg.Go(func() {
			if bulk {
				var hashes []chainhash.Hash
				for _, batch := range part {
					for k := range batch {
						hashes = append(hashes, batch[k].Hash)
					}
				}

				if missing, _ := to.(*DiskTxMap).MoveFrom(from.(*DiskTxMap), hashes, make([]bool, len(hashes))); missing >= 0 {
					b.Error("remainder tx not found")
				}

				return
			}

			for _, batch := range part {
				for k := range batch {
					parents, found := from.Get(batch[k].Hash)
					if !found {
						b.Error("remainder tx not found")
						return
					}

					to.SetIfNotExists(batch[k].Hash, parents)
				}
			}
		})
	}

	wg.Wait()
}
