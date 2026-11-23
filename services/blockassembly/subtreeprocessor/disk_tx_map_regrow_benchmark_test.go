package subtreeprocessor

import (
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// BenchmarkDiskTxMap_IndexRefill measures inserting into an index after a
// clear: the remainder share MoveFrom inserts inside moveForwardBlock (1/16 of
// the high-water mark, the ratio seen on the scaling cluster) and the full
// refill over the following block interval. "kept" refills maps that clear()
// left at their high-water capacity; "fresh" refills newly made maps. Inserts
// run one goroutine per range of shards, as MoveFrom groups them.
func BenchmarkDiskTxMap_IndexRefill(b *testing.B) {
	if testing.Short() {
		b.Skip("large index, skipped in short mode")
	}

	const highWater = 32 << 20

	hashes := genBenchHashes(highWater, 1)

	for _, fill := range []struct {
		name string
		n    int
	}{{"remainder", highWater / 16}, {"full", highWater}} {
		for _, fresh := range []bool{false, true} {
			name := "kept"
			if fresh {
				name = "fresh"
			}

			b.Run(fmt.Sprintf("%s/%s", fill.name, name), func(b *testing.B) {
				var shards [numIndexShards]map[chainhash.Hash]uint64
				for i := range shards {
					shards[i] = make(map[chainhash.Hash]uint64)
				}

				byShard := make([][]chainhash.Hash, numIndexShards)
				for _, h := range hashes[:fill.n] {
					byShard[shardOf(h)] = append(byShard[shardOf(h)], h)
				}

				// Grow every shard to the high-water mark once.
				for _, h := range hashes {
					shards[shardOf(h)][h] = 0
				}

				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					b.StopTimer()

					for s := range shards {
						if fresh {
							shards[s] = make(map[chainhash.Hash]uint64)
						} else {
							clear(shards[s])
						}
					}

					runtime.GC()
					b.StartTimer()

					workers := runtime.GOMAXPROCS(0)
					per := (numIndexShards + workers - 1) / workers

					var wg sync.WaitGroup

					for start := 0; start < numIndexShards; start += per {
						wg.Go(func() {
							for s := start; s < min(start+per, numIndexShards); s++ {
								m := shards[s]
								for _, h := range byShard[s] {
									m[h] = 1
								}
							}
						})
					}

					wg.Wait()
				}
			})
		}
	}
}
