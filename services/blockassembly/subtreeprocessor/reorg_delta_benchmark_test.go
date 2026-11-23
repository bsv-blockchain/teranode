package subtreeprocessor

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/stretchr/testify/require"
)

// BenchmarkReorgSortedHashSetFind times the position-returning lookup of the reorg delta
// on a frozen set, probing members in random order, which is what the delta does for the
// nodes both sides have. The set is in process memory and the probes are one goroutine's,
// so this is the cost of one probe with a warm directory; a real 1.2B-hash set misses
// the TLB and the cache far more (design section 4.4).
func BenchmarkReorgSortedHashSetFind(b *testing.B) {
	sizes := []int{1 << 20, 1 << 22}
	if testing.Short() {
		sizes = sizes[:1] // CI runs these with -short and a budget for the whole filter
	}

	for _, n := range sizes {
		// the benchmark function runs again for every b.N the framework tries: build once
		var (
			hashes []chainhash.Hash
			set    *SortedHashSet
		)

		b.Run(fmt.Sprintf("n=%dM", n>>20), func(b *testing.B) {
			if set == nil {
				hashes = randHashes(n, 5)
				set = buildSet(b, hashes, n, 4)

				rand.New(rand.NewSource(6)).Shuffle(len(hashes), func(i, j int) { hashes[i], hashes[j] = hashes[j], hashes[i] }) //nolint:gosec // deterministic benchmark data
			}

			var sum uint64

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				pos, ok := set.Find(hashes[i%n])
				if !ok {
					b.Fatal("member not found")
				}

				sum += pos
			}

			b.StopTimer()
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "probes/s")

			if sum == 0 && b.N > 1 {
				b.Fatal("positions all zero")
			}
		})
	}
}

// BenchmarkReorgDelta times computeReorgDelta for a lost block of 1M nodes against a winner
// set of 2M hashes (two forward blocks), at 50% and 99% of the lost nodes in the winners. The
// subtrees are real files in the memory blob store and are read and parsed in every
// iteration; the set is built once, outside the timer.
func BenchmarkReorgDelta(b *testing.B) {
	overlaps := []float64{0.5, 0.99}
	if testing.Short() {
		overlaps = overlaps[:1]
	}

	for _, overlap := range overlaps {
		// the benchmark function runs again for every b.N the framework tries: build once
		var (
			stp       *SubtreeProcessor
			d         *deltaBlocks
			winners   *SortedHashSet
			lostNodes int
		)

		b.Run(fmt.Sprintf("lost=1M/winners=2M/overlap=%.0f%%", overlap*100), func(b *testing.B) {
			if stp == nil {
				var store *blob_memory.Memory

				stp, store = newTxMapTestProcessor(b)
				d = newDeltaBlocks(b, store, deltaCase{
					lostBlocks: 1, winBlocks: 2, subtrees: 16, subtreeSize: 1 << 16,
					overlap: overlap, seed: 2269,
				})

				var err error

				winners, err = stp.buildUnionSet(context.Background(), d.winners)
				require.NoError(b, err)

				for _, st := range d.lostLeaves[0] {
					lostNodes += len(st)
				}
			}

			ctx := context.Background()
			workers := stp.settings.BlockAssembly.SubtreeProcessorConcurrentReads

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				delta, err := stp.computeReorgDelta(ctx, d.lost, winners, workers)
				if err != nil {
					b.Fatal(err)
				}

				if delta.Hits == 0 {
					b.Fatal("no hits")
				}
			}

			b.StopTimer()
			b.ReportMetric(float64(lostNodes)*float64(b.N)/b.Elapsed().Seconds(), "nodes/s")
		})
	}
}
