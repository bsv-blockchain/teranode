package model

import (
	"math"
	"runtime"
	"sync"
	"sync/atomic"

	txmap "github.com/bsv-blockchain/go-tx-map"
)

// Block validation builds a SplitSwissMapUint64 txMap of every transaction
// (used by checkDuplicateTransactions). At 600M-tx scale it is tens of GB.
//
// It is allocated per block, sized to what the block needs bucket by
// bucket: each of the 8192 buckets gets its share of the entries plus the
// headroom hash variance needs (bucketCapacity), about 2% at that scale. It is
// not kept between blocks: an idle node holds none, and no block is handed a map
// sized for another. Duplicate inputs are found without a map, see
// inpoint_dedup.go.

// txMapBuckets is the bucket count of every block txMap.
const txMapBuckets uint16 = 8192

// maxTxMapPrealloc caps how many entries the txMap is preallocated for. A map
// still grows past the cap on insert; the cap only stops a count nobody has
// validated yet from driving the allocation. On the in-memory load path the tx
// count is len(Subtrees) x subtree 0's length, taken before the other subtrees
// are fetched, so a header with valid PoW could otherwise claim a body that asks
// for terabytes. A variable so tests can shrink it.
var maxTxMapPrealloc uint64 = 1 << 30

// maxBucketCapacity bounds one bucket's preallocation well below the point where
// dolthub/swiss's uint32 group arithmetic, (n + groupLoad - 1) / groupLoad,
// wraps to a single group.
const maxBucketCapacity = 1 << 31

// bucketCapacity is the entry count to preallocate per bucket for n entries
// spread over buckets: the mean share plus five standard deviations of the
// Binomial(n, 1/buckets) spread, plus a small constant for tiny maps. The
// fullest of 8192 buckets sits ~4.3 sd above the mean, so a bucket rarely fills,
// and one that does grows alone. At 588M entries this is ~1.9% over the mean.
func bucketCapacity(n uint64, buckets uint16) uint32 {
	mean := math.Ceil(float64(n) / float64(buckets))
	need := mean + 5*math.Sqrt(mean) + 8

	if need >= maxBucketCapacity {
		return maxBucketCapacity
	}

	return uint32(need)
}

// buildBuckets calls build(i) for every i in [0, n) across GOMAXPROCS
// goroutines. Allocating tens of GB of bucket arrays one after another would
// put seconds of zeroing on the validation path. build must only touch bucket i.
func buildBuckets(n int, build func(i int)) {
	var (
		next atomic.Int64
		wg   sync.WaitGroup
	)

	for w := 0; w < runtime.GOMAXPROCS(0); w++ {
		wg.Go(func() {
			for {
				i := int(next.Add(1)) - 1
				if i >= n {
					return
				}

				build(i)
			}
		})
	}

	wg.Wait()
}

// newBlockTxMap returns a SplitSwissMapUint64 sized for n entries (capped at
// maxTxMapPrealloc). The buckets are built here rather than through the
// constructor's hint, which adds a flat 20% per bucket.
func newBlockTxMap(n uint64) *txmap.SplitSwissMapUint64 {
	need := bucketCapacity(min(n, maxTxMapPrealloc), txMapBuckets)

	m := txmap.NewSplitSwissMapUint64(0, txMapBuckets)

	// Map returns the live bucket map. Go maps do not take concurrent writes, so
	// the buckets are built in parallel into a slice and assigned afterwards;
	// the placeholders they replace are a single group each.
	built := make([]*txmap.SwissMapUint64, txMapBuckets)
	buildBuckets(int(txMapBuckets), func(i int) { built[i] = txmap.NewSwissMapUint64(need) })

	buckets := m.Map()
	for i, b := range built {
		buckets[uint16(i)] = b //nolint:gosec // i < txMapBuckets
	}

	return m
}
