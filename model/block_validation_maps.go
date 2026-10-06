package model

import (
	"math"
	"runtime"
	"sync"
	"sync/atomic"

	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/dolthub/swiss"
)

// Block validation builds two large maps per block: a SplitSwissMapUint64
// txMap of every transaction (used by checkDuplicateTransactions) and a
// SplitSyncedParentMap of every spent inpoint (used by validOrderAndBlessed).
// At 600M-tx scale each is tens of GB.
//
// Both are allocated per block, sized to what the block needs bucket by
// bucket: each of the 8192 buckets gets its share of the entries plus the
// headroom hash variance needs (bucketCapacity), about 2% at that scale. They
// are not kept between blocks: an idle node holds neither, and no block is
// handed a map sized for another. They used to be pooled in size classes that
// stepped 4x with 20% on top, which gave a 588M-tx block a ~210 GB
// parent-spends map for ~590M inputs and kept it for the next block.

// txMapBuckets is the bucket count of every block txMap.
const txMapBuckets uint16 = 8192

// parentSpendsBuckets is the bucket count of every in-memory parent-spends map.
const parentSpendsBuckets uint16 = 8192

// maxTxMapPrealloc and maxParentSpendsPrealloc cap how many entries a map is
// preallocated for. A map still grows past the cap on insert; the cap only
// stops a count nobody has validated yet from driving the allocation. On the
// in-memory load path the tx count is len(Subtrees) x subtree 0's length,
// taken before the other subtrees are fetched, so a header with valid PoW
// could otherwise claim a body that asks for terabytes. Variables so tests can
// shrink them.
var (
	maxTxMapPrealloc        uint64 = 1 << 30
	maxParentSpendsPrealloc uint64 = 1 << 32
)

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

// newBlockParentSpendsMap returns a SplitSyncedParentMap sized for
// expectedInpoints entries (capped at maxParentSpendsPrealloc).
func newBlockParentSpendsMap(expectedInpoints uint64) *SplitSyncedParentMap {
	return NewSplitSyncedParentMap(parentSpendsBuckets, min(expectedInpoints, maxParentSpendsPrealloc))
}

// minMeasuredTxs is the smallest block whose inputs per transaction are
// recorded. A small block's ratio says little about the next large one, and
// two tiny consolidation blocks would otherwise size the next block's map.
var minMeasuredTxs uint64 = 1 << 16

// parentSpendsMeasuredMarginPct is the margin added to a measured ratio, since
// the next block's mix of inputs is not exactly the last one's. An undershoot
// is cheap: a full bucket grows by an eighth (SplitSyncedParentMap.SetIfNotExists).
const parentSpendsMeasuredMarginPct = 3

// inpointsPerTx holds the inputs per non-coinbase transaction, x1000 and
// rounded up, of the last two successful in-memory validOrderAndBlessed runs
// over at least minMeasuredTxs transactions. 0 means not measured.
var inpointsPerTx struct {
	mu       sync.Mutex
	last     uint64
	previous uint64
}

// recordInpointsPerTx records the inputs a validated block carried. entryCount
// includes the coinbase placeholder, which has no inputs.
func recordInpointsPerTx(inpoints, entryCount uint64) {
	if entryCount <= minMeasuredTxs {
		return
	}

	milli := math.Ceil(float64(inpoints) * 1000 / float64(entryCount-1))
	if milli < 1 || milli > math.MaxUint32 {
		return
	}

	inpointsPerTx.mu.Lock()
	inpointsPerTx.previous = inpointsPerTx.last
	inpointsPerTx.last = uint64(milli)
	inpointsPerTx.mu.Unlock()
}

// sizingInpointsPerTxMilli is the ratio the next parent-spends map is sized
// from: the smaller of the last two measurements, so one consolidation-heavy
// block does not oversize the map of the block after it. A shift that persists
// for two blocks is followed, a drop at once. 0 means not measured yet.
func sizingInpointsPerTxMilli() uint64 {
	inpointsPerTx.mu.Lock()
	defer inpointsPerTx.mu.Unlock()

	if inpointsPerTx.previous == 0 {
		return inpointsPerTx.last
	}

	return min(inpointsPerTx.last, inpointsPerTx.previous)
}

// inMemoryParentSpendsCapacity is the inpoint count to size the in-memory
// parent-spends map for, for entryCount transactions. With a measured ratio
// (observedMilli inputs per tx, x1000) it clamps that to between one input per
// transaction, which every non-coinbase transaction has, and the configured
// multiplier, then adds parentSpendsMeasuredMarginPct. A chain sustaining more
// than about multiplier x 1.03 inputs per tx regrows its buckets every block,
// so raise the multiplier for such a chain. Before anything has been measured it uses
// one input per transaction. Buckets that turn out short grow by an eighth.
func inMemoryParentSpendsCapacity(entryCount, observedMilli, multiplier uint64) uint64 {
	if observedMilli == 0 {
		return max(entryCount, 1)
	}

	// Clamp before the margin: clamping after it sizes a chain sustaining just
	// above the multiplier at exactly the multiplier, so every bucket regrows
	// every block.
	ratio := max(1, min(float64(observedMilli)/1000, float64(max(multiplier, 1))))
	ratio = ratio * (100 + parentSpendsMeasuredMarginPct) / 100

	capacity := math.Ceil(float64(entryCount) * ratio)

	switch {
	case capacity < 1:
		return 1
	case capacity >= math.MaxUint64:
		return math.MaxUint64
	default:
		return uint64(capacity)
	}
}

// inMemoryParentSpendsCapacity is the inpoint count validOrderAndBlessed sizes
// this block's in-memory parent-spends map for: its loaded entry count at the
// recorded inputs per tx, with multiplier (block_parentSpendsCapacityMultiplier)
// as the ceiling.
func (b *Block) inMemoryParentSpendsCapacity(multiplier uint64) uint64 {
	return inMemoryParentSpendsCapacity(b.txMapEntryCount(), sizingInpointsPerTxMilli(), multiplier)
}

// buildParentSpendsBuckets sizes every bucket of s for perBucket entries.
func buildParentSpendsBuckets(s *SplitSyncedParentMap, perBucket uint32) {
	buildBuckets(len(s.buckets), func(i int) {
		s.buckets[i].m = swiss.NewMap[subtreepkg.Inpoint, struct{}](perBucket)
	})
}
