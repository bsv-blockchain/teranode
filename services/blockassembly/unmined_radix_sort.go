package blockassembly

import (
	mathbits "math/bits"
	"sort"
	"sync"

	"github.com/bsv-blockchain/teranode/stores/utxo"
)

// unminedRadixDigitBits is the width of one radix digit: 2048 buckets per pass.
const unminedRadixDigitBits = 11

// sortUnminedByCreatedAt orders txs exactly as a stable sort by CreatedAt followed
// by orderEqualCreatedAtRunsParentsFirst, using workers goroutines.
//
// Each tx's CreatedAt is read once and packed with the tx's position into a
// uint64: (CreatedAt-min) in the high bits, position in the low bits. An LSD radix
// sort on the CreatedAt bits only is stable and starts from the original order, so
// ties keep their original order, which is what the stable comparison sort gives.
// The passes run over a contiguous array instead of comparing through pointers, so
// a reload of a billion txs sorts in seconds instead of tens of minutes. It holds
// three transient arrays of len(txs) words (keys, scratch, reordered pointers).
//
// It returns false when the CreatedAt range and the position need more than 64
// bits together; the comparison sort then runs instead, with the same order.
func sortUnminedByCreatedAt(txs []*utxo.UnminedTransaction, workers int) bool {
	n := len(txs)
	if n < 2 {
		return true
	}

	workers = max(1, min(workers, n))

	minKey, maxKey := unminedCreatedAtRange(txs, workers)

	keyBits := mathbits.Len64(uint64(maxKey - minKey))
	idxBits := mathbits.Len64(uint64(n - 1))

	if keyBits+idxBits > 64 {
		sort.SliceStable(txs, func(i, j int) bool {
			return txs[i].CreatedAt < txs[j].CreatedAt
		})

		orderRunsParentsFirstParallel(txs, workers)

		return false
	}

	keys := make([]uint64, n)

	parallelRanges(n, workers, func(lo, hi int) {
		for i := lo; i < hi; i++ {
			keys[i] = uint64(txs[i].CreatedAt-minKey)<<idxBits | uint64(i)
		}
	})

	keys = radixSortKeyBits(keys, idxBits, idxBits+keyBits, workers)

	idxMask := uint64(1)<<idxBits - 1
	sorted := make([]*utxo.UnminedTransaction, n)

	parallelRanges(n, workers, func(lo, hi int) {
		for i := lo; i < hi; i++ {
			sorted[i] = txs[keys[i]&idxMask]
		}
	})

	parallelRanges(n, workers, func(lo, hi int) {
		copy(txs[lo:hi], sorted[lo:hi])
	})

	orderRunsParentsFirstParallel(txs, workers)

	return true
}

// radixSortFits reports whether txs can take the packed radix path.
func radixSortFits(txs []*utxo.UnminedTransaction) bool {
	if len(txs) < 2 {
		return true
	}

	minKey, maxKey := unminedCreatedAtRange(txs, 1)

	return mathbits.Len64(uint64(maxKey-minKey))+mathbits.Len64(uint64(len(txs)-1)) <= 64
}

// unminedCreatedAtRange returns the smallest and largest CreatedAt in txs.
func unminedCreatedAtRange(txs []*utxo.UnminedTransaction, workers int) (int, int) {
	mins := make([]int, workers)
	maxs := make([]int, workers)

	parallelRangesIndexed(len(txs), workers, func(w, lo, hi int) {
		lowest, highest := txs[lo].CreatedAt, txs[lo].CreatedAt

		for i := lo + 1; i < hi; i++ {
			v := txs[i].CreatedAt
			lowest = min(lowest, v)
			highest = max(highest, v)
		}

		mins[w], maxs[w] = lowest, highest
	})

	lowest, highest := mins[0], maxs[0]

	for w := 1; w < workers; w++ {
		lowest = min(lowest, mins[w])
		highest = max(highest, maxs[w])
	}

	return lowest, highest
}

// radixSortKeyBits stably sorts keys by bits [fromBit, toBit) with parallel LSD
// passes and returns the sorted slice, which is either keys or a scratch slice.
func radixSortKeyBits(keys []uint64, fromBit, toBit, workers int) []uint64 {
	n := len(keys)
	if toBit <= fromBit {
		return keys
	}

	const buckets = 1 << unminedRadixDigitBits

	scratch := make([]uint64, n)
	counts := make([][buckets]int, workers)

	for shift := fromBit; shift < toBit; shift += unminedRadixDigitBits {
		digitMask := uint64(buckets - 1)
		if remaining := toBit - shift; remaining < unminedRadixDigitBits {
			digitMask = uint64(1)<<remaining - 1
		}

		src, dst := keys, scratch

		parallelRangesIndexed(n, workers, func(w, lo, hi int) {
			c := &counts[w]
			*c = [buckets]int{}

			for _, k := range src[lo:hi] {
				c[(k>>shift)&digitMask]++
			}
		})

		// Each worker's bucket starts after every earlier bucket, and after the same
		// bucket of every earlier worker, which keeps the pass stable.
		offset := 0

		for d := 0; d < buckets; d++ {
			for w := 0; w < workers; w++ {
				c := counts[w][d]
				counts[w][d] = offset
				offset += c
			}
		}

		parallelRangesIndexed(n, workers, func(w, lo, hi int) {
			c := &counts[w]

			for _, k := range src[lo:hi] {
				d := (k >> shift) & digitMask
				dst[c[d]] = k
				c[d]++
			}
		})

		keys, scratch = dst, src
	}

	return keys
}

// orderRunsParentsFirstParallel runs orderEqualCreatedAtRunsParentsFirst on
// segments of txs, already sorted by CreatedAt, cut only between runs of equal
// CreatedAt so that no run is split.
func orderRunsParentsFirstParallel(txs []*utxo.UnminedTransaction, workers int) {
	n := len(txs)
	bounds := make([]int, 0, workers+1)
	bounds = append(bounds, 0)

	for w := 1; w < workers; w++ {
		cut := w * n / workers
		if cut <= bounds[len(bounds)-1] {
			continue
		}

		for cut < n && txs[cut].CreatedAt == txs[cut-1].CreatedAt {
			cut++
		}

		if cut < n {
			bounds = append(bounds, cut)
		}
	}

	bounds = append(bounds, n)

	var wg sync.WaitGroup

	for s := 0; s+1 < len(bounds); s++ {
		lo, hi := bounds[s], bounds[s+1]

		wg.Add(1)

		go func() {
			defer wg.Done()

			orderEqualCreatedAtRunsParentsFirst(txs[lo:hi])
		}()
	}

	wg.Wait()
}

// parallelRanges splits [0, n) into workers contiguous ranges and runs fn on each.
func parallelRanges(n, workers int, fn func(lo, hi int)) {
	parallelRangesIndexed(n, workers, func(_, lo, hi int) { fn(lo, hi) })
}

// parallelRangesIndexed is parallelRanges with the range's worker index. Every
// worker index in [0, workers) runs once, possibly on an empty range.
func parallelRangesIndexed(n, workers int, fn func(w, lo, hi int)) {
	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		lo, hi := w*n/workers, (w+1)*n/workers

		wg.Add(1)

		go func() {
			defer wg.Done()

			fn(w, lo, hi)
		}()
	}

	wg.Wait()
}
