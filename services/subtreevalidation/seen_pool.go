package subtreevalidation

import (
	"sync"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// seenPoolMaxEntries is the largest duplicate-check map kept for reuse: a
// full-size subtree. clear keeps a map's capacity, so a bigger one would pin
// its memory in the pool.
const seenPoolMaxEntries = 1 << 20

// seenSet is ValidateSubtreeInternal's duplicate-check map with the size it
// was made for (clear keeps that capacity).
type seenSet struct {
	m    map[chainhash.Hash]struct{}
	size int
}

// seenPool recycles seenSets. Each holds a subtree's tx hashes (tens of MB for
// a full subtree); allocating one per subtree took the heap lock and faulted
// in fresh pages every time.
var seenPool = sync.Pool{
	New: func() any { return &seenSet{} },
}

// seenOversizeFactor is how much larger than needed a pooled set may be.
// Inserting a few thousand hashes into a table sized for 1M costs ~15x more
// than into a fresh one, so a much bigger set goes back for a big subtree.
const seenOversizeFactor = 4

// getSeenSet returns an empty set with room for n hashes.
func getSeenSet(n int) *seenSet {
	s := seenPool.Get().(*seenSet)

	if s.m != nil && s.size > seenOversizeFactor*max(n, 1) {
		seenPool.Put(s)

		s = &seenSet{}
	}

	if s.m == nil || s.size < n {
		s.m = make(map[chainhash.Hash]struct{}, n)
		s.size = n
	}

	return s
}

// putSeenSet clears s and returns it to the pool, unless it was made for more
// than a full subtree. s must not be used afterwards.
func putSeenSet(s *seenSet) {
	if s.size > seenPoolMaxEntries {
		return
	}

	clear(s.m)
	seenPool.Put(s)
}
