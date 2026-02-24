package subtreeprocessor

import (
	"sync"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// hashSlicePool pools []chainhash.Hash slices to reduce GC pressure during reorgs.
// Reorgs allocate large hash slices (potentially millions of 32-byte entries) that
// become garbage immediately after use. Pooling these avoids repeated multi-MB allocations.
var hashSlicePool = sync.Pool{}

// getHashSlice returns a *[]chainhash.Hash from the pool or allocates a new one.
// The returned slice has length 0 and at least the requested capacity.
func getHashSlice(capacity int) *[]chainhash.Hash {
	if v := hashSlicePool.Get(); v != nil {
		s := v.(*[]chainhash.Hash)
		if cap(*s) >= capacity {
			*s = (*s)[:0]
			return s
		}
	}
	s := make([]chainhash.Hash, 0, capacity)
	return &s
}

// putHashSlice returns a slice to the pool for reuse.
func putHashSlice(s *[]chainhash.Hash) {
	if s == nil {
		return
	}
	// Clear references to allow GC of underlying hash data
	clear(*s)
	*s = (*s)[:0]
	hashSlicePool.Put(s)
}
