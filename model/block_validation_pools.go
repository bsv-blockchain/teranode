package model

import (
	"sync"

	txmap "github.com/bsv-blockchain/go-tx-map"
)

// Block validation builds two large maps per block: a SplitSwissMapUint64
// txMap of every transaction (used by checkDuplicateTransactions) and a
// SplitSyncedParentMap of every spent inpoint (used by validOrderAndBlessed).
// At 654M-tx scale each map carries ~30 GB of backing storage that is
// allocated fresh per block and immediately discarded. The pools below
// recycle those backings across blocks via dolthub/swiss.Map.Clear, which
// empties the map without releasing its group/ctrl arrays.
//
// Pools are keyed by approximate size class (ceiling-power-of-two of the
// expected entry count) so a 1024-tx block does not retain a 1M-tx
// backing and vice-versa. sync.Pool's natural per-GC drainage handles
// network-load shifts: when subtree size drops from 1M back to 1K the
// large-class pool simply ages out.
//
// Callers pass the same `n` to Put as they used at Get so the map returns
// to the correct class.

// txMapBuckets is the fixed bucket count for every pooled SplitSwissMapUint64.
// All call sites use this value so pooled maps are interchangeable.
const txMapBuckets uint16 = 8192

// txMapSizeClasses are the rounded-up entry-count buckets for txMap reuse.
// Each call site picks the smallest class >= its expected entry count.
// Maps requested with n above the maximum class are allocated fresh and
// dropped on Put (the pool will not retain them).
var txMapSizeClasses = []uint32{
	1 << 12, // 4K
	1 << 14, // 16K
	1 << 16, // 64K
	1 << 18, // 256K
	1 << 20, // 1M
	1 << 22, // 4M
	1 << 24, // 16M
	1 << 26, // 64M
	1 << 28, // 256M
	1 << 30, // 1B
}

var txMapPools = func() []*sync.Pool {
	pools := make([]*sync.Pool, len(txMapSizeClasses))
	for i, class := range txMapSizeClasses {
		c := class
		pools[i] = &sync.Pool{
			New: func() interface{} {
				return txmap.NewSplitSwissMapUint64(c, txMapBuckets)
			},
		}
	}
	return pools
}()

// txMapClassIdxFor returns the smallest size-class index that holds n
// entries, or -1 if n exceeds every class (caller allocates fresh + skips
// the pool).
func txMapClassIdxFor(n uint32) int {
	for i, class := range txMapSizeClasses {
		if n <= class {
			return i
		}
	}
	return -1
}

// GetTxMap returns a *SplitSwissMapUint64 sized for at least n entries.
// Drawn from the pool when n fits a known size class; allocated fresh
// otherwise. Pass the same n to PutTxMap.
func GetTxMap(n uint32) *txmap.SplitSwissMapUint64 {
	idx := txMapClassIdxFor(n)
	if idx < 0 {
		return txmap.NewSplitSwissMapUint64(n, txMapBuckets)
	}
	return txMapPools[idx].Get().(*txmap.SplitSwissMapUint64)
}

// PutTxMap clears m and returns it to the size-class pool keyed by n.
// n must match the value passed to GetTxMap. Maps that did not come from
// the pool (n above max class) are dropped.
func PutTxMap(m *txmap.SplitSwissMapUint64, n uint32) {
	if m == nil {
		return
	}
	idx := txMapClassIdxFor(n)
	if idx < 0 {
		return
	}
	m.Clear()
	txMapPools[idx].Put(m)
}

// parentSpendsBuckets is the fixed bucket count for every pooled
// SplitSyncedParentMap.
const parentSpendsBuckets uint16 = 8192

// parentSpendsSizeClasses are rounded-up entry-count buckets for the
// parent-spends-map pool. Inpoints per tx average ~3 so the call site
// multiplies tx count by 3 when sizing.
var parentSpendsSizeClasses = []uint64{
	1 << 14, // 16K
	1 << 16, // 64K
	1 << 18, // 256K
	1 << 20, // 1M
	1 << 22, // 4M
	1 << 24, // 16M
	1 << 26, // 64M
	1 << 28, // 256M
	1 << 30, // 1B
	1 << 32, // 4B
}

var parentSpendsPools = func() []*sync.Pool {
	pools := make([]*sync.Pool, len(parentSpendsSizeClasses))
	for i, class := range parentSpendsSizeClasses {
		c := class
		pools[i] = &sync.Pool{
			New: func() interface{} {
				return NewSplitSyncedParentMap(parentSpendsBuckets, c)
			},
		}
	}
	return pools
}()

func parentSpendsClassIdxFor(n uint64) int {
	for i, class := range parentSpendsSizeClasses {
		if n <= class {
			return i
		}
	}
	return -1
}

// GetParentSpendsMap returns a *SplitSyncedParentMap sized for at least
// expectedInpoints entries. Pass the same value to PutParentSpendsMap.
func GetParentSpendsMap(expectedInpoints uint64) *SplitSyncedParentMap {
	idx := parentSpendsClassIdxFor(expectedInpoints)
	if idx < 0 {
		return NewSplitSyncedParentMap(parentSpendsBuckets, expectedInpoints)
	}
	return parentSpendsPools[idx].Get().(*SplitSyncedParentMap)
}

// PutParentSpendsMap clears m and returns it to the size-class pool keyed
// by expectedInpoints. expectedInpoints must match the value passed to
// GetParentSpendsMap.
func PutParentSpendsMap(m *SplitSyncedParentMap, expectedInpoints uint64) {
	if m == nil {
		return
	}
	idx := parentSpendsClassIdxFor(expectedInpoints)
	if idx < 0 {
		return
	}
	m.Clear()
	parentSpendsPools[idx].Put(m)
}
