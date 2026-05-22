package model

import (
	"sync"

	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/dolthub/swiss"
)

// ParentSpendsMap is the interface for tracking spent inpoints during block validation.
// Both SplitSyncedParentMap (in-memory) and DiskParentSpendsMap (disk-backed) implement this.
type ParentSpendsMap interface {
	SetIfNotExists(inpoint subtreepkg.Inpoint) bool
}

type swissInpointBucket struct {
	mu sync.Mutex
	m  *swiss.Map[subtreepkg.Inpoint, struct{}]
}

type SplitSyncedParentMap struct {
	buckets     []swissInpointBucket
	nrOfBuckets uint16
}

func NewSplitSyncedParentMap(nrOfBuckets uint16, expectedInpoints ...uint64) *SplitSyncedParentMap {
	perBucket := uint32(0)
	if len(expectedInpoints) > 0 && expectedInpoints[0] > 0 {
		// 20% headroom per bucket absorbs natural Binomial(N, 1/B) hash variance
		// and prevents the underlying dolthub/swiss map from rehashing mid-block.
		perBucket = uint32((expectedInpoints[0] + expectedInpoints[0]/5) / uint64(nrOfBuckets))
	}
	s := &SplitSyncedParentMap{
		buckets:     make([]swissInpointBucket, nrOfBuckets),
		nrOfBuckets: nrOfBuckets,
	}
	for i := uint16(0); i < nrOfBuckets; i++ {
		s.buckets[i].m = swiss.NewMap[subtreepkg.Inpoint, struct{}](perBucket)
	}
	return s
}

func (s *SplitSyncedParentMap) SetIfNotExists(inpoint subtreepkg.Inpoint) bool {
	idx := txmap.Bytes2Uint16Buckets(inpoint.Hash, s.nrOfBuckets)
	b := &s.buckets[idx]

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.m.Has(inpoint) {
		return false
	}

	b.m.Put(inpoint, struct{}{})

	return true
}

// Clear empties every bucket without releasing the per-bucket dolthub/swiss
// group/ctrl backing storage. Intended for sync.Pool reuse: a multi-GB
// SplitSyncedParentMap can be reset in milliseconds and handed back to the
// pool without re-allocating any of its bucket maps.
//
// Each bucket's mutex is taken in turn; callers must ensure no other
// goroutine is using the map.
func (s *SplitSyncedParentMap) Clear() {
	for i := range s.buckets {
		b := &s.buckets[i]
		b.mu.Lock()
		b.m.Clear()
		b.mu.Unlock()
	}
}

// NrOfBuckets returns the number of buckets the map was constructed with.
// Useful for pool size-class keying.
func (s *SplitSyncedParentMap) NrOfBuckets() uint16 {
	return s.nrOfBuckets
}
