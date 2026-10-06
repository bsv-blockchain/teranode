package model

import (
	"sync"

	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/dolthub/swiss"
)

// ParentSpendsMap is the interface for tracking spent inpoints during block validation.
// Both SplitSyncedParentMap (in-memory) and DiskParentSpendsMap (disk-backed) implement this.
type ParentSpendsMap interface {
	// SetIfNotExists records the inpoint. inserted=true means newly recorded;
	// inserted=false means it was already present (a duplicate spend). A non-nil
	// error indicates a storage/capacity failure and MUST halt validation — the
	// caller must not treat an error as either "new" or "duplicate".
	SetIfNotExists(inpoint subtreepkg.Inpoint) (inserted bool, err error)
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
	var expected uint64
	if len(expectedInpoints) > 0 {
		expected = expectedInpoints[0]
	}

	// Each bucket's share plus the headroom hash variance needs (bucketCapacity),
	// so a bucket rarely fills; one that does grows alone, see SetIfNotExists.
	s := &SplitSyncedParentMap{
		buckets:     make([]swissInpointBucket, nrOfBuckets),
		nrOfBuckets: nrOfBuckets,
	}
	buildParentSpendsBuckets(s, bucketCapacity(expected, nrOfBuckets))

	return s
}

// inpointBucketMix is an odd multiplier that folds the output index into the
// bucket choice.
const inpointBucketMix = 0x9e3779b1

// inpointBucket picks the bucket for an inpoint from its parent hash and output
// index. Keyed by the hash alone, every spent output of one parent lands in one
// bucket, which the per-bucket headroom (sized for independent keys) does not
// cover: a block spending 200K outputs of a fan-out parent would fill that
// bucket many times over while every other bucket sat at its mean.
func inpointBucket(inpoint subtreepkg.Inpoint, nrOfBuckets uint16) uint16 {
	h := uint32(inpoint.Hash[0])<<8 | uint32(inpoint.Hash[1])
	h ^= inpoint.Index * inpointBucketMix
	h ^= h >> 16

	return uint16(h % uint32(nrOfBuckets)) //nolint:gosec // < nrOfBuckets
}

func (s *SplitSyncedParentMap) SetIfNotExists(inpoint subtreepkg.Inpoint) (bool, error) {
	b := &s.buckets[inpointBucket(inpoint, s.nrOfBuckets)]

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.m.Has(inpoint) {
		return false, nil
	}

	if b.m.Capacity() <= 0 {
		b.grow()
	}

	b.m.Put(inpoint, struct{}{})

	return true, nil
}

// grow rebuilds a full bucket with an eighth more room. Left to itself the swiss
// map doubles on the next Put, so a parent-spends map sized from an estimate
// that came out a few percent short would end up at up to twice its need.
// Callers hold b.mu.
func (b *swissInpointBucket) grow() {
	count := b.m.Count()
	grown := swiss.NewMap[subtreepkg.Inpoint, struct{}](uint32(count + count/8 + 16)) //nolint:gosec // a bucket holds far below 2^32

	b.m.Iter(func(k subtreepkg.Inpoint, _ struct{}) bool {
		grown.Put(k, struct{}{})
		return false
	})

	b.m = grown
}

// Clear empties every bucket without releasing the per-bucket dolthub/swiss
// group/ctrl backing storage.
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

// Length returns the number of inpoints recorded. Callers must ensure no other
// goroutine is inserting, or the count is only a snapshot.
func (s *SplitSyncedParentMap) Length() uint64 {
	var n uint64

	for i := range s.buckets {
		b := &s.buckets[i]
		b.mu.Lock()
		n += uint64(b.m.Count()) //nolint:gosec // a count is never negative
		b.mu.Unlock()
	}

	return n
}

// NrOfBuckets returns the number of buckets the map was constructed with.
func (s *SplitSyncedParentMap) NrOfBuckets() uint16 {
	return s.nrOfBuckets
}
