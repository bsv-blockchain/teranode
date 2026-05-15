package pruner

import (
	"sync"
	"sync/atomic"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// prunedTxSetMaxEntries caps the in-memory size of a PrunedTxSet for safety on workloads
// that don't fit the tight-chain pattern this optimisation was designed for. Sessions on
// production-scale deployments can scan hundreds of millions of records (~500M observed),
// and a workload where most parents live in prior blocks would keep every TXID added.
// At ~96 bytes per entry (32-byte hash + Go map overhead), 10M entries is ~1 GB worst case.
// Once the cap is hit, Add() becomes a no-op and the skip optimisation degrades to baseline
// for the remainder of the session.
const prunedTxSetMaxEntries = 10_000_000

// PrunedTxSet is a concurrent sharded set tracking TXIDs of records pruned during a session.
// It is used to skip wasteful parent updates for parents that have already been pruned.
//
// Sharding picks a bucket from h[0]&mask, so it relies on a uniform distribution of the
// first byte of the key. SHA-256-derived TXIDs satisfy this; do not reuse for non-cryptographic
// keys without revisiting the shard() function.
//
// Memory is bounded by maxEntries: once Len() reaches the cap, subsequent Add() calls become
// no-ops. This protects against workloads where most parents are from prior blocks (so the
// CheckAndRemove churn never reclaims entries) and Len() would otherwise grow with the number
// of TXs pruned in the session — which on production-scale workloads can be hundreds of
// millions per session. Saturation silently degrades the skip optimisation back to baseline
// behaviour (parent updates fire normally for any TXID added after the cap).
type PrunedTxSet struct {
	shards     []prunedTxShard
	mask       uint8 // shardCount - 1, for fast modulo via bitwise AND; uint8 is sufficient because shardCount is capped at 256
	count      atomic.Int64
	maxEntries int64 // soft cap on Len(); 0 means unlimited
}

type prunedTxShard struct {
	mu sync.Mutex
	m  map[chainhash.Hash]struct{}
}

// NewPrunedTxSet creates a new PrunedTxSet with the given number of shards and a soft entry cap.
// shardCount must be a power of 2 (will be rounded up if not) and is capped at 256.
// maxEntries is a soft cap; Add() becomes a no-op once Len() reaches it. Pass 0 for unlimited.
func NewPrunedTxSet(shardCount int, maxEntries int) *PrunedTxSet {
	// Round up to next power of 2
	n := 1
	for n < shardCount {
		n <<= 1
	}
	if n > 256 {
		n = 256 // cap at 256 shards (must fit in mask uint8)
	}

	s := &PrunedTxSet{
		shards:     make([]prunedTxShard, n),
		mask:       uint8(n - 1),
		maxEntries: int64(maxEntries),
	}
	for i := range s.shards {
		s.shards[i].m = make(map[chainhash.Hash]struct{}, 64)
	}
	return s
}

func (s *PrunedTxSet) shard(h chainhash.Hash) *prunedTxShard {
	return &s.shards[h[0]&s.mask]
}

// Add registers a TXID as pruned. Duplicate adds are idempotent and do not affect the count.
// Once Len() reaches the configured maxEntries cap, further adds are silent no-ops.
func (s *PrunedTxSet) Add(h chainhash.Hash) {
	if s.maxEntries > 0 && s.count.Load() >= s.maxEntries {
		return
	}
	sh := s.shard(h)
	sh.mu.Lock()
	_, exists := sh.m[h]
	if !exists {
		sh.m[h] = struct{}{}
	}
	sh.mu.Unlock()
	if !exists {
		s.count.Add(1)
	}
}

// Contains checks if a TXID is in the set without removing it.
func (s *PrunedTxSet) Contains(h chainhash.Hash) bool {
	sh := s.shard(h)
	sh.mu.Lock()
	_, ok := sh.m[h]
	sh.mu.Unlock()
	return ok
}

// CheckAndRemove checks if a TXID is in the set. If found, removes it and returns true.
func (s *PrunedTxSet) CheckAndRemove(h chainhash.Hash) bool {
	sh := s.shard(h)
	sh.mu.Lock()
	_, ok := sh.m[h]
	if ok {
		delete(sh.m, h)
	}
	sh.mu.Unlock()
	if ok {
		s.count.Add(-1)
	}
	return ok
}

// Len returns the approximate number of entries in the set.
func (s *PrunedTxSet) Len() int {
	return int(s.count.Load())
}

// Saturated reports whether the set has reached its maxEntries cap.
// Reads are eventually consistent under concurrent Add/Remove.
func (s *PrunedTxSet) Saturated() bool {
	return s.maxEntries > 0 && s.count.Load() >= s.maxEntries
}
