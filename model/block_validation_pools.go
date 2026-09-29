package model

import (
	"sync"
	"time"

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
// backing and vice-versa. Load shifts are handled in two steps: the last
// cleared map of a class is parked for up to idleMapMaxAge (or until another
// class has to allocate, see evictIdleExcept) and then left to sync.Pool, whose
// per-GC drainage lets the class age out when subtree size drops from 1M back
// to 1K.
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

// txMapPools have no New: GetTxMap has to see an empty pool so it can wait for
// an in-flight recycle of that class before allocating (see recycleTracker).
var txMapPools = func() []*sync.Pool {
	pools := make([]*sync.Pool, len(txMapSizeClasses))
	for i := range txMapSizeClasses {
		pools[i] = &sync.Pool{}
	}
	return pools
}()

var txMapRecycles = newRecycleTrackers(len(txMapSizeClasses), idleMapMaxAge)

// txMapClassIdxFor returns the smallest size-class index that holds n
// entries, or -1 if n exceeds every class (caller allocates fresh + skips
// the pool). n is 64-bit because a block's transaction count is: post-Genesis
// BSV has no block-size limit, and narrowing the count to uint32 turned a
// consensus-valid block with more than 2^32 transactions into an eternally
// retried processing error (issue 1428).
func txMapClassIdxFor(n uint64) int {
	for i, class := range txMapSizeClasses {
		if n <= uint64(class) {
			return i
		}
	}
	return -1
}

// txMapConstructorWrapThreshold is the largest hint go-tx-map's
// NewSplitSwissMapUint64 handles exactly. It derives its per-bucket size as
// (hint + hint/5) computed in uint32, so hints above this wrap and yield an
// arbitrary preallocation unrelated to the count (math.MaxUint32 wraps to
// ~859M entries).
//
// The value mirrors that dependency's 20% headroom factor, which it does not
// export, so it cannot be imported and must be revisited on a go-tx-map bump
// (currently v1.4.1). TestTxMapAllocHintAvoidsConstructorOverflow asserts this
// really is the last exact value and that one past it wraps, so a changed factor
// fails the test rather than silently invalidating the bound below.
const txMapConstructorWrapThreshold uint64 = 3579139413

// txMapAllocHint bounds a 64-bit entry count to the preallocation hint passed
// to the map constructor. It caps preallocation only — the swiss maps resize on
// insert, so capacity is not limited by it.
//
// The bound is the largest pooled size class rather than math.MaxUint32 for two
// reasons. The constructor preallocates EAGERLY from the hint at roughly 46
// bytes per entry (see the note in TestGetTxMap_OversizedAllocatesFresh about
// keeping such allocations out of tests), so a hint near the top of the uint32
// range asks for well over 100 GB. Second, it wraps above
// txMapConstructorWrapThreshold, and the largest class sits safely below that.
//
// The cost this accepts: a count in the band between the largest class and the
// wrap threshold previously got an exact preallocation and now gets the
// largest-class hint, so filling it rehashes mid-fill across all buckets with a
// transient peak around 1.5x the final size. That is the intended trade — an
// unvalidated number should not drive an exact multi-GB allocation. The count
// is never the peer-supplied TransactionCount: the separate pass derives it from
// the loaded body (txMapEntryCount), and the in-memory load path from
// len(Subtrees) x subtree 0 only after the subtree list is bound to the header's
// merkle root (getAndValidateSubtreesWithDedup). So the band is only reachable
// by a body committed to under a valid-PoW header.
//
// The function is deliberately total (a min, not an assertion) so it stays
// correct for any input, although its only current call site reaches it just
// when n exceeds every class.
func txMapAllocHint(n uint64) uint32 {
	largestClass := txMapSizeClasses[len(txMapSizeClasses)-1]
	if n > uint64(largestClass) {
		return largestClass
	}

	return uint32(n)
}

// parentSpendsAllocHint bounds an inpoint count to the capacity hint passed to
// NewSplitSyncedParentMap, for the same reason txMapAllocHint exists: that
// constructor preallocates eagerly per bucket and computes
// uint32((n + n/5) / nrOfBuckets), so an unbounded hint both asks for an
// unbounded allocation and can truncate into an arbitrary per-bucket size. The
// bound is the largest pooled size class, which keeps the division exact.
//
// A block that genuinely carries more inpoints than the largest class still
// holds them — the map grows on insert — it just is not preallocated for them.
func parentSpendsAllocHint(expectedInpoints uint64) uint64 {
	largestClass := parentSpendsSizeClasses[len(parentSpendsSizeClasses)-1]
	if expectedInpoints > largestClass {
		return largestClass
	}

	return expectedInpoints
}

// GetTxMap returns a *SplitSwissMapUint64 for n entries. Drawn from the pool
// when n fits a known size class; allocated fresh otherwise, with the
// preallocation hint bounded by txMapAllocHint instead of failing on counts
// beyond uint32 (issue 1428). A bounded hint sizes the initial allocation only:
// the map still holds n entries, growing on insert. Pass the same n to
// PutTxMap.
func GetTxMap(n uint64) *txmap.SplitSwissMapUint64 {
	m, _ := getTxMap(n)
	return m
}

// getTxMap is GetTxMap that also reports whether the map was allocated fresh
// rather than reused, which putTxMap needs to decide whether to keep it.
func getTxMap(n uint64) (m *txmap.SplitSwissMapUint64, fresh bool) {
	idx := txMapClassIdxFor(n)
	if idx < 0 {
		evictIdleExcept(txMapRecycles, txMapPools, -1)
		return txmap.NewSplitSwissMapUint64(txMapAllocHint(n), txMapBuckets), true
	}
	if pooled := txMapRecycles[idx].take(txMapPools[idx]); pooled != nil {
		return pooled.(*txmap.SplitSwissMapUint64), false
	}
	evictIdleExcept(txMapRecycles, txMapPools, idx)
	return txmap.NewSplitSwissMapUint64(txMapSizeClasses[idx], txMapBuckets), true
}

// txMapPoolable reports whether a released map filled with length entries goes
// back to class idx. Filled past the class, never: pooling it would retain an
// oversized backing for every later block that draws the class (the same
// reason over-max maps are dropped). Filled no more than the class below would
// hold, only if it came from the pool: the background Clear writes every slot,
// which for a freshly allocated, mostly untouched map makes the whole eager
// backing resident for nothing. A map drawn from the pool is resident already,
// and dropping it would just make the next Get of the class (a retry after an
// error, typically, since an honest block fills its class) allocate a second.
func txMapPoolable(length int, idx int, fresh bool) bool {
	if uint64(length) > uint64(txMapSizeClasses[idx]) { // nolint: gosec
		return false
	}

	return !fresh || idx == 0 || uint64(length) > uint64(txMapSizeClasses[idx-1]) // nolint: gosec
}

// evictIdleExcept moves the parked maps of every class but keep to their
// pools, where the GC can drain them. Called when a class has to allocate: a
// parked map cannot be collected, so after block sizes move to a new class the
// old class's map would otherwise stay resident beside the new one until its
// idle timer fires. keep < 0 evicts every class.
func evictIdleExcept(trackers []*recycleTracker, pools []*sync.Pool, keep int) {
	for i, t := range trackers {
		if i != keep {
			t.evictIdle(pools[i])
		}
	}
}

// PutTxMap clears m and returns it to the size-class pool keyed by n.
// n must match the value passed to GetTxMap. Maps that did not come from
// the pool (n above max class) are dropped.
//
// Dropping them is deliberate, not an oversight, even though an over-max map is
// built with the largest class's hint and buckets and so is interchangeable in
// shape with that pool's contents: such a map has been *filled* past the class
// it was sized for, so pooling it would retain an oversized backing for every
// later block that draws the largest class. The cost is that consecutive
// over-max blocks each allocate fresh.
func PutTxMap(m *txmap.SplitSwissMapUint64, n uint64) {
	putTxMap(m, n, false)
}

// putTxMap is PutTxMap for a map whose origin is known; fresh must be what
// getTxMap reported for it.
func putTxMap(m *txmap.SplitSwissMapUint64, n uint64, fresh bool) {
	if m == nil {
		return
	}
	idx := txMapClassIdxFor(n)
	if idx < 0 {
		return
	}
	if !txMapPoolable(m.Length(), idx, fresh) {
		return
	}
	recycleInBackground(txMapRecycles[idx], txMapPools[idx], m, m.Clear)
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

// parentSpendsPools have no New, for the same reason as txMapPools.
var parentSpendsPools = func() []*sync.Pool {
	pools := make([]*sync.Pool, len(parentSpendsSizeClasses))
	for i := range parentSpendsSizeClasses {
		pools[i] = &sync.Pool{}
	}
	return pools
}()

var parentSpendsRecycles = newRecycleTrackers(len(parentSpendsSizeClasses), idleMapMaxAge)

func parentSpendsClassIdxFor(n uint64) int {
	for i, class := range parentSpendsSizeClasses {
		if n <= class {
			return i
		}
	}
	return -1
}

// GetParentSpendsMap returns a *SplitSyncedParentMap for expectedInpoints
// entries. Drawn from the pool when the count fits a known size class; allocated
// fresh otherwise, with the preallocation hint bounded by parentSpendsAllocHint.
// A bounded hint sizes the initial allocation only — the map still holds every
// inpoint, growing on insert. Pass the same expectedInpoints to
// PutParentSpendsMap: the pool class is chosen from the unbounded value, so an
// over-max map is dropped on Put rather than retained (see PutTxMap for why).
func GetParentSpendsMap(expectedInpoints uint64) *SplitSyncedParentMap {
	idx := parentSpendsClassIdxFor(expectedInpoints)
	if idx < 0 {
		evictIdleExcept(parentSpendsRecycles, parentSpendsPools, -1)
		return NewSplitSyncedParentMap(parentSpendsBuckets, parentSpendsAllocHint(expectedInpoints))
	}
	if m := parentSpendsRecycles[idx].take(parentSpendsPools[idx]); m != nil {
		return m.(*SplitSyncedParentMap)
	}
	evictIdleExcept(parentSpendsRecycles, parentSpendsPools, idx)
	return NewSplitSyncedParentMap(parentSpendsBuckets, parentSpendsSizeClasses[idx])
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
	recycleInBackground(parentSpendsRecycles[idx], parentSpendsPools[idx], m, m.Clear)
}

// recycleInBackground clears a released map and then hands it on, off the
// caller's goroutine. Clearing a map sized for a large block takes seconds
// (~4s for a ~470M-entry txMap), and both callers release at the end of block
// validation, on the path before the block is accepted. The map is only handed
// on after clear returns, so Get never sees a dirty map.
func recycleInBackground(t *recycleTracker, pool *sync.Pool, m interface{}, clear func()) {
	t.start()

	go func() {
		clear()
		t.finish(pool, m)
	}()
}

// idleMapMaxAge is how long a cleared map stays parked in its class's idle
// slot before it moves to the sync.Pool, whose per-GC drain then releases it if
// nothing draws the class again. Two target block intervals: long enough that
// consecutive blocks of a class reuse one map instead of allocating a second,
// short enough that a class nobody draws any more is let go.
const idleMapMaxAge = 20 * time.Minute

// recycleTracker coordinates the recycles of one size class with the Gets for
// it, so a Get finds a cleared map whenever one exists instead of allocating a
// second map of the class, which for the largest class is tens of GB.
//
// Two gaps need covering, and sync.Pool covers neither: a Put lands in the
// putting goroutine's per-P slot, which a Get on another P does not see, and the
// race detector drops pooled items at random.
//   - A Get during the clear waits for the recycle and receives the map directly
//     (handoff). The next block's Get lands milliseconds into its validation,
//     inside the previous block's multi-second clear.
//   - A clear that finishes with nobody waiting parks the map in the idle slot,
//     which any goroutine can take from, for up to maxIdle. Only then, or when
//     the slot is already taken, does it go to the sync.Pool.
type recycleTracker struct {
	mu      sync.Mutex
	cond    *sync.Cond
	maxIdle time.Duration
	pending int
	waiters int
	handoff interface{}
	idle    interface{}
	// idleGen changes whenever the idle slot is filled or emptied, so an expiry
	// timer only evicts the map it was armed for.
	idleGen uint64
}

func newRecycleTracker(maxIdle time.Duration) *recycleTracker {
	t := &recycleTracker{maxIdle: maxIdle}
	t.cond = sync.NewCond(&t.mu)

	return t
}

func newRecycleTrackers(n int, maxIdle time.Duration) []*recycleTracker {
	trackers := make([]*recycleTracker, n)
	for i := range trackers {
		trackers[i] = newRecycleTracker(maxIdle)
	}

	return trackers
}

func (t *recycleTracker) start() {
	t.mu.Lock()
	t.pending++
	t.mu.Unlock()
}

// finish passes a cleared map to a waiting Get if there is one, parks it in the
// idle slot if that is free, and pools it otherwise. Everything happens under
// the lock so a Get never observes the recycle as done before the map is
// reachable.
func (t *recycleTracker) finish(pool *sync.Pool, m interface{}) {
	t.mu.Lock()
	t.pending--

	switch {
	case t.waiters > 0 && t.handoff == nil:
		t.handoff = m
	case t.idle == nil:
		t.idle = m
		t.idleGen++
		gen := t.idleGen

		time.AfterFunc(t.maxIdle, func() { t.expireIdle(pool, gen) })
	default:
		pool.Put(m)
	}

	t.mu.Unlock()
	t.cond.Broadcast()
}

// evictIdle moves whatever map is parked to pool.
func (t *recycleTracker) evictIdle(pool *sync.Pool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.idle != nil {
		pool.Put(t.idle)
		t.idle = nil
		t.idleGen++
	}
}

// expireIdle moves the parked map to pool if it is still the one parked at gen.
func (t *recycleTracker) expireIdle(pool *sync.Pool, gen uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.idle != nil && t.idleGen == gen {
		pool.Put(t.idle)
		t.idle = nil
		t.idleGen++
	}
}

// take returns a cleared map of this class if one exists, waiting out any
// in-flight recycle rather than returning empty-handed. nil means there is
// nothing to reuse and the caller allocates.
func (t *recycleTracker) take(pool *sync.Pool) interface{} {
	t.mu.Lock()
	defer t.mu.Unlock()

	for {
		if t.handoff != nil {
			m := t.handoff
			t.handoff = nil

			return m
		}

		if t.idle != nil {
			m := t.idle
			t.idle = nil
			t.idleGen++

			return m
		}

		if m := pool.Get(); m != nil {
			return m
		}

		if t.pending == 0 {
			return nil
		}

		t.waiters++
		t.cond.Wait()
		t.waiters--
	}
}

// waitRecycled blocks until no recycle of this class is in flight. It does
// not wait for the idle slot, which holds finished recycles.
func (t *recycleTracker) waitRecycled() {
	t.mu.Lock()
	for t.pending > 0 {
		t.cond.Wait()
	}
	t.mu.Unlock()
}

// waitForRecycles blocks until every in-flight recycle has handed on its map.
func waitForRecycles() {
	for _, t := range txMapRecycles {
		t.waitRecycled()
	}

	for _, t := range parentSpendsRecycles {
		t.waitRecycled()
	}
}
