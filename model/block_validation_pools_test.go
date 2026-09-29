package model

import (
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/require"
)

func TestGetTxMap_ReusesPooledInstance(t *testing.T) {
	// First Get + Put returns instance to pool. Class 0, so a one-entry fill
	// belongs to the class and the map is kept (see txMapPoolable).
	m1 := GetTxMap(1 << 12)
	require.NotNil(t, m1)
	var h chainhash.Hash
	h[0] = 0x42
	require.NoError(t, m1.Put(h, 1))
	require.Equal(t, 1, m1.Length())
	PutTxMap(m1, 1<<12)
	waitForRecycles()

	// Second Get for the same size class should yield a cleared map.
	// We can't guarantee it's the same instance because sync.Pool is
	// allowed to drop entries, but if we do get a pooled instance the
	// length must be zero.
	m2 := GetTxMap(1 << 12)
	require.NotNil(t, m2)
	require.Equal(t, 0, m2.Length(), "pooled map must be cleared before reuse")
	PutTxMap(m2, 1<<12)
}

func TestGetTxMap_OversizedAllocatesFresh(t *testing.T) {
	// An n above the largest size class is not pooled: GetTxMap allocates fresh
	// and PutTxMap drops it rather than retaining a giant map. Verify that
	// contract through the classification and the Put-drop path.
	//
	// This previously did `m := GetTxMap(2<<30)` — actually allocating a real
	// ~2-billion-entry swiss map. Its constructor writes one control byte per
	// group across 8192 sub-maps (~3 GiB of dirtied pages), which ballooned to
	// ~10 GiB RSS under -race and made model.test the dominant memory user in CI
	// (#1051) — all to exercise a branch that guards blocks larger than any that
	// can exist. (The unbounded fresh allocation for adversarial n is a separate
	// production-hardening concern, not addressed here.)
	require.Equal(t, -1, txMapClassIdxFor(2<<30), "n above the max size class must not map to a pool")

	// PutTxMap with an oversized n must drop the map (idx -1) without panicking.
	m := GetTxMap(1 << 12)
	require.NotNil(t, m)
	require.NotPanics(t, func() { PutTxMap(m, 2<<30) })
}

func TestGetTxMap_DifferentSizeClassesAreSeparate(t *testing.T) {
	// Put a small map, Get a larger one — must not be the same instance.
	small := GetTxMap(1 << 12) // 4K class
	var h chainhash.Hash
	h[0] = 0x01
	require.NoError(t, small.Put(h, 1))
	PutTxMap(small, 1<<12)

	large := GetTxMap(1 << 22) // 4M class
	require.NotNil(t, large)
	require.Equal(t, 0, large.Length())
	// Sanity: the large map is not the small one we just put back.
	require.NotSame(t, small, large)
	PutTxMap(large, 1<<22)
}

func TestGetParentSpendsMap_RoundTrip(t *testing.T) {
	m1 := GetParentSpendsMap(1_000_000)
	require.NotNil(t, m1)
	require.Equal(t, parentSpendsBuckets, m1.NrOfBuckets())

	// Insert a few inpoints.
	for i := 0; i < 100; i++ {
		var inp subtreepkg.Inpoint
		inp.Hash[0] = byte(i)
		inp.Index = uint32(i)
		ok, err := m1.SetIfNotExists(inp)
		require.NoError(t, err)
		require.True(t, ok)
	}
	PutParentSpendsMap(m1, 1_000_000)
	waitForRecycles()

	// Re-Get should produce a cleared map for the same size class.
	m2 := GetParentSpendsMap(1_000_000)
	require.NotNil(t, m2)
	// Every previously-inserted inpoint must be absent.
	for i := 0; i < 100; i++ {
		var inp subtreepkg.Inpoint
		inp.Hash[0] = byte(i)
		inp.Index = uint32(i)
		ok, err := m2.SetIfNotExists(inp)
		require.NoError(t, err)
		require.True(t, ok, "cleared map should accept inpoint")
	}
	PutParentSpendsMap(m2, 1_000_000)
}

func TestPools_NilSafe(t *testing.T) {
	// Defensive: PutTxMap(nil) and PutParentSpendsMap(nil) must not panic.
	require.NotPanics(t, func() {
		PutTxMap(nil, 100)
		PutParentSpendsMap(nil, 100)
	})
}

func TestTxMapPool_ConcurrentReuse(t *testing.T) {
	// Sanity check that simultaneous Get/Put traffic doesn't race or
	// hand the same instance to two goroutines at once.
	const goroutines = 16
	const iters = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				m := GetTxMap(1 << 12)
				require.Equal(t, 0, m.Length())
				var h chainhash.Hash
				h[0] = byte(g)
				h[1] = byte(i)
				require.NoError(t, m.Put(h, uint64(g*iters+i)))
				PutTxMap(m, 1<<12)
			}
		}(g)
	}
	wg.Wait()
}

// TestRecycleInBackground pins that releasing a map does not block the caller
// on Clear. Clearing a ~470M-entry txMap took ~4s inline at the end of
// Block.Valid, on the critical path before the block is accepted.
func TestRecycleInBackground(t *testing.T) {
	tracker := newRecycleTracker(time.Hour)
	releaseClear := make(chan struct{})

	returned := make(chan struct{})

	go func() {
		recycleInBackground(tracker, &sync.Pool{}, struct{}{}, func() { <-releaseClear })
		close(returned)
	}()

	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("recycleInBackground blocked on clear")
	}

	idle := make(chan struct{})

	go func() {
		tracker.waitRecycled()
		close(idle)
	}()

	select {
	case <-idle:
		t.Fatal("recycle reported done before clear finished")
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseClear)

	select {
	case <-idle:
	case <-time.After(5 * time.Second):
		t.Fatal("recycle never finished")
	}
}

// The recycleTracker tests below use a tracker and pool of their own: the
// package pools are shared with every other test in the package, whose
// releases leave recycles and parked maps behind.

// TestRecycleTracker_WaitingTakeGetsHandoff pins that a take landing while a
// map of its class is still being cleared waits for that map instead of
// returning empty-handed and making the caller allocate a second one.
func TestRecycleTracker_WaitingTakeGetsHandoff(t *testing.T) {
	tracker := newRecycleTracker(time.Hour)
	pool := &sync.Pool{}
	m := &struct{ id int }{1}
	releaseClear := make(chan struct{})

	recycleInBackground(tracker, pool, m, func() { <-releaseClear })

	got := make(chan interface{})

	go func() { got <- tracker.take(pool) }()

	select {
	case <-got:
		t.Fatal("take returned while a map of its class was still being cleared")
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseClear)

	select {
	case g := <-got:
		require.Same(t, m, g, "take must hand out the recycled map, not nothing")
	case <-time.After(5 * time.Second):
		t.Fatal("take never returned")
	}
}

// TestRecycleTracker_IdleMapVisibleToAnyGoroutine pins the idle slot: a map
// whose clear finished with nobody waiting is still found by a take on another
// goroutine. Through sync.Pool alone it would sit in the recycling goroutine's
// per-P slot, which a take running on another P does not see.
func TestRecycleTracker_IdleMapVisibleToAnyGoroutine(t *testing.T) {
	tracker := newRecycleTracker(time.Hour)
	pool := &sync.Pool{}
	m := &struct{ id int }{2}

	recycleInBackground(tracker, pool, m, func() {})
	tracker.waitRecycled()

	got := make(chan interface{})

	go func() { got <- tracker.take(pool) }()

	require.Same(t, m, <-got)
	require.Nil(t, tracker.take(pool), "the parked map is handed out once")
}

// TestRecycleTracker_IdleMapExpiresIntoPool pins that a parked map is not held
// for good: after maxIdle it moves to the sync.Pool, whose per-GC drain then
// releases it when block sizes shrink.
func TestRecycleTracker_IdleMapExpiresIntoPool(t *testing.T) {
	tracker := newRecycleTracker(10 * time.Millisecond)
	pool := &sync.Pool{}

	recycleInBackground(tracker, pool, &struct{ id int }{3}, func() {})
	tracker.waitRecycled()

	require.Eventually(t, func() bool {
		tracker.mu.Lock()
		defer tracker.mu.Unlock()

		return tracker.idle == nil
	}, 5*time.Second, 5*time.Millisecond, "the parked map must leave the idle slot after maxIdle")
}

// TestRecycleTracker_StaleExpiryKeepsNewerMap pins the generation check: the
// timer armed for a map that was already taken must not evict a newer map
// parked in the slot since.
func TestRecycleTracker_StaleExpiryKeepsNewerMap(t *testing.T) {
	tracker := newRecycleTracker(time.Hour)
	pool := &sync.Pool{}
	first := &struct{ id int }{4}
	second := &struct{ id int }{5}

	recycleInBackground(tracker, pool, first, func() {})
	tracker.waitRecycled()

	tracker.mu.Lock()
	firstGen := tracker.idleGen
	tracker.mu.Unlock()

	require.Same(t, first, tracker.take(pool))

	recycleInBackground(tracker, pool, second, func() {})
	tracker.waitRecycled()

	tracker.expireIdle(pool, firstGen)

	require.Same(t, second, tracker.take(pool), "a stale expiry must not evict the newer map")
}

// TestTxMapPoolable pins which released maps are pooled. Filled past its class,
// a map would retain an oversized backing, so it is always dropped. Filled far
// below its class, only a map allocated fresh for this block is dropped: the
// background Clear would make its mostly untouched eager backing resident. A map
// drawn from the pool is resident already, and dropping it would make the next
// block (a retry after an error, typically) allocate a second one.
func TestTxMapPoolable(t *testing.T) {
	idx := txMapClassIdxFor(1 << 20) // the 1M class
	below := int(txMapSizeClasses[idx-1])
	class := int(txMapSizeClasses[idx])

	require.True(t, txMapPoolable(class, idx, true), "a full class is pooled")
	require.True(t, txMapPoolable(below+1, idx, true), "just above the class below is pooled")
	require.False(t, txMapPoolable(class+1, idx, false), "past the class is dropped")
	require.False(t, txMapPoolable(class+1, idx, true), "past the class is dropped")
	require.False(t, txMapPoolable(below, idx, true), "a fresh map the class below would hold is dropped")
	require.True(t, txMapPoolable(0, idx, false), "a pooled map is kept however little it holds")
	require.True(t, txMapPoolable(0, 0, true), "the smallest class keeps any fill")
}

// TestEvictIdleExcept pins that a class which had to allocate releases the
// maps parked in the other classes: a parked map cannot be collected, so after
// block sizes move to a new class the old class's map would otherwise stay
// pinned beside the new one until its timer fires.
func TestEvictIdleExcept(t *testing.T) {
	trackers := []*recycleTracker{newRecycleTracker(time.Hour), newRecycleTracker(time.Hour), newRecycleTracker(time.Hour)}
	pools := []*sync.Pool{{}, {}, {}}

	for i := range trackers {
		recycleInBackground(trackers[i], pools[i], &struct{ id int }{i}, func() {})
		trackers[i].waitRecycled()
	}

	evictIdleExcept(trackers, pools, 1)

	for i, tr := range trackers {
		tr.mu.Lock()
		parked := tr.idle != nil
		tr.mu.Unlock()

		require.Equal(t, i == 1, parked, "class %d", i)
	}
}
