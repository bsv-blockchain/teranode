package expiringmap

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestExpiringMap_WithMaxSize_CapEnforced(t *testing.T) {
	m := New[string, int](time.Hour).WithMaxSize(3)
	defer m.Stop()

	m.Set("a", 1)
	time.Sleep(2 * time.Millisecond)
	m.Set("b", 2)
	time.Sleep(2 * time.Millisecond)
	m.Set("c", 3)
	time.Sleep(2 * time.Millisecond)
	m.Set("d", 4)
	time.Sleep(2 * time.Millisecond)
	m.Set("e", 5)

	require.LessOrEqual(t, m.Len(), 3)

	_, aExists := m.Get("a")
	_, bExists := m.Get("b")
	_, cExists := m.Get("c")
	_, dExists := m.Get("d")
	_, eExists := m.Get("e")
	require.False(t, aExists, "oldest entry 'a' should have been evicted")
	require.False(t, bExists, "second-oldest entry 'b' should have been evicted")
	require.True(t, cExists, "middle entry 'c' should still be present")
	require.True(t, dExists, "recent entry 'd' should still be present")
	require.True(t, eExists, "newest entry 'e' should still be present")
}

func TestExpiringMap_WithMaxSize_UpdateExistingKey(t *testing.T) {
	m := New[string, int](time.Hour).WithMaxSize(3)
	defer m.Stop()

	m.Set("a", 1)
	m.Set("b", 2)
	m.Set("c", 3)
	require.Equal(t, 3, m.Len())

	m.Set("b", 22)
	require.Equal(t, 3, m.Len())

	v, ok := m.Get("a")
	require.True(t, ok)
	require.Equal(t, 1, v)

	v, ok = m.Get("b")
	require.True(t, ok)
	require.Equal(t, 22, v)

	v, ok = m.Get("c")
	require.True(t, ok)
	require.Equal(t, 3, v)
}

func TestExpiringMap_WithMaxSize_EvictionChannelFires(t *testing.T) {
	ch := make(chan []int, 4)
	m := New[string, int](time.Hour).WithEvictionChannel(ch).WithMaxSize(3)
	defer m.Stop()

	m.Set("a", 1)
	time.Sleep(2 * time.Millisecond)
	m.Set("b", 2)
	time.Sleep(2 * time.Millisecond)
	m.Set("c", 3)
	time.Sleep(2 * time.Millisecond)
	m.Set("d", 4)

	select {
	case items := <-ch:
		require.Len(t, items, 1)
		require.Equal(t, 1, items[0])
	case <-time.After(time.Second):
		t.Fatal("expected eviction-channel notification on cap eviction")
	}
}

func TestExpiringMap_WithMaxSize_EvictionFunctionFires(t *testing.T) {
	var evictedKey string
	var evictedValue int

	m := New[string, int](time.Hour).
		WithEvictionFunction(func(k string, v int) bool {
			evictedKey = k
			evictedValue = v
			return true
		}).
		WithMaxSize(2)
	defer m.Stop()

	m.Set("a", 1)
	time.Sleep(2 * time.Millisecond)
	m.Set("b", 2)
	time.Sleep(2 * time.Millisecond)
	m.Set("c", 3)

	require.Equal(t, "a", evictedKey)
	require.Equal(t, 1, evictedValue)
	require.Equal(t, 2, m.Len())
}

func TestExpiringMap_WithMaxSize_EvictionFunctionVeto(t *testing.T) {
	vetoKey := "a"
	m := New[string, int](time.Hour).
		WithEvictionFunction(func(k string, v int) bool {
			return k != vetoKey
		}).
		WithMaxSize(2)
	defer m.Stop()

	m.Set("a", 1)
	time.Sleep(2 * time.Millisecond)
	m.Set("b", 2)
	time.Sleep(2 * time.Millisecond)
	m.Set("c", 3)

	_, aExists := m.Get("a")
	require.True(t, aExists, "vetoed entry should not be evicted")
	require.LessOrEqual(t, m.Len(), 2, "cap must hold even when eviction is vetoed")

	_, cExists := m.Get("c")
	require.False(t, cExists, "new entry must be dropped when eviction is vetoed")

	vetoKey = ""
	time.Sleep(2 * time.Millisecond)
	m.Set("d", 4)

	require.LessOrEqual(t, m.Len(), 2, "cap must hold after veto-then-non-veto sequence")
	_, dExists := m.Get("d")
	require.True(t, dExists, "non-vetoed insert after a veto should succeed")
}

func TestExpiringMap_WithMaxSize_ZeroIsUnbounded(t *testing.T) {
	m := New[int, int](time.Hour).WithMaxSize(0)
	defer m.Stop()

	for i := 0; i < 100; i++ {
		m.Set(i, i)
	}

	require.Equal(t, 100, m.Len())
}

func TestExpiringMap_NoMaxSize_DefaultUnbounded(t *testing.T) {
	m := New[int, int](time.Hour)
	defer m.Stop()

	for i := 0; i < 100; i++ {
		m.Set(i, i)
	}

	require.Equal(t, 100, m.Len())
}

// TestExpiringMap_CleanEvictionSendDoesNotBlockUnderLock guards against the TTL
// clean() path performing a blocking eviction-channel send while holding m.mu.
// With an unbuffered, unconsumed eviction channel, a blocking send would stall
// clean() under the write lock and deadlock all Get/Set/Delete/Len. The send
// must be non-blocking (drop-on-full), so clean() always returns.
func TestExpiringMap_CleanEvictionSendDoesNotBlockUnderLock(t *testing.T) {
	// expire != 0 would start the hourly ticker; use a long expiry so the
	// background goroutine never fires during the test and we drive clean()
	// directly.
	ch := make(chan []int) // unbuffered, no reader
	m := New[string, int](time.Hour).WithEvictionChannel(ch)

	// Insert an already-expired item so clean() selects it for eviction.
	m.mu.Lock()
	m.items["k"] = &itemWrapper[int]{
		item:    1,
		expiry:  time.Now().Add(-time.Hour).UnixNano(),
		addedAt: time.Now().Add(-time.Hour).UnixNano(),
	}
	m.mu.Unlock()

	done := make(chan struct{})
	go func() {
		m.clean()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("clean() blocked on eviction-channel send while holding m.mu")
	}

	// The expired item must still have been removed even though the
	// notification was dropped.
	_, ok := m.Get("k")
	require.False(t, ok, "expired item should be evicted")
}

// TestExpiringMap_SetIfAbsent pins the one-lock insert the legacy announcement
// workers use to claim a hash: a present key is left alone, an absent one is
// written, and an entry past its expiry that the sweeper has not yet removed
// counts as absent, as it does for Get.
func TestExpiringMap_SetIfAbsent(t *testing.T) {
	m := New[string, int](time.Hour)
	defer m.Stop()

	require.True(t, m.SetIfAbsent("a", 1), "an absent key is inserted")
	require.False(t, m.SetIfAbsent("a", 2), "a present key is left alone")

	v, ok := m.Get("a")
	require.True(t, ok)
	require.Equal(t, 1, v)

	// Expire the entry by hand so the sweeper cannot race the assertion.
	m.mu.Lock()
	m.items["a"].expiry = time.Now().Add(-time.Second).UnixNano()
	m.mu.Unlock()

	require.True(t, m.SetIfAbsent("a", 3), "an expired entry not yet swept counts as absent")

	v, ok = m.Get("a")
	require.True(t, ok)
	require.Equal(t, 3, v)
}

// TestExpiringMap_WithRandomEviction_HoldsTheCap pins the second cap policy:
// at the cap, an arbitrary entry goes rather than the oldest, so an insert
// costs one map step rather than a scan of every entry.
func TestExpiringMap_WithRandomEviction_HoldsTheCap(t *testing.T) {
	m := New[int, int](time.Hour).WithMaxSize(100).WithRandomEviction()
	defer m.Stop()

	for i := 0; i < 1000; i++ {
		m.Set(i, i)
	}

	require.Equal(t, 100, m.Len())

	require.True(t, m.SetIfAbsent(5000, 1))
	require.Equal(t, 100, m.Len(), "SetIfAbsent honours the cap")

	before := m.Len()
	m.Set(5000, 2)
	require.Equal(t, before, m.Len(), "updating a present key evicts nothing")

	v, ok := m.Get(5000)
	require.True(t, ok)
	require.Equal(t, 2, v)
}

// TestExpiringMap_SetIfAbsent_OneWinnerUnderConcurrency pins what SetIfAbsent
// is for: of many goroutines claiming one key at once, exactly one is told it
// wrote. A Get followed by a Set lets several see the key absent and all
// claim it. Each round releases every goroutine together on a fresh key, and
// two hundred rounds make an interleaving that splits the check from the
// write all but certain to show. Run it under -race as well.
func TestExpiringMap_SetIfAbsent_OneWinnerUnderConcurrency(t *testing.T) {
	const goroutines, rounds = 64, 200

	m := New[int, int](time.Hour)
	defer m.Stop()

	for round := 0; round < rounds; round++ {
		var (
			wins  atomic.Int32
			wg    sync.WaitGroup
			start = make(chan struct{})
		)

		for g := 0; g < goroutines; g++ {
			wg.Add(1)

			go func(g int) {
				defer wg.Done()

				<-start

				if m.SetIfAbsent(round, g) {
					wins.Add(1)
				}
			}(g)
		}

		close(start)
		wg.Wait()

		require.Equal(t, int32(1), wins.Load(), "round %d: exactly one claim of a key must succeed", round)
	}
}

// TestExpiringMap_WithRandomEviction_DoesNotSeekTheOldest pins the policy
// WithRandomEviction selects, not only the cap. The oldest-entry policy finds
// the oldest entry by scanning every entry on each insert at the cap; the
// random policy takes whichever entry the map yields first. Insertion times
// are set by hand, oldest first, so the oldest-entry policy has exactly one
// answer each time, the lowest key still present, and would evict 0, 1, 2 and
// so on in order. The random policy matching that order for a hundred
// evictions running has a vanishing probability.
func TestExpiringMap_WithRandomEviction_DoesNotSeekTheOldest(t *testing.T) {
	const size = 100

	var evicted []int

	m := New[int, int](time.Hour).WithMaxSize(size).WithRandomEviction().
		WithEvictionFunction(func(k, _ int) bool {
			evicted = append(evicted, k)

			return true
		})
	defer m.Stop()

	setAt := func(k int) {
		m.Set(k, k)

		m.mu.Lock()
		if item, ok := m.items[k]; ok {
			item.addedAt = int64(k)
		}
		m.mu.Unlock()
	}

	for k := 0; k < 2*size; k++ {
		setAt(k)
	}

	require.Equal(t, size, m.Len(), "the cap holds")
	require.Len(t, evicted, size, "one eviction per insert past the cap")

	oldestFirst := make([]int, size)
	for i := range oldestFirst {
		oldestFirst[i] = i
	}

	require.NotEqual(t, oldestFirst, evicted, "random eviction must not evict oldest first, which costs a scan of every entry per insert")
}
