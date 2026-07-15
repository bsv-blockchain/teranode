package util

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sentinelError is a minimal error type for the propagation tests. Its identity (not a Teranode
// error code) is what require.Same asserts, proving verbatim propagation. Using a custom type
// avoids the standard library "errors" package (forbidden module-wide) while still side-stepping
// *errors.Error.Is's code-based matching, so a same-code look-alike cannot pass.
type sentinelError struct{ msg string }

func (e *sentinelError) Error() string { return e.msg }

var errLeaderBoom = &sentinelError{msg: "leader boom"}

func TestNewExpiringConcurrentCache(t *testing.T) {
	cache := NewExpiringConcurrentCache[string, int](time.Second)

	assert.NotNil(t, cache)
	assert.NotNil(t, cache.cache)
	assert.NotNil(t, cache.wg)
	assert.Empty(t, cache.wg)
}

func TestGetOrSetCacheHit(t *testing.T) {
	cache := NewExpiringConcurrentCache[string, string](time.Minute)

	// First call should fetch and cache
	fetchCalled := false
	val, err := cache.GetOrSet("key1", func() (string, bool, error) {
		fetchCalled = true
		return "value1", true, nil
	})

	require.NoError(t, err)
	assert.Equal(t, "value1", val)
	assert.True(t, fetchCalled)

	// Second call should hit cache, not call fetch
	fetchCalled = false
	val, err = cache.GetOrSet("key1", func() (string, bool, error) {
		fetchCalled = true
		return "different", true, nil
	})

	require.NoError(t, err)
	assert.Equal(t, "value1", val)
	assert.False(t, fetchCalled)
}

func TestGetOrSetCacheMiss(t *testing.T) {
	cache := NewExpiringConcurrentCache[string, string](time.Minute)

	fetchCalled := false
	val, err := cache.GetOrSet("key1", func() (string, bool, error) {
		fetchCalled = true
		return "value1", true, nil
	})

	require.NoError(t, err)
	assert.Equal(t, "value1", val)
	assert.True(t, fetchCalled)
}

func TestGetOrSetNoCache(t *testing.T) {
	cache := NewExpiringConcurrentCache[string, string](time.Minute)

	// First call with shouldCache=false
	val, err := cache.GetOrSet("key1", func() (string, bool, error) {
		return "value1", false, nil
	})

	require.NoError(t, err)
	assert.Equal(t, "value1", val)

	// Second call should call fetch again since it wasn't cached
	fetchCalled := false
	val, err = cache.GetOrSet("key1", func() (string, bool, error) {
		fetchCalled = true
		return "value2", true, nil
	})

	require.NoError(t, err)
	assert.Equal(t, "value2", val)
	assert.True(t, fetchCalled)
}

func TestCacheExpiration(t *testing.T) {
	cache := NewExpiringConcurrentCache[string, string](100 * time.Millisecond)

	// Cache a value
	val, err := cache.GetOrSet("key1", func() (string, bool, error) {
		return "value1", true, nil
	})

	require.NoError(t, err)
	assert.Equal(t, "value1", val)

	// Should still be cached immediately
	fetchCalled := false
	val, err = cache.GetOrSet("key1", func() (string, bool, error) {
		fetchCalled = true
		return "different", true, nil
	})

	require.NoError(t, err)
	assert.Equal(t, "value1", val)
	assert.False(t, fetchCalled)

	// Wait for expiration
	time.Sleep(200 * time.Millisecond)

	// Should fetch again after expiration
	fetchCalled = false
	val, err = cache.GetOrSet("key1", func() (string, bool, error) {
		fetchCalled = true
		return "value2", true, nil
	})

	require.NoError(t, err)
	assert.Equal(t, "value2", val)
	assert.True(t, fetchCalled)
}

func TestExpiredItemRefetch(t *testing.T) {
	cache := NewExpiringConcurrentCache[string, int](50 * time.Millisecond)

	// Cache initial value
	val, err := cache.GetOrSet("key1", func() (int, bool, error) {
		return 42, true, nil
	})

	require.NoError(t, err)
	assert.Equal(t, 42, val)

	// Wait for expiration
	time.Sleep(100 * time.Millisecond)

	// Should fetch new value
	val, err = cache.GetOrSet("key1", func() (int, bool, error) {
		return 99, true, nil
	})

	require.NoError(t, err)
	assert.Equal(t, 99, val)
}

func TestConcurrentGetOrSetSingleFetch(t *testing.T) {
	cache := NewExpiringConcurrentCache[string, string](time.Minute)

	var fetchCount int64
	const numGoroutines = 10

	var wg sync.WaitGroup
	results := make([]string, numGoroutines)
	errs := make([]error, numGoroutines)

	// Start multiple goroutines requesting the same key
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			val, err := cache.GetOrSet("shared-key", func() (string, bool, error) {
				atomic.AddInt64(&fetchCount, 1)
				time.Sleep(10 * time.Millisecond) // Simulate work
				return "shared-value", true, nil
			})

			results[index] = val
			errs[index] = err
		}(i)
	}

	wg.Wait()

	// Only one fetch should have occurred
	assert.Equal(t, int64(1), atomic.LoadInt64(&fetchCount))

	// All goroutines should get the same result
	for i := 0; i < numGoroutines; i++ {
		assert.NoError(t, errs[i])
		assert.Equal(t, "shared-value", results[i])
	}
}

func TestConcurrentGetOrSetDifferentKeys(t *testing.T) {
	cache := NewExpiringConcurrentCache[string, int](time.Minute)

	var fetchCount int64
	const numGoroutines = 10

	var wg sync.WaitGroup
	results := make([]int, numGoroutines)
	errs := make([]error, numGoroutines)

	// Start goroutines requesting different keys
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			key := "key-" + string(rune('a'+index))
			val, err := cache.GetOrSet(key, func() (int, bool, error) {
				atomic.AddInt64(&fetchCount, 1)
				return index * 10, true, nil
			})

			results[index] = val
			errs[index] = err
		}(i)
	}

	wg.Wait()

	// Each key should have been fetched once
	assert.Equal(t, int64(numGoroutines), atomic.LoadInt64(&fetchCount))

	// Each goroutine should get its expected result
	for i := 0; i < numGoroutines; i++ {
		assert.NoError(t, errs[i])
		assert.Equal(t, i*10, results[i])
	}
}

func TestConcurrentGetOrSetMixedOperations(t *testing.T) {
	cache := NewExpiringConcurrentCache[int, string](time.Minute)

	var fetchCount int64
	const numGoroutines = 20

	var wg sync.WaitGroup

	// Mix of operations: some share keys, some don't
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			// Every 3rd goroutine uses the same key
			key := index % 3

			val, err := cache.GetOrSet(key, func() (string, bool, error) {
				atomic.AddInt64(&fetchCount, 1)
				time.Sleep(5 * time.Millisecond)
				return "value-" + string(rune('0'+key)), true, nil
			})

			assert.NoError(t, err)
			assert.Equal(t, "value-"+string(rune('0'+key)), val)
		}(i)
	}

	wg.Wait()

	// Should have fetched only 3 times (once per unique key)
	assert.Equal(t, int64(3), atomic.LoadInt64(&fetchCount))
}

func TestGetOrSetFetchError(t *testing.T) {
	cache := NewExpiringConcurrentCache[string, string](time.Minute)

	expectedErr := errors.NewError("fetch failed")
	val, err := cache.GetOrSet("key1", func() (string, bool, error) {
		return "", false, expectedErr
	})

	assert.Equal(t, expectedErr, err)
	assert.Equal(t, "", val) // Should return zero value

	// Subsequent call should try again (error not cached)
	fetchCalled := false
	val, err = cache.GetOrSet("key1", func() (string, bool, error) {
		fetchCalled = true
		return "success", true, nil
	})

	require.NoError(t, err)
	assert.Equal(t, "success", val)
	assert.True(t, fetchCalled)
}

func TestGetOrSetConcurrentFetchError(t *testing.T) {
	cache := NewExpiringConcurrentCache[string, string](time.Minute)

	var fetchCount int64
	expectedErr := errors.NewError("concurrent fetch failed")
	const numGoroutines = 5

	var wg sync.WaitGroup
	errs := make([]error, numGoroutines)

	// Start multiple goroutines requesting the same key that will error
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			_, err := cache.GetOrSet("error-key", func() (string, bool, error) {
				atomic.AddInt64(&fetchCount, 1)
				time.Sleep(10 * time.Millisecond)
				return "", false, expectedErr
			})

			errs[index] = err
		}(i)
	}

	wg.Wait()

	// Leaders and waiters share the flight's outcome; post-cleanup fresh leaders re-fetch. Either
	// way the fetch runs at least once and at most once per goroutine.
	fetchCountValue := atomic.LoadInt64(&fetchCount)
	require.Greater(t, fetchCountValue, int64(0), "At least one fetch should occur")
	require.LessOrEqual(t, fetchCountValue, int64(numGoroutines), "At most one fetch per goroutine")

	// With error propagation, every goroutine — leaders and waiters alike — returns the real error.
	for i := 0; i < numGoroutines; i++ {
		require.Error(t, errs[i])
		require.ErrorIs(t, errs[i], expectedErr)
	}
}

func TestGetOrSetNilValue(t *testing.T) {
	cache := NewExpiringConcurrentCache[string, *string](time.Minute)

	val, err := cache.GetOrSet("key1", func() (*string, bool, error) {
		return nil, true, nil
	})

	require.NoError(t, err)
	assert.Nil(t, val)

	// Should hit cache on second call
	fetchCalled := false
	val, err = cache.GetOrSet("key1", func() (*string, bool, error) {
		fetchCalled = true
		s := "not nil"
		return &s, true, nil
	})

	require.NoError(t, err)
	assert.Nil(t, val)
	assert.False(t, fetchCalled)
}

func TestGetOrSetZeroValue(t *testing.T) {
	cache := NewExpiringConcurrentCache[string, int](time.Minute)

	val, err := cache.GetOrSet("key1", func() (int, bool, error) {
		return 0, true, nil
	})

	require.NoError(t, err)
	assert.Equal(t, 0, val)

	// Should hit cache on second call
	fetchCalled := false
	val, err = cache.GetOrSet("key1", func() (int, bool, error) {
		fetchCalled = true
		return 42, true, nil
	})

	require.NoError(t, err)
	assert.Equal(t, 0, val)
	assert.False(t, fetchCalled)
}

func TestRaceConditions(t *testing.T) {
	cache := NewExpiringConcurrentCache[int, string](time.Minute)

	const numGoroutines = 100
	const numKeys = 10

	var wg sync.WaitGroup
	fetchCounts := make([]int64, numKeys)

	// Stress test with many concurrent operations
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			key := index % numKeys

			val, err := cache.GetOrSet(key, func() (string, bool, error) {
				atomic.AddInt64(&fetchCounts[key], 1)
				time.Sleep(time.Millisecond) // Small delay to increase contention
				return "value-" + string(rune('0'+key)), true, nil
			})

			assert.NoError(t, err)
			assert.Equal(t, "value-"+string(rune('0'+key)), val)
		}(i)
	}

	wg.Wait()

	// Each key should have been fetched exactly once
	for i := 0; i < numKeys; i++ {
		assert.Equal(t, int64(1), atomic.LoadInt64(&fetchCounts[i]),
			"Key %d should have been fetched exactly once", i)
	}
}

func TestGetOrSetWaitGroupCleanup(t *testing.T) {
	cache := NewExpiringConcurrentCache[string, string](time.Minute)

	// Perform a fetch
	_, err := cache.GetOrSet("key1", func() (string, bool, error) {
		return "value1", true, nil
	})
	require.NoError(t, err)

	// Verify wait group map is cleaned up
	cache.mu.RLock()
	assert.Empty(t, cache.wg)
	cache.mu.RUnlock()
}

func TestGetOrSetNonCachedResultAvailableToWaiters(t *testing.T) {
	cache := NewExpiringConcurrentCache[string, string](time.Minute)

	var firstFetchCount, secondFetchCount int64
	var wg sync.WaitGroup
	results := make([]string, 2)
	errs := make([]error, 2)

	// First goroutine fetches but doesn't cache
	wg.Add(1)
	go func() {
		defer wg.Done()
		val, err := cache.GetOrSet("key1", func() (string, bool, error) {
			atomic.AddInt64(&firstFetchCount, 1)
			time.Sleep(50 * time.Millisecond)
			return "not-cached-value", false, nil // shouldCache = false
		})
		results[0] = val
		errs[0] = err
	}()

	// Second goroutine waits for the first
	time.Sleep(10 * time.Millisecond) // Ensure first goroutine starts first
	wg.Add(1)
	go func() {
		defer wg.Done()
		val, err := cache.GetOrSet("key1", func() (string, bool, error) {
			atomic.AddInt64(&secondFetchCount, 1)
			return "second-fetch-value", false, nil
		})
		results[1] = val
		errs[1] = err
	}()

	wg.Wait()

	// Both should succeed
	for i := 0; i < 2; i++ {
		assert.NoError(t, errs[i])
	}

	// Due to race condition in the implementation, the second goroutine might get either:
	// 1. The result from the first goroutine if timing works out
	// 2. Its own fetch result if there's a race condition
	assert.Equal(t, int64(1), atomic.LoadInt64(&firstFetchCount), "First fetch should occur exactly once")

	// Check if second goroutine got the first result or had to fetch its own
	if results[1] == "not-cached-value" {
		// Second goroutine got the first result (ideal case)
		assert.Equal(t, int64(0), atomic.LoadInt64(&secondFetchCount), "Second fetch should not occur")
		assert.Equal(t, results[0], results[1], "Both should get the same result")
	} else {
		// Race condition occurred - second goroutine had to fetch its own result
		assert.Equal(t, int64(1), atomic.LoadInt64(&secondFetchCount), "Second fetch occurred due to race condition")
		assert.Equal(t, "second-fetch-value", results[1])
	}

	// Value should not be in cache since shouldCache was false
	cache.mu.RLock()
	_, found := cache.cache.Get("key1")
	cache.mu.RUnlock()
	assert.False(t, found)
}

func TestGetOrSetConcurrentCacheAndNoCache(t *testing.T) {
	cache := NewExpiringConcurrentCache[string, string](time.Minute)

	var wg sync.WaitGroup
	var fetchCount int64
	results := make([]string, 2)
	errs := make([]error, 2)

	// First goroutine caches the result
	wg.Add(1)
	go func() {
		defer wg.Done()
		val, err := cache.GetOrSet("key1", func() (string, bool, error) {
			atomic.AddInt64(&fetchCount, 1)
			time.Sleep(50 * time.Millisecond)
			return "cached-value", true, nil
		})
		results[0] = val
		errs[0] = err
	}()

	// Second goroutine tries to get the same key while first is fetching
	time.Sleep(10 * time.Millisecond)
	wg.Add(1)
	go func() {
		defer wg.Done()
		val, err := cache.GetOrSet("key1", func() (string, bool, error) {
			atomic.AddInt64(&fetchCount, 1)
			return "should-not-be-called", false, nil
		})
		results[1] = val
		errs[1] = err
	}()

	wg.Wait()

	// Only one fetch should have occurred
	assert.Equal(t, int64(1), atomic.LoadInt64(&fetchCount))

	// Both should get the same result
	for i := 0; i < 2; i++ {
		assert.NoError(t, errs[i])
		assert.Equal(t, "cached-value", results[i])
	}

	// Value should be in cache
	cache.mu.RLock()
	val, found := cache.cache.Get("key1")
	cache.mu.RUnlock()
	assert.True(t, found)
	assert.Equal(t, "cached-value", val)
}

func TestGetOrSetMultipleKeysConcurrently(t *testing.T) {
	cache := NewExpiringConcurrentCache[string, int](time.Minute)

	const numKeys = 5
	const numGoroutinesPerKey = 20

	var wg sync.WaitGroup
	fetchCounts := make([]int64, numKeys)

	// For each key, start multiple goroutines
	for keyIndex := 0; keyIndex < numKeys; keyIndex++ {
		for goroutineIndex := 0; goroutineIndex < numGoroutinesPerKey; goroutineIndex++ {
			wg.Add(1)
			go func(ki, gi int) {
				defer wg.Done()

				key := "key-" + string(rune('a'+ki))
				val, err := cache.GetOrSet(key, func() (int, bool, error) {
					atomic.AddInt64(&fetchCounts[ki], 1)
					time.Sleep(10 * time.Millisecond)
					return ki * 100, true, nil
				})

				assert.NoError(t, err)
				assert.Equal(t, ki*100, val)
			}(keyIndex, goroutineIndex)
		}
	}

	wg.Wait()

	// Each key should have been fetched exactly once
	for i := 0; i < numKeys; i++ {
		assert.Equal(t, int64(1), atomic.LoadInt64(&fetchCounts[i]),
			"Key %d should have been fetched exactly once", i)
	}
}

// TestGetOrSetConcurrentDifferentKeysDoNotSerialize verifies that fetches for different keys can
// be in flight at the same time. Before the fix, fetchFunc ran while holding the global write
// lock, so the second key's fetch could not start until the first finished — the "started"
// signal for the second goroutine would never arrive and the bounded select would time out.
func TestGetOrSetConcurrentDifferentKeysDoNotSerialize(t *testing.T) {
	cache := NewExpiringConcurrentCache[string, string](time.Minute)

	release := make(chan struct{})

	var releaseOnce sync.Once
	releaseBarrier := func() { releaseOnce.Do(func() { close(release) }) }
	// Always release the barrier so blocked fetchFunc goroutines drain, even on the timeout path.
	defer releaseBarrier()

	var started sync.WaitGroup
	started.Add(2)

	var done sync.WaitGroup
	done.Add(2)

	results := make([]string, 2)
	errs := make([]error, 2)

	fetch := func(index int, key, value string) {
		go func() {
			defer done.Done()
			val, err := cache.GetOrSet(key, func() (string, bool, error) {
				started.Done() // signal this fetch has started
				<-release      // block until both fetches are confirmed in flight
				return value, true, nil
			})
			results[index] = val
			errs[index] = err
		}()
	}

	fetch(0, "key-a", "value-a")
	fetch(1, "key-b", "value-b")

	// Both fetches must be in flight simultaneously.
	bothStarted := make(chan struct{})
	go func() {
		started.Wait()
		close(bothStarted)
	}()

	select {
	case <-bothStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("both fetches did not start concurrently; fetchFunc appears serialized under the lock")
	}

	// Release the barrier and let both calls return, bounded so a publish/return-path
	// deadlock fails fast instead of hanging until the go test timeout.
	releaseBarrier()

	doneAll := make(chan struct{})
	go func() {
		done.Wait()
		close(doneAll)
	}()

	select {
	case <-doneAll:
	case <-time.After(10 * time.Second):
		t.Fatal("fetches did not complete after the barrier was released")
	}

	require.NoError(t, errs[0])
	require.NoError(t, errs[1])
	require.Equal(t, "value-a", results[0])
	require.Equal(t, "value-b", results[1])
}

// TestGetOrSetReentrantFetchDoesNotDeadlock verifies that a fetchFunc may itself call GetOrSet for
// a different key. Before the fix, the outer call held the global write lock across fetchFunc, so
// the inner call blocked on its very first RLock — a deadlock. With the fix, no lock is held during
// fetchFunc, so the re-entrant call proceeds.
func TestGetOrSetReentrantFetchDoesNotDeadlock(t *testing.T) {
	cache := NewExpiringConcurrentCache[string, string](time.Minute)

	done := make(chan struct{})

	var result string
	var err error

	go func() {
		defer close(done)
		result, err = cache.GetOrSet("key-outer", func() (string, bool, error) {
			inner, innerErr := cache.GetOrSet("key-inner", func() (string, bool, error) {
				return "inner-value", true, nil
			})
			if innerErr != nil {
				return "", false, innerErr
			}
			return "outer+" + inner, true, nil
		})
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("re-entrant GetOrSet deadlocked")
	}

	require.NoError(t, err)
	require.Equal(t, "outer+inner-value", result)
}

// TestGetOrSetWaitersReceiveLeaderError verifies that when the leader's fetchFunc fails, a waiter on
// the same flight receives the leader's real error verbatim (pointer identity) rather than the
// generic "failed to get value after waiting" message, and never re-fetches. Ordering is made fully
// deterministic via the testHookWaiterAboutToWait seam, with no time.Sleep used for sequencing.
// Must not use t.Parallel().
func TestGetOrSetWaitersReceiveLeaderError(t *testing.T) {
	cache := NewExpiringConcurrentCache[string, string](time.Minute)

	leaderStarted := make(chan struct{})
	parked := make(chan struct{})
	releaseLeader := make(chan struct{})

	var waiterFetchCalls atomic.Int64

	var goroutines sync.WaitGroup
	goroutines.Add(2)

	// Idempotent release so the leader is unblocked on every path.
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseLeader) }) }

	// Once-guarded parked signal fired from the waiter path via the test hook.
	var parkOnce sync.Once
	testHookWaiterAboutToWait = func() { parkOnce.Do(func() { close(parked) }) }

	// Teardown ordered so the hook reset runs strictly after the drain (cleanups run LIFO: register
	// the reset first so it runs last, after both goroutines that can read it have returned).
	t.Cleanup(func() { testHookWaiterAboutToWait = nil }) // registered 1st => runs LAST
	t.Cleanup(func() {                                    // registered 2nd => runs FIRST
		release() // unblock a leader still parked on a failure/timeout path
		doneDrain := make(chan struct{})
		go func() { goroutines.Wait(); close(doneDrain) }()
		select {
		case <-doneDrain:
		case <-time.After(10 * time.Second):
			t.Error("leader/waiter goroutines did not drain")
		}
	})

	var leaderErr, waiterErr error

	// Leader: parks inside fetchFunc so the in-flight entry is pinned, then fails.
	go func() {
		defer goroutines.Done()
		_, leaderErr = cache.GetOrSet("key", func() (string, bool, error) {
			close(leaderStarted)
			<-releaseLeader
			return "", false, errLeaderBoom
		})
	}()

	select {
	case <-leaderStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("leader fetch did not start")
	}

	// Waiter: must join the flight and never run its own fetchFunc.
	go func() {
		defer goroutines.Done()
		_, waiterErr = cache.GetOrSet("key", func() (string, bool, error) {
			waiterFetchCalls.Add(1)
			return "x", false, nil
		})
	}()

	// Positive evidence the waiter captured the in-flight entry and committed to the waiter branch.
	select {
	case <-parked:
	case <-time.After(10 * time.Second):
		t.Fatal("waiter did not reach the wait hook")
	}

	// Release the leader: it publishes wgw.err and tears down the flight.
	release()

	// Drain both goroutines before reading their results.
	doneDrain := make(chan struct{})
	go func() { goroutines.Wait(); close(doneDrain) }()
	select {
	case <-doneDrain:
	case <-time.After(10 * time.Second):
		t.Fatal("leader/waiter goroutines did not complete")
	}

	// Verbatim propagation, not a re-fetch or a wrap.
	require.Same(t, errLeaderBoom, waiterErr)
	// Waiter did not re-fetch.
	require.Equal(t, int64(0), waiterFetchCalls.Load())
	// Corroborating.
	require.ErrorIs(t, waiterErr, errLeaderBoom)
	// Leader returns its own error.
	require.Same(t, errLeaderBoom, leaderErr)
	require.NotContains(t, waiterErr.Error(), "failed to get value after waiting")
}

// TestGetOrSetErrorNotCachedRetriesNextCall verifies that a failed fetch is not cached: a later call
// for the same key runs a fresh fetchFunc and, on success, caches the value for subsequent calls.
func TestGetOrSetErrorNotCachedRetriesNextCall(t *testing.T) {
	cache := NewExpiringConcurrentCache[string, string](time.Minute)

	// First call fails; the error propagates verbatim.
	_, err := cache.GetOrSet("key", func() (string, bool, error) {
		return "", false, errLeaderBoom
	})
	require.Same(t, errLeaderBoom, err)

	// Second call for the same key runs a fresh fetchFunc (error was not cached) and succeeds.
	fetchCalled := false
	val, err := cache.GetOrSet("key", func() (string, bool, error) {
		fetchCalled = true
		return "ok", true, nil
	})
	require.NoError(t, err)
	require.Equal(t, "ok", val)
	require.True(t, fetchCalled)

	// Third call hits the cache; fetchFunc must not run.
	fetchCalled = false
	val, err = cache.GetOrSet("key", func() (string, bool, error) {
		fetchCalled = true
		return "different", true, nil
	})
	require.NoError(t, err)
	require.Equal(t, "ok", val)
	require.False(t, fetchCalled)
}

// TestGetOrSetReentrantSameKeyDeadlocks exercises the documented, unsupported same-key re-entry:
// a fetchFunc that calls GetOrSet for the key it is fetching deadlocks on the in-flight waitgroup.
// Reproducing that in-process would leak a permanently-blocked goroutine, so the deadlock is run in
// a re-exec child of this test binary that the parent bounds and kills — nothing leaks in the
// parent. It requires the standard `go test` execution model (os.Args[0] is the re-runnable test
// binary) and must not use t.Parallel().
func TestGetOrSetReentrantSameKeyDeadlocks(t *testing.T) {
	if os.Getenv("GO_TEST_REENTRANT_SAMEKEY") == "1" {
		cache := NewExpiringConcurrentCache[string, string](time.Minute)
		returned := make(chan struct{})
		go func() {
			_, _ = cache.GetOrSet("k", func() (string, bool, error) {
				// Marker at the ACTUAL re-entry point. os.Stdout is an unbuffered *os.File, so this
				// Fprintln is a direct write syscall — flushed before the inner call blocks below.
				fmt.Fprintln(os.Stdout, "reentrant-call-started")

				v, e := cache.GetOrSet("k", func() (string, bool, error) { // same key -> deadlock by contract
					return "inner", true, nil
				})
				return v, true, e
			})
			close(returned) // reached ONLY if the unsupported call did NOT deadlock (contract broke)
		}()
		select {
		case <-returned:
			os.Exit(42) // distinct code => "same-key re-entry returned instead of deadlocking"
		case <-time.After(30 * time.Second): // armed timer keeps the runtime deadlock detector quiet
			os.Exit(0) // unreached in practice; the parent kills the child first
		}
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run", "^TestGetOrSetReentrantSameKeyDeadlocks$")
	cmd.Env = append(os.Environ(), "GO_TEST_REENTRANT_SAMEKEY=1")
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, cmd.Start())

	waitErr := make(chan error, 1)
	go func() { waitErr <- cmd.Wait() }() // single reaper; buffered so it never blocks

	reaped := false
	t.Cleanup(func() { // runs on EVERY exit path, incl. an early t.Fatal or a missing marker
		_ = cmd.Process.Kill() // idempotent/harmless if already dead
		if !reaped {
			<-waitErr // reap so no orphan/zombie child remains
		}
	})

	// 1. Wait until the child PROVES it reached the re-entrant call.
	markerSeen := make(chan struct{})
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			if strings.Contains(sc.Text(), "reentrant-call-started") {
				close(markerSeen)
				return
			}
		}
	}()
	select {
	case <-markerSeen:
	case err := <-waitErr:
		reaped = true
		t.Fatalf("child exited (%v) before reaching the re-entrant call", err)
	case <-time.After(3 * time.Second):
		t.Fatal("child never reached the re-entrant GetOrSet call")
	}

	// 2. Decision rule: past the marker, the child must NOT exit within the bound — proving the
	//    documented same-key deadlock.
	select {
	case err := <-waitErr:
		reaped = true
		t.Fatalf("child exited (%v) instead of deadlocking; the same-key contract may have changed", err)
	case <-time.After(1 * time.Second):
		// Still blocked after the bound => deadlock confirmed.
	}

	// 3. Kill and reap.
	require.NoError(t, cmd.Process.Kill())
	<-waitErr
	reaped = true
}
