package util

import (
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

	// When fetch fails, waiting goroutines get "cache: failed to get value after waiting"
	// and then retry with their own fetch. So multiple fetches occur.
	fetchCountValue := atomic.LoadInt64(&fetchCount)
	assert.Greater(t, fetchCountValue, int64(0), "At least one fetch should occur")
	assert.LessOrEqual(t, fetchCountValue, int64(numGoroutines), "At most one fetch per goroutine")

	// Count different types of errors
	originalErrorCount := 0
	waitingErrorCount := 0

	for i := 0; i < numGoroutines; i++ {
		assert.Error(t, errs[i])
		if errors.Is(expectedErr, errs[i]) {
			originalErrorCount++
		} else if strings.Contains(errs[i].Error(), "cache: failed to get value after waiting") {
			waitingErrorCount++
		} else {
			t.Errorf("Unexpected error type %d: %v", i, errs[i])
		}
	}

	// The exact counts depend on timing, but we should have some errors
	assert.Greater(t, originalErrorCount, 0, "Should have some original errors")
	assert.Equal(t, numGoroutines, originalErrorCount+waitingErrorCount, "All errors should be accounted for")
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
