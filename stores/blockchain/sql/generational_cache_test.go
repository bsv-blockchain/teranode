package sql

import (
	"encoding/binary"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/stretchr/testify/require"
)

func TestGenerationalCache_PreventStaleWrites(t *testing.T) {
	gc := NewGenerationalCache(0)
	defer gc.Stop()

	key := chainhash.Hash{1, 2, 3}

	// Start an operation (captures generation 0)
	op := gc.NewOp(key)

	// Simulate cache invalidation while operation is in progress
	gc.DeleteAll()

	// Attempt to cache the now-stale result
	cached := op.Set("stale data", 1*time.Hour)

	// Should NOT cache because generation changed
	require.False(t, cached, "stale result should not be cached after invalidation")

	// Verify nothing was cached
	newOp := gc.NewOp(key)
	item := newOp.Get()
	require.Nil(t, item, "cache should be empty after rejecting stale write")
}

func TestGenerationalCache_AllowFreshWrites(t *testing.T) {
	gc := NewGenerationalCache(0)
	defer gc.Stop()

	key := chainhash.Hash{1, 2, 3}

	// Start an operation and immediately cache result (no invalidation)
	op := gc.NewOp(key)
	cached := op.Set("fresh data", 1*time.Hour)

	// Should cache successfully
	require.True(t, cached, "fresh result should be cached")

	// Verify data was cached
	newOp := gc.NewOp(key)
	item := newOp.Get()
	require.NotNil(t, item, "cache should contain the value")
	require.Equal(t, "fresh data", item.Value())
}

func TestGenerationalCache_MultipleInvalidations(t *testing.T) {
	gc := NewGenerationalCache(0)
	defer gc.Stop()

	key := chainhash.Hash{1, 2, 3}

	// Start multiple operations
	op1 := gc.NewOp(key)
	gc.DeleteAll() // Invalidate after op1
	op2 := gc.NewOp(key)
	gc.DeleteAll() // Invalidate after op2
	op3 := gc.NewOp(key)

	// Only op3 should be able to cache
	require.False(t, op1.Set("data1", 1*time.Hour), "op1 should be stale")
	require.False(t, op2.Set("data2", 1*time.Hour), "op2 should be stale")
	require.True(t, op3.Set("data3", 1*time.Hour), "op3 should be fresh")

	// Verify only the latest data was cached
	newOp := gc.NewOp(key)
	item := newOp.Get()
	require.NotNil(t, item)
	require.Equal(t, "data3", item.Value())
}

func TestGenerationalCache_ConcurrentOperations(t *testing.T) {
	gc := NewGenerationalCache(0)
	defer gc.Stop()

	key := chainhash.Hash{1, 2, 3}
	const numGoroutines = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Simulate concurrent operations and invalidations
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()

			op := gc.NewOp(key)
			time.Sleep(1 * time.Millisecond) // Simulate work

			// Randomly invalidate cache
			if id%10 == 0 {
				gc.DeleteAll()
			}

			// Try to cache result
			op.Set(id, 1*time.Hour)
		}(i)
	}

	wg.Wait()

	// Cache should have at most one value (the last successful write)
	// This test mainly ensures no panics occur with concurrent access
	newOp := gc.NewOp(key)
	item := newOp.Get()
	if item != nil {
		t.Logf("Final cached value: %v", item.Value())
	}
}

func TestGenerationalCache_StopMultipleTimes(t *testing.T) {
	gc := NewGenerationalCache(0)

	// Should not panic when called multiple times
	require.NotPanics(t, func() {
		gc.Stop()
		gc.Stop()
		gc.Stop()
	}, "Stop should be safe to call multiple times")
}

func TestGenerationalCache_GetBeforeSet(t *testing.T) {
	gc := NewGenerationalCache(0)
	defer gc.Stop()

	key := chainhash.Hash{1, 2, 3}

	// Get on empty cache should return nil
	op := gc.NewOp(key)
	item := op.Get()
	require.Nil(t, item, "cache miss should return nil")
}

func TestGenerationalCache_TTLExpiration(t *testing.T) {
	gc := NewGenerationalCache(0)
	defer gc.Stop()

	key := chainhash.Hash{1, 2, 3}

	// Cache with very short TTL
	op := gc.NewOp(key)
	cached := op.Set("expiring data", 50*time.Millisecond)
	require.True(t, cached)

	// Verify it's there immediately
	newOp := gc.NewOp(key)
	item := newOp.Get()
	require.NotNil(t, item)

	// Wait for expiration
	time.Sleep(100 * time.Millisecond)

	// Should be gone
	expiredOp := gc.NewOp(key)
	item = expiredOp.Get()
	require.Nil(t, item, "item should have expired")
}

func TestGenerationalCache_DifferentKeys(t *testing.T) {
	gc := NewGenerationalCache(0)
	defer gc.Stop()

	key1 := chainhash.Hash{1}
	key2 := chainhash.Hash{2}

	// Cache two different keys
	op1 := gc.NewOp(key1)
	op1.Set("data1", 1*time.Hour)

	op2 := gc.NewOp(key2)
	op2.Set("data2", 1*time.Hour)

	// Invalidate cache
	gc.DeleteAll()

	// Both should be cleared
	newOp1 := gc.NewOp(key1)
	require.Nil(t, newOp1.Get(), "key1 should be cleared")

	newOp2 := gc.NewOp(key2)
	require.Nil(t, newOp2.Get(), "key2 should be cleared")
}

func TestGenerationalCache_SetReturnValue(t *testing.T) {
	gc := NewGenerationalCache(0)
	defer gc.Stop()

	key := chainhash.Hash{1, 2, 3}

	t.Run("returns true when cached", func(t *testing.T) {
		op := gc.NewOp(key)
		result := op.Set("test", 1*time.Hour)
		require.True(t, result, "Set should return true when value is cached")
	})

	t.Run("returns false when generation changed", func(t *testing.T) {
		op := gc.NewOp(key)
		gc.DeleteAll() // Invalidate
		result := op.Set("test", 1*time.Hour)
		require.False(t, result, "Set should return false when generation changed")
	})
}

func TestGenerationalCache_Capacity(t *testing.T) {
	gc := NewGenerationalCache(2)
	defer gc.Stop()
	for i := byte(1); i <= 3; i++ {
		require.True(t, gc.NewOp(chainhash.Hash{i}).Set(i, time.Hour))
	}
	require.Nil(t, gc.NewOp(chainhash.Hash{1}).Get())
	require.NotNil(t, gc.NewOp(chainhash.Hash{2}).Get())
	require.NotNil(t, gc.NewOp(chainhash.Hash{3}).Get())
}

func TestGenerationalCache_HeaderByteBudget(t *testing.T) {
	gc := NewGenerationalCache(100000)
	defer gc.Stop()
	value := [2]interface{}{make([]*model.BlockHeader, 8192), make([]*model.BlockHeaderMeta, 8192)}
	hotKey := chainhash.Hash{100}
	require.True(t, gc.NewOp(hotKey).Set(true, time.Hour))
	for i := byte(1); i <= 16; i++ {
		require.True(t, gc.NewOp(chainhash.Hash{i}).Set(value, time.Hour))
	}
	require.False(t, gc.NewOp(chainhash.Hash{17}).Set(value, time.Hour))
	require.Nil(t, gc.NewOp(chainhash.Hash{17}).Get())
	require.NotNil(t, gc.NewOp(chainhash.Hash{1}).Get(), "budget exhaustion must not evict existing responses")
	require.NotNil(t, gc.NewOp(hotKey).Get(), "header queries must not flush unrelated hot-path entries")
	require.True(t, gc.NewOp(chainhash.Hash{101}).Set(true, time.Hour), "uncharged values remain cacheable")
	oversized := [2]interface{}{make([]*model.BlockHeader, 140000), make([]*model.BlockHeaderMeta, 140000)}
	require.False(t, gc.NewOp(chainhash.Hash{18}).Set(oversized, time.Hour))
	require.Nil(t, gc.NewOp(chainhash.Hash{18}).Get())
	gc.DeleteAll()
	require.True(t, gc.NewOp(chainhash.Hash{19}).Set(value, time.Hour))
}

func TestGenerationalCache_HeaderBudgetReclaimed(t *testing.T) {
	value := [2]interface{}{make([]*model.BlockHeader, 8192), make([]*model.BlockHeaderMeta, 8192)}

	t.Run("expiry", func(t *testing.T) {
		gc := NewGenerationalCache(0)
		defer gc.Stop()
		for i := byte(1); i <= 16; i++ {
			require.True(t, gc.NewOp(chainhash.Hash{i}).Set(value, 50*time.Millisecond))
		}
		require.Eventually(t, func() bool { return gc.ttlCache.Len() == 0 }, time.Second, time.Millisecond)
		require.Eventually(t, func() bool {
			return gc.NewOp(chainhash.Hash{17}).Set(value, time.Hour)
		}, time.Second, time.Millisecond, "expired entries must return their header budget without chain invalidation")
	})

	t.Run("capacity eviction", func(t *testing.T) {
		gc := NewGenerationalCache(2)
		defer gc.Stop()
		for i := byte(1); i <= 32; i++ {
			require.Eventually(t, func() bool {
				return gc.NewOp(chainhash.Hash{i}).Set(value, time.Hour)
			}, time.Second, time.Millisecond, "eviction must return the departed entry's charge")
		}
		require.Equal(t, 2, gc.ttlCache.Len())
		require.NotNil(t, gc.NewOp(chainhash.Hash{32}).Get())
	})

	t.Run("replacement", func(t *testing.T) {
		gc := NewGenerationalCache(0)
		defer gc.Stop()
		key := chainhash.Hash{1}
		for i := 0; i < 32; i++ {
			require.True(t, gc.NewOp(key).Set(value, time.Hour), "replacing one entry must not accumulate charges")
		}
	})

	t.Run("replacement with uncharged value", func(t *testing.T) {
		gc := NewGenerationalCache(0)
		defer gc.Stop()
		fullBudget := [2]interface{}{make([]*model.BlockHeader, 131072), make([]*model.BlockHeaderMeta, 131072)}
		key := chainhash.Hash{1}
		require.True(t, gc.NewOp(key).Set(fullBudget, time.Hour))
		require.True(t, gc.NewOp(key).Set(true, time.Hour))
		require.True(t, gc.NewOp(chainhash.Hash{2}).Set(fullBudget, time.Hour))
		require.Equal(t, true, gc.NewOp(key).Get().Value())
	})
}

func TestGenerationalCache_DeletedEntriesReleaseBudget(t *testing.T) {
	gc := NewGenerationalCache(0)
	defer gc.Stop()
	value := [2]interface{}{make([]*model.BlockHeader, 131072), make([]*model.BlockHeaderMeta, 131072)}
	key := chainhash.Hash{1}
	require.True(t, gc.NewOp(key).Set(value, time.Hour))
	gc.ttlCache.Delete(key)
	require.True(t, gc.NewOp(chainhash.Hash{2}).Set(value, time.Hour))
	require.False(t, gc.NewOp(key).Set(value, time.Hour), "live entries still consume the budget")
}

func TestGenerationalCache_BookkeepingBoundedAfterEviction(t *testing.T) {
	gc := NewGenerationalCache(2)
	defer gc.Stop()
	for i := uint32(0); i < 4096; i++ {
		var key chainhash.Hash
		binary.LittleEndian.PutUint32(key[:], i)
		require.True(t, gc.NewOp(key).Set([]uint32{1}, time.Hour))
	}
	require.LessOrEqual(t, len(gc.headerCharges), 1024, "small evicted values must not accumulate unbounded accounting")
}

func TestGenerationalCache_DeleteAllDoesNotSpawnPerEntryGoroutines(t *testing.T) {
	gc := NewGenerationalCache(10000)
	defer gc.Stop()
	for i := uint32(0); i < 10000; i++ {
		var key chainhash.Hash
		binary.LittleEndian.PutUint32(key[:], i)
		require.True(t, gc.NewOp(key).Set(true, time.Hour))
	}
	baseline := runtime.NumGoroutine()
	stop := make(chan struct{})
	observed := make(chan int, 1)
	go func() {
		peak := baseline
		for {
			peak = max(peak, runtime.NumGoroutine())
			select {
			case <-stop:
				observed <- peak
				return
			default:
				runtime.Gosched()
			}
		}
	}()
	gc.DeleteAll()
	close(stop)
	require.Less(t, <-observed-baseline, 1000, "invalidation must not start one callback goroutine per cached entry")
}

func TestGenerationalCache_ConcurrentHeaderAccounting(t *testing.T) {
	gc := NewGenerationalCache(4)
	defer gc.Stop()
	var wg sync.WaitGroup
	for worker := byte(0); worker < 8; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := byte(0); i < 64; i++ {
				op := gc.NewOp(chainhash.Hash{worker, i % 8})
				op.Set(make([]uint32, int(i)+1), time.Millisecond)
				if i%11 == 0 {
					gc.DeleteAll()
				}
			}
		}()
	}
	wg.Wait()
	gc.DeleteAll()
	key := chainhash.Hash{100}
	require.True(t, gc.NewOp(key).Set(make([]uint32, 10000), time.Hour))
	gc.writeMu.Lock()
	headerBytes := gc.headerBytes
	gc.writeMu.Unlock()
	require.Equal(t, uint64(40000), headerBytes)
}
