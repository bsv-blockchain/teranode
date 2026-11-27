package p2p

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBoundedTTLCache_EvictsOldestAtCap(t *testing.T) {
	var c boundedTTLCache[int]
	c.setMaxSize(3)
	exp := time.Now().Add(time.Minute)

	c.Set("a", 1, exp)
	c.Set("b", 2, exp)
	c.Set("c", 3, exp)
	c.Set("d", 4, exp)

	require.Equal(t, 3, c.Len())
	_, ok := c.Get("a", time.Now())
	require.False(t, ok, "oldest insert is evicted first")
	for _, k := range []string{"b", "c", "d"} {
		_, ok := c.Get(k, time.Now())
		require.True(t, ok, "%s must survive", k)
	}
}

func TestBoundedTTLCache_RefreshMovesKeyToNewest(t *testing.T) {
	var c boundedTTLCache[int]
	c.setMaxSize(3)
	exp := time.Now().Add(time.Minute)

	c.Set("a", 1, exp)
	c.Set("b", 2, exp)
	c.Set("c", 3, exp)
	c.Set("a", 10, exp)
	c.Set("d", 4, exp)

	v, ok := c.Get("a", time.Now())
	require.True(t, ok, "a refreshed key is the newest, not the eviction victim")
	require.Equal(t, 10, v)
	_, ok = c.Get("b", time.Now())
	require.False(t, ok, "the oldest un-refreshed key is evicted")
}

func TestBoundedTTLCache_ExpiryOnGetAndSweep(t *testing.T) {
	var c boundedTTLCache[int]
	now := time.Now()

	c.Set("expired", 1, now.Add(-time.Second))
	c.Set("fresh", 2, now.Add(time.Minute))
	c.Set("swept", 3, now.Add(-time.Second))

	_, ok := c.Get("expired", now)
	require.False(t, ok, "an expired entry is a miss")
	require.Equal(t, 2, c.Len(), "the expired entry is dropped on lookup")

	require.Equal(t, 1, c.DeleteExpired(now))
	require.Equal(t, 1, c.Len())
	_, ok = c.Get("fresh", now)
	require.True(t, ok)
}

func TestBoundedTTLCache_ZeroValueIsBounded(t *testing.T) {
	var c boundedTTLCache[struct{}]
	exp := time.Now().Add(time.Minute)

	for i := 0; i < defaultPeerMapMaxSize+5; i++ {
		c.Set(fmt.Sprintf("k%d", i), struct{}{}, exp)
	}

	require.Equal(t, defaultPeerMapMaxSize, c.Len(), "an unconfigured cache falls back to the default cap")
}

func TestBoundedTTLCache_LoweredCapDrainsOnInsert(t *testing.T) {
	var c boundedTTLCache[int]
	exp := time.Now().Add(time.Minute)
	for i := 0; i < 10; i++ {
		c.Set(fmt.Sprintf("k%d", i), i, exp)
	}

	c.setMaxSize(4)
	c.Set("new", 1, exp)

	require.Equal(t, 4, c.Len(), "a new key drains an over-cap cache down to the cap")
}

// Cap evictions are counted for the periodic sweep's capacity diagnostic and
// the counter resets on read.
func TestBoundedTTLCache_CountsEvictions(t *testing.T) {
	var c boundedTTLCache[int]
	c.setMaxSize(2)
	expires := time.Now().Add(time.Minute)

	c.Set("a", 1, expires)
	c.Set("b", 2, expires)
	require.Zero(t, c.EvictionsSinceLastRead())

	c.Set("c", 3, expires)
	c.Set("d", 4, expires)
	require.Equal(t, 2, c.EvictionsSinceLastRead())
	require.Zero(t, c.EvictionsSinceLastRead(), "the counter resets on read")
}
