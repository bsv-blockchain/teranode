package p2p

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSeenHashCache_Check(t *testing.T) {
	now := time.Now()

	t.Run("first announcement publishes", func(t *testing.T) {
		var c seenHashCache

		publish, repeats := c.Check("hash-a", "peer-1", now)
		require.True(t, publish)
		require.Zero(t, repeats)
		require.Equal(t, 1, c.Len())
	})

	t.Run("distinct announcers publish up to the budget", func(t *testing.T) {
		var c seenHashCache

		for i := 0; i < seenHashMaxPublishersPerHash; i++ {
			publish, repeats := c.Check("hash-a", fmt.Sprintf("peer-%d", i), now)
			require.True(t, publish, "announcer %d is within the publisher budget", i)
			require.Zero(t, repeats)
		}

		publish, repeats := c.Check("hash-a", "peer-late", now)
		require.False(t, publish, "the budget must be spent")
		require.Zero(t, repeats, "a new announcer is never a repeater")
	})

	t.Run("same peer re-announcing is suppressed and counted", func(t *testing.T) {
		var c seenHashCache

		c.Check("hash-a", "peer-1", now)

		for want := 1; want <= 4; want++ {
			publish, repeats := c.Check("hash-a", "peer-1", now)
			require.False(t, publish, "a peer's own repeats must never re-publish")
			require.Equal(t, want, repeats)
		}
	})

	t.Run("expired window is treated as fresh", func(t *testing.T) {
		var c seenHashCache

		c.Check("hash-a", "peer-1", now)

		later := now.Add(defaultSeenHashCacheTTL)

		publish, repeats := c.Check("hash-a", "peer-1", later)
		require.True(t, publish)
		require.Zero(t, repeats, "repeat counts must reset with the window")

		publish, repeats = c.Check("hash-a", "peer-1", later)
		require.False(t, publish)
		require.Equal(t, 1, repeats)
	})

	t.Run("cap evicts the oldest entry", func(t *testing.T) {
		c := seenHashCache{maxSize: 3}

		c.Check("hash-1", "peer-1", now)
		c.Check("hash-2", "peer-1", now)
		c.Check("hash-3", "peer-1", now)
		c.Check("hash-4", "peer-1", now)

		require.Equal(t, 3, c.Len())

		publish, _ := c.Check("hash-1", "peer-1", now)
		require.True(t, publish, "oldest entry must have been evicted")

		publish, repeats := c.Check("hash-4", "peer-1", now)
		require.False(t, publish, "newest entry must survive")
		require.Equal(t, 1, repeats)
	})

	t.Run("announcer counts per hash are bounded", func(t *testing.T) {
		var c seenHashCache

		for i := 0; i < seenHashMaxAnnouncersPerHash+10; i++ {
			_, repeats := c.Check("hash-a", fmt.Sprintf("peer-%d", i), now)
			require.Zero(t, repeats, "untracked announcers must never be reported as repeaters")
		}

		// A tracked announcer keeps counting even once the bound is hit.
		publish, repeats := c.Check("hash-a", "peer-0", now)
		require.False(t, publish)
		require.Equal(t, 1, repeats)
	})
}

func TestSeenHashCache_PublishFailed(t *testing.T) {
	now := time.Now()

	t.Run("returned grant allows a retry by the same peer", func(t *testing.T) {
		var c seenHashCache

		publish, _ := c.Check("hash-a", "peer-1", now)
		require.True(t, publish)

		c.PublishFailed("hash-a")

		publish, repeats := c.Check("hash-a", "peer-1", now)
		require.True(t, publish, "with no grant stuck, even a repeat may retry the publish")
		require.Equal(t, 1, repeats, "the failed publish must not reset spam accounting")
	})

	t.Run("returned grant does not extend the budget for repeats", func(t *testing.T) {
		var c seenHashCache

		c.Check("hash-a", "peer-1", now) // grant 1 sticks
		c.Check("hash-a", "peer-2", now) // grant 2 sticks
		c.PublishFailed("hash-a")        // one grant returned, published back to 1

		publish, _ := c.Check("hash-a", "peer-1", now)
		require.False(t, publish, "a repeat must not publish while another grant is stuck")

		publish, _ = c.Check("hash-a", "peer-3", now)
		require.True(t, publish, "a new announcer may take the returned budget")
	})

	t.Run("no-ops on unknown hash and at zero", func(t *testing.T) {
		var c seenHashCache

		require.NotPanics(t, func() { c.PublishFailed("never-stored") })

		c.Check("hash-a", "peer-1", now)
		c.PublishFailed("hash-a")
		require.NotPanics(t, func() { c.PublishFailed("hash-a") })

		publish, _ := c.Check("hash-a", "peer-1", now)
		require.True(t, publish)
	})
}

// The cache is called from defaultGossipHandlerConcurrency workers per topic;
// hammer it from several goroutines and hold the publisher-budget invariant.
func TestSeenHashCache_ConcurrentChecks(t *testing.T) {
	var (
		c       seenHashCache
		granted atomic.Int64
		wg      sync.WaitGroup
	)

	now := time.Now()

	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				if publish, _ := c.Check("hash-contested", fmt.Sprintf("peer-%d-%d", g, i), now); publish {
					granted.Add(1)
				}
			}
		}(g)
	}
	wg.Wait()

	require.Equal(t, int64(seenHashMaxPublishersPerHash), granted.Load(),
		"exactly the publisher budget may be granted across concurrent distinct announcers")
}

func TestSeenHashCache_DeleteExpired(t *testing.T) {
	var c seenHashCache

	now := time.Now()

	c.Check("hash-old", "peer-1", now)
	c.Check("hash-new", "peer-1", now.Add(time.Minute))

	removed := c.DeleteExpired(now.Add(defaultSeenHashCacheTTL))
	require.Equal(t, 1, removed)
	require.Equal(t, 1, c.Len())

	publish, _ := c.Check("hash-new", "peer-2", now.Add(time.Minute))
	require.True(t, publish, "unexpired entry must survive the sweep with its budget intact")
}

func TestSeenHashCache_Clear(t *testing.T) {
	c := seenHashCache{maxSize: 5}

	now := time.Now()

	c.Check("hash-a", "peer-1", now)
	c.Check("hash-b", "peer-1", now)
	c.Clear()

	require.Zero(t, c.Len())

	publish, repeats := c.Check("hash-a", "peer-1", now)
	require.True(t, publish, "cleared entries must be treated as fresh")
	require.Zero(t, repeats)
	require.Equal(t, 5, c.maxSize, "the configured cap must survive Clear")
}
