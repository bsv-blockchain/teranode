package p2p

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSeenHashCache_Check(t *testing.T) {
	now := time.Now()

	t.Run("first announcement is fresh", func(t *testing.T) {
		var c seenHashCache

		duplicate, repeats := c.Check("hash-a", "peer-1", now)
		require.False(t, duplicate)
		require.Zero(t, repeats)
		require.Equal(t, 1, c.Len())
	})

	t.Run("second peer announcing the same hash is a duplicate but not a repeat", func(t *testing.T) {
		var c seenHashCache

		c.Check("hash-a", "peer-1", now)

		duplicate, repeats := c.Check("hash-a", "peer-2", now)
		require.True(t, duplicate)
		require.Zero(t, repeats, "a different peer's first announcement must not count as a repeat")
	})

	t.Run("same peer re-announcing counts repeats", func(t *testing.T) {
		var c seenHashCache

		c.Check("hash-a", "peer-1", now)

		for want := 1; want <= 4; want++ {
			duplicate, repeats := c.Check("hash-a", "peer-1", now)
			require.True(t, duplicate)
			require.Equal(t, want, repeats)
		}
	})

	t.Run("expired window is treated as fresh", func(t *testing.T) {
		var c seenHashCache

		c.Check("hash-a", "peer-1", now)

		later := now.Add(defaultSeenHashCacheTTL)

		duplicate, repeats := c.Check("hash-a", "peer-1", later)
		require.False(t, duplicate)
		require.Zero(t, repeats, "repeat counts must reset with the window")

		duplicate, repeats = c.Check("hash-a", "peer-1", later)
		require.True(t, duplicate)
		require.Equal(t, 1, repeats)
	})

	t.Run("cap evicts the oldest entry", func(t *testing.T) {
		c := seenHashCache{maxSize: 3}

		c.Check("hash-1", "peer-1", now)
		c.Check("hash-2", "peer-1", now)
		c.Check("hash-3", "peer-1", now)
		c.Check("hash-4", "peer-1", now)

		require.Equal(t, 3, c.Len())

		duplicate, _ := c.Check("hash-1", "peer-1", now)
		require.False(t, duplicate, "oldest entry must have been evicted")

		duplicate, _ = c.Check("hash-4", "peer-1", now)
		require.True(t, duplicate, "newest entry must survive")
	})

	t.Run("announcer counts per hash are bounded", func(t *testing.T) {
		var c seenHashCache

		c.Check("hash-a", "peer-0", now)
		for i := 1; i < seenHashMaxAnnouncersPerHash+10; i++ {
			duplicate, repeats := c.Check("hash-a", fmt.Sprintf("peer-%d", i), now)
			require.True(t, duplicate, "every announcement after the first is a duplicate")
			require.Zero(t, repeats, "untracked announcers must never be reported as repeaters")
		}

		// A tracked announcer keeps counting even once the bound is hit.
		duplicate, repeats := c.Check("hash-a", "peer-0", now)
		require.True(t, duplicate)
		require.Equal(t, 1, repeats)
	})
}

func TestSeenHashCache_Forget(t *testing.T) {
	var c seenHashCache

	now := time.Now()

	c.Check("hash-a", "peer-1", now)
	c.Forget("hash-a")

	duplicate, repeats := c.Check("hash-a", "peer-2", now)
	require.False(t, duplicate, "a forgotten hash must be treated as fresh")
	require.Zero(t, repeats)

	require.NotPanics(t, func() { c.Forget("never-stored") })
}

func TestSeenHashCache_DeleteExpired(t *testing.T) {
	var c seenHashCache

	now := time.Now()

	c.Check("hash-old", "peer-1", now)
	c.Check("hash-new", "peer-1", now.Add(time.Minute))

	removed := c.DeleteExpired(now.Add(defaultSeenHashCacheTTL))
	require.Equal(t, 1, removed)
	require.Equal(t, 1, c.Len())

	duplicate, _ := c.Check("hash-new", "peer-2", now.Add(time.Minute))
	require.True(t, duplicate, "unexpired entry must survive the sweep")
}
