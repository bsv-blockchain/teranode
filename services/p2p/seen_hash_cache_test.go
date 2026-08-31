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

	t.Run("publish budget re-opens each publish window, accounting survives", func(t *testing.T) {
		var c seenHashCache

		for i := 0; i < seenHashMaxPublishersPerHash; i++ {
			c.Check("hash-a", fmt.Sprintf("peer-%d", i), now)
		}

		publish, _ := c.Check("hash-a", "peer-captured", now)
		require.False(t, publish, "budget spent within the window")

		later := now.Add(seenHashPublishWindow)

		publish, _ = c.Check("hash-a", "peer-honest", later)
		require.True(t, publish, "a fresh distinct announcer must publish once the window rolls over")

		publish, repeats := c.Check("hash-a", "peer-0", later)
		require.False(t, publish, "a repeat must not publish while a rollover grant is stuck")
		require.Equal(t, 1, repeats, "repeat accounting must survive the publish-window rollover")
	})

	t.Run("configured publisher budget overrides the default", func(t *testing.T) {
		var c seenHashCache
		c.setLimits(0, 1, 0) // budget 1, size and TTL on defaults

		publish, _ := c.Check("hash-a", "peer-1", now)
		require.True(t, publish)

		publish, _ = c.Check("hash-a", "peer-2", now)
		require.False(t, publish, "a configured budget of 1 must stop the second distinct announcer")
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

	t.Run("persistent announcer cannot hold the rollover grant across consecutive windows", func(t *testing.T) {
		var c seenHashCache

		// Window 0: a fleet fills the announcer tracking and takes every
		// distinct grant, so from window 1 on the rollover retry grant is the
		// only reachable one.
		for i := 0; i < seenHashMaxAnnouncersPerHash; i++ {
			c.Check("hash-a", fmt.Sprintf("attacker-%d", i), now)
		}

		// Window 1: attacker-0 (a window-0 grantee) hammers the boundary. It
		// must be refused the retry grant, leaving it open for the honest
		// announcer arriving later in the window.
		w1 := now.Add(seenHashPublishWindow)
		for i := 0; i < 3; i++ {
			publish, _ := c.Check("hash-a", "attacker-0", w1)
			require.False(t, publish, "last window's grantee must not win the rollover grant")
		}

		publish, _ := c.Check("hash-a", "peer-honest", w1)
		require.True(t, publish, "the honest late announcer must reach Kafka within a window")

		// Window 2: the honest announcer held window 1's grant, so now IT is
		// refused and attacker-0 (which held nothing in window 1) is eligible
		// again - the grant rotates rather than being owned.
		w2 := w1.Add(seenHashPublishWindow)
		publish, _ = c.Check("hash-a", "peer-honest", w2)
		require.False(t, publish, "last window's grantee is refused, whoever it is")
		publish, _ = c.Check("hash-a", "attacker-0", w2)
		require.True(t, publish, "a peer that held nothing last window is eligible again")
	})

	t.Run("untracked announcer cannot drain the budget across windows", func(t *testing.T) {
		var c seenHashCache

		// Fill the announcer tracking and spend the budget.
		for i := 0; i < seenHashMaxAnnouncersPerHash; i++ {
			c.Check("hash-a", fmt.Sprintf("peer-%d", i), now)
		}

		// Roll the publish window: the budget re-opens.
		later := now.Add(seenHashPublishWindow)

		// An announcer the bound excluded has prev == 0 on every announcement;
		// it must not read as a fresh distinct announcer each time, or one
		// identity would take the whole re-opened budget every window. It may
		// take at most the single published == 0 retry grant.
		publish, repeats := c.Check("hash-a", "peer-untracked", later)
		require.True(t, publish, "the rollover retry grant is reachable once")
		require.Zero(t, repeats)

		for i := 0; i < 3; i++ {
			publish, repeats = c.Check("hash-a", "peer-untracked", later)
			require.False(t, publish, "an untracked announcer must not consume the distinct-announcer budget")
			require.Zero(t, repeats, "untracked announcers stay uncounted")
		}

		// The remaining re-opened budget must still be available to a peer the
		// tracking KNOWS is new... which cannot exist while the map is full, so
		// a tracked repeat must also not take it.
		publish, _ = c.Check("hash-a", "peer-0", later)
		require.False(t, publish, "a tracked repeat must not take the re-opened budget")
	})
}

func TestSeenHashCache_PublishFailed(t *testing.T) {
	now := time.Now()

	t.Run("returned grant allows a retry by the same peer", func(t *testing.T) {
		var c seenHashCache

		publish, _ := c.Check("hash-a", "peer-1", now)
		require.True(t, publish)

		c.PublishFailed("hash-a", "peer-1")

		publish, repeats := c.Check("hash-a", "peer-1", now)
		require.True(t, publish, "with no grant stuck, even a repeat may retry the publish")
		require.Equal(t, 1, repeats, "the failed publish must not reset spam accounting")
	})

	t.Run("returned grant does not make the peer last window's grantee", func(t *testing.T) {
		var c seenHashCache

		// Sparse topology: one announcer, and its only publish is dropped.
		publish, _ := c.Check("hash-a", "peer-1", now)
		require.True(t, publish)
		c.PublishFailed("hash-a", "peer-1")

		// Its re-announcement in the very next window must be granted: a grant
		// that never reached Kafka must not count as holding last window.
		w1 := now.Add(seenHashPublishWindow)
		publish, repeats := c.Check("hash-a", "peer-1", w1)
		require.True(t, publish, "a peer whose only grant was returned may retry in the next window")
		require.Equal(t, 1, repeats, "the repeat accounting is unaffected")
	})

	t.Run("returned grant does not extend the budget for repeats", func(t *testing.T) {
		var c seenHashCache

		c.Check("hash-a", "peer-1", now)    // grant 1 sticks
		c.Check("hash-a", "peer-2", now)    // grant 2 sticks
		c.PublishFailed("hash-a", "peer-1") // one grant returned, published back to 1

		publish, _ := c.Check("hash-a", "peer-1", now)
		require.False(t, publish, "a repeat must not publish while another grant is stuck")

		publish, _ = c.Check("hash-a", "peer-3", now)
		require.True(t, publish, "a new announcer may take the returned budget")
	})

	t.Run("no-ops on unknown hash and at zero", func(t *testing.T) {
		var c seenHashCache

		require.NotPanics(t, func() { c.PublishFailed("never-stored", "peer-1") })

		c.Check("hash-a", "peer-1", now)
		c.PublishFailed("hash-a", "peer-1")
		require.NotPanics(t, func() { c.PublishFailed("hash-a", "peer-1") })

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
