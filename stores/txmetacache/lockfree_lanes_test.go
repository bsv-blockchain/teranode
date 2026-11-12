package txmetacache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestLaneCache_SaturationIsCounted holds every lane, as more concurrent
// writers than lanes would, and checks that a writer waiting for one is
// counted (so a canary shows lane saturation before it shows up as CPU) and
// proceeds once a lane frees up.
func TestLaneCache_SaturationIsCounted(t *testing.T) {
	c, err := NewLaneCache(1024 * 1024)
	require.NoError(t, err)

	for i := range c.lanes {
		require.True(t, c.lanes[i].owner.CompareAndSwap(0, 1))
	}

	k := mixedKey(1)
	done := make(chan error, 1)

	go func() { done <- c.Set(k[:], []byte("v")) }()

	require.Eventually(t, func() bool {
		var s Stats
		c.UpdateStats(&s)

		return s.LaneWaits > 0
	}, 5*time.Second, time.Millisecond, "a writer finding no free lane must be counted")

	select {
	case <-done:
		t.Fatal("Set completed while every lane was held")
	default:
	}

	c.lanes[len(c.lanes)/2].owner.Store(0)

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Set did not proceed after a lane was freed")
	}

	var dst []byte
	require.True(t, c.Get(&dst, k[:]))
	require.Equal(t, "v", string(dst))
}

// TestLaneCache_LaneCountAtProductionSizes pins the lane count at the cache
// sizes subtree nodes run with: more than half of lnMaxLanes, so writers only
// outnumber lanes in a burst (which LaneWaits then counts).
func TestLaneCache_LaneCountAtProductionSizes(t *testing.T) {
	for _, mb := range []int{256, 1024, 4096} {
		c, err := NewLaneCache(mb * 1024 * 1024)
		require.NoError(t, err)
		require.Greater(t, len(c.lanes), lnMaxLanes/2, "%d MB cache", mb)
		require.LessOrEqual(t, len(c.lanes), lnMaxLanes, "%d MB cache", mb)
	}
}

// TestLaneCache_ResetRefreshesSnapshots keeps the lfLane.snap invariant after a
// Reset: every pre-reset record is lapped, so the snapshot must show it, or
// publish would keep treating dead slots as live until the next refresh.
func TestLaneCache_ResetRefreshesSnapshots(t *testing.T) {
	c, err := NewLaneCache(1024 * 1024)
	require.NoError(t, err)

	for i := uint64(0); i < 100; i++ {
		k := mixedKey(i)
		require.NoError(t, c.Set(k[:], lfValue(i)))
	}

	c.Reset()

	for i := range c.lanes {
		require.Equal(t, c.lanes[i].tail.Load(), c.lanes[i].snap.Load(), "lane %d", i)
	}
}

// TestLaneCache_LappedEntriesCountAsEvictions checks that entries a ring wrap
// overwrites are reported as evictions, so the evictions metric shows cache
// pressure and not only explicit deletes, and that the count never goes down
// (the metric is a gauge read with rate()).
func TestLaneCache_LappedEntriesCountAsEvictions(t *testing.T) {
	c, err := NewLaneCache(1024 * 1024)
	require.NoError(t, err)

	for i := uint64(0); i < 100; i++ {
		k := mixedKey(i)
		require.NoError(t, c.Set(k[:], lfValue(i)))
	}

	var s Stats
	c.UpdateStats(&s)
	require.Zero(t, s.Evictions, "nothing has been lapped yet")

	var last uint64

	for round := uint64(1); round <= 5; round++ {
		for i := uint64(0); i < 50_000; i++ {
			k := mixedKey(round*1_000_000 + i)
			require.NoError(t, c.Set(k[:], lfValue(i)))
		}

		s = Stats{}
		c.UpdateStats(&s)

		require.Positive(t, s.Evictions, "round %d: a lapped ring must report evictions", round)
		require.GreaterOrEqual(t, s.Evictions, last, "round %d: evictions must not decrease", round)
		require.Equal(t, s.TotalElementsAdded, s.Evictions+s.ValidEntriesCount, "round %d: every insert is live or evicted", round)

		last = s.Evictions
	}
}

// Reset drops every entry: they all count as evicted, the count still never
// goes down, and entries inserted after the Reset are live.
func TestLaneCache_ResetCountsEverythingAsEvicted(t *testing.T) {
	c, err := NewLaneCache(1024 * 1024)
	require.NoError(t, err)

	for i := uint64(0); i < 50_000; i++ {
		k := mixedKey(i)
		require.NoError(t, c.Set(k[:], lfValue(i)))
	}

	var before Stats
	c.UpdateStats(&before)

	c.Reset()

	for i := uint64(0); i < 100; i++ {
		k := mixedKey(1_000_000 + i)
		require.NoError(t, c.Set(k[:], lfValue(i)))
	}

	var after Stats
	c.UpdateStats(&after)

	require.Equal(t, uint64(50_000), after.Evictions, "every entry from before the Reset is evicted")
	require.GreaterOrEqual(t, after.Evictions, before.Evictions)
	require.Equal(t, uint64(100), after.ValidEntriesCount, "entries inserted after the Reset are live")
}
