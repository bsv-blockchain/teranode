package netsync

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestParkIsBoundedByTheReadAheadDepth is what replaces the entry cap. With the
// committer stopped, the number of blocks outstanding must never exceed the
// depth, because the pass never names more than that many heights above the
// committed tip. This is the property the whole of 2026-09-12 was spent failing
// to achieve by counting.
//
// Fix round 1: as first written, this test could not fail. cacheManager wires
// legacy_maxBlocksInTransitPerPeer to the SAME value as the read-ahead depth,
// which is right for the tests it was built for but wrong here — with only one
// peer in this harness, a per-peer cap equal to the depth binds independently
// of it, so a depth that stopped doing its job would never show up: the
// reviewer proved this by forcing the depth to 1<<20 against an unmodified
// harness and watching outstanding stay at 8 anyway. Cleared here the same way
// TestWantedRange_TheParkNeverExceedsTheReadAheadDepth's own propertyDepth
// comment clears every other bound: set past the block-size ladder's top rung
// (20, the ceiling with no block yet sampled) and past depth itself, so the
// wanted range is the only candidate explanation for where a pass stops.
// legacy_blockDownloadWindow (node-wide, default 1024) is already far above
// depth and untouched by cacheManager, so it does not need clearing here.
func TestParkIsBoundedByTheReadAheadDepth(t *testing.T) {
	const depth = int32(8)

	sm, _, rec := cacheManager(t, 500, depth)
	sm.settings.Legacy.MaxBlocksInTransitPerPeer = 1000

	// The committer never runs: the committed height stays where it is, so every
	// pass names the same range and nothing drains.
	for i := 0; i < 20; i++ {
		sm.fetchHeaderBlocks()
	}

	require.True(t, WaitUntil(func() bool { return rec.count() > 0 }, 5*time.Second),
		"sanity: the passes must have asked for something")

	require.LessOrEqual(t, sm.blockDownloads.Len(), int(depth),
		"with the committer stopped, no more than the read-ahead depth may ever be outstanding")
}
