package netsync

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

func TestWantedBlocksFromCache_IsTheRunAboveTheCommittedTip(t *testing.T) {
	parent := chainhash.Hash{0xaa}
	headers := chainOfHeaders(parent, 10)

	sm := newRaceManager(t)
	sm.headerCache = newHeaderCache()
	require.True(t, sm.headerCache.Fill(parent, 101, headers))

	got := sm.wantedBlocksFromCache(100, 4)

	require.Len(t, got, 4, "the range is bounded by the depth, whatever the cache holds")
	require.Equal(t, int32(101), got[0].height, "and starts one above the best block processed")
	require.Equal(t, headers[0].BlockHash(), got[0].hash)
	require.Equal(t, int32(104), got[3].height)
}

// TestWantedBlocksFromCache_StopsAtTheFirstHeightTheCacheCannotName is the rule
// that keeps the park from filling above a hole. A block whose parent never
// arrives cannot commit, so blocks fetched past a gap only wait.
func TestWantedBlocksFromCache_StopsAtTheFirstHeightTheCacheCannotName(t *testing.T) {
	parent := chainhash.Hash{0xaa}

	sm := newRaceManager(t)
	sm.headerCache = newHeaderCache()
	require.True(t, sm.headerCache.Fill(parent, 101, chainOfHeaders(parent, 3)))

	got := sm.wantedBlocksFromCache(100, 10)

	require.Len(t, got, 3, "the cache names three heights, so three is the range")
}

func TestWantedBlocksFromCache_IsEmptyWithNoCache(t *testing.T) {
	sm := newRaceManager(t)

	require.Empty(t, sm.wantedBlocksFromCache(100, 4),
		"no cache means nothing can be named, so the pass asks for nothing and the next getheaders fixes it")
}

// TestWantedBlocksFromCache_ClampsANonPositiveDepthToOne pins the floor. A
// depth of zero would ask for nothing for ever, which is a stall dressed as a
// setting.
func TestWantedBlocksFromCache_ClampsANonPositiveDepthToOne(t *testing.T) {
	parent := chainhash.Hash{0xaa}

	sm := newRaceManager(t)
	sm.headerCache = newHeaderCache()
	require.True(t, sm.headerCache.Fill(parent, 101, chainOfHeaders(parent, 10)))

	got := sm.wantedBlocksFromCache(100, 0)

	require.Len(t, got, 1, "a depth below one is clamped to one, never to zero")
}

// TestUnownedBlocks_DropsBlocksAlreadyOnDisk is the deletion the whole model
// turns on: a block we hold is not wanted, whatever any index says.
func TestUnownedBlocks_DropsBlocksAlreadyOnDisk(t *testing.T) {
	sm := newRaceManager(t)
	sm.ctx = context.Background()
	park, _ := newTestPark(t, "")
	sm.blockPark = park

	held := chainhash.Hash{0x01}
	wanted := chainhash.Hash{0x02}

	require.NoError(t, sm.blockPark.store.Set(context.Background(), held[:], parkFileType, []byte("body"), parkOpts...))

	got := sm.unownedBlocks([]wantedBlock{
		{height: 101, hash: held},
		{height: 102, hash: wanted},
	})

	require.Len(t, got, 1, "the block already on disk must not be asked for again")
	require.Equal(t, wanted, got[0].hash)
}

func TestUnownedBlocks_StillDropsBlocksAPeerOwes(t *testing.T) {
	sm := newRaceManager(t)
	sm.ctx = context.Background()
	park, _ := newTestPark(t, "")
	sm.blockPark = park

	p, _ := schedulerPeer(t, sm, 1, 1000)

	owed := chainhash.Hash{0x03}
	free := chainhash.Hash{0x04}

	require.True(t, sm.blockDownloads.Add(p, owed))

	got := sm.unownedBlocks([]wantedBlock{
		{height: 101, hash: owed},
		{height: 102, hash: free},
	})

	require.Len(t, got, 1, "a block inside its retry window has an owner who may still deliver")
	require.Equal(t, free, got[0].hash)

	_ = time.Second
}

// TestWantedBlocks_UsesTheScaledLookaheadCeilingNotTheFlatWindow drives the real
// path end to end, the way a live headers round does: fillHeaderCache lands a
// batch in the cache, and wantedBlocks reads it back through
// lookaheadCeilingLocked.
//
// This is the test the fix round found missing. Every existing ceiling test
// hand-seeded the header list, a structure nothing in production fills any
// more, so they proved lookaheadCeilingLocked correct in isolation while the
// integrated path — fillHeaderCache into wantedBlocks — silently answered "no
// ceiling" on every real node, because its old anchor fallback needed a header
// list front that was never there. Two later tasks write tests that assume the
// park cannot exceed the read-ahead depth; against that bug they would have
// passed for nothing, bounded instead by the flat node-wide window.
//
// legacy_blockDownloadLowerWindow and legacy_blockDownloadWindow are set to
// wildly different values on purpose, so a wantedBlocks that fell back to the
// window cannot be mistaken for one reading the scaled ceiling: 1000 blocks
// back would be obvious against the 5 asserted here. The block-size ladder is
// parked at half its top rung rather than left at its default top — at the top
// rung scaling is a no-op ratio of one, so a ceiling of 10 would not
// distinguish "scaled" from "the unscaled lower window itself".
func TestWantedBlocks_UsesTheScaledLookaheadCeilingNotTheFlatWindow(t *testing.T) {
	sm := newHeaderCacheManager(t)

	tipHash := mockCommittedTip(t, sm, 100, 0)

	sm.settings.Legacy.BlockDownloadLowerWindow = 10
	sm.settings.Legacy.BlockDownloadWindow = 1000

	sm.blockSizeTracker = newBlockSizeTracker(10)
	sm.blockSizeTracker.addBlockSize(150 * 1024 * 1024)
	require.Equal(t, 10, sm.blockSizeTracker.calculateMaxInFlightBlocks(),
		"sanity: the ladder is at half its top rung, so scaling is neither a no-op nor a floor")

	peer, _, _ := connectRacePeer(t, 211, 1000)

	var nonce uint32
	msg, _ := linkedHeaders(tipHash, 20, &nonce)

	sm.fillHeaderCache(peer, msg)

	got := sm.wantedBlocks(100)

	require.Len(t, got, 5,
		"the scaled ceiling (10 * 10/20 = 5) must bind, not the unscaled lower window (10) and not the node-wide window (1000)")
	require.Equal(t, int32(101), got[0].height)
	require.Equal(t, int32(105), got[len(got)-1].height)
}
