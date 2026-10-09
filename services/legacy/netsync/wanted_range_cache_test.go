package netsync

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
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

	held := heldRecord(t, sm, 0x01)
	wanted := chainhash.Hash{0x02}

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

// TestWantedBlocks_UsesTheConfiguredWindowNotTheHeaderListFront drives the real
// path end to end, the way a live headers round does: fillHeaderCache lands a
// batch in the cache, and wantedBlocks reads it back through
// legacy_blockDownloadWindow.
//
// This is the test the fix round found missing. Every existing depth test
// hand-seeded the header list, a structure nothing in production fills any
// more, so they proved the depth arithmetic correct in isolation while the
// integrated path — fillHeaderCache into wantedBlocks — silently answered "no
// limit" on every real node, because its old anchor fallback needed a header
// list front that was never there.
func TestWantedBlocks_UsesTheConfiguredWindowNotTheHeaderListFront(t *testing.T) {
	sm := newHeaderCacheManager(t)

	tipHash := mockCommittedTip(t, sm, 100, 0)

	sm.settings.Legacy.BlockDownloadWindow = 5

	peer, _, _ := connectRacePeer(t, 211, 1000)

	var nonce uint32
	msg, _ := linkedHeaders(tipHash, 20, &nonce)

	sm.fillHeaderCache(peer, msg)

	got := sm.wantedBlocks(100)

	require.Len(t, got, 5, "the configured window must bind, not the header list's own front")
	require.Equal(t, int32(101), got[0].height)
	require.Equal(t, int32(105), got[len(got)-1].height)
}

// TestWantedBlocksFromCache_NamesNothingFromAnUnprovenBelowCheckpointRun is the
// fetch half of GHSA-gggq-8f59-4jm9: below the last checkpoint no block is
// requested from a header run that has not yet matched a pinned checkpoint
// hash, however many heights the run names. The first fill reaches height 6 of
// a checkpoint pinned at 10, so the cache names six heights and the node wants
// none of them; the second fill extends the run through the checkpoint, the
// match proves heights 1 to 10, and those become wantable. Heights 11 and 12
// sit above the final checkpoint, where nothing is quick-routed and no proof
// can exist, so they are wantable too, which is why a depth-12 ask returns 12.
func TestWantedBlocksFromCache_NamesNothingFromAnUnprovenBelowCheckpointRun(t *testing.T) {
	parent := chainhash.Hash{0xaa}
	headers, hashes := linkedRun(parent, 12)

	sm := newRaceManager(t)
	sm.headerCache = newHeaderCache().WithCheckpoints([]chaincfg.Checkpoint{{Height: 10, Hash: &hashes[9]}})

	require.True(t, sm.headerCache.Fill(parent, 1, headers[:6]))
	require.Equal(t, 6, sm.headerCache.Len(), "the unproven run still names its heights, which keeps the walk going")
	require.Empty(t, sm.wantedBlocksFromCache(0, 10), "but nothing below the checkpoint is wanted until a pinned hash is matched")

	require.True(t, sm.headerCache.Fill(hashes[5], 1, headers[6:]), "the second reply extends the run through the checkpoint")
	require.Equal(t, int32(10), sm.headerCache.ProvenTo())

	wanted := sm.wantedBlocksFromCache(0, 10)
	require.Len(t, wanted, 10)

	for i, w := range wanted {
		require.Equal(t, int32(i+1), w.height) //nolint:gosec // a small loop index
		require.Equal(t, hashes[i], w.hash, "height %d must carry the run's own hash", i+1)
	}

	require.Len(t, sm.wantedBlocksFromCache(0, 12), 12, "heights above the final checkpoint need no proof")
}

// TestWantedBlocksFromCache_HeightsAboveTheFinalCheckpointNeedNoProof pins the
// boundary on its own: a six-header run over a checkpoint pinned at 3 is
// wantable in full after one fill, proven for 1 to 3 and unproven above, because
// above the final checkpoint every block takes full validation and the proof
// decides nothing.
func TestWantedBlocksFromCache_HeightsAboveTheFinalCheckpointNeedNoProof(t *testing.T) {
	parent := chainhash.Hash{0xab}
	headers, hashes := linkedRun(parent, 6)

	sm := newRaceManager(t)
	sm.headerCache = newHeaderCache().WithCheckpoints([]chaincfg.Checkpoint{{Height: 3, Hash: &hashes[2]}})

	require.True(t, sm.headerCache.Fill(parent, 1, headers))

	wanted := sm.wantedBlocksFromCache(0, 10)
	require.Len(t, wanted, 6, "every named height is wantable: 1 to 3 by proof, 4 to 6 by being above the final checkpoint")

	for i := range hashes {
		height := int32(i + 1) //nolint:gosec // a small loop index
		require.Equal(t, height <= 3, sm.headerCache.Proven(hashes[i]), "proof covers exactly the matched prefix (height %d)", height)
	}
}
