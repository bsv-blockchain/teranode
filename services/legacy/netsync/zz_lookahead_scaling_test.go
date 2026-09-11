package netsync

import (
	"container/list"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/require"
)

// TestLookaheadCeilingScalesWithBlockSize is the rule that makes one setting
// honest across the whole chain.
//
// legacy_blockDownloadLowerWindow is a count of blocks, and a count means
// completely different things at different heights: 128 blocks is 128 KB of
// read-ahead near the genesis era and 440 GB of it at height 759,000 where
// blocks measure 3.44 GB. It is also the bound that decides how much disk the
// park can hold, now that the two byte budgets beside it are gone, so getting it
// wrong at the top of the chain is a disk-space question rather than a tuning
// one.
//
// The scaling reuses the ladder the node already derives from its rolling
// average block size, which governs per-peer in-flight blocks and the quick
// window's depth. The read-ahead depth was the one bound ignoring it.
func TestLookaheadCeilingScalesWithBlockSize(t *testing.T) {
	const (
		mb = 1024 * 1024
		gb = 1024 * mb
	)

	ceilingFor := func(t *testing.T, avgBlockSize int64) int64 {
		t.Helper()

		tSettings := &settings.Settings{}
		tSettings.Legacy.BlockDownloadLowerWindow = 128
		tSettings.Legacy.BlockDownloadWindow = 1024

		sm := &SyncManager{
			settings:         tSettings,
			headerList:       list.New(),
			blockSizeTracker: newBlockSizeTracker(10),
		}

		if avgBlockSize > 0 {
			sm.blockSizeTracker.addBlockSize(avgBlockSize)
		}

		hash := chainhash.Hash{0x01}
		sm.headerList.PushBack(&headerNode{height: 1000, hash: &hash})

		ceiling, ok := sm.lookaheadCeilingLocked()
		require.True(t, ok)

		// The ceiling is a height, so the depth is what it adds to the front.
		return ceiling - 1000
	}

	// Small blocks: the configured depth in full, because the ladder is at its
	// top and the ratio is one.
	require.Equal(t, int64(128), ceilingFor(t, 1*mb),
		"with small blocks the configured depth applies unchanged")

	// The eras in between, each a step down the ladder.
	require.Equal(t, int64(64), ceilingFor(t, 150*mb), "100 MB blocks: ladder 10 of 20")
	require.Equal(t, int64(32), ceilingFor(t, 300*mb), "200 MB blocks: ladder 5 of 20")
	require.Equal(t, int64(19), ceilingFor(t, 700*mb), "500 MB blocks: ladder 3 of 20")
	require.Equal(t, int64(12), ceilingFor(t, 1*gb+1), "1 GB blocks: ladder 2 of 20")

	// The era mainnet is in. Six blocks of 3.44 GB is about 20 GB of park disk,
	// against 440 GB at the unscaled depth.
	require.Equal(t, int64(6), ceilingFor(t, 3*gb),
		"at mainnet's current block size the depth collapses to single figures")
}

// TestLookaheadCeilingNeverReachesZero: a depth of zero stops the download walk
// asking for anything at all, which is a stall rather than a conservative
// setting. The scaling divides, so it has to have a floor.
func TestLookaheadCeilingNeverReachesZero(t *testing.T) {
	tSettings := &settings.Settings{}
	tSettings.Legacy.BlockDownloadLowerWindow = 4 // small enough that 4*1/20 rounds to zero
	tSettings.Legacy.BlockDownloadWindow = 1024

	sm := &SyncManager{
		settings:         tSettings,
		headerList:       list.New(),
		blockSizeTracker: newBlockSizeTracker(10),
	}

	sm.blockSizeTracker.addBlockSize(3 * 1024 * 1024 * 1024)

	hash := chainhash.Hash{0x02}
	sm.headerList.PushBack(&headerNode{height: 500, hash: &hash})

	ceiling, ok := sm.lookaheadCeilingLocked()
	require.True(t, ok)
	require.Equal(t, int64(501), ceiling,
		"a small configured depth against huge blocks must still fetch one block, not none")
}

// TestLookaheadCeilingWithoutATracker covers the managers built as struct
// literals throughout this package's tests, which have no size tracker at all.
// They must get the configured depth rather than a panic or a zero.
func TestLookaheadCeilingWithoutATracker(t *testing.T) {
	tSettings := &settings.Settings{}
	tSettings.Legacy.BlockDownloadLowerWindow = 128

	sm := &SyncManager{settings: tSettings, headerList: list.New()}

	hash := chainhash.Hash{0x03}
	sm.headerList.PushBack(&headerNode{height: 10, hash: &hash})

	ceiling, ok := sm.lookaheadCeilingLocked()
	require.True(t, ok)
	require.Equal(t, int64(138), ceiling)
}
