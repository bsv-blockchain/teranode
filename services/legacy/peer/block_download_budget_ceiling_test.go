package peer

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestBlockDownloadBudgetNeverExceedsItsDerivedMaximum is the one-clock
// invariant, pinned as a property rather than as a number.
//
// svnode has ONE clock: the figure that bounds a block download is the figure
// its timeout fires on, and its BlockDownloadTracker has no expiry of its own at
// all (src/net/block_download_tracker.h). This node keeps a second record, the
// netsync download ledger, whose ownership ceiling is derived from
// MaxBlockDownloadBudget. If a live budget could ever exceed that maximum the
// two clocks would disagree again: a peer still legitimately sending would
// outlive the record saying we asked it, and its finished block would arrive
// looking unrequested, costing it the whole association.
//
// So the property is that no settings combination the node can load, at any peer
// count whatsoever, produces a live budget longer than the maximum derived from
// those same settings. The peer counts deliberately run far past the bound the
// configuration implies, because the bound is what the ceiling is derived from
// and the live count is not policed against it.
func TestBlockDownloadBudgetNeverExceedsItsDerivedMaximum(t *testing.T) {
	base := settings.NewSettings()

	intervals := []time.Duration{time.Second, time.Minute, 10 * time.Minute, 2 * time.Hour}
	windows := []int{1, 16, 1024, 100_000}
	depths := []int{1, 4, 16, 1024}
	bases := []int64{0, 1, 100, 600, 5000, 30744574}
	perPeers := []int64{-50, 0, 50, 1000}
	// 1_000_000 peers is not reachable, and that is the point: the invariant
	// must not rest on the live count staying inside the configured bound.
	peerCounts := []int{0, 1, 2, 8, 64, 65, 1000, 1_000_000}

	for _, interval := range intervals {
		params := *base.ChainCfgParams
		params.TargetTimePerBlock = interval

		for _, window := range windows {
			for _, depth := range depths {
				for _, basePercent := range bases {
					for _, perPeer := range perPeers {
						tSettings := settings.NewSettings()
						tSettings.Legacy.BlockDownloadWindow = window
						tSettings.Legacy.MaxBlocksInTransitPerPeer = depth
						tSettings.Legacy.BlockDownloadTimeoutBasePercent = basePercent
						tSettings.Legacy.BlockDownloadTimeoutBaseIBDPercent = basePercent * 6
						tSettings.Legacy.BlockDownloadTimeoutPerPeerPercent = perPeer

						ceiling := MaxBlockDownloadBudget(tSettings, interval)

						for _, peers := range peerCounts {
							for _, catchingUp := range []bool{false, true} {
								p := &Peer{settings: tSettings, logger: ulogger.TestLogger{}}
								p.cfg = Config{
									ChainParams:             &params,
									CatchingUp:              func() bool { return catchingUp },
									PeersWithBlockDownloads: func() int { return peers },
								}

								require.LessOrEqual(t, p.blockDownloadBudget(), ceiling,
									"interval %s, window %d, depth %d, base %d%%, perPeer %d%%, %d peers, catchingUp %v: a live budget longer than the derived maximum lets the download ledger expire under a transfer the peer layer is still keeping alive",
									interval, window, depth, basePercent, perPeer, peers, catchingUp)
							}
						}
					}
				}
			}
		}
	}
}

// TestMaxBlockDownloadBudgetIsNeverShorterThanTheFloor guards the other
// direction. MaxBlockDownloadTime is the shortest ceiling any fetch gets, so a
// maximum below it would describe a budget that cannot happen and would cap the
// live budget below its own floor.
func TestMaxBlockDownloadBudgetIsNeverShorterThanTheFloor(t *testing.T) {
	tSettings := settings.NewSettings()

	for _, interval := range []time.Duration{0, -time.Minute, time.Nanosecond, 10 * time.Minute} {
		require.GreaterOrEqual(t, MaxBlockDownloadBudget(tSettings, interval), MaxBlockDownloadTime,
			"interval %s", interval)
	}

	require.GreaterOrEqual(t, MaxBlockDownloadBudget(nil, 10*time.Minute), MaxBlockDownloadTime,
		"an unwired settings pointer must not produce a ceiling shorter than the floor")
}

// TestMaxPeersWithBlockDownloadsFollowsTheConfiguration states the bound the
// ceiling is derived from, so a change to either setting shows up here rather
// than silently moving the ceiling. The node-wide window spread at the
// configured per-peer depth is how many peers the scheduler's contiguous runs
// put work on: at the shipped 1024 over 16 that is 64.
func TestMaxPeersWithBlockDownloadsFollowsTheConfiguration(t *testing.T) {
	tests := []struct {
		window int
		depth  int
		want   int
	}{
		{1024, 16, 64},
		{1024, 1, 1024},
		{1, 16, 1},
		{100, 16, 7},
		{0, 0, 1},
		{-5, -5, 1},
	}

	for _, tt := range tests {
		tSettings := settings.NewSettings()
		tSettings.Legacy.BlockDownloadWindow = tt.window
		tSettings.Legacy.MaxBlocksInTransitPerPeer = tt.depth

		require.Equal(t, tt.want, maxPeersWithBlockDownloads(tSettings),
			"window %d over depth %d", tt.window, tt.depth)
	}
}

// TestMaxBlockDownloadBudgetAtShippedSettings states the figure the download
// ledger's ownership ceiling actually takes on mainnet, so the number in the
// comments is checked rather than asserted in prose: 64 peers, so 600% for
// catch-up plus 63 others at 50%, of a ten-minute interval.
func TestMaxBlockDownloadBudgetAtShippedSettings(t *testing.T) {
	tSettings := settings.NewSettings()

	require.Equal(t, 375*time.Minute, MaxBlockDownloadBudget(tSettings, 10*time.Minute))
}
