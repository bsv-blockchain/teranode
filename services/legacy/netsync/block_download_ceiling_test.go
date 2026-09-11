package netsync

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestLedgerCeiling_OutlivesEveryDownloadBudgetThePeerLayerCanGrant is the
// property, not the number: for any settings the node can load, the ledger's
// ownership ceiling is at least the longest download budget the peer layer can
// grant on that chain. If it is ever shorter, a block the peer layer is still
// legitimately receiving loses its owner mid-transfer, and when it lands
// handleBlockMsg reads it as unrequested: the peer is disconnected with "Got
// unrequested block" and a completed multi-gigabyte download is thrown away.
//
// svnode cannot have this bug because it has one clock. Its BlockDownloadTracker
// has no expiry at all (src/net/block_download_tracker.h) and the only timer is
// the per-block in-flight timeout in DetectStalling
// (src/net/net_processing.cpp:5446), computed from the very figure that bounds
// the transfer. Deriving both sides here from the same settings is what puts
// this node back on one clock.
func TestLedgerCeiling_OutlivesEveryDownloadBudgetThePeerLayerCanGrant(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	windows := []int{1, 16, 1024, 100_000}
	depths := []int{1, 4, 16, 1024}
	bases := []int64{0, 100, 600, 5000}
	perPeers := []int64{0, 50, 1000}
	intervals := []time.Duration{time.Second, time.Minute, 10 * time.Minute, 2 * time.Hour}

	for _, window := range windows {
		for _, depth := range depths {
			for _, base := range bases {
				for _, perPeer := range perPeers {
					tSettings.Legacy.BlockDownloadWindow = window
					tSettings.Legacy.MaxBlocksInTransitPerPeer = depth
					tSettings.Legacy.BlockDownloadTimeoutBasePercent = base
					tSettings.Legacy.BlockDownloadTimeoutBaseIBDPercent = base * 6
					tSettings.Legacy.BlockDownloadTimeoutPerPeerPercent = perPeer

					for _, interval := range intervals {
						params := chaincfg.MainNetParams
						params.TargetTimePerBlock = interval

						ceiling := blockRequestAssignmentCeiling(tSettings, &params)

						require.GreaterOrEqual(t, ceiling, peerpkg.MaxBlockDownloadBudget(tSettings, interval),
							"window %d, depth %d, base %d%%, perPeer %d%%, interval %s: the ledger must not expire an assignment the peer layer is still honouring",
							window, depth, base, perPeer, interval)

						require.GreaterOrEqual(t, ceiling, blockRequestAssignmentTTL,
							"window %d, depth %d, base %d%%, perPeer %d%%, interval %s: the derived ceiling may only widen the hour this replaced",
							window, depth, base, perPeer, interval)
					}
				}
			}
		}
	}
}

// TestLedgerCeiling_TheEightPeerCatchUpTransferStillHasItsOwner is the same
// invariant as the case that actually bites, stated in the units the bug
// appeared in.
//
// A peer sharing our downlink with seven others during catch-up is given 950% of
// a ten-minute interval, which is 95 minutes, and the peer layer extends its
// deadline for the whole of that as long as bytes keep arriving. The ledger's
// clock starts earlier still, at the getdata. At a flat hour the record was gone
// while the transfer was 35 minutes from its own ceiling.
func TestLedgerCeiling_TheEightPeerCatchUpTransferStillHasItsOwner(t *testing.T) {
	const eightPeerCatchUpBudget = 95 * time.Minute

	tSettings := test.CreateBaseTestSettings(t)
	params := chaincfg.MainNetParams

	tr, advance := newTestTracker(blockRequestAssignmentCeiling(tSettings, &params))

	p := newTestPeer(t, "localhost:18333")
	h := chainhash.Hash{0x9c}

	require.True(t, tr.Add(p, h), "sanity: the getdata is recorded")

	advance(eightPeerCatchUpBudget)

	require.True(t, tr.HasOwner(p, h),
		"a block still inside the peer layer's own %s budget must still have an owner, or it lands as an unrequested block and costs the peer its association", eightPeerCatchUpBudget)
}
