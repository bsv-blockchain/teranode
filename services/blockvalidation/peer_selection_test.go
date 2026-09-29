package blockvalidation

import (
	"context"
	"math"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/services/p2p"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/jellydator/ttlcache/v3"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

// peerSelectionP2PClient is a minimal P2PClientI that serves a fixed peer list.
// All other interface methods are no-ops; tests that only exercise peer filtering
// never reach the reputation-reporting paths.
type peerSelectionP2PClient struct {
	maliciousAbortP2PClient
	peers []*p2p.PeerInfo
}

func (c *peerSelectionP2PClient) GetPeersForCatchup(_ context.Context) ([]*p2p.PeerInfo, error) {
	return c.peers, nil
}

func newPeerSelectionServer(peers []*p2p.PeerInfo, maxUnvalidatedLead uint32) *Server {
	cfg := &settings.Settings{}
	cfg.P2P.MaxUnvalidatedAdvertisedHeightLead = maxUnvalidatedLead

	return &Server{
		logger:    ulogger.TestLogger{},
		settings:  cfg,
		p2pClient: &peerSelectionP2PClient{peers: peers},
		peerMaliciousCache: ttlcache.New[string, bool](
			ttlcache.WithTTL[string, bool](peerMaliciousCacheTTL),
			ttlcache.WithDisableTouchOnHit[string, bool](),
		),
	}
}

// TestSelectBestPeersForCatchup_AbsurdHeightRejectedWithoutValidatedWork verifies
// that a peer advertising math.MaxUint32 as its height but carrying no locally
// validated chainwork is never eligible as a catchup source. The lead bound is
// measured against localBestHeight, not targetHeight, so it holds for every target.
func TestSelectBestPeersForCatchup_AbsurdHeightRejectedWithoutValidatedWork(t *testing.T) {
	ctx := context.Background()

	peers := []*p2p.PeerInfo{
		{
			ID:         peer.ID("absurd-peer"),
			Height:     math.MaxUint32,
			DataHubURL: "http://absurd-peer/api/v1",
			// No ValidatedBlockHash, no ValidatedChainWork — purely self-reported.
		},
	}

	// maxUnvalidatedLead=10_000; math.MaxUint32 > localBestHeight(0) + 10_000
	// for any reasonable localBestHeight, so the peer must be rejected.
	srv := newPeerSelectionServer(peers, 10_000)

	for _, targetHeight := range []uint32{0, 100, 50_000, 700_000} {
		got, err := srv.selectBestPeersForCatchup(ctx, targetHeight, nil, 0)
		require.NoError(t, err)
		require.Empty(t, got, "peer with Height=MaxUint32 and no validated work must be rejected for target %d", targetHeight)
	}
}

// TestSelectBestPeersForCatchup_ValidatedWorkPeerIncluded verifies that a peer
// with locally-validated chainwork exceeding the local tip is returned in the
// validated-ahead tier, while an unvalidated peer with a plausible height is
// returned in the probe tier. Both tiers must be present, and the validated-ahead
// peer must come first (M2 ordering). The validated triple must round-trip intact.
func TestSelectBestPeersForCatchup_ValidatedWorkPeerIncluded(t *testing.T) {
	ctx := context.Background()

	localWork := []byte{0x00, 0x00, 0x01}
	peerWork := []byte{0x00, 0x00, 0x02} // ahead of local

	validatedHash, err := chainhash.NewHashFromStr("000000000000000000000000000000000000000000000000000000000000aaaa")
	require.NoError(t, err)

	const targetHeight = uint32(100)
	const localBestHeight = uint32(90)

	peers := []*p2p.PeerInfo{
		{
			// Unvalidated peer: advertises a plausible height, no validated work.
			// Returned by the p2p server first (simulating higher reputation), but
			// must end up after the validated-ahead peer in the result.
			ID:         peer.ID("unvalidated-peer"),
			Height:     targetHeight + 5,
			DataHubURL: "http://unvalidated/api/v1",
		},
		{
			// Validated peer: validated chainwork exceeds local tip.
			ID:                 peer.ID("validated-peer"),
			Height:             targetHeight + 1,
			DataHubURL:         "http://validated/api/v1",
			ValidatedHeight:    targetHeight + 1,
			ValidatedBlockHash: validatedHash,
			ValidatedChainWork: peerWork,
		},
	}

	srv := newPeerSelectionServer(peers, 10_000)
	got, err := srv.selectBestPeersForCatchup(ctx, targetHeight, localWork, localBestHeight)
	require.NoError(t, err)
	require.Len(t, got, 2, "both peers should be eligible")

	// Validated-ahead peer must be first regardless of p2p server order.
	require.Equal(t, peer.ID("validated-peer").String(), got[0].ID, "validated-ahead peer must come first")
	require.Equal(t, peer.ID("unvalidated-peer").String(), got[1].ID, "probe peer must come second")

	// Validated chainwork must round-trip without loss.
	require.NotNil(t, got[0].ValidatedBlockHash)
	require.Equal(t, peerWork, got[0].ValidatedChainWork)
	require.Equal(t, targetHeight+uint32(1), got[0].ValidatedHeight)
}

// TestSelectBestPeersForCatchup_ValidatedBehindTipProbeEligible verifies that a peer
// whose validated chainwork does NOT exceed the local tip is still probe-eligible when
// its advertised height meets the target and is within the lead bound. A peer that has
// been validated is still a known-good source for the specific block we need.
func TestSelectBestPeersForCatchup_ValidatedBehindTipProbeEligible(t *testing.T) {
	ctx := context.Background()

	localWork := []byte{0x00, 0x00, 0x05}
	peerWork := []byte{0x00, 0x00, 0x03} // behind local

	validatedHash, err := chainhash.NewHashFromStr("000000000000000000000000000000000000000000000000000000000000bbbb")
	require.NoError(t, err)

	peers := []*p2p.PeerInfo{
		{
			ID:                 peer.ID("behind-peer"),
			Height:             200,
			DataHubURL:         "http://behind/api/v1",
			ValidatedHeight:    200,
			ValidatedBlockHash: validatedHash,
			ValidatedChainWork: peerWork,
		},
	}

	srv := newPeerSelectionServer(peers, 10_000)
	// localBestHeight=180: peer.Height(200) <= 180+10000=10180 → probe-eligible.
	got, err := srv.selectBestPeersForCatchup(ctx, 100, localWork, 180)
	require.NoError(t, err)
	require.Len(t, got, 1, "peer with validated work not ahead but plausible height must be probe-eligible")
	require.Equal(t, peer.ID("behind-peer").String(), got[0].ID)
}

// TestSelectBestPeersForCatchup_NoDataHubURLExcluded verifies that listen-only
// peers (no DataHub URL) are excluded regardless of their validated work.
func TestSelectBestPeersForCatchup_NoDataHubURLExcluded(t *testing.T) {
	ctx := context.Background()

	localWork := []byte{0x00, 0x00, 0x01}
	peerWork := []byte{0x00, 0x00, 0x02}

	validatedHash, err := chainhash.NewHashFromStr("000000000000000000000000000000000000000000000000000000000000cccc")
	require.NoError(t, err)

	peers := []*p2p.PeerInfo{
		{
			ID:                 peer.ID("listen-only-peer"),
			Height:             200,
			DataHubURL:         "", // listen-only
			ValidatedHeight:    200,
			ValidatedBlockHash: validatedHash,
			ValidatedChainWork: peerWork,
		},
	}

	srv := newPeerSelectionServer(peers, 10_000)
	got, err := srv.selectBestPeersForCatchup(ctx, 100, localWork, 180)
	require.NoError(t, err)
	require.Empty(t, got, "peer with no DataHub URL must be excluded even if validated work is ahead")
}

// TestSelectBestPeersForCatchup_ProbeBoundedByLocalTip verifies that the lead cap
// is measured against our own best height, not the target block's height. Exactly
// maxLead ahead is accepted; maxLead+1 is rejected.
func TestSelectBestPeersForCatchup_ProbeBoundedByLocalTip(t *testing.T) {
	ctx := context.Background()

	const localBestHeight = uint32(1_000)
	const maxLead = uint32(10)
	const targetHeight = uint32(500) // well below localBestHeight — doesn't affect the bound

	atExactLead := &p2p.PeerInfo{
		ID:         peer.ID("at-exact-lead"),
		Height:     localBestHeight + maxLead, // 1010
		DataHubURL: "http://exact/api/v1",
	}
	onePastLead := &p2p.PeerInfo{
		ID:         peer.ID("one-past-lead"),
		Height:     localBestHeight + maxLead + 1, // 1011
		DataHubURL: "http://past/api/v1",
	}

	srv := newPeerSelectionServer([]*p2p.PeerInfo{atExactLead, onePastLead}, maxLead)
	got, err := srv.selectBestPeersForCatchup(ctx, targetHeight, nil, localBestHeight)
	require.NoError(t, err)
	require.Len(t, got, 1, "only the peer at exactly the lead boundary should be accepted")
	require.Equal(t, peer.ID("at-exact-lead").String(), got[0].ID)
}

// TestSelectBestPeersForCatchup_HostileTargetHeightDoesNotNarrowProbe verifies that
// a hostile block with a low height field cannot exclude honest mainnet peers.
// If the lead were measured from targetHeight, height=0 would require peers within
// [0, maxLead], excluding all mainnet peers. Bounding from localBestHeight instead
// keeps honest peers reachable regardless of the target value.
func TestSelectBestPeersForCatchup_HostileTargetHeightDoesNotNarrowProbe(t *testing.T) {
	ctx := context.Background()

	const localBestHeight = uint32(500_000)
	const maxLead = uint32(10_000)
	const hostileTargetHeight = uint32(0) // hostile: would exclude mainnet peers if used as bound

	honestPeer := &p2p.PeerInfo{
		ID:         peer.ID("honest-mainnet-peer"),
		Height:     localBestHeight + 5, // plausible mainnet height
		DataHubURL: "http://honest/api/v1",
	}

	srv := newPeerSelectionServer([]*p2p.PeerInfo{honestPeer}, maxLead)
	got, err := srv.selectBestPeersForCatchup(ctx, hostileTargetHeight, nil, localBestHeight)
	require.NoError(t, err)
	require.Len(t, got, 1, "honest mainnet peer must be reachable despite hostile low targetHeight")
	require.Equal(t, peer.ID("honest-mainnet-peer").String(), got[0].ID)
}

// TestSelectBestPeersForCatchup_NilLocalWorkValidatedPeerProbeEligible verifies that
// when localChainWork is nil (blockchain client unavailable), validated peers are not
// silently dropped — they remain eligible via the probe path if their height is
// plausible. The nil case must not fail closed by excluding all known-good peers.
func TestSelectBestPeersForCatchup_NilLocalWorkValidatedPeerProbeEligible(t *testing.T) {
	ctx := context.Background()

	validatedHash, err := chainhash.NewHashFromStr("000000000000000000000000000000000000000000000000000000000000dddd")
	require.NoError(t, err)

	const localBestHeight = uint32(500)
	const targetHeight = uint32(400)

	peers := []*p2p.PeerInfo{
		{
			ID:                 peer.ID("validated-peer"),
			Height:             490,
			DataHubURL:         "http://validated/api/v1",
			ValidatedHeight:    490,
			ValidatedBlockHash: validatedHash,
			ValidatedChainWork: []byte{0x00, 0x00, 0x05},
		},
	}

	srv := newPeerSelectionServer(peers, 10_000)
	// localChainWork=nil: aheadByValidatedWork is always false, but probeEligible
	// must still admit the peer (Height 490 >= target 400 and <= 500+10000).
	got, err := srv.selectBestPeersForCatchup(ctx, targetHeight, nil, localBestHeight)
	require.NoError(t, err)
	require.Len(t, got, 1, "validated peer must be probe-eligible when localChainWork is nil")
}
