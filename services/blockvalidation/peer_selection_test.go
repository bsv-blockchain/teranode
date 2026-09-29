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
// validated chainwork is never eligible as a catchup source, regardless of the
// target height.
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

	// maxUnvalidatedLead=10_000; math.MaxUint32 - targetHeight >> 10_000
	// for any reasonable targetHeight, so the peer must be rejected.
	srv := newPeerSelectionServer(peers, 10_000)

	for _, targetHeight := range []uint32{0, 100, 50_000, 700_000} {
		got, err := srv.selectBestPeersForCatchup(ctx, targetHeight, nil)
		require.NoError(t, err)
		require.Empty(t, got, "peer with Height=MaxUint32 and no validated work must be rejected for target %d", targetHeight)
	}
}

// TestSelectBestPeersForCatchup_ValidatedWorkPeerIncluded verifies that a peer
// with locally-validated chainwork exceeding the local tip is returned, its
// validated triple round-trips without loss, and an unvalidated peer with a
// plausible height is also included as a probe candidate.
func TestSelectBestPeersForCatchup_ValidatedWorkPeerIncluded(t *testing.T) {
	ctx := context.Background()

	localWork := []byte{0x00, 0x00, 0x01}
	peerWork := []byte{0x00, 0x00, 0x02} // ahead of local

	validatedHash, err := chainhash.NewHashFromStr("000000000000000000000000000000000000000000000000000000000000aaaa")
	require.NoError(t, err)

	targetHeight := uint32(100)

	peers := []*p2p.PeerInfo{
		{
			// Unvalidated peer: advertises a plausible height, no validated work.
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
	got, err := srv.selectBestPeersForCatchup(ctx, targetHeight, localWork)
	require.NoError(t, err)
	require.Len(t, got, 2, "both peers should be eligible")

	ids := make([]string, len(got))
	for i, p := range got {
		ids[i] = p.ID
	}
	require.Contains(t, ids, peer.ID("validated-peer").String(), "validated peer must be included")
	require.Contains(t, ids, peer.ID("unvalidated-peer").String(), "plausible-height unvalidated peer must be included as probe")

	// Validated chainwork must round-trip without loss.
	for _, p := range got {
		if p.ID == peer.ID("validated-peer").String() {
			require.NotNil(t, p.ValidatedBlockHash)
			require.Equal(t, peerWork, p.ValidatedChainWork)
			require.Equal(t, targetHeight+uint32(1), p.ValidatedHeight)
		}
	}
}

// TestSelectBestPeersForCatchup_ValidatedPeerBehindLocalTipExcluded verifies
// that a peer whose validated chainwork does NOT exceed the local tip is excluded,
// and does not fall through to the probe path (it has been observed and is not ahead).
func TestSelectBestPeersForCatchup_ValidatedPeerBehindLocalTipExcluded(t *testing.T) {
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
	got, err := srv.selectBestPeersForCatchup(ctx, 100, localWork)
	require.NoError(t, err)
	require.Empty(t, got, "peer with validated work not exceeding local tip must be excluded")
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
	got, err := srv.selectBestPeersForCatchup(ctx, 100, localWork)
	require.NoError(t, err)
	require.Empty(t, got, "peer with no DataHub URL must be excluded even if validated work is ahead")
}
