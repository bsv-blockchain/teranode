package blockvalidation

import (
	"context"
	"math"
	"math/big"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/p2p"
	"github.com/bsv-blockchain/teranode/settings"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
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

// TestSelectBestPeersForCatchup_BelowTargetOnlyViaValidatedWork verifies the target
// floor of the probe tier: a peer advertising a height below the block we need is not
// probed, while a peer whose locally validated work is ahead of our tip is admitted
// whatever height it advertises, because the validated tier does not read the
// advertised height at all.
func TestSelectBestPeersForCatchup_BelowTargetOnlyViaValidatedWork(t *testing.T) {
	ctx := context.Background()

	validatedHash, err := chainhash.NewHashFromStr("000000000000000000000000000000000000000000000000000000000000eeee")
	require.NoError(t, err)

	const localBestHeight = uint32(1_000)
	const targetHeight = uint32(1_005)

	belowTargetProbe := &p2p.PeerInfo{
		ID:         peer.ID("below-target-probe"),
		Height:     targetHeight - 1,
		DataHubURL: "http://below/api/v1",
	}
	belowTargetValidated := &p2p.PeerInfo{
		ID:                 peer.ID("below-target-validated"),
		Height:             targetHeight - 1,
		DataHubURL:         "http://validated/api/v1",
		ValidatedHeight:    targetHeight - 1,
		ValidatedBlockHash: validatedHash,
		ValidatedChainWork: []byte{0x00, 0x00, 0x09},
	}

	srv := newPeerSelectionServer([]*p2p.PeerInfo{belowTargetProbe, belowTargetValidated}, 10_000)
	got, err := srv.selectBestPeersForCatchup(ctx, targetHeight, []byte{0x00, 0x00, 0x01}, localBestHeight)
	require.NoError(t, err)
	require.Len(t, got, 1, "a probe peer below the target height must not be selected")
	require.Equal(t, peer.ID("below-target-validated").String(), got[0].ID)
}

// bestHeaderRecorder wraps a real sqlitememory-backed blockchain client and counts
// GetBestBlockHeader calls. With err set it fails them instead, to drive the tip-read
// failure path; otherwise it delegates, so the tip is whatever the real store holds.
type bestHeaderRecorder struct {
	blockchain.ClientI

	err   error
	calls int
}

func (c *bestHeaderRecorder) GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	c.calls++

	if c.err != nil {
		return nil, nil, c.err
	}

	return c.ClientI.GetBestBlockHeader(ctx)
}

func newBestHeaderRecorder(t *testing.T, err error) (*bestHeaderRecorder, blockchain_store.Store) {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)

	store, storeErr := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, storeErr)

	realClient, clientErr := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, store, nil, nil)
	require.NoError(t, clientErr)

	return &bestHeaderRecorder{ClientI: realClient, err: err}, store
}

// TestTryAlternativePeersForCatchup_GatesOnLocalTip drives the alternative-peer walk
// end to end against a real local tip at height 1 and checks that the tip read from
// the blockchain client reaches the selector: its chainwork admits a validated-ahead
// peer whose advertised height is beyond the probe window, and its height opens the
// probe window for a peer that would be out of range if the tip were read as zero.
// The validated-ahead peer is tried first even though the p2p service lists it last;
// the far probe peer is never tried.
func TestTryAlternativePeersForCatchup_GatesOnLocalTip(t *testing.T) {
	ctx := context.Background()

	recorder, store := newBestHeaderRecorder(t, nil)

	genesis, _, err := store.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	storeMinedBlock(ctx, t, store, forkHeadersFrom(t, genesis, 1, 1, 0)[0], 1)

	_, tipMeta, err := store.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(1), tipMeta.Height, "the stored block must be the local tip")

	aheadWork := new(big.Int).Add(new(big.Int).SetBytes(tipMeta.ChainWork), big.NewInt(1)).Bytes()

	validatedHash, err := chainhash.NewHashFromStr("000000000000000000000000000000000000000000000000000000000000ffff")
	require.NoError(t, err)

	// With the tip at height 1 and a lead of 2 the probe window tops out at 3. Read as
	// zero, it would top out at 2 and exclude probe-near.
	const maxLead = uint32(2)

	probeNear := &p2p.PeerInfo{
		ID:         peer.ID("probe-near"),
		Height:     3,
		DataHubURL: "http://probe-near/api/v1",
	}
	probeFar := &p2p.PeerInfo{
		ID:         peer.ID("probe-far"),
		Height:     4,
		DataHubURL: "http://probe-far/api/v1",
	}
	validatedAhead := &p2p.PeerInfo{
		ID:                 peer.ID("validated-ahead"),
		Height:             100,
		DataHubURL:         "http://validated-ahead/api/v1",
		ValidatedHeight:    2,
		ValidatedBlockHash: validatedHash,
		ValidatedChainWork: aheadWork,
	}

	srv := newPeerSelectionServer([]*p2p.PeerInfo{probeNear, probeFar, validatedAhead}, maxLead)
	srv.blockchainClient = recorder
	srv.processBlockNotify = ttlcache.New[chainhash.Hash, bool]()
	srv.catchupAlternatives = ttlcache.New[chainhash.Hash, []processBlockCatchup]()

	var tried []string

	srv.catchupFunc = func(_ context.Context, _ *model.Block, peerID, _ string) error {
		tried = append(tried, peerID)

		return errors.NewServiceError("peer %s could not serve the block", peerID)
	}

	prev := chainhash.HashH([]byte("alt-tip-prev"))
	mr := chainhash.HashH([]byte("alt-tip-merkle"))
	block := &model.Block{
		Header: &model.BlockHeader{HashPrevBlock: &prev, HashMerkleRoot: &mr},
		Height: 2,
	}

	succeeded, stoppedOnLocalPanic := srv.tryAlternativePeersForCatchup(ctx, block, "")
	require.False(t, succeeded)
	require.False(t, stoppedOnLocalPanic)
	require.Equal(t, 1, recorder.calls)
	require.Equal(t, []string{peer.ID("validated-ahead").String(), peer.ID("probe-near").String()}, tried,
		"the validated-ahead peer must be tried first, the near probe second, and the far probe never")
}

// TestTryAlternativePeersForCatchup_TipReadErrorBoundsProbeAtZero pins what happens
// when the local tip cannot be read: no work is known, so no peer is validated-ahead,
// and the probe window is measured from height zero.
func TestTryAlternativePeersForCatchup_TipReadErrorBoundsProbeAtZero(t *testing.T) {
	ctx := context.Background()

	const maxLead = uint32(10_000)

	inWindow := &p2p.PeerInfo{
		ID:         peer.ID("in-window"),
		Height:     maxLead,
		DataHubURL: "http://in-window/api/v1",
	}
	pastWindow := &p2p.PeerInfo{
		ID:         peer.ID("past-window"),
		Height:     maxLead + 1,
		DataHubURL: "http://past-window/api/v1",
	}

	srv := newPeerSelectionServer([]*p2p.PeerInfo{inWindow, pastWindow}, maxLead)
	tipClient, _ := newBestHeaderRecorder(t, errors.NewServiceError("blockchain unavailable"))
	srv.blockchainClient = tipClient

	var tried []string

	srv.catchupFunc = func(_ context.Context, _ *model.Block, peerID, _ string) error {
		tried = append(tried, peerID)

		return errors.NewServiceError("peer %s could not serve the block", peerID)
	}

	prev := chainhash.HashH([]byte("alt-err-prev"))
	mr := chainhash.HashH([]byte("alt-err-merkle"))
	block := &model.Block{
		Header: &model.BlockHeader{HashPrevBlock: &prev, HashMerkleRoot: &mr},
		Height: 100,
	}

	succeeded, _ := srv.tryAlternativePeersForCatchup(ctx, block, "")
	require.False(t, succeeded)
	require.Equal(t, 1, tipClient.calls)
	require.Equal(t, []string{peer.ID("in-window").String()}, tried)
}

// TestTryAlternativePeersForCatchup_NoP2PClientSkipsTipRead verifies the early return
// when there is no p2p client: the walk cannot select anyone, so it must not spend a
// blockchain RPC reading the local tip first.
func TestTryAlternativePeersForCatchup_NoP2PClientSkipsTipRead(t *testing.T) {
	tipClient, _ := newBestHeaderRecorder(t, nil)

	srv := &Server{
		logger:           ulogger.TestLogger{},
		settings:         &settings.Settings{},
		blockchainClient: tipClient,
	}

	prev := chainhash.HashH([]byte("alt-nop2p-prev"))
	mr := chainhash.HashH([]byte("alt-nop2p-merkle"))
	block := &model.Block{Header: &model.BlockHeader{HashPrevBlock: &prev, HashMerkleRoot: &mr}}

	succeeded, stoppedOnLocalPanic := srv.tryAlternativePeersForCatchup(context.Background(), block, "")
	require.False(t, succeeded)
	require.False(t, stoppedOnLocalPanic)
	require.Zero(t, tipClient.calls, "no tip read may happen without a p2p client")
}
