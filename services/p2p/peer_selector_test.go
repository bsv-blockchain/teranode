package p2p

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

func newSelectorForTest() *PeerSelector {
	return NewPeerSelector(ulogger.TestLogger{}, &settings.Settings{
		P2P: settings.P2PSettings{
			AllowPrunedNodeFallback:     true,
			FullDeliveryFreshnessWindow: 24 * time.Hour,
		},
	})
}

func newPeer(id string, height uint32, storage string, rep float64, ban int32) *blockchain.PeerInfo {
	return &blockchain.PeerInfo{
		ID:              id,
		Height:          height,
		Storage:         storage,
		ReputationScore: rep,
		BanScore:        ban,
		DataHubURL:      "http://" + id + ".example",
		BlockHash:       selectorTestHashNoFail(),
	}
}

func selectorTestHashNoFail() *chainhash.Hash {
	hash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	return hash
}

func advertisedProbeCriteria(localHeight int32) SelectionCriteria {
	return SelectionCriteria{
		LocalHeight:                  localHeight,
		AllowAdvertisedProbe:         true,
		UnprovenProbeBudgetRemaining: 3,
		FullDeliveryFreshnessWindow:  24 * time.Hour,
	}
}

func validatedWorkCriteria(localChainWork []byte) SelectionCriteria {
	return SelectionCriteria{
		LocalChainWork:               localChainWork,
		FullDeliveryFreshnessWindow:  24 * time.Hour,
		UnprovenProbeBudgetRemaining: 3,
	}
}

func withValidatedWork(p *blockchain.PeerInfo, height uint32, work []byte) *blockchain.PeerInfo {
	p.ValidatedHeight = height
	p.ValidatedBlockHash = selectorTestHashNoFail()
	p.ValidatedChainWork = work
	return p
}

func withRecentFullBlockDelivery(p *blockchain.PeerInfo) *blockchain.PeerInfo {
	p.BlocksReceived = 1
	p.LastBlockTime = time.Now()
	return p
}

func TestPeerSelector_SelectSyncPeer_PrefersFullNode(t *testing.T) {
	ps := newSelectorForTest()

	peers := []*blockchain.PeerInfo{
		newPeer("a", 100, "pruned", 90, 0),
		newPeer("b", 100, "full", 60, 0),
	}

	got := ps.SelectSyncPeer(peers, advertisedProbeCriteria(50))
	require.Equal(t, "b", got, "full storage must beat pruned regardless of reputation")
}

func TestPeerSelector_SelectSyncPeer_FallbackToPrunedUsesValidatedTieBreakers(t *testing.T) {
	ps := newSelectorForTest()

	peers := []*blockchain.PeerInfo{
		newPeer("high", 200, "pruned", 80, 0),
		newPeer("low", 100, "pruned", 80, 0),
	}
	peers[1].ValidatedHeight = 10

	got := ps.SelectSyncPeer(peers, advertisedProbeCriteria(50))
	require.Equal(t, "low", got)
}

func TestPeerSelector_SelectSyncPeer_ForcedPeerSticky(t *testing.T) {
	ps := newSelectorForTest()

	peers := []*blockchain.PeerInfo{
		newPeer("forced-id", 1, "pruned", 0, 999),
		newPeer("better-id", 100, "full", 99, 0),
	}

	got := ps.SelectSyncPeer(peers, SelectionCriteria{
		LocalHeight:  0,
		ForcedPeerID: "forced-id",
	})
	require.Equal(t, "forced-id", got, "forced peer overrides eligibility filters")
}

func TestPeerSelector_SelectSyncPeer_ForcedPeerNotConnected(t *testing.T) {
	ps := newSelectorForTest()

	peers := []*blockchain.PeerInfo{
		newPeer("a", 100, "full", 90, 0),
	}

	got := ps.SelectSyncPeer(peers, SelectionCriteria{
		LocalHeight:  0,
		ForcedPeerID: "missing",
	})
	require.Empty(t, got, "missing forced peer means no selection, not fallback")
}

func TestPeerSelector_SelectSyncPeer_PreviousPeerSecondChoiceWhenTopMatches(t *testing.T) {
	ps := newSelectorForTest()

	peers := []*blockchain.PeerInfo{
		newPeer("a", 100, "full", 90, 0),
		newPeer("b", 100, "full", 80, 0),
	}

	criteria := advertisedProbeCriteria(50)
	criteria.PreviousPeer = "a"
	got := ps.SelectSyncPeer(peers, criteria)
	require.Equal(t, "b", got, "rotate off the previous peer if it would be top again")
}

func TestPeerSelector_SelectSyncPeer_SkipsLowReputation(t *testing.T) {
	ps := newSelectorForTest()

	peers := []*blockchain.PeerInfo{
		newPeer("low-rep", 100, "full", 10, 0),
		newPeer("ok", 100, "full", 50, 0),
	}

	got := ps.SelectSyncPeer(peers, advertisedProbeCriteria(50))
	require.Equal(t, "ok", got)
}

func TestPeerSelector_SelectSyncPeer_RejectsZeroHeight(t *testing.T) {
	ps := newSelectorForTest()

	peers := []*blockchain.PeerInfo{
		newPeer("zero", 0, "full", 90, 0),
	}

	got := ps.SelectSyncPeer(peers, advertisedProbeCriteria(0))
	require.Empty(t, got, "peer with zero height is never eligible")
}

func TestPeerSelector_SelectSyncPeer_SyncCooldownExcludesRecentlyAttempted(t *testing.T) {
	ps := newSelectorForTest()

	peers := []*blockchain.PeerInfo{
		{
			ID:              "recent",
			Height:          100,
			Storage:         "full",
			ReputationScore: 80,
			DataHubURL:      "http://recent.example",
			BlockHash:       selectorTestHashNoFail(),
			LastSyncAttempt: time.Now().Add(-30 * time.Second),
		},
		newPeer("fresh", 100, "full", 70, 0),
	}

	criteria := advertisedProbeCriteria(50)
	criteria.SyncAttemptCooldown = time.Minute
	got := ps.SelectSyncPeer(peers, criteria)
	require.Equal(t, "fresh", got, "peer within cooldown must be skipped")
}

func TestPeerSelector_SelectSyncPeer_PrunedFallbackDisabled(t *testing.T) {
	ps := NewPeerSelector(ulogger.TestLogger{}, &settings.Settings{
		P2P: settings.P2PSettings{
			AllowPrunedNodeFallback: false,
		},
	})

	peers := []*blockchain.PeerInfo{newPeer("p", 100, "pruned", 80, 0)}

	got := ps.SelectSyncPeer(peers, advertisedProbeCriteria(50))
	require.Empty(t, got, "no fallback, no full node, no peer")
}

func TestPeerSelector_SelectSyncPeer_TieBreakOnAvgResponseTime(t *testing.T) {
	ps := newSelectorForTest()

	peers := []*blockchain.PeerInfo{
		{
			ID: "fast", Height: 100, Storage: "full", ReputationScore: 80,
			DataHubURL: "http://fast.example", BlockHash: selectorTestHashNoFail(), AvgResponseTimeMs: 50,
		},
		{
			ID: "slow", Height: 100, Storage: "full", ReputationScore: 80,
			DataHubURL: "http://slow.example", BlockHash: selectorTestHashNoFail(), AvgResponseTimeMs: 500,
		},
	}

	got := ps.SelectSyncPeer(peers, advertisedProbeCriteria(50))
	require.Equal(t, "fast", got)
}

func TestPeerSelector_SelectSyncPeer_PrefersRecentFullBlockDeliveryOverHeaderOnly(t *testing.T) {
	ps := newSelectorForTest()

	proven := withRecentFullBlockDelivery(withValidatedWork(newPeer("proven", 100, "full", 80, 0), 100, []byte{0x03}))
	headerOnly := withValidatedWork(newPeer("header-only", 1_000, "full", 90, 0), 1_000, []byte{0x05})

	got := ps.SelectSyncPeer([]*blockchain.PeerInfo{headerOnly, proven}, validatedWorkCriteria([]byte{0x01}))
	require.Equal(t, "proven", got)
}

func TestPeerSelector_SelectSyncPeer_PrefersHigherValidatedWorkWithinDeliveryTier(t *testing.T) {
	ps := newSelectorForTest()

	lowerWork := withRecentFullBlockDelivery(withValidatedWork(newPeer("lower", 200, "full", 80, 0), 200, []byte{0x03}))
	higherWork := withRecentFullBlockDelivery(withValidatedWork(newPeer("higher", 100, "full", 80, 0), 100, []byte{0x05}))

	got := ps.SelectSyncPeer([]*blockchain.PeerInfo{lowerWork, higherWork}, validatedWorkCriteria([]byte{0x01}))
	require.Equal(t, "higher", got)
}

func TestPeerSelector_SelectSyncPeer_RejectsNoValidatedWorkWhenProbeDisabled(t *testing.T) {
	ps := newSelectorForTest()

	peers := []*blockchain.PeerInfo{newPeer("advertised", 100, "full", 80, 0)}

	got := ps.SelectSyncPeer(peers, SelectionCriteria{
		LocalHeight:                 50,
		LocalChainWork:              []byte{0x01},
		FullDeliveryFreshnessWindow: 24 * time.Hour,
	})
	require.Empty(t, got)
}

func TestPeerSelector_SelectSyncPeer_UsesValidatedHeightOnlyAsTieBreaker(t *testing.T) {
	ps := newSelectorForTest()

	lowerValidatedHeight := withValidatedWork(newPeer("low-validated", 1_000, "full", 80, 0), 100, []byte{0x03})
	higherValidatedHeight := withValidatedWork(newPeer("high-validated", 100, "full", 80, 0), 200, []byte{0x03})

	got := ps.SelectSyncPeer([]*blockchain.PeerInfo{lowerValidatedHeight, higherValidatedHeight}, validatedWorkCriteria([]byte{0x01}))
	require.Equal(t, "high-validated", got)
}

func TestPeerSelector_SelectSyncPeer_StoragePenaltyDemotesFullClaim(t *testing.T) {
	ps := newSelectorForTest()

	penalizedFull := withValidatedWork(newPeer("penalized", 200, "full", 90, 0), 200, []byte{0x05})
	penalizedFull.FullStoragePenaltyUntil = time.Now().Add(time.Hour)
	full := withValidatedWork(newPeer("full", 100, "full", 80, 0), 100, []byte{0x03})

	got := ps.SelectSyncPeer([]*blockchain.PeerInfo{penalizedFull, full}, validatedWorkCriteria([]byte{0x01}))
	require.Equal(t, "full", got)
}

func TestPeerSelector_SelectSyncPeer_UnprovenProbeBudgetBoundsHeaderOnlyPeers(t *testing.T) {
	ps := newSelectorForTest()
	peers := []*blockchain.PeerInfo{
		newPeer("probe-a", 100, "full", 80, 0),
		newPeer("probe-b", 101, "full", 80, 0),
	}

	noBudget := advertisedProbeCriteria(50)
	noBudget.UnprovenProbeBudgetRemaining = 0
	require.Empty(t, ps.SelectSyncPeer(peers, noBudget))

	boundedBudget := advertisedProbeCriteria(50)
	boundedBudget.UnprovenProbeBudgetRemaining = 3
	require.NotEmpty(t, ps.SelectSyncPeer(peers, boundedBudget))
}
