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

func TestPeerSelector_SelectFromCandidates_RandomTiebreakCoversWholeTopBand(t *testing.T) {
	ps := newSelectorForTest()

	// Three peers tied on every merit criterion, plus one strictly worse peer.
	// Stubbing the random source proves each band member is reachable and the
	// worse peer never is.
	tied := []string{"tied-1", "tied-2", "tied-3"}
	seen := map[string]bool{}
	for want := range len(tied) {
		peers := []*blockchain.PeerInfo{
			newPeer("tied-2", 100, "full", 80, 0),
			newPeer("tied-3", 100, "full", 80, 0),
			newPeer("tied-1", 100, "full", 80, 0),
			newPeer("worse", 100, "full", 50, 0),
		}

		ps.randIntN = func(n int) int {
			require.Equal(t, 3, n, "top band must contain exactly the three tied peers")
			return want
		}

		got := ps.SelectSyncPeer(peers, advertisedProbeCriteria(50))
		require.Contains(t, tied, got)
		require.NotEqual(t, "worse", got)
		seen[got] = true
	}
	require.Len(t, seen, len(tied), "every band member must be reachable via the random index")
}

func TestPeerSelector_SelectSyncPeer_GrindableIDCannotCaptureSelection(t *testing.T) {
	ps := newSelectorForTest()

	// The attacker ID sorts lexicographically before every honest ID. With the
	// old ID tiebreak it would win 100% of the time; with a uniform random
	// tiebreak every one of the 4 tied peers must win at least once over 200
	// rounds. P(some peer never wins) <= 4 * 0.75^200 ~ 1e-24, so this cannot
	// flake, and it also catches any fixed-index selection, not just index 0.
	ids := []string{"000000-ground-attacker-id", "honest-a", "honest-b", "honest-c"}
	wins := map[string]int{}
	const rounds = 200
	for range rounds {
		peers := []*blockchain.PeerInfo{
			newPeer(ids[0], 100, "full", 80, 0),
			newPeer(ids[1], 100, "full", 80, 0),
			newPeer(ids[2], 100, "full", 80, 0),
			newPeer(ids[3], 100, "full", 80, 0),
		}
		wins[ps.SelectSyncPeer(peers, advertisedProbeCriteria(50))]++
	}
	for _, id := range ids {
		require.Positive(t, wins[id], "every tied peer must win at least once, got %v", wins)
	}
	require.Less(t, wins[ids[0]], rounds, "attacker with lexicographically smallest ID must not win every selection")
}

func TestPeerSelector_SelectSyncPeer_PreviousPeerExcludedFromTiedTopBand(t *testing.T) {
	ps := newSelectorForTest()

	// Previous peer ties with two other candidates, so after it is excluded
	// the random draw still runs over a band of two; it must never be re-drawn.
	for range 50 {
		peers := []*blockchain.PeerInfo{
			newPeer("prev", 100, "full", 80, 0),
			newPeer("other-a", 100, "full", 80, 0),
			newPeer("other-b", 100, "full", 80, 0),
		}
		criteria := advertisedProbeCriteria(50)
		criteria.PreviousPeer = "prev"
		got := ps.SelectSyncPeer(peers, criteria)
		require.Contains(t, []string{"other-a", "other-b"}, got)
	}
}

func TestPeerSelector_ComparePeerCandidates_Antisymmetric(t *testing.T) {
	ps := newSelectorForTest()
	now := time.Now()
	window := 24 * time.Hour

	// Peers varying one criterion at a time plus mixed combinations; the
	// hand-written three-way comparator must satisfy compare(a,b) == -compare(b,a)
	// and compare(a,a) == 0 for sort.Slice and the band scan to be sound.
	peers := []*blockchain.PeerInfo{
		newPeer("base", 100, "full", 80, 0),
		withRecentFullBlockDelivery(newPeer("proven", 100, "full", 80, 0)),
		withValidatedWork(newPeer("more-work", 100, "full", 80, 0), 100, []byte{0x05}),
		withValidatedWork(newPeer("less-work", 100, "full", 80, 0), 100, []byte{0x03}),
		newPeer("high-rep", 100, "full", 95, 0),
		newPeer("banned-ish", 100, "full", 80, 30),
		withValidatedWork(newPeer("tall", 100, "full", 80, 0), 500, []byte{0x03}),
		withRecentFullBlockDelivery(withValidatedWork(newPeer("mixed", 100, "full", 60, 10), 200, []byte{0x04})),
	}
	peers[4].AvgResponseTimeMs = 50
	peers[5].AvgResponseTimeMs = 500

	for _, a := range peers {
		for _, b := range peers {
			ab := ps.comparePeerCandidates(a, b, now, window)
			ba := ps.comparePeerCandidates(b, a, now, window)
			require.Equal(t, -ba, ab, "compare(%s,%s) must be antisymmetric", a.ID, b.ID)
		}
		require.Zero(t, ps.comparePeerCandidates(a, a, now, window), "compare(%s,%s) must be 0", a.ID, a.ID)
	}
}

func TestPeerSelector_SelectSyncPeer_PreviousPeerKeptWhenOnlyCandidate(t *testing.T) {
	ps := newSelectorForTest()

	peers := []*blockchain.PeerInfo{newPeer("prev", 100, "full", 80, 0)}
	criteria := advertisedProbeCriteria(50)
	criteria.PreviousPeer = "prev"
	require.Equal(t, "prev", ps.SelectSyncPeer(peers, criteria), "sole candidate is selected even if it was the previous peer")
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

// TestPeerSelector_SelectSyncPeer_SkipsBlacklistedDataHubURL: the operator
// blacklist is enforced at sync-peer selection so a DataHub URL stored in the
// registry before its host was blacklisted can never be chosen for catchup.
func TestPeerSelector_SelectSyncPeer_SkipsBlacklistedDataHubURL(t *testing.T) {
	ps := NewPeerSelector(ulogger.TestLogger{}, &settings.Settings{
		P2P: settings.P2PSettings{
			AllowPrunedNodeFallback:     true,
			FullDeliveryFreshnessWindow: 24 * time.Hour,
		},
		SubtreeValidation: settings.SubtreeValidationSettings{
			BlacklistedBaseURLs: map[string]struct{}{"http://evil.example": {}},
		},
	})

	good := newPeer("good", 100, "full", 60, 0)
	bad := newPeer("bad", 200, "full", 90, 0)
	bad.DataHubURL = "http://evil.example:8080/api" // same host as blacklist entry

	got := ps.SelectSyncPeer([]*blockchain.PeerInfo{bad, good}, advertisedProbeCriteria(50))
	require.Equal(t, "good", got, "peer with blacklisted DataHub URL must not win selection")

	got = ps.SelectSyncPeer([]*blockchain.PeerInfo{bad}, advertisedProbeCriteria(50))
	require.Empty(t, got, "a blacklisted peer must not be selected even as the only candidate")
}
