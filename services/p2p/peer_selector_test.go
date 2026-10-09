package p2p

import (
	"context"
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

	got := ps.SelectSyncPeer(context.Background(), peers, advertisedProbeCriteria(50))
	require.Equal(t, "b", got, "full storage must beat pruned regardless of reputation")
}

func TestPeerSelector_SelectSyncPeer_FallbackToPrunedUsesValidatedTieBreakers(t *testing.T) {
	ps := newSelectorForTest()

	peers := []*blockchain.PeerInfo{
		newPeer("high", 200, "pruned", 80, 0),
		newPeer("low", 100, "pruned", 80, 0),
	}
	peers[1].ValidatedHeight = 10

	got := ps.SelectSyncPeer(context.Background(), peers, advertisedProbeCriteria(50))
	require.Equal(t, "low", got)
}

func TestPeerSelector_SelectSyncPeer_ForcedPeerSticky(t *testing.T) {
	ps := newSelectorForTest()

	peers := []*blockchain.PeerInfo{
		newPeer("forced-id", 1, "pruned", 0, 999),
		newPeer("better-id", 100, "full", 99, 0),
	}

	got := ps.SelectSyncPeer(context.Background(), peers, SelectionCriteria{
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

	got := ps.SelectSyncPeer(context.Background(), peers, SelectionCriteria{
		LocalHeight:  0,
		ForcedPeerID: "missing",
	})
	require.Empty(t, got, "missing forced peer means no selection, not fallback")
}

// A failed previous peer is rotated off even when it still ranks top, so the
// coordinator gets a genuine replacement.
func TestPeerSelector_SelectSyncPeer_FailedPreviousPeerRotatedOffWhenTopMatches(t *testing.T) {
	ps := newSelectorForTest()

	peers := []*blockchain.PeerInfo{
		newPeer("a", 100, "full", 90, 0),
		newPeer("b", 100, "full", 80, 0),
	}

	criteria := advertisedProbeCriteria(50)
	criteria.PreviousPeer = "a"
	criteria.PreviousPeerFailed = true
	got := ps.SelectSyncPeer(context.Background(), peers, criteria)
	require.Equal(t, "b", got, "a failed previous peer must be rotated off even if it would be top again")
}

// Hysteresis: a previous peer that is still top-ranked is kept, so a healthy
// sync peer is not swapped out (and catchup re-triggered) for no gain.
func TestPeerSelector_SelectSyncPeer_PreviousPeerKeptWhenStillTop(t *testing.T) {
	ps := newSelectorForTest()

	peers := []*blockchain.PeerInfo{
		newPeer("a", 100, "full", 90, 0),
		newPeer("b", 100, "full", 80, 0),
	}

	criteria := advertisedProbeCriteria(50)
	criteria.PreviousPeer = "a"
	require.Equal(t, "a", ps.SelectSyncPeer(context.Background(), peers, criteria))
}

// Hysteresis must not pin a materially worse incumbent: a challenger outside the
// incumbent's band still wins.
func TestPeerSelector_SelectSyncPeer_MateriallyBetterChallengerBeatsPreviousPeer(t *testing.T) {
	ps := newSelectorForTest()

	peers := []*blockchain.PeerInfo{
		newPeer("weak-incumbent", 100, "full", 40, 0),
		newPeer("strong", 100, "full", 95, 0),
	}

	criteria := advertisedProbeCriteria(50)
	criteria.PreviousPeer = "weak-incumbent"
	require.Equal(t, "strong", ps.SelectSyncPeer(context.Background(), peers, criteria),
		"a materially better challenger must take the slot from the incumbent")
}

// Two peers that are equal on every criterion must not alternate the sync slot
// cycle after cycle (issue: deterministic selection oscillates A<->B).
func TestPeerSelector_SelectSyncPeer_EqualCandidatesDoNotOscillate(t *testing.T) {
	ps := newSelectorForTest()

	criteria := advertisedProbeCriteria(50)
	first := ps.SelectSyncPeer(context.Background(), []*blockchain.PeerInfo{
		newPeer("a", 100, "full", 80, 0),
		newPeer("b", 100, "full", 80, 0),
	}, criteria)
	require.Contains(t, []string{"a", "b"}, first)

	for range 20 {
		criteria.PreviousPeer = first
		got := ps.SelectSyncPeer(context.Background(), []*blockchain.PeerInfo{
			newPeer("a", 100, "full", 80, 0),
			newPeer("b", 100, "full", 80, 0),
		}, criteria)
		require.Equal(t, first, got, "equal candidates must not alternate the sync slot absent a failure")
	}
}

// Near-equal (not exactly tied) candidates are also non-oscillating and share
// the load: no single peer wins every fresh selection.
func TestPeerSelector_SelectSyncPeer_NearEqualCandidatesShareLoad(t *testing.T) {
	ps := newSelectorForTest()

	newNearEqualPeers := func() []*blockchain.PeerInfo {
		// Reputation spread of 4 and response times within 25ms / 25% keep all
		// three inside one band even though none is an exact tie.
		a := newPeer("near-a", 100, "full", 80, 0)
		a.AvgResponseTimeMs = 100
		b := newPeer("near-b", 100, "full", 82, 1)
		b.AvgResponseTimeMs = 110
		c := newPeer("near-c", 100, "full", 84, 2)
		c.AvgResponseTimeMs = 120
		return []*blockchain.PeerInfo{a, b, c}
	}

	// Fresh nodes (no previous peer) must spread over the whole band instead of
	// herding onto the single highest-reputation peer.
	// P(some band member never wins in 100 rounds) <= 3 * (2/3)^100 ~ 7e-18.
	wins := map[string]int{}
	for range 100 {
		wins[ps.SelectSyncPeer(context.Background(), newNearEqualPeers(), advertisedProbeCriteria(50))]++
	}
	require.Len(t, wins, 3, "every near-equal peer must win at least once, got %v", wins)

	// An established node keeps its incumbent even though it is not the
	// nominally best peer in the band.
	criteria := advertisedProbeCriteria(50)
	criteria.PreviousPeer = "near-a"
	for range 20 {
		require.Equal(t, "near-a", ps.SelectSyncPeer(context.Background(), newNearEqualPeers(), criteria))
	}
}

func TestPeerSelector_PeersNearEqual(t *testing.T) {
	ps := newSelectorForTest()
	now := time.Now()
	window := 24 * time.Hour

	base := newPeer("base", 100, "full", 80, 0)
	base.AvgResponseTimeMs = 100

	withResponse := func(p *blockchain.PeerInfo, ms int64) *blockchain.PeerInfo {
		p.AvgResponseTimeMs = ms
		return p
	}

	tests := []struct {
		name  string
		other *blockchain.PeerInfo
		want  bool
	}{
		{"identical", withResponse(newPeer("x", 100, "full", 80, 0), 100), true},
		{"reputation within margin", withResponse(newPeer("x", 100, "full", 85, 0), 100), true},
		{"reputation beyond margin", withResponse(newPeer("x", 100, "full", 86, 0), 100), false},
		{"ban score within margin", withResponse(newPeer("x", 100, "full", 80, 5), 100), true},
		{"ban score beyond margin", withResponse(newPeer("x", 100, "full", 80, 6), 100), false},
		{"response time within absolute margin", withResponse(newPeer("x", 100, "full", 80, 0), 124), true},
		{"response time within relative margin", withResponse(newPeer("x", 100, "full", 80, 0), 125), true},
		{"response time beyond both margins", withResponse(newPeer("x", 100, "full", 80, 0), 126), false},
		{"unmeasured response time", newPeer("x", 100, "full", 80, 0), false},
		{"different validated work", withResponse(withValidatedWork(newPeer("x", 100, "full", 80, 0), 100, []byte{0x05}), 100), false},
		{"different validated height", withResponse(withValidatedWork(newPeer("x", 100, "full", 80, 0), 101, nil), 100), false},
		{"proven delivery differs", withResponse(withRecentFullBlockDelivery(newPeer("x", 100, "full", 80, 0)), 100), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, ps.peersNearEqual(base, tt.other, now, window))
			require.Equal(t, tt.want, ps.peersNearEqual(tt.other, base, now, window), "must be symmetric")
		})
	}

	require.True(t, ps.peersNearEqual(base, base, now, window), "must be reflexive")
}

func TestPeerSelector_SelectSyncPeer_SkipsLowReputation(t *testing.T) {
	ps := newSelectorForTest()

	peers := []*blockchain.PeerInfo{
		newPeer("low-rep", 100, "full", 10, 0),
		newPeer("ok", 100, "full", 50, 0),
	}

	got := ps.SelectSyncPeer(context.Background(), peers, advertisedProbeCriteria(50))
	require.Equal(t, "ok", got)
}

func TestPeerSelector_SelectSyncPeer_RejectsZeroHeight(t *testing.T) {
	ps := newSelectorForTest()

	peers := []*blockchain.PeerInfo{
		newPeer("zero", 0, "full", 90, 0),
	}

	got := ps.SelectSyncPeer(context.Background(), peers, advertisedProbeCriteria(0))
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
	got := ps.SelectSyncPeer(context.Background(), peers, criteria)
	require.Equal(t, "fresh", got, "peer within cooldown must be skipped")
}

// The retry cooldown must not evict the incumbent from its own selection: every
// activation records a sync attempt, so applying the cooldown to the incumbent
// would rotate the slot on each evaluation inside the cooldown window.
func TestPeerSelector_SelectSyncPeer_SyncCooldownExemptsHealthyIncumbent(t *testing.T) {
	ps := newSelectorForTest()

	newPeers := func() []*blockchain.PeerInfo {
		incumbent := newPeer("incumbent", 100, "full", 80, 0)
		incumbent.LastSyncAttempt = time.Now().Add(-30 * time.Second)
		return []*blockchain.PeerInfo{incumbent, newPeer("fresh", 100, "full", 80, 0)}
	}

	criteria := advertisedProbeCriteria(50)
	criteria.SyncAttemptCooldown = time.Minute
	criteria.PreviousPeer = "incumbent"
	require.Equal(t, "incumbent", ps.SelectSyncPeer(context.Background(), newPeers(), criteria),
		"a healthy incumbent must not be excluded by its own sync-attempt cooldown")

	criteria.PreviousPeerFailed = true
	require.Equal(t, "fresh", ps.SelectSyncPeer(context.Background(), newPeers(), criteria),
		"a failed incumbent stays subject to the cooldown")
}

func TestPeerSelector_SelectSyncPeer_PrunedFallbackDisabled(t *testing.T) {
	ps := NewPeerSelector(ulogger.TestLogger{}, &settings.Settings{
		P2P: settings.P2PSettings{
			AllowPrunedNodeFallback: false,
		},
	})

	peers := []*blockchain.PeerInfo{newPeer("p", 100, "pruned", 80, 0)}

	got := ps.SelectSyncPeer(context.Background(), peers, advertisedProbeCriteria(50))
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

	got := ps.SelectSyncPeer(context.Background(), peers, advertisedProbeCriteria(50))
	require.Equal(t, "fast", got)
}

func TestPeerSelector_SelectSyncPeer_PrefersRecentFullBlockDeliveryOverHeaderOnly(t *testing.T) {
	ps := newSelectorForTest()

	proven := withRecentFullBlockDelivery(withValidatedWork(newPeer("proven", 100, "full", 80, 0), 100, []byte{0x03}))
	headerOnly := withValidatedWork(newPeer("header-only", 1_000, "full", 90, 0), 1_000, []byte{0x05})

	got := ps.SelectSyncPeer(context.Background(), []*blockchain.PeerInfo{headerOnly, proven}, validatedWorkCriteria([]byte{0x01}))
	require.Equal(t, "proven", got)
}

func TestPeerSelector_SelectSyncPeer_PrefersHigherValidatedWorkWithinDeliveryTier(t *testing.T) {
	ps := newSelectorForTest()

	lowerWork := withRecentFullBlockDelivery(withValidatedWork(newPeer("lower", 200, "full", 80, 0), 200, []byte{0x03}))
	higherWork := withRecentFullBlockDelivery(withValidatedWork(newPeer("higher", 100, "full", 80, 0), 100, []byte{0x05}))

	got := ps.SelectSyncPeer(context.Background(), []*blockchain.PeerInfo{lowerWork, higherWork}, validatedWorkCriteria([]byte{0x01}))
	require.Equal(t, "higher", got)
}

func TestPeerSelector_SelectSyncPeer_RejectsNoValidatedWorkWhenProbeDisabled(t *testing.T) {
	ps := newSelectorForTest()

	peers := []*blockchain.PeerInfo{newPeer("advertised", 100, "full", 80, 0)}

	got := ps.SelectSyncPeer(context.Background(), peers, SelectionCriteria{
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

	got := ps.SelectSyncPeer(context.Background(), []*blockchain.PeerInfo{lowerValidatedHeight, higherValidatedHeight}, validatedWorkCriteria([]byte{0x01}))
	require.Equal(t, "high-validated", got)
}

func TestPeerSelector_SelectSyncPeer_StoragePenaltyDemotesFullClaim(t *testing.T) {
	ps := newSelectorForTest()

	penalizedFull := withValidatedWork(newPeer("penalized", 200, "full", 90, 0), 200, []byte{0x05})
	penalizedFull.FullStoragePenaltyUntil = time.Now().Add(time.Hour)
	full := withValidatedWork(newPeer("full", 100, "full", 80, 0), 100, []byte{0x03})

	got := ps.SelectSyncPeer(context.Background(), []*blockchain.PeerInfo{penalizedFull, full}, validatedWorkCriteria([]byte{0x01}))
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

		got := ps.SelectSyncPeer(context.Background(), peers, advertisedProbeCriteria(50))
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
		wins[ps.SelectSyncPeer(context.Background(), peers, advertisedProbeCriteria(50))]++
	}
	for _, id := range ids {
		require.Positive(t, wins[id], "every tied peer must win at least once, got %v", wins)
	}
	require.Less(t, wins[ids[0]], rounds, "attacker with lexicographically smallest ID must not win every selection")
}

func TestPeerSelector_SelectSyncPeer_FailedPreviousPeerExcludedFromTiedTopBand(t *testing.T) {
	ps := newSelectorForTest()

	// Failed previous peer ties with two other candidates, so after it is
	// excluded the random draw still runs over a band of two; it must never be
	// re-drawn.
	for range 50 {
		peers := []*blockchain.PeerInfo{
			newPeer("prev", 100, "full", 80, 0),
			newPeer("other-a", 100, "full", 80, 0),
			newPeer("other-b", 100, "full", 80, 0),
		}
		criteria := advertisedProbeCriteria(50)
		criteria.PreviousPeer = "prev"
		criteria.PreviousPeerFailed = true
		got := ps.SelectSyncPeer(context.Background(), peers, criteria)
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
	criteria.PreviousPeerFailed = true
	require.Equal(t, "prev", ps.SelectSyncPeer(context.Background(), peers, criteria), "sole candidate is selected even if it was the failed previous peer")
}

func TestPeerSelector_SelectSyncPeer_UnprovenProbeBudgetBoundsHeaderOnlyPeers(t *testing.T) {
	ps := newSelectorForTest()
	peers := []*blockchain.PeerInfo{
		newPeer("probe-a", 100, "full", 80, 0),
		newPeer("probe-b", 101, "full", 80, 0),
	}

	noBudget := advertisedProbeCriteria(50)
	noBudget.UnprovenProbeBudgetRemaining = 0
	require.Empty(t, ps.SelectSyncPeer(context.Background(), peers, noBudget))

	boundedBudget := advertisedProbeCriteria(50)
	boundedBudget.UnprovenProbeBudgetRemaining = 3
	require.NotEmpty(t, ps.SelectSyncPeer(context.Background(), peers, boundedBudget))
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

	got := ps.SelectSyncPeer(context.Background(), []*blockchain.PeerInfo{bad, good}, advertisedProbeCriteria(50))
	require.Equal(t, "good", got, "peer with blacklisted DataHub URL must not win selection")

	got = ps.SelectSyncPeer(context.Background(), []*blockchain.PeerInfo{bad}, advertisedProbeCriteria(50))
	require.Empty(t, got, "a blacklisted peer must not be selected even as the only candidate")
}
