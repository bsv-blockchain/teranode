package p2p

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

func newSelectorForTest() *PeerSelector {
	return NewPeerSelector(ulogger.TestLogger{}, &settings.Settings{
		P2P: settings.P2PSettings{
			AllowPrunedNodeFallback: true,
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
	}
}

func TestPeerSelector_SelectSyncPeer_PrefersFullNode(t *testing.T) {
	ps := newSelectorForTest()

	peers := []*blockchain.PeerInfo{
		newPeer("a", 100, "pruned", 90, 0),
		newPeer("b", 100, "full", 60, 0),
	}

	got := ps.SelectSyncPeer(peers, SelectionCriteria{LocalHeight: 50})
	require.Equal(t, "b", got, "full storage must beat pruned regardless of reputation")
}

func TestPeerSelector_SelectSyncPeer_FallbackToPrunedPrefersLowerHeight(t *testing.T) {
	ps := newSelectorForTest()

	// Same reputation so the height tiebreaker decides. Pruned-mode prefers
	// LOWER height (younger pruning window) per existing logic.
	peers := []*blockchain.PeerInfo{
		newPeer("high", 200, "pruned", 80, 0),
		newPeer("low", 100, "pruned", 80, 0),
	}

	got := ps.SelectSyncPeer(peers, SelectionCriteria{LocalHeight: 50})
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

	got := ps.SelectSyncPeer(peers, SelectionCriteria{LocalHeight: 50, PreviousPeer: "a"})
	require.Equal(t, "b", got, "rotate off the previous peer if it would be top again")
}

func TestPeerSelector_SelectSyncPeer_SkipsLowReputation(t *testing.T) {
	ps := newSelectorForTest()

	peers := []*blockchain.PeerInfo{
		newPeer("low-rep", 100, "full", 10, 0),
		newPeer("ok", 100, "full", 50, 0),
	}

	got := ps.SelectSyncPeer(peers, SelectionCriteria{LocalHeight: 50})
	require.Equal(t, "ok", got)
}

func TestPeerSelector_SelectSyncPeer_RejectsZeroHeight(t *testing.T) {
	ps := newSelectorForTest()

	peers := []*blockchain.PeerInfo{
		newPeer("zero", 0, "full", 90, 0),
	}

	got := ps.SelectSyncPeer(peers, SelectionCriteria{LocalHeight: 0})
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
			LastSyncAttempt: time.Now().Add(-30 * time.Second),
		},
		newPeer("fresh", 100, "full", 70, 0),
	}

	got := ps.SelectSyncPeer(peers, SelectionCriteria{
		LocalHeight:         50,
		SyncAttemptCooldown: time.Minute,
	})
	require.Equal(t, "fresh", got, "peer within cooldown must be skipped")
}

func TestPeerSelector_SelectSyncPeer_PrunedFallbackDisabled(t *testing.T) {
	ps := NewPeerSelector(ulogger.TestLogger{}, &settings.Settings{
		P2P: settings.P2PSettings{
			AllowPrunedNodeFallback: false,
		},
	})

	peers := []*blockchain.PeerInfo{newPeer("p", 100, "pruned", 80, 0)}

	got := ps.SelectSyncPeer(peers, SelectionCriteria{LocalHeight: 50})
	require.Empty(t, got, "no fallback, no full node, no peer")
}

func TestPeerSelector_SelectSyncPeer_TieBreakOnAvgResponseTime(t *testing.T) {
	ps := newSelectorForTest()

	peers := []*blockchain.PeerInfo{
		{
			ID: "fast", Height: 100, Storage: "full", ReputationScore: 80,
			DataHubURL: "http://fast.example", AvgResponseTimeMs: 50,
		},
		{
			ID: "slow", Height: 100, Storage: "full", ReputationScore: 80,
			DataHubURL: "http://slow.example", AvgResponseTimeMs: 500,
		},
	}

	got := ps.SelectSyncPeer(peers, SelectionCriteria{LocalHeight: 50})
	require.Equal(t, "fast", got)
}
