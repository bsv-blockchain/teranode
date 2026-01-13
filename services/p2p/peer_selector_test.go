package p2p

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/assert"
)

const (
	testPeer1 = "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ"
	testPeer2 = "12D3KooWEyX7hgdXy8zUjCs9CqvMGpB5dKVFj9MX2nUBLwajdSZH"
	testPeer3 = "12D3KooWQYVQJfrw4RZnNHgRxGFLXoXswE5wuoUBgWpeJYeGDjvA"
	testPeer4 = "12D3KooWB9kmtfHg5Ct1Sj5DX6fmqRnatrXnE5zMRg25d6rbwLzp"
)

func TestPeerSelector_SelectSyncPeer_NoPeers(t *testing.T) {
	logger := ulogger.New("test")
	ps := NewPeerSelector(logger, nil)

	// Empty peer list
	selected := ps.SelectSyncPeer([]*PeerInfo{}, SelectionCriteria{
		LocalHeight: 100,
	})

	assert.Equal(t, peer.ID(""), selected, "Should return empty peer ID when no peers")
}

func TestSelector_SkipsPeerMarkedUnhealthyByHealthChecker(t *testing.T) {
	logger := ulogger.New("test")
	ps := NewPeerSelector(logger, nil)

	// Health check servers: one OK, one 500
	okSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer okSrv.Close()

	failSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer failSrv.Close()

	// Registry
	registry := NewPeerRegistry()

	// Add two peers
	healthyID := peer.ID("H")
	unhealthyID := peer.ID("U")
	registry.AddPeer(healthyID, "")
	registry.AddPeer(unhealthyID, "")
	// Set heights so both are ahead
	registry.UpdateHeight(healthyID, 120, "hashH")
	registry.UpdateHeight(unhealthyID, 125, "hashU")
	// Assign DataHub URLs
	registry.UpdateDataHubURL(healthyID, okSrv.URL)
	registry.UpdateDataHubURL(unhealthyID, failSrv.URL)
	// Mark URLs responsive to satisfy selector
	registry.UpdateURLResponsiveness(healthyID, true)
	registry.UpdateURLResponsiveness(unhealthyID, true)

	// Run immediate health checks

	// Fetch peers and select
	peers := registry.GetAllPeers()
	selected := ps.SelectSyncPeer(peers, SelectionCriteria{LocalHeight: 100})

	assert.Equal(t, healthyID, selected, "selector should skip peer marked unhealthy by health checker")
}

func TestPeerSelector_SelectSyncPeer_NoEligiblePeers(t *testing.T) {
	logger := ulogger.New("test")
	ps := NewPeerSelector(logger, nil)

	// Create peers that are all banned
	peers := []*PeerInfo{
		CreateTestPeerInfo(peer.ID("A"), 110, true, true, ""), // banned
		CreateTestPeerInfo(peer.ID("B"), 120, true, true, ""), // banned
	}

	selected := ps.SelectSyncPeer(peers, SelectionCriteria{
		LocalHeight: 100,
	})

	assert.Equal(t, peer.ID(""), selected, "Should return empty when all peers are banned")
}

func TestPeerSelector_SelectSyncPeer_NoPeersAhead(t *testing.T) {
	logger := ulogger.New("test")
	ps := NewPeerSelector(logger, nil)

	// Create peers that are all behind or at same height
	peers := []*PeerInfo{
		CreateTestPeerInfo(peer.ID("A"), 90, true, false, "http://test.com"),  // behind
		CreateTestPeerInfo(peer.ID("B"), 100, true, false, "http://test.com"), // same height
		CreateTestPeerInfo(peer.ID("C"), 95, true, false, "http://test.com"),  // behind
	}
	// Mark URLs as responsive
	for _, p := range peers {
		p.URLResponsive = true
	}

	selected := ps.SelectSyncPeer(peers, SelectionCriteria{
		LocalHeight: 100,
	})

	assert.Equal(t, peer.ID(""), selected, "Should return empty when no peers are ahead")
}

func TestPeerSelector_SelectSyncPeer_BasicSelection(t *testing.T) {
	logger := ulogger.New("test")
	ps := NewPeerSelector(logger, nil)

	peer1, _ := peer.Decode(testPeer1)
	peer2, _ := peer.Decode(testPeer2)
	peer3, _ := peer.Decode(testPeer3)
	peer4, _ := peer.Decode(testPeer4)

	// Create peers with different heights
	peers := []*PeerInfo{
		CreateTestPeerInfo(peer1, 90, true, false, "http://test.com"),  // behind
		CreateTestPeerInfo(peer2, 110, true, false, "http://test.com"), // ahead
		CreateTestPeerInfo(peer3, 120, true, false, "http://test.com"), // ahead more
		CreateTestPeerInfo(peer4, 100, true, false, "http://test.com"), // same height
	}
	// Mark URLs as responsive
	for _, p := range peers {
		p.URLResponsive = true
	}

	selected := ps.SelectSyncPeer(peers, SelectionCriteria{
		LocalHeight: 100,
	})

	assert.Contains(t, []peer.ID{peer2, peer3}, selected, "Should select a peer that is ahead")
}

func TestPeerSelector_SelectSyncPeer_PreferLowerBanScore(t *testing.T) {
	logger := ulogger.New("test")
	ps := NewPeerSelector(logger, nil)

	// Create peers with different ban scores
	peers := []*PeerInfo{
		{
			ID:              peer.ID("A"),
			Height:          110,
			ReputationScore: 80.0, // Good reputation
			IsBanned:        false,
			BanScore:        50,
			DataHubURL:      "http://test.com",
			URLResponsive:   true,
			Storage:         "full",
		},
		{
			ID:              peer.ID("B"),
			Height:          110,
			ReputationScore: 80.0, // Good reputation
			IsBanned:        false,
			BanScore:        10, // Lower ban score, should be preferred
			DataHubURL:      "http://test.com",
			URLResponsive:   true,
			Storage:         "full",
		},
		{
			ID:              peer.ID("C"),
			Height:          110,
			ReputationScore: 80.0, // Good reputation
			IsBanned:        false,
			BanScore:        30,
			DataHubURL:      "http://test.com",
			URLResponsive:   true,
			Storage:         "full",
		},
	}

	// Run multiple times to account for randomization
	selections := make(map[peer.ID]int)
	for i := 0; i < 100; i++ {
		selected := ps.SelectSyncPeer(peers, SelectionCriteria{
			LocalHeight: 100,
		})
		selections[selected]++
	}

	// Peer B with lowest ban score should be selected most often
	assert.Greater(t, selections[peer.ID("B")], 50, "Peer with lowest ban score should be selected most")
}

func TestPeerSelector_SelectSyncPeer_PreferHigherHeight(t *testing.T) {
	logger := ulogger.New("test")
	ps := NewPeerSelector(logger, nil)

	// Create peers with same ban score but different heights
	peers := []*PeerInfo{
		{
			ID:              peer.ID("A"),
			Height:          110,
			ReputationScore: 80.0, // Good reputation
			IsBanned:        false,
			BanScore:        10,
			DataHubURL:      "http://test.com",
			URLResponsive:   true,
			Storage:         "full",
		},
		{
			ID:              peer.ID("B"),
			Height:          120,  // Higher, should be preferred
			ReputationScore: 80.0, // Good reputation
			IsBanned:        false,
			BanScore:        10,
			DataHubURL:      "http://test.com",
			URLResponsive:   true,
			Storage:         "full",
		},
		{
			ID:              peer.ID("C"),
			Height:          115,
			ReputationScore: 80.0, // Good reputation
			IsBanned:        false,
			BanScore:        10,
			DataHubURL:      "http://test.com",
			URLResponsive:   true,
			Storage:         "full",
		},
	}

	// Since all have same ban score, should select the one with highest height (B)
	selected := ps.SelectSyncPeer(peers, SelectionCriteria{
		LocalHeight: 100,
	})

	assert.Equal(t, peer.ID("B"), selected, "Should select peer with highest height when ban scores are equal")
}

func TestPeerSelector_SelectSyncPeer_RequireHealthy(t *testing.T) {
	logger := ulogger.New("test")
	ps := NewPeerSelector(logger, nil)

	peers := []*PeerInfo{
		CreateTestPeerInfo(peer.ID("A"), 110, false, false, "http://test.com"), // unhealthy
		CreateTestPeerInfo(peer.ID("B"), 120, true, false, "http://test.com"),  // healthy
		CreateTestPeerInfo(peer.ID("C"), 115, false, false, "http://test.com"), // unhealthy
	}
	// Mark URLs as responsive
	for _, p := range peers {
		p.URLResponsive = true
	}

	selected := ps.SelectSyncPeer(peers, SelectionCriteria{
		LocalHeight: 100,
	})

	assert.Equal(t, peer.ID("B"), selected, "Should only select healthy peer")
}

func TestPeerSelector_SelectSyncPeer_RequireDataHub(t *testing.T) {
	logger := ulogger.New("test")
	ps := NewPeerSelector(logger, nil)

	peers := []*PeerInfo{
		CreateTestPeerInfo(peer.ID("A"), 110, true, false, ""),               // no DataHub
		CreateTestPeerInfo(peer.ID("B"), 120, true, false, "http://hub.com"), // has DataHub
		CreateTestPeerInfo(peer.ID("C"), 115, true, false, ""),               // no DataHub
	}

	selected := ps.SelectSyncPeer(peers, SelectionCriteria{
		LocalHeight: 100,
	})

	assert.Equal(t, peer.ID("B"), selected, "Should only select peer with DataHub")
}

func TestPeerSelector_SelectSyncPeer_RequireResponsiveURL(t *testing.T) {
	logger := ulogger.New("test")
	ps := NewPeerSelector(logger, nil)

	peers := []*PeerInfo{
		{
			ID:              peer.ID("A"),
			Height:          110,
			ReputationScore: 80.0, // Good reputation
			DataHubURL:      "http://hub1.com",
			URLResponsive:   false, // not responsive
			Storage:         "full",
		},
		{
			ID:              peer.ID("B"),
			Height:          120,
			ReputationScore: 80.0, // Good reputation
			DataHubURL:      "http://hub2.com",
			URLResponsive:   true, // responsive
			Storage:         "full",
		},
		{
			ID:              peer.ID("C"),
			Height:          115,
			ReputationScore: 80.0, // Good reputation
			DataHubURL:      "",
			URLResponsive:   false, // no URL
			Storage:         "full",
		},
	}

	selected := ps.SelectSyncPeer(peers, SelectionCriteria{
		LocalHeight: 100,
	})

	assert.Equal(t, peer.ID("B"), selected, "Should only select peer with responsive URL")
}

func TestPeerSelector_SelectSyncPeer_ForcedPeer(t *testing.T) {
	logger := ulogger.New("test")
	ps := NewPeerSelector(logger, nil)

	// Create multiple eligible peers
	peers := []*PeerInfo{
		CreateTestPeerInfo(peer.ID("A"), 110, true, false, "http://test.com"),
		CreateTestPeerInfo(peer.ID("B"), 120, true, false, "http://test.com"),
		CreateTestPeerInfo(peer.ID("C"), 115, true, false, "http://test.com"),
	}
	// Mark URLs as responsive
	for _, p := range peers {
		p.URLResponsive = true
	}

	// Force selection of peer B
	selected := ps.SelectSyncPeer(peers, SelectionCriteria{
		LocalHeight:  100,
		ForcedPeerID: peer.ID("B"),
	})

	assert.Equal(t, peer.ID("B"), selected, "Should select forced peer")

	// Force selection of non-existent peer
	selected = ps.SelectSyncPeer(peers, SelectionCriteria{
		LocalHeight:  100,
		ForcedPeerID: peer.ID("Z"),
	})

	assert.Equal(t, peer.ID(""), selected, "Should return empty for non-existent forced peer")

	// Force selection of ineligible peer (banned) - should still select it
	peers[1].IsBanned = true
	selected = ps.SelectSyncPeer(peers, SelectionCriteria{
		LocalHeight:  100,
		ForcedPeerID: peer.ID("B"),
	})

	assert.Equal(t, peer.ID("B"), selected, "Should still select forced peer even if ineligible")
}

func TestPeerSelector_SelectSyncPeer_InvalidHeight(t *testing.T) {
	logger := ulogger.New("test")
	ps := NewPeerSelector(logger, nil)

	peers := []*PeerInfo{
		{
			ID:              peer.ID("A"),
			Height:          0,    // Invalid height
			ReputationScore: 80.0, // Good reputation
			DataHubURL:      "http://test.com",
			URLResponsive:   true,
			Storage:         "full",
		},
		{
			ID:              peer.ID("B"),
			Height:          -1,   // Invalid height
			ReputationScore: 80.0, // Good reputation
			DataHubURL:      "http://test.com",
			URLResponsive:   true,
			Storage:         "full",
		},
		{
			ID:              peer.ID("C"),
			Height:          110,  // Valid height
			ReputationScore: 80.0, // Good reputation
			DataHubURL:      "http://test.com",
			URLResponsive:   true,
			Storage:         "full",
		},
	}

	selected := ps.SelectSyncPeer(peers, SelectionCriteria{
		LocalHeight: 100,
	})

	assert.Equal(t, peer.ID("C"), selected, "Should skip peers with invalid heights")
}

func TestPeerSelector_SelectSyncPeer_ComplexCriteria(t *testing.T) {
	logger := ulogger.New("test")
	ps := NewPeerSelector(logger, nil)

	peers := []*PeerInfo{
		{
			ID:              peer.ID("A"),
			Height:          110,
			ReputationScore: 15.0, // Low reputation // fails health check
			IsBanned:        false,
			DataHubURL:      "http://hub.com",
			URLResponsive:   true,
			BanScore:        0,
			Storage:         "full",
		},
		{
			ID:              peer.ID("B"),
			Height:          120,
			ReputationScore: 80.0, // Good reputation
			IsBanned:        true, // fails ban check
			DataHubURL:      "http://hub.com",
			URLResponsive:   true,
			BanScore:        100,
			Storage:         "full",
		},
		{
			ID:              peer.ID("C"),
			Height:          115,
			ReputationScore: 80.0, // Good reputation
			IsBanned:        false,
			DataHubURL:      "", // fails DataHub requirement
			URLResponsive:   false,
			BanScore:        10,
			Storage:         "full",
		},
		{
			ID:              peer.ID("D"),
			Height:          125,
			ReputationScore: 80.0, // Good reputation
			IsBanned:        false,
			DataHubURL:      "http://hub.com",
			URLResponsive:   false, // fails responsive URL check
			BanScore:        20,
			Storage:         "full",
		},
		{
			ID:              peer.ID("E"),
			Height:          130,
			ReputationScore: 80.0, // Good reputation
			IsBanned:        false,
			DataHubURL:      "http://hub.com",
			URLResponsive:   true, // passes all checks
			BanScore:        5,
			Storage:         "full",
		},
	}

	selected := ps.SelectSyncPeer(peers, SelectionCriteria{
		LocalHeight: 100,
	})

	assert.Equal(t, peer.ID("E"), selected, "Should select only peer meeting all criteria")
}

func TestPeerSelector_isEligible(t *testing.T) {
	logger := ulogger.New("test")
	ps := NewPeerSelector(logger, nil)

	tests := []struct {
		name     string
		peer     *PeerInfo
		criteria SelectionCriteria
		expected bool
	}{
		{
			name: "healthy peer passes basic criteria",
			peer: &PeerInfo{
				ID:              peer.ID("A"),
				Height:          100,
				ReputationScore: 80.0, // Good reputation
				IsBanned:        false,
				DataHubURL:      "http://test.com",
				URLResponsive:   true,
				Storage:         "full",
			},
			criteria: SelectionCriteria{},
			expected: true,
		},
		{
			name: "banned peer is always excluded",
			peer: &PeerInfo{
				ID:              peer.ID("B"),
				Height:          100,
				ReputationScore: 80.0, // Good reputation
				IsBanned:        true,
				DataHubURL:      "http://test.com",
				URLResponsive:   true,
				Storage:         "full",
			},
			criteria: SelectionCriteria{},
			expected: false,
		},
		{
			name: "unhealthy peer fails health requirement",
			peer: &PeerInfo{
				ID:              peer.ID("C"),
				Height:          100,
				ReputationScore: 15.0, // Low reputation
				Storage:         "full",
			},
			criteria: SelectionCriteria{},
			expected: false,
		},
		{
			name: "peer without DataHub fails DataHub requirement",
			peer: &PeerInfo{
				ID:              peer.ID("D"),
				Height:          100,
				ReputationScore: 80.0, // Good reputation
				DataHubURL:      "",
				Storage:         "full",
			},
			criteria: SelectionCriteria{},
			expected: false,
		},
		{
			name: "peer with unresponsive URL fails responsive requirement",
			peer: &PeerInfo{
				ID:              peer.ID("E"),
				Height:          100,
				ReputationScore: 80.0, // Good reputation
				DataHubURL:      "http://hub.com",
				URLResponsive:   false,
				Storage:         "full",
			},
			criteria: SelectionCriteria{},
			expected: false,
		},
		{
			name: "peer with invalid height fails",
			peer: &PeerInfo{
				ID:              peer.ID("F"),
				Height:          0,
				ReputationScore: 80.0, // Good reputation
				Storage:         "full",
			},
			criteria: SelectionCriteria{},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ps.isEligible(tt.peer, tt.criteria)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestPeerSelector_DeterministicSelectionAmongEqualPeers(t *testing.T) {
	logger := ulogger.New("test")
	ps := NewPeerSelector(logger, nil)

	// Create multiple peers with same ban score and height
	peers := []*PeerInfo{
		{
			ID:              peer.ID("A"),
			Height:          110,
			ReputationScore: 80.0, // Good reputation
			BanScore:        10,
			DataHubURL:      "http://test.com",
			URLResponsive:   true,
			Storage:         "full",
		},
		{
			ID:              peer.ID("B"),
			Height:          110,
			ReputationScore: 80.0, // Good reputation
			BanScore:        10,
			DataHubURL:      "http://test.com",
			URLResponsive:   true,
			Storage:         "full",
		},
		{
			ID:              peer.ID("C"),
			Height:          110,
			ReputationScore: 80.0, // Good reputation
			BanScore:        10,
			DataHubURL:      "http://test.com",
			URLResponsive:   true,
			Storage:         "full",
		},
	}

	// Selection should be deterministic - always select the first peer (sorted by ID)
	selected := ps.SelectSyncPeer(peers, SelectionCriteria{
		LocalHeight: 100,
	})
	assert.Equal(t, peer.ID("A"), selected, "Should select first peer alphabetically when all else is equal")

	// If previous peer was A, should select B
	selected = ps.SelectSyncPeer(peers, SelectionCriteria{
		LocalHeight:  100,
		PreviousPeer: peer.ID("A"),
	})
	assert.Equal(t, peer.ID("B"), selected, "Should select next peer when previous was the first")
}
