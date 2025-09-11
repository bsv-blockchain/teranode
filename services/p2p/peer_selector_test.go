package p2p

import (
	"testing"

	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/assert"
)

func TestPeerSelector_SelectSyncPeer_NoPeers(t *testing.T) {
	logger := ulogger.New("test")
	ps := NewPeerSelector(logger)

	// Empty peer list
	selected := ps.SelectSyncPeer([]*PeerInfo{}, SelectionCriteria{
		LocalHeight: 100,
	})

	assert.Equal(t, peer.ID(""), selected, "Should return empty peer ID when no peers")
}

func TestPeerSelector_SelectSyncPeer_NoEligiblePeers(t *testing.T) {
	logger := ulogger.New("test")
	ps := NewPeerSelector(logger)

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
	ps := NewPeerSelector(logger)

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
	ps := NewPeerSelector(logger)

	// Create peers with different heights
	peers := []*PeerInfo{
		CreateTestPeerInfo(peer.ID("A"), 90, true, false, "http://test.com"),  // behind
		CreateTestPeerInfo(peer.ID("B"), 110, true, false, "http://test.com"), // ahead
		CreateTestPeerInfo(peer.ID("C"), 120, true, false, "http://test.com"), // ahead more
		CreateTestPeerInfo(peer.ID("D"), 100, true, false, "http://test.com"), // same height
	}
	// Mark URLs as responsive
	for _, p := range peers {
		p.URLResponsive = true
	}

	selected := ps.SelectSyncPeer(peers, SelectionCriteria{
		LocalHeight: 100,
	})

	assert.Contains(t, []peer.ID{"B", "C"}, selected, "Should select a peer that is ahead")
}

func TestPeerSelector_SelectSyncPeer_PreferLowerBanScore(t *testing.T) {
	logger := ulogger.New("test")
	ps := NewPeerSelector(logger)

	// Create peers with different ban scores
	peers := []*PeerInfo{
		{
			ID:            peer.ID("A"),
			Height:        110,
			IsHealthy:     true,
			IsBanned:      false,
			BanScore:      50,
			DataHubURL:    "http://test.com",
			URLResponsive: true,
		},
		{
			ID:            peer.ID("B"),
			Height:        110,
			IsHealthy:     true,
			IsBanned:      false,
			BanScore:      10, // Lower ban score, should be preferred
			DataHubURL:    "http://test.com",
			URLResponsive: true,
		},
		{
			ID:            peer.ID("C"),
			Height:        110,
			IsHealthy:     true,
			IsBanned:      false,
			BanScore:      30,
			DataHubURL:    "http://test.com",
			URLResponsive: true,
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
	ps := NewPeerSelector(logger)

	// Create peers with same ban score but different heights
	peers := []*PeerInfo{
		{
			ID:            peer.ID("A"),
			Height:        110,
			IsHealthy:     true,
			IsBanned:      false,
			BanScore:      10,
			DataHubURL:    "http://test.com",
			URLResponsive: true,
		},
		{
			ID:            peer.ID("B"),
			Height:        120, // Higher, should be preferred
			IsHealthy:     true,
			IsBanned:      false,
			BanScore:      10,
			DataHubURL:    "http://test.com",
			URLResponsive: true,
		},
		{
			ID:            peer.ID("C"),
			Height:        115,
			IsHealthy:     true,
			IsBanned:      false,
			BanScore:      10,
			DataHubURL:    "http://test.com",
			URLResponsive: true,
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
	ps := NewPeerSelector(logger)

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
	ps := NewPeerSelector(logger)

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
	ps := NewPeerSelector(logger)

	peers := []*PeerInfo{
		{
			ID:            peer.ID("A"),
			Height:        110,
			IsHealthy:     true,
			DataHubURL:    "http://hub1.com",
			URLResponsive: false, // not responsive
		},
		{
			ID:            peer.ID("B"),
			Height:        120,
			IsHealthy:     true,
			DataHubURL:    "http://hub2.com",
			URLResponsive: true, // responsive
		},
		{
			ID:            peer.ID("C"),
			Height:        115,
			IsHealthy:     true,
			DataHubURL:    "",
			URLResponsive: false, // no URL
		},
	}

	selected := ps.SelectSyncPeer(peers, SelectionCriteria{
		LocalHeight: 100,
	})

	assert.Equal(t, peer.ID("B"), selected, "Should only select peer with responsive URL")
}

func TestPeerSelector_SelectSyncPeer_ForcedPeer(t *testing.T) {
	logger := ulogger.New("test")
	ps := NewPeerSelector(logger)

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
	ps := NewPeerSelector(logger)

	peers := []*PeerInfo{
		{
			ID:            peer.ID("A"),
			Height:        0, // Invalid height
			IsHealthy:     true,
			DataHubURL:    "http://test.com",
			URLResponsive: true,
		},
		{
			ID:            peer.ID("B"),
			Height:        -1, // Invalid height
			IsHealthy:     true,
			DataHubURL:    "http://test.com",
			URLResponsive: true,
		},
		{
			ID:            peer.ID("C"),
			Height:        110, // Valid height
			IsHealthy:     true,
			DataHubURL:    "http://test.com",
			URLResponsive: true,
		},
	}

	selected := ps.SelectSyncPeer(peers, SelectionCriteria{
		LocalHeight: 100,
	})

	assert.Equal(t, peer.ID("C"), selected, "Should skip peers with invalid heights")
}

func TestPeerSelector_SelectSyncPeer_ComplexCriteria(t *testing.T) {
	logger := ulogger.New("test")
	ps := NewPeerSelector(logger)

	peers := []*PeerInfo{
		{
			ID:            peer.ID("A"),
			Height:        110,
			IsHealthy:     false, // fails health check
			IsBanned:      false,
			DataHubURL:    "http://hub.com",
			URLResponsive: true,
			BanScore:      0,
		},
		{
			ID:            peer.ID("B"),
			Height:        120,
			IsHealthy:     true,
			IsBanned:      true, // fails ban check
			DataHubURL:    "http://hub.com",
			URLResponsive: true,
			BanScore:      100,
		},
		{
			ID:            peer.ID("C"),
			Height:        115,
			IsHealthy:     true,
			IsBanned:      false,
			DataHubURL:    "", // fails DataHub requirement
			URLResponsive: false,
			BanScore:      10,
		},
		{
			ID:            peer.ID("D"),
			Height:        125,
			IsHealthy:     true,
			IsBanned:      false,
			DataHubURL:    "http://hub.com",
			URLResponsive: false, // fails responsive URL check
			BanScore:      20,
		},
		{
			ID:            peer.ID("E"),
			Height:        130,
			IsHealthy:     true,
			IsBanned:      false,
			DataHubURL:    "http://hub.com",
			URLResponsive: true, // passes all checks
			BanScore:      5,
		},
	}

	selected := ps.SelectSyncPeer(peers, SelectionCriteria{
		LocalHeight: 100,
	})

	assert.Equal(t, peer.ID("E"), selected, "Should select only peer meeting all criteria")
}

func TestPeerSelector_isEligible(t *testing.T) {
	logger := ulogger.New("test")
	ps := NewPeerSelector(logger)

	tests := []struct {
		name     string
		peer     *PeerInfo
		criteria SelectionCriteria
		expected bool
	}{
		{
			name: "healthy peer passes basic criteria",
			peer: &PeerInfo{
				ID:            peer.ID("A"),
				Height:        100,
				IsHealthy:     true,
				IsBanned:      false,
				DataHubURL:    "http://test.com",
				URLResponsive: true,
			},
			criteria: SelectionCriteria{},
			expected: true,
		},
		{
			name: "banned peer is always excluded",
			peer: &PeerInfo{
				ID:            peer.ID("B"),
				Height:        100,
				IsHealthy:     true,
				IsBanned:      true,
				DataHubURL:    "http://test.com",
				URLResponsive: true,
			},
			criteria: SelectionCriteria{},
			expected: false,
		},
		{
			name: "unhealthy peer fails health requirement",
			peer: &PeerInfo{
				ID:        peer.ID("C"),
				Height:    100,
				IsHealthy: false,
			},
			criteria: SelectionCriteria{},
			expected: false,
		},
		{
			name: "peer without DataHub fails DataHub requirement",
			peer: &PeerInfo{
				ID:         peer.ID("D"),
				Height:     100,
				IsHealthy:  true,
				DataHubURL: "",
			},
			criteria: SelectionCriteria{},
			expected: false,
		},
		{
			name: "peer with unresponsive URL fails responsive requirement",
			peer: &PeerInfo{
				ID:            peer.ID("E"),
				Height:        100,
				IsHealthy:     true,
				DataHubURL:    "http://hub.com",
				URLResponsive: false,
			},
			criteria: SelectionCriteria{},
			expected: false,
		},
		{
			name: "peer with invalid height fails",
			peer: &PeerInfo{
				ID:        peer.ID("F"),
				Height:    0,
				IsHealthy: true,
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
	ps := NewPeerSelector(logger)

	// Create multiple peers with same ban score and height
	peers := []*PeerInfo{
		{
			ID:            peer.ID("A"),
			Height:        110,
			IsHealthy:     true,
			BanScore:      10,
			DataHubURL:    "http://test.com",
			URLResponsive: true,
		},
		{
			ID:            peer.ID("B"),
			Height:        110,
			IsHealthy:     true,
			BanScore:      10,
			DataHubURL:    "http://test.com",
			URLResponsive: true,
		},
		{
			ID:            peer.ID("C"),
			Height:        110,
			IsHealthy:     true,
			BanScore:      10,
			DataHubURL:    "http://test.com",
			URLResponsive: true,
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
