package p2p

import (
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPeerRegistry_AddPeer(t *testing.T) {
	pr := NewPeerRegistry()
	peerID := peer.ID("test-peer-1")

	// Add a new peer
	pr.AddPeer(peerID)

	// Verify peer was added
	info, exists := pr.GetPeer(peerID)
	require.True(t, exists, "Peer should exist after adding")
	assert.Equal(t, peerID, info.ID)
	assert.True(t, info.IsHealthy, "New peer should be healthy by default")
	assert.False(t, info.IsBanned, "New peer should not be banned")
	assert.NotZero(t, info.ConnectedAt, "ConnectedAt should be set")
	assert.NotZero(t, info.LastMessageTime, "LastMessageTime should be set")
	assert.Equal(t, info.ConnectedAt, info.LastMessageTime, "LastMessageTime should initially equal ConnectedAt")

	// Adding same peer again should not reset data
	originalTime := info.ConnectedAt
	time.Sleep(10 * time.Millisecond)
	pr.AddPeer(peerID)

	info, _ = pr.GetPeer(peerID)
	assert.Equal(t, originalTime, info.ConnectedAt, "ConnectedAt should not change on re-add")
}

func TestPeerRegistry_RemovePeer(t *testing.T) {
	pr := NewPeerRegistry()
	peerID := peer.ID("test-peer-1")

	// Add then remove
	pr.AddPeer(peerID)
	pr.RemovePeer(peerID)

	// Verify peer was removed
	_, exists := pr.GetPeer(peerID)
	assert.False(t, exists, "Peer should not exist after removal")

	// Remove non-existent peer should not panic
	pr.RemovePeer(peer.ID("non-existent"))
}

func TestPeerRegistry_UpdateLastMessageTime(t *testing.T) {
	pr := NewPeerRegistry()
	peerID := peer.ID("test-peer-1")

	// Add peer
	pr.AddPeer(peerID)

	// Get initial last message time (should be set to connection time)
	info1, exists := pr.GetPeer(peerID)
	require.True(t, exists, "Peer should exist")
	assert.NotZero(t, info1.LastMessageTime, "LastMessageTime should be initialized")
	assert.Equal(t, info1.ConnectedAt, info1.LastMessageTime, "LastMessageTime should initially equal ConnectedAt")

	// Wait a bit and update last message time
	time.Sleep(50 * time.Millisecond)
	pr.UpdateLastMessageTime(peerID)

	// Verify last message time was updated
	info2, exists := pr.GetPeer(peerID)
	require.True(t, exists, "Peer should still exist")
	assert.True(t, info2.LastMessageTime.After(info1.LastMessageTime), "LastMessageTime should be updated")
	assert.Equal(t, info1.ConnectedAt, info2.ConnectedAt, "ConnectedAt should not change")

	// Update for non-existent peer should not panic
	pr.UpdateLastMessageTime(peer.ID("non-existent"))
}

func TestPeerRegistry_GetAllPeers(t *testing.T) {
	pr := NewPeerRegistry()

	// Start with empty registry
	peers := pr.GetAllPeers()
	assert.Empty(t, peers, "Registry should start empty")

	// Add multiple peers
	ids := GenerateTestPeerIDs(3)
	for _, id := range ids {
		pr.AddPeer(id)
	}

	// Get all peers
	peers = pr.GetAllPeers()
	assert.Len(t, peers, 3, "Should have 3 peers")

	// Verify returned copies (not references)
	if len(peers) > 0 {
		originalHeight := peers[0].Height
		peers[0].Height = 999

		info, _ := pr.GetPeer(peers[0].ID)
		assert.Equal(t, originalHeight, info.Height, "Modifying returned peer should not affect registry")
	}
}

func TestPeerRegistry_UpdateHeight(t *testing.T) {
	pr := NewPeerRegistry()
	peerID := peer.ID("test-peer-1")

	pr.AddPeer(peerID)
	pr.UpdateHeight(peerID, 12345, "block-hash-12345")

	info, exists := pr.GetPeer(peerID)
	require.True(t, exists)
	assert.Equal(t, int32(12345), info.Height)
	assert.Equal(t, "block-hash-12345", info.BlockHash)

	// Update non-existent peer should not panic
	pr.UpdateHeight(peer.ID("non-existent"), 100, "hash")
}

func TestPeerRegistry_UpdateDataHubURL(t *testing.T) {
	pr := NewPeerRegistry()
	peerID := peer.ID("test-peer-1")

	pr.AddPeer(peerID)
	pr.UpdateDataHubURL(peerID, "http://datahub.test")

	info, exists := pr.GetPeer(peerID)
	require.True(t, exists)
	assert.Equal(t, "http://datahub.test", info.DataHubURL)
}

func TestPeerRegistry_UpdateHealth(t *testing.T) {
	pr := NewPeerRegistry()
	peerID := peer.ID("test-peer-1")

	pr.AddPeer(peerID)

	// Initially healthy
	info, _ := pr.GetPeer(peerID)
	assert.True(t, info.IsHealthy)

	// Mark as unhealthy
	pr.UpdateHealth(peerID, false)
	info, _ = pr.GetPeer(peerID)
	assert.False(t, info.IsHealthy)
	assert.NotZero(t, info.LastHealthCheck)

	// Mark as healthy again
	pr.UpdateHealth(peerID, true)
	info, _ = pr.GetPeer(peerID)
	assert.True(t, info.IsHealthy)
}

func TestPeerRegistry_UpdateBanStatus(t *testing.T) {
	pr := NewPeerRegistry()
	peerID := peer.ID("test-peer-1")

	pr.AddPeer(peerID)
	pr.UpdateBanStatus(peerID, 50, false)

	info, _ := pr.GetPeer(peerID)
	assert.Equal(t, 50, info.BanScore)
	assert.False(t, info.IsBanned)

	// Ban the peer
	pr.UpdateBanStatus(peerID, 100, true)
	info, _ = pr.GetPeer(peerID)
	assert.Equal(t, 100, info.BanScore)
	assert.True(t, info.IsBanned)
}

func TestPeerRegistry_UpdateNetworkStats(t *testing.T) {
	pr := NewPeerRegistry()
	peerID := peer.ID("test-peer-1")

	pr.AddPeer(peerID)
	pr.UpdateNetworkStats(peerID, 1024)

	info, _ := pr.GetPeer(peerID)
	assert.Equal(t, uint64(1024), info.BytesReceived)
	assert.NotZero(t, info.LastBlockTime)
}

func TestPeerRegistry_UpdateURLResponsiveness(t *testing.T) {
	pr := NewPeerRegistry()
	peerID := peer.ID("test-peer-1")

	pr.AddPeer(peerID)
	pr.UpdateDataHubURL(peerID, "http://test.com")

	// Initially not responsive
	info, _ := pr.GetPeer(peerID)
	assert.False(t, info.URLResponsive)

	// Mark as responsive
	pr.UpdateURLResponsiveness(peerID, true)
	info, _ = pr.GetPeer(peerID)
	assert.True(t, info.URLResponsive)
	assert.NotZero(t, info.LastURLCheck)
}

func TestPeerRegistry_PeerCount(t *testing.T) {
	pr := NewPeerRegistry()

	assert.Equal(t, 0, pr.PeerCount())

	// Add peers
	ids := GenerateTestPeerIDs(5)
	for i, id := range ids {
		pr.AddPeer(id)
		assert.Equal(t, i+1, pr.PeerCount())
	}

	// Remove peers
	for i, id := range ids {
		pr.RemovePeer(id)
		assert.Equal(t, len(ids)-i-1, pr.PeerCount())
	}
}

func TestPeerRegistry_ConcurrentAccess(t *testing.T) {
	pr := NewPeerRegistry()
	done := make(chan bool)

	// Multiple goroutines adding/updating/removing peers
	go func() {
		for i := 0; i < 100; i++ {
			id := peer.ID(string(rune('A' + i%10)))
			pr.AddPeer(id)
			pr.UpdateHeight(id, int32(i), "hash")
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 100; i++ {
			id := peer.ID(string(rune('A' + i%10)))
			pr.UpdateHealth(id, i%2 == 0)
			pr.UpdateBanStatus(id, i, i > 50)
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 100; i++ {
			pr.GetAllPeers()
			pr.PeerCount()
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 100; i++ {
			id := peer.ID(string(rune('A' + i%10)))
			if i%5 == 0 {
				pr.RemovePeer(id)
			}
		}
		done <- true
	}()

	// Wait for all goroutines
	for i := 0; i < 4; i++ {
		<-done
	}

	// Should not panic and registry should be in consistent state
	peers := pr.GetAllPeers()
	assert.NotNil(t, peers)
}

func TestPeerRegistry_GetPeerReturnsCopy(t *testing.T) {
	pr := NewPeerRegistry()
	peerID := peer.ID("test-peer-1")

	pr.AddPeer(peerID)
	pr.UpdateHeight(peerID, 100, "hash-100")

	// Get peer info
	info1, _ := pr.GetPeer(peerID)
	info2, _ := pr.GetPeer(peerID)

	// Modify one copy
	info1.Height = 200

	// Other copy should be unchanged
	assert.Equal(t, int32(100), info2.Height)

	// Original in registry should be unchanged
	info3, _ := pr.GetPeer(peerID)
	assert.Equal(t, int32(100), info3.Height)
}
