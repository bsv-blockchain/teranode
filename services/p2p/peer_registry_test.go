package p2p

import (
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPeerRegistry_AddPeer(t *testing.T) {
	pr := NewPeerRegistry()

	peerID := peer.ID("test-peer-1")

	testHash, _ := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")

	pr.Put(peerID, "", 12345, testHash, "")

	// Verify peer was added
	info, exists := pr.Get(peerID)
	require.True(t, exists, "Peer should exist after adding")
	assert.Equal(t, peerID, info.ID)
	assert.Equal(t, uint32(12345), info.Height)
	assert.Equal(t, testHash.String(), info.BlockHash.String())

	assert.True(t, info.ReputationScore >= 20.0, "New peer should be healthy by default")
	assert.False(t, info.IsBanned, "New peer should not be banned")
	assert.NotZero(t, info.ConnectedAt, "ConnectedAt should be set")
	assert.NotZero(t, info.LastMessageTime, "LastMessageTime should be set")
	assert.Equal(t, info.ConnectedAt, info.LastMessageTime, "LastMessageTime should initially equal ConnectedAt")

	// Adding same peer again should not reset data
	originalTime := info.ConnectedAt
	time.Sleep(10 * time.Millisecond)
	pr.Put(peerID, "", 0, nil, "")

	info, _ = pr.Get(peerID)
	assert.Equal(t, originalTime, info.ConnectedAt, "ConnectedAt should not change on re-add")
}

func TestPeerRegistry_RemovePeer(t *testing.T) {
	pr := NewPeerRegistry()
	peerID := peer.ID("test-peer-1")

	// Add then remove
	pr.Put(peerID, "", 0, nil, "")
	pr.Remove(peerID)

	// Verify peer was removed
	_, exists := pr.Get(peerID)
	assert.False(t, exists, "Peer should not exist after removal")

	// Remove non-existent peer should not panic
	pr.Remove(peer.ID("non-existent"))
}

func TestPeerRegistry_UpdateLastMessageTime(t *testing.T) {
	pr := NewPeerRegistry()
	peerID := peer.ID("test-peer-1")

	// Add peer
	pr.Put(peerID, "", 0, nil, "")

	// Get initial last message time (should be set to connection time)
	info1, exists := pr.Get(peerID)
	require.True(t, exists, "Peer should exist")
	assert.NotZero(t, info1.LastMessageTime, "LastMessageTime should be initialized")
	assert.Equal(t, info1.ConnectedAt, info1.LastMessageTime, "LastMessageTime should initially equal ConnectedAt")

	// Wait a bit and update last message time
	time.Sleep(50 * time.Millisecond)
	pr.UpdateLastMessageTime(peerID)

	// Verify last message time was updated
	info2, exists := pr.Get(peerID)
	require.True(t, exists, "Peer should still exist")
	assert.True(t, info2.LastMessageTime.After(info1.LastMessageTime), "LastMessageTime should be updated")
	assert.Equal(t, info1.ConnectedAt, info2.ConnectedAt, "ConnectedAt should not change")

	// Update for non-existent peer should not panic
	pr.UpdateLastMessageTime(peer.ID("non-existent"))
}

func TestPeerRegistry_GetAllPeers(t *testing.T) {
	pr := NewPeerRegistry()

	// Start with empty registry
	peers := pr.GetAll()
	assert.Empty(t, peers, "Registry should start empty")

	// Add multiple peers
	ids := GenerateTestPeerIDs(3)
	for _, id := range ids {
		pr.Put(id, "", 0, nil, "")
	}

	// Get all peers
	peers = pr.GetAll()
	assert.Len(t, peers, 3, "Should have 3 peers")

	// Verify returned copies (not references)
	if len(peers) > 0 {
		originalHeight := peers[0].Height
		peers[0].Height = 999

		info, _ := pr.Get(peers[0].ID)
		assert.Equal(t, originalHeight, info.Height, "Modifying returned peer should not affect registry")
	}
}

func TestPeerRegistry_UpdateDataHubURL(t *testing.T) {
	pr := NewPeerRegistry()
	peerID := peer.ID("test-peer-1")

	pr.Put(peerID, "", 0, nil, "http://datahub.test")

	info, exists := pr.Get(peerID)
	require.True(t, exists)
	assert.Equal(t, "http://datahub.test", info.DataHubURL)
}

func TestPeerRegistry_UpdateHealth(t *testing.T) {
	pr := NewPeerRegistry()
	peerID := peer.ID("test-peer-1")

	pr.Put(peerID, "", 0, nil, "")

	// Initially healthy
	info, _ := pr.Get(peerID)
	assert.True(t, info.ReputationScore >= 20.0)

	// Mark as unhealthy (low reputation)
	pr.UpdateReputation(peerID, 15.0)
	info, _ = pr.Get(peerID)
	assert.False(t, info.ReputationScore >= 20.0)

	// Mark as healthy again
	pr.UpdateReputation(peerID, 80.0)
	info, _ = pr.Get(peerID)
	assert.True(t, info.ReputationScore >= 20.0)
}

func TestPeerRegistry_UpdateBanStatus(t *testing.T) {
	pr := NewPeerRegistry()
	peerID := peer.ID("test-peer-1")

	pr.Put(peerID, "", 0, nil, "")
	pr.UpdateBanStatus(peerID, 50, false)

	info, _ := pr.Get(peerID)
	assert.Equal(t, 50, info.BanScore)
	assert.False(t, info.IsBanned)

	// Ban the peer
	pr.UpdateBanStatus(peerID, 100, true)
	info, _ = pr.Get(peerID)
	assert.Equal(t, 100, info.BanScore)
	assert.True(t, info.IsBanned)
}

func TestPeerRegistry_UpdateNetworkStats(t *testing.T) {
	pr := NewPeerRegistry()
	peerID := peer.ID("test-peer-1")

	pr.Put(peerID, "", 0, nil, "")
	pr.UpdateNetworkStats(peerID, 1024)

	info, _ := pr.Get(peerID)
	assert.Equal(t, uint64(1024), info.BytesReceived)
	assert.NotZero(t, info.LastBlockTime)
}

func TestPeerRegistry_PeerCount(t *testing.T) {
	pr := NewPeerRegistry()

	assert.Equal(t, 0, pr.PeerCount())

	// Add peers
	ids := GenerateTestPeerIDs(5)
	for i, id := range ids {
		pr.Put(id, "", 0, nil, "")
		assert.Equal(t, i+1, pr.PeerCount())
	}

	// Remove peers
	for i, id := range ids {
		pr.Remove(id)
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
			pr.Put(id, "", uint32(i), nil, "")
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 100; i++ {
			id := peer.ID(string(rune('A' + i%10)))
			pr.UpdateBanStatus(id, i, i > 50)
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 100; i++ {
			pr.GetAll()
			pr.PeerCount()
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 100; i++ {
			id := peer.ID(string(rune('A' + i%10)))
			if i%5 == 0 {
				pr.Remove(id)
			}
		}
		done <- true
	}()

	// Wait for all goroutines
	for i := 0; i < 4; i++ {
		<-done
	}

	// Should not panic and registry should be in consistent state
	peers := pr.GetAll()
	assert.NotNil(t, peers)
}

func TestPeerRegistry_GetPeerReturnsCopy(t *testing.T) {
	pr := NewPeerRegistry()
	peerID := peer.ID("test-peer-1")

	pr.Put(peerID, "", 100, nil, "")

	// Get peer info
	info1, _ := pr.Get(peerID)
	info2, _ := pr.Get(peerID)

	// Modify one copy
	info1.Height = 200

	// Other copy should be unchanged
	assert.Equal(t, uint32(100), info2.Height)

	// Original in registry should be unchanged
	info3, _ := pr.Get(peerID)
	assert.Equal(t, uint32(100), info3.Height)
}

func TestPeerRegistry_ClearAllSyncAttempts(t *testing.T) {
	pr := NewPeerRegistry()

	peer1 := peer.ID("peer-1")
	peer2 := peer.ID("peer-2")
	peer3 := peer.ID("peer-3")

	pr.Put(peer1, "", 0, nil, "")
	pr.Put(peer2, "", 0, nil, "")
	pr.Put(peer3, "", 0, nil, "")

	// Set LastSyncAttempt for peer1 and peer2
	pr.RecordSyncAttempt(peer1)
	pr.RecordSyncAttempt(peer2)

	info1, _ := pr.Get(peer1)
	info2, _ := pr.Get(peer2)
	info3, _ := pr.Get(peer3)
	require.False(t, info1.LastSyncAttempt.IsZero())
	require.False(t, info2.LastSyncAttempt.IsZero())
	require.True(t, info3.LastSyncAttempt.IsZero())

	cleared := pr.ClearAllSyncAttempts()
	assert.Equal(t, 2, cleared, "Should clear sync attempts for peers with non-zero LastSyncAttempt")

	info1, _ = pr.Get(peer1)
	info2, _ = pr.Get(peer2)
	info3, _ = pr.Get(peer3)
	assert.True(t, info1.LastSyncAttempt.IsZero())
	assert.True(t, info2.LastSyncAttempt.IsZero())
	assert.True(t, info3.LastSyncAttempt.IsZero())
}

// Catchup-related tests

func TestPeerRegistry_RecordCatchupAttempt(t *testing.T) {
	pr := NewPeerRegistry()
	peerID := peer.ID("test-peer-1")

	pr.Put(peerID, "", 0, nil, "")

	// Initial state
	info, _ := pr.Get(peerID)
	assert.Equal(t, int64(0), info.InteractionAttempts)
	assert.True(t, info.LastInteractionAttempt.IsZero())

	// Record first attempt
	pr.RecordInteractionAttempt(peerID)
	info, _ = pr.Get(peerID)
	assert.Equal(t, int64(1), info.InteractionAttempts)
	assert.False(t, info.LastInteractionAttempt.IsZero())

	firstAttemptTime := info.LastInteractionAttempt

	// Record second attempt
	time.Sleep(10 * time.Millisecond)
	pr.RecordInteractionAttempt(peerID)
	info, _ = pr.Get(peerID)
	assert.Equal(t, int64(2), info.InteractionAttempts)
	assert.True(t, info.LastInteractionAttempt.After(firstAttemptTime))

	// Attempt on non-existent peer should not panic
	pr.RecordInteractionAttempt(peer.ID("non-existent"))
}

func TestPeerRegistry_RecordCatchupSuccess(t *testing.T) {
	pr := NewPeerRegistry()
	peerID := peer.ID("test-peer-1")

	pr.Put(peerID, "", 0, nil, "")

	// Initial state
	info, _ := pr.Get(peerID)
	assert.Equal(t, int64(0), info.InteractionSuccesses)
	assert.True(t, info.LastInteractionSuccess.IsZero())
	assert.Equal(t, time.Duration(0), info.AvgResponseTime)

	// Record first success with 100ms duration
	pr.RecordInteractionSuccess(peerID, 100*time.Millisecond)
	info, _ = pr.Get(peerID)
	assert.Equal(t, int64(1), info.InteractionSuccesses)
	assert.False(t, info.LastInteractionSuccess.IsZero())
	assert.Equal(t, 100*time.Millisecond, info.AvgResponseTime)

	// Record second success with 200ms duration
	// Should calculate weighted average: 80% of 100ms + 20% of 200ms = 120ms
	time.Sleep(10 * time.Millisecond)
	pr.RecordInteractionSuccess(peerID, 200*time.Millisecond)
	info, _ = pr.Get(peerID)
	assert.Equal(t, int64(2), info.InteractionSuccesses)
	expectedAvg := time.Duration(int64(float64(100*time.Millisecond)*0.8 + float64(200*time.Millisecond)*0.2))
	assert.Equal(t, expectedAvg, info.AvgResponseTime)

	// Success on non-existent peer should not panic
	pr.RecordInteractionSuccess(peer.ID("non-existent"), 100*time.Millisecond)
}

func TestPeerRegistry_RecordCatchupFailure(t *testing.T) {
	pr := NewPeerRegistry()
	peerID := peer.ID("test-peer-1")

	pr.Put(peerID, "", 0, nil, "")

	// Initial state
	info, _ := pr.Get(peerID)
	assert.Equal(t, int64(0), info.InteractionFailures)
	assert.True(t, info.LastInteractionFailure.IsZero())

	// Record first failure
	pr.RecordInteractionFailure(peerID)
	info, _ = pr.Get(peerID)
	assert.Equal(t, int64(1), info.InteractionFailures)
	assert.False(t, info.LastInteractionFailure.IsZero())

	firstFailureTime := info.LastInteractionFailure

	// Record second failure
	time.Sleep(10 * time.Millisecond)
	pr.RecordInteractionFailure(peerID)
	info, _ = pr.Get(peerID)
	assert.Equal(t, int64(2), info.InteractionFailures)
	assert.True(t, info.LastInteractionFailure.After(firstFailureTime))

	// Failure on non-existent peer should not panic
	pr.RecordInteractionFailure(peer.ID("non-existent"))
}

func TestPeerRegistry_RecordCatchupMalicious(t *testing.T) {
	pr := NewPeerRegistry()
	peerID := peer.ID("test-peer-1")

	pr.Put(peerID, "", 0, nil, "")

	// Initial state
	info, _ := pr.Get(peerID)
	assert.Equal(t, int64(0), info.MaliciousCount)

	// Record malicious behavior
	pr.RecordMaliciousInteraction(peerID)
	info, _ = pr.Get(peerID)
	assert.Equal(t, int64(1), info.MaliciousCount)

	pr.RecordMaliciousInteraction(peerID)
	info, _ = pr.Get(peerID)
	assert.Equal(t, int64(2), info.MaliciousCount)

	// Malicious on non-existent peer should not panic
	pr.RecordMaliciousInteraction(peer.ID("non-existent"))
}

func TestPeerRegistry_UpdateCatchupReputation(t *testing.T) {
	pr := NewPeerRegistry()
	peerID := peer.ID("test-peer-1")

	pr.Put(peerID, "", 0, nil, "")

	// Initial state - should have default reputation of 50
	info, _ := pr.Get(peerID)
	assert.Equal(t, float64(50), info.ReputationScore)

	// Update to valid score
	pr.UpdateReputation(peerID, 75.5)
	info, _ = pr.Get(peerID)
	assert.Equal(t, 75.5, info.ReputationScore)

	// Test clamping - score above 100
	pr.UpdateReputation(peerID, 150.0)
	info, _ = pr.Get(peerID)
	assert.Equal(t, 100.0, info.ReputationScore)

	// Test clamping - score below 0
	pr.UpdateReputation(peerID, -50.0)
	info, _ = pr.Get(peerID)
	assert.Equal(t, 0.0, info.ReputationScore)

	// Update on non-existent peer should not panic
	pr.UpdateReputation(peer.ID("non-existent"), 50.0)
}

func TestPeerRegistry_GetPeersForCatchup(t *testing.T) {
	pr := NewPeerRegistry()

	// Add multiple peers with different states
	ids := GenerateTestPeerIDs(5)

	// Peer 0: Healthy with DataHub URL, good reputation
	pr.Put(ids[0], "", 0, nil, "http://peer0.test")
	pr.UpdateReputation(ids[0], 90.0)

	// Peer 1: Healthy with DataHub URL, medium reputation
	pr.Put(ids[1], "", 0, nil, "http://peer1.test")
	pr.UpdateReputation(ids[1], 50.0)

	// Peer 2: Low reputation with DataHub URL (should be excluded)
	pr.Put(ids[2], "", 0, nil, "http://peer2.test")
	pr.UpdateReputation(ids[2], 15.0)

	// Peer 3: Healthy but no DataHub URL (should be excluded)
	pr.Put(ids[3], "", 0, nil, "")
	pr.UpdateReputation(ids[3], 85.0)

	// Peer 4: Healthy with DataHub URL but banned (should be excluded)
	pr.Put(ids[4], "", 0, nil, "http://peer4.test")
	pr.UpdateBanStatus(ids[4], 100, true)
	pr.UpdateReputation(ids[4], 95.0)

	// Get peers for catchup
	peers := pr.GetPeersForCatchup()

	// Should return peers 0, 1, and 2 (with DataHub URL and not banned)
	// Peer 3 is excluded (no DataHub URL), Peer 4 is excluded (banned)
	require.Len(t, peers, 3)

	// Should be sorted by reputation (highest first)
	assert.Equal(t, ids[0], peers[0].ID, "Peer 0 should be first (highest reputation)")
	assert.Equal(t, 90.0, peers[0].ReputationScore)
	assert.Equal(t, ids[1], peers[1].ID, "Peer 1 should be second")
	assert.Equal(t, 50.0, peers[1].ReputationScore)
	assert.Equal(t, ids[2], peers[2].ID, "Peer 2 should be third")
	assert.Equal(t, 15.0, peers[2].ReputationScore)
}

func TestPeerRegistry_GetPeersForCatchup_SameReputation(t *testing.T) {
	pr := NewPeerRegistry()

	ids := GenerateTestPeerIDs(3)

	// All peers have same reputation, but different success times
	baseTime := time.Now()

	// Peer 0: Last success 1 hour ago
	pr.Put(ids[0], "", 0, nil, "http://peer0.test")
	pr.UpdateReputation(ids[0], 75.0)
	pr.RecordInteractionSuccess(ids[0], 100*time.Millisecond)
	// Manually set last success to older time
	pr.peers[ids[0]].LastInteractionSuccess = baseTime.Add(-1 * time.Hour)

	// Peer 1: Last success 10 minutes ago (most recent)
	pr.Put(ids[1], "", 0, nil, "http://peer1.test")
	pr.UpdateReputation(ids[1], 75.0)
	pr.RecordInteractionSuccess(ids[1], 100*time.Millisecond)
	pr.peers[ids[1]].LastInteractionSuccess = baseTime.Add(-10 * time.Minute)

	// Peer 2: Last success 30 minutes ago
	pr.Put(ids[2], "", 0, nil, "http://peer2.test")
	pr.UpdateReputation(ids[2], 75.0)
	pr.RecordInteractionSuccess(ids[2], 100*time.Millisecond)
	pr.peers[ids[2]].LastInteractionSuccess = baseTime.Add(-30 * time.Minute)

	peers := pr.GetPeersForCatchup()

	require.Len(t, peers, 3)
	// When reputation is equal, should sort by most recent success first
	assert.Equal(t, ids[1], peers[0].ID, "Peer 1 should be first (most recent success)")
	assert.Equal(t, ids[2], peers[1].ID, "Peer 2 should be second")
	assert.Equal(t, ids[0], peers[2].ID, "Peer 0 should be last (oldest success)")
}

func TestPeerRegistry_CatchupMetrics_ConcurrentAccess(t *testing.T) {
	pr := NewPeerRegistry()
	peerID, _ := peer.Decode(testPeer1)
	pr.Put(peerID, "", 0, nil, "http://test.com")
	pr.UpdateReputation(peerID, 80.0)

	done := make(chan bool)

	// Concurrent attempts
	go func() {
		for i := 0; i < 100; i++ {
			pr.RecordInteractionAttempt(peerID)
		}
		done <- true
	}()

	// Concurrent successes
	go func() {
		for i := 0; i < 50; i++ {
			pr.RecordInteractionSuccess(peerID, time.Duration(i)*time.Millisecond)
		}
		done <- true
	}()

	// Concurrent failures
	go func() {
		for i := 0; i < 30; i++ {
			pr.RecordInteractionFailure(peerID)
		}
		done <- true
	}()

	// Concurrent reputation updates
	go func() {
		for i := 0; i < 100; i++ {
			pr.UpdateReputation(peerID, float64(i%101))
		}
		done <- true
	}()

	// Concurrent reads
	go func() {
		for i := 0; i < 100; i++ {
			pr.GetPeersForCatchup()
		}
		done <- true
	}()

	// Wait for all
	for i := 0; i < 5; i++ {
		<-done
	}

	// Verify final state is consistent
	info, exists := pr.Get(peerID)
	require.True(t, exists)
	assert.Equal(t, int64(100), info.InteractionAttempts)
	assert.Equal(t, int64(50), info.InteractionSuccesses)
	assert.Equal(t, int64(30), info.InteractionFailures)
	assert.NotZero(t, info.AvgResponseTime)
}

func TestSanitizePeerName(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "normal name",
			input:    "Bitcoin-SV-Node-1.0",
			expected: "Bitcoin-SV-Node-1.0",
		},
		{
			name:     "name with spaces",
			input:    "My Bitcoin Node",
			expected: "My Bitcoin Node",
		},
		{
			name:     "XSS attempt with script tags",
			input:    "<script>alert('xss')</script>",
			expected: "scriptalert(xss)/script",
		},
		{
			name:     "HTML injection attempt",
			input:    "<img src=x onerror=alert(1)>",
			expected: "img src=x onerror=alert(1)",
		},
		{
			name:     "name too long",
			input:    strings.Repeat("A", 200),
			expected: strings.Repeat("A", maxPeerNameLength),
		},
		{
			name:     "control characters",
			input:    "Node\x00\x01\x02\x03Name",
			expected: "NodeName",
		},
		{
			name:     "special chars removed",
			input:    "Node<>&'\"\\Name",
			expected: "NodeName",
		},
		{
			name:     "unicode characters removed",
			input:    "Node™®©Name",
			expected: "NodeName",
		},
		{
			name:     "safe punctuation allowed",
			input:    "teranode-v1.0_test/node.1",
			expected: "teranode-v1.0_test/node.1",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "whitespace only",
			input:    "   ",
			expected: "",
		},
		{
			name:     "leading and trailing whitespace",
			input:    "  Node Name  ",
			expected: "Node Name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizePeerName(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
