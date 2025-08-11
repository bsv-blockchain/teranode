package p2p

import (
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPeerStateManager(t *testing.T) {
	t.Run("add_and_remove_peers", func(t *testing.T) {
		psm := NewPeerStateManager()

		peerID1 := peer.ID("peer1")
		peerID2 := peer.ID("peer2")

		// Add peers
		psm.AddPeer(peerID1, true)
		psm.AddPeer(peerID2, false)

		// Check they exist
		state1, exists1 := psm.GetPeerState(peerID1)
		assert.True(t, exists1)
		assert.True(t, state1.syncCandidate)

		state2, exists2 := psm.GetPeerState(peerID2)
		assert.True(t, exists2)
		assert.False(t, state2.syncCandidate)

		// Remove peer1
		psm.RemovePeer(peerID1)

		// Check peer1 is gone
		_, exists1 = psm.GetPeerState(peerID1)
		assert.False(t, exists1)

		// Check peer2 still exists
		_, exists2 = psm.GetPeerState(peerID2)
		assert.True(t, exists2)
	})

	t.Run("get_sync_candidates", func(t *testing.T) {
		psm := NewPeerStateManager()

		peer1 := peer.ID("peer1")
		peer2 := peer.ID("peer2")
		peer3 := peer.ID("peer3")

		psm.AddPeer(peer1, true)  // sync candidate
		psm.AddPeer(peer2, false) // not a sync candidate
		psm.AddPeer(peer3, true)  // sync candidate

		candidates := psm.GetSyncCandidates()
		assert.Len(t, candidates, 2)
		assert.Contains(t, candidates, peer1)
		assert.Contains(t, candidates, peer3)
		assert.NotContains(t, candidates, peer2)
	})

	t.Run("update_sync_candidate", func(t *testing.T) {
		psm := NewPeerStateManager()

		peerID := peer.ID("peer1")
		psm.AddPeer(peerID, false)

		// Initially not a sync candidate
		candidates := psm.GetSyncCandidates()
		assert.Len(t, candidates, 0)

		// Update to be a sync candidate
		psm.SetSyncCandidate(peerID, true)

		// Now should be a sync candidate
		candidates = psm.GetSyncCandidates()
		assert.Len(t, candidates, 1)
		assert.Contains(t, candidates, peerID)
	})

	t.Run("get_all_peers", func(t *testing.T) {
		psm := NewPeerStateManager()

		peer1 := peer.ID("peer1")
		peer2 := peer.ID("peer2")
		peer3 := peer.ID("peer3")

		psm.AddPeer(peer1, true)
		psm.AddPeer(peer2, false)
		psm.AddPeer(peer3, true)

		allPeers := psm.GetAllPeers()
		assert.Len(t, allPeers, 3)
		assert.Contains(t, allPeers, peer1)
		assert.Contains(t, allPeers, peer2)
		assert.Contains(t, allPeers, peer3)
	})
}

func TestSyncPeerState(t *testing.T) {
	t.Run("network_speed_validation", func(t *testing.T) {
		sps := &syncPeerState{
			recvBytes:         0,
			recvBytesLastTick: 0,
			lastBlockTime:     time.Now(),
			violations:        0,
			ticks:             0,
		}

		// First tick - no violations (needs baseline)
		violations := sps.validNetworkSpeed(1000, 30*time.Second)
		assert.Equal(t, 0, violations)

		// Update network stats - good speed
		sps.updateNetwork(60000) // 60KB in 30s = 2KB/s > 1KB/s threshold
		violations = sps.validNetworkSpeed(1000, 30*time.Second)
		assert.Equal(t, 0, violations)

		// Update with slow speed
		sps.updateNetwork(60500) // Only 500 bytes in 30s < 1KB/s threshold
		violations = sps.validNetworkSpeed(1000, 30*time.Second)
		assert.Equal(t, 1, violations)

		// Another slow update
		sps.updateNetwork(61000) // Still slow
		violations = sps.validNetworkSpeed(1000, 30*time.Second)
		assert.Equal(t, 2, violations)

		// Good speed resets violations
		sps.updateNetwork(91000) // 30KB in 30s = 1KB/s = threshold
		violations = sps.validNetworkSpeed(1000, 30*time.Second)
		assert.Equal(t, 0, violations)
	})

	t.Run("last_block_time", func(t *testing.T) {
		sps := &syncPeerState{
			lastBlockTime: time.Now().Add(-5 * time.Minute),
		}

		// Check initial time
		oldTime := sps.getLastBlockTime()
		assert.True(t, time.Since(oldTime) >= 5*time.Minute)

		// Update block time
		sps.updateLastBlockTime()

		// Check updated time
		newTime := sps.getLastBlockTime()
		assert.True(t, time.Since(newTime) < 1*time.Second)
		assert.True(t, newTime.After(oldTime))
	})

	t.Run("violation_tracking", func(t *testing.T) {
		sps := &syncPeerState{
			violations: 2,
		}

		assert.Equal(t, 2, sps.getViolations())

		// Simulate network speed check that adds violation
		sps.mu.Lock()
		sps.violations++
		sps.mu.Unlock()

		assert.Equal(t, 3, sps.getViolations())
	})

	t.Run("concurrent_access", func(t *testing.T) {
		sps := &syncPeerState{
			recvBytes:     0,
			lastBlockTime: time.Now(),
		}

		// Simulate concurrent access
		done := make(chan bool)

		// Writer goroutine
		go func() {
			for i := 0; i < 100; i++ {
				sps.updateNetwork(uint64(i * 1000))
				sps.updateLastBlockTime()
				time.Sleep(1 * time.Millisecond)
			}
			done <- true
		}()

		// Reader goroutine
		go func() {
			for i := 0; i < 100; i++ {
				_ = sps.validNetworkSpeed(1000, 30*time.Second)
				_ = sps.getLastBlockTime()
				_ = sps.getViolations()
				time.Sleep(1 * time.Millisecond)
			}
			done <- true
		}()

		// Wait for both goroutines
		<-done
		<-done

		// If we get here without deadlock/panic, concurrent access is safe
		require.True(t, true)
	})
}
