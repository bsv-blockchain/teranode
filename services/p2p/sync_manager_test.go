package p2p

import (
	"context"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSyncManager_SelectSyncPeer(t *testing.T) {
	t.Run("selects_peer_with_highest_block", func(t *testing.T) {
		sm := NewSyncManager(ulogger.New("test"), &chaincfg.MainNetParams)

		// Mock peer heights
		peerHeights := map[peer.ID]int32{
			peer.ID("peer1"): 100,
			peer.ID("peer2"): 150, // Highest
			peer.ID("peer3"): 120,
		}

		// Set callbacks
		sm.SetPeerHeightCallback(func(p peer.ID) int32 {
			return peerHeights[p]
		})
		sm.SetLocalHeightCallback(func() uint32 {
			return 90 // Local height below all peers
		})

		// Add peers as sync candidates
		for peerID := range peerHeights {
			sm.peerStates.AddPeer(peerID, true)
		}

		// Select sync peer multiple times
		selections := make(map[peer.ID]int)
		for i := 0; i < 10; i++ {
			selected := sm.selectSyncPeer()
			selections[selected]++
		}

		// All selections should be from peers ahead (peer1, peer2, peer3)
		// Since they're all ahead, any could be selected
		for peerID, count := range selections {
			if count > 0 {
				height := peerHeights[peerID]
				assert.Greater(t, height, int32(90), "Selected peer should be ahead of local height")
			}
		}
	})

	t.Run("prefers_peers_ahead_over_same_height", func(t *testing.T) {
		sm := NewSyncManager(ulogger.New("test"), &chaincfg.MainNetParams)

		peerHeights := map[peer.ID]int32{
			peer.ID("peer1"): 100, // Same as local
			peer.ID("peer2"): 105, // Ahead
			peer.ID("peer3"): 100, // Same as local
		}

		sm.SetPeerHeightCallback(func(p peer.ID) int32 {
			return peerHeights[p]
		})
		sm.SetLocalHeightCallback(func() uint32 {
			return 100
		})

		// Add all peers
		for peerID := range peerHeights {
			sm.peerStates.AddPeer(peerID, true)
		}

		// Select multiple times
		selections := make(map[peer.ID]int)
		for i := 0; i < 20; i++ {
			selected := sm.selectSyncPeer()
			selections[selected]++
		}

		// peer2 should always be selected (only peer ahead)
		assert.Equal(t, 20, selections[peer.ID("peer2")], "Should always select the peer ahead")
		assert.Equal(t, 0, selections[peer.ID("peer1")], "Should not select peer at same height when better option exists")
		assert.Equal(t, 0, selections[peer.ID("peer3")], "Should not select peer at same height when better option exists")
	})

	t.Run("falls_back_to_same_height_peers", func(t *testing.T) {
		sm := NewSyncManager(ulogger.New("test"), &chaincfg.MainNetParams)

		peerHeights := map[peer.ID]int32{
			peer.ID("peer1"): 100, // Same as local
			peer.ID("peer2"): 100, // Same as local
			peer.ID("peer3"): 95,  // Behind
		}

		sm.SetPeerHeightCallback(func(p peer.ID) int32 {
			return peerHeights[p]
		})
		sm.SetLocalHeightCallback(func() uint32 {
			return 100
		})

		// Add all peers
		for peerID := range peerHeights {
			sm.peerStates.AddPeer(peerID, true)
		}

		// Select multiple times
		selections := make(map[peer.ID]int)
		for i := 0; i < 20; i++ {
			selected := sm.selectSyncPeer()
			if selected != "" {
				selections[selected]++
			}
		}

		// Should only select from peer1 and peer2 (at same height)
		assert.Greater(t, selections[peer.ID("peer1")]+selections[peer.ID("peer2")], 0)
		assert.Equal(t, 0, selections[peer.ID("peer3")], "Should never select peer behind us")
	})

	t.Run("ignores_peers_behind", func(t *testing.T) {
		sm := NewSyncManager(ulogger.New("test"), &chaincfg.MainNetParams)

		peerHeights := map[peer.ID]int32{
			peer.ID("peer1"): 90, // Behind
			peer.ID("peer2"): 80, // Behind
			peer.ID("peer3"): 95, // Behind
		}

		sm.SetPeerHeightCallback(func(p peer.ID) int32 {
			return peerHeights[p]
		})
		sm.SetLocalHeightCallback(func() uint32 {
			return 100
		})

		// Add all peers
		for peerID := range peerHeights {
			sm.peerStates.AddPeer(peerID, true)
		}

		// Should return empty (no suitable peers)
		selected := sm.selectSyncPeer()
		assert.Equal(t, peer.ID(""), selected, "Should not select any peer when all are behind")
	})

	t.Run("handles_no_sync_candidates", func(t *testing.T) {
		sm := NewSyncManager(ulogger.New("test"), &chaincfg.MainNetParams)

		sm.SetPeerHeightCallback(func(p peer.ID) int32 {
			return 150
		})
		sm.SetLocalHeightCallback(func() uint32 {
			return 100
		})

		// Add peers but not as sync candidates
		sm.peerStates.AddPeer(peer.ID("peer1"), false)
		sm.peerStates.AddPeer(peer.ID("peer2"), false)

		selected := sm.selectSyncPeer()
		assert.Equal(t, peer.ID(""), selected, "Should not select non-candidates")
	})
}

func TestSyncManager_PeerManagement(t *testing.T) {
	t.Run("adds_and_removes_peers", func(t *testing.T) {
		sm := NewSyncManager(ulogger.New("test"), &chaincfg.MainNetParams)

		peerID := peer.ID("test-peer")

		// Add peer
		sm.AddPeer(peerID)

		// Check peer was added
		state, exists := sm.peerStates.GetPeerState(peerID)
		assert.True(t, exists)
		assert.NotNil(t, state)

		// Remove peer
		sm.RemovePeer(peerID)

		// Check peer was removed
		_, exists = sm.peerStates.GetPeerState(peerID)
		assert.False(t, exists)
	})

	t.Run("removes_sync_peer_triggers_new_selection", func(t *testing.T) {
		sm := NewSyncManager(ulogger.New("test"), &chaincfg.MainNetParams)

		// Manually set a sync peer
		syncPeerID := peer.ID("sync-peer")
		sm.syncPeer = syncPeerID
		sm.syncPeerState = &syncPeerState{
			lastBlockTime: time.Now(),
		}

		// Remove the sync peer
		sm.RemovePeer(syncPeerID)

		// Sync peer should be cleared
		assert.Equal(t, peer.ID(""), sm.syncPeer)
		assert.Nil(t, sm.syncPeerState)
	})
}

func TestSyncManager_HealthChecks(t *testing.T) {
	t.Run("switches_peer_on_network_violations", func(t *testing.T) {
		sm := NewSyncManager(ulogger.New("test"), &chaincfg.MainNetParams)

		// Set up a sync peer with violations
		sm.syncPeer = peer.ID("slow-peer")
		sm.syncPeerState = &syncPeerState{
			lastBlockTime: time.Now(),
			violations:    3, // Max violations reached
			ticks:         1,
		}

		// Run health check
		sm.checkSyncPeer()

		// Should have cleared sync peer due to violations
		assert.Equal(t, peer.ID(""), sm.syncPeer)
		assert.Nil(t, sm.syncPeerState)
	})

	t.Run("switches_peer_on_stale_blocks", func(t *testing.T) {
		sm := NewSyncManager(ulogger.New("test"), &chaincfg.MainNetParams)

		// Set up a sync peer with old last block time
		sm.syncPeer = peer.ID("stale-peer")
		sm.syncPeerState = &syncPeerState{
			lastBlockTime: time.Now().Add(-5 * time.Minute), // Older than 3 minute threshold
			violations:    0,
			ticks:         1,
		}

		// Run health check
		sm.checkSyncPeer()

		// Should have cleared sync peer due to stale blocks
		assert.Equal(t, peer.ID(""), sm.syncPeer)
		assert.Nil(t, sm.syncPeerState)
	})

	t.Run("keeps_healthy_peer", func(t *testing.T) {
		sm := NewSyncManager(ulogger.New("test"), &chaincfg.MainNetParams)

		healthyPeer := peer.ID("healthy-peer")
		sm.syncPeer = healthyPeer
		sm.syncPeerState = &syncPeerState{
			lastBlockTime: time.Now(),
			violations:    0,
			ticks:         1,
		}

		// Run health check
		sm.checkSyncPeer()

		// Should keep the healthy peer
		assert.Equal(t, healthyPeer, sm.syncPeer)
		assert.NotNil(t, sm.syncPeerState)
	})
}

func TestSyncManager_IsSyncCandidate(t *testing.T) {
	t.Run("regtest_accepts_all_peers", func(t *testing.T) {
		sm := NewSyncManager(ulogger.New("test"), &chaincfg.RegressionNetParams)

		sm.SetPeerIPsCallback(func(p peer.ID) []string {
			if p == peer.ID("local-peer") {
				return []string{"127.0.0.1"}
			}
			return []string{"192.168.1.1"}
		})

		// All peers should be candidates in regtest (for local testing)
		assert.True(t, sm.isSyncCandidate(peer.ID("local-peer")))
		assert.True(t, sm.isSyncCandidate(peer.ID("remote-peer")))
	})

	t.Run("mainnet_all_peers", func(t *testing.T) {
		sm := NewSyncManager(ulogger.New("test"), &chaincfg.MainNetParams)

		// All peers should be candidates on mainnet
		assert.True(t, sm.isSyncCandidate(peer.ID("any-peer")))
	})
}

func TestSyncManager_Lifecycle(t *testing.T) {
	t.Run("start_and_stop", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		sm := NewSyncManager(ulogger.New("test"), &chaincfg.MainNetParams)

		// Start the sync manager
		sm.Start(ctx)

		// Should have a ticker
		require.Eventually(t, func() bool {
			sm.mu.RLock()
			defer sm.mu.RUnlock()
			return sm.syncPeerTicker != nil
		}, 100*time.Millisecond, 10*time.Millisecond)

		// Stop the sync manager
		cancel()

		// Give it time to shut down
		time.Sleep(50 * time.Millisecond)
	})
}

func TestSyncManager_UpdateCallbacks(t *testing.T) {
	t.Run("update_peer_height_triggers_sync", func(t *testing.T) {
		sm := NewSyncManager(ulogger.New("test"), &chaincfg.MainNetParams)

		sm.SetLocalHeightCallback(func() uint32 {
			return 100
		})

		peerID := peer.ID("peer1")
		sm.peerStates.AddPeer(peerID, true)

		// Update peer to be ahead of us (should consider for sync)
		sm.UpdatePeerHeight(peerID, 105)

		// Give time for sync peer selection
		time.Sleep(10 * time.Millisecond)
	})

	t.Run("update_sync_peer_network", func(t *testing.T) {
		sm := NewSyncManager(ulogger.New("test"), &chaincfg.MainNetParams)

		syncPeer := peer.ID("sync-peer")
		sm.syncPeer = syncPeer
		sm.syncPeerState = &syncPeerState{
			recvBytes: 1000,
		}

		// Update network stats
		sm.UpdateSyncPeerNetwork(syncPeer, 2000)

		// Check it was updated
		assert.Equal(t, uint64(2000), sm.syncPeerState.recvBytes)
	})

	t.Run("update_sync_peer_block_time", func(t *testing.T) {
		sm := NewSyncManager(ulogger.New("test"), &chaincfg.MainNetParams)

		syncPeer := peer.ID("sync-peer")
		sm.syncPeer = syncPeer
		sm.syncPeerState = &syncPeerState{
			lastBlockTime: time.Now().Add(-1 * time.Hour),
		}

		oldTime := sm.syncPeerState.getLastBlockTime()

		// Update block time
		sm.UpdateSyncPeerBlockTime(syncPeer)

		newTime := sm.syncPeerState.getLastBlockTime()
		assert.True(t, newTime.After(oldTime))
	})
}
