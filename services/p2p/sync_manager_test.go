package p2p

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSyncManager_SelectSyncPeer(t *testing.T) {
	tSettings := createBaseTestSettings()
	tSettings.ChainCfgParams = &chaincfg.RegressionNetParams

	t.Run("selects_peer_with_highest_block", func(t *testing.T) {
		sm := NewSyncManager(ulogger.New("test"), tSettings)

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

		// Select sync peer multiple times - should always be deterministic
		for i := 0; i < 10; i++ {
			selected := sm.selectSyncPeer()
			// Should always select peer2 (highest at 150)
			assert.Equal(t, peer.ID("peer2"), selected, "Should always select the peer with highest height")
		}
	})

	t.Run("prefers_peers_ahead_over_same_height", func(t *testing.T) {
		sm := NewSyncManager(ulogger.New("test"), tSettings)

		peerHeights := map[peer.ID]int32{
			peer.ID("peer1"): 100, // Same as local
			peer.ID("peer2"): 105, // Ahead (should be selected)
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

		// Select multiple times - should be deterministic
		for i := 0; i < 20; i++ {
			selected := sm.selectSyncPeer()
			// peer2 should always be selected (only peer ahead)
			assert.Equal(t, peer.ID("peer2"), selected, "Should always select the peer ahead")
		}
	})

	t.Run("falls_back_to_same_height_peers", func(t *testing.T) {
		sm := NewSyncManager(ulogger.New("test"), tSettings)

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

		// Should select deterministically - peer with highest ID at same height
		selected := sm.selectSyncPeer()
		assert.Equal(t, peer.ID("peer2"), selected, "Should select peer with highest ID at same height")

		// Verify consistency
		for i := 0; i < 10; i++ {
			selected := sm.selectSyncPeer()
			assert.Equal(t, peer.ID("peer2"), selected, "Should consistently select the same peer")
		}
	})

	t.Run("ignores_peers_behind", func(t *testing.T) {
		sm := NewSyncManager(ulogger.New("test"), tSettings)

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
		sm := NewSyncManager(ulogger.New("test"), tSettings)

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

	t.Run("selects_highest_among_multiple_ahead", func(t *testing.T) {
		sm := NewSyncManager(ulogger.New("test"), tSettings)

		peerHeights := map[peer.ID]int32{
			peer.ID("peer1"): 110,
			peer.ID("peer2"): 120,
			peer.ID("peer3"): 115,
			peer.ID("peer4"): 125, // Highest
			peer.ID("peer5"): 105,
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

		// Should always select peer4 (highest at 125)
		for i := 0; i < 10; i++ {
			selected := sm.selectSyncPeer()
			assert.Equal(t, peer.ID("peer4"), selected, "Should select peer with highest height")
		}
	})

	t.Run("uses_peer_id_as_tiebreaker", func(t *testing.T) {
		sm := NewSyncManager(ulogger.New("test"), tSettings)

		// All peers at same height
		peerHeights := map[peer.ID]int32{
			peer.ID("peer_b"): 110,
			peer.ID("peer_a"): 110,
			peer.ID("peer_c"): 110,
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

		// Should select peer_c (highest peer ID alphabetically)
		for i := 0; i < 10; i++ {
			selected := sm.selectSyncPeer()
			assert.Equal(t, peer.ID("peer_c"), selected, "Should use peer ID as tiebreaker")
		}
	})
}

func TestSyncManager_PeerManagement(t *testing.T) {
	tSettings := createBaseTestSettings()
	tSettings.ChainCfgParams = &chaincfg.MainNetParams

	t.Run("adds_and_removes_peers", func(t *testing.T) {
		sm := NewSyncManager(ulogger.New("test"), tSettings)

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
		sm := NewSyncManager(ulogger.New("test"), tSettings)

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
	tSettings := createBaseTestSettings()
	tSettings.ChainCfgParams = &chaincfg.MainNetParams

	t.Run("clears_sync_peer_when_caught_up", func(t *testing.T) {
		sm := NewSyncManager(ulogger.New("test"), tSettings)

		// Set up callbacks
		sm.SetPeerHeightCallback(func(p peer.ID) int32 {
			if p == peer.ID("sync-peer") {
				return 100 // Sync peer at height 100
			}
			return 95
		})
		sm.SetLocalHeightCallback(func() uint32 {
			return 100 // We've caught up
		})

		// Set up a sync peer
		sm.syncPeer = peer.ID("sync-peer")
		sm.syncPeerState = &syncPeerState{
			lastBlockTime: time.Now(),
			violations:    0,
			ticks:         1,
		}

		// Run health check
		sm.checkSyncPeer()

		// Should have cleared sync peer because we caught up
		assert.Equal(t, peer.ID(""), sm.syncPeer)
		assert.Nil(t, sm.syncPeerState)
	})

	t.Run("switches_peer_on_network_violations", func(t *testing.T) {
		sm := NewSyncManager(ulogger.New("test"), tSettings)

		// Set up callbacks - peer is still ahead
		sm.SetPeerHeightCallback(func(p peer.ID) int32 {
			if p == peer.ID("slow-peer") {
				return 110
			}
			return 95
		})
		sm.SetLocalHeightCallback(func() uint32 {
			return 100
		})

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
		sm := NewSyncManager(ulogger.New("test"), tSettings)

		// Set up callbacks - peer is still ahead
		sm.SetPeerHeightCallback(func(p peer.ID) int32 {
			if p == peer.ID("stale-peer") {
				return 110
			}
			return 95
		})
		sm.SetLocalHeightCallback(func() uint32 {
			return 100
		})

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
		sm := NewSyncManager(ulogger.New("test"), tSettings)

		// Set up callbacks - peer is still ahead
		sm.SetPeerHeightCallback(func(p peer.ID) int32 {
			if p == peer.ID("healthy-peer") {
				return 110
			}
			return 95
		})
		sm.SetLocalHeightCallback(func() uint32 {
			return 100
		})

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
	tSettings := createBaseTestSettings()
	tSettings.ChainCfgParams = &chaincfg.MainNetParams

	t.Run("regtest_accepts_all_peers", func(t *testing.T) {
		sm := NewSyncManager(ulogger.New("test"), tSettings)

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
		sm := NewSyncManager(ulogger.New("test"), tSettings)

		// All peers should be candidates on mainnet
		assert.True(t, sm.isSyncCandidate(peer.ID("any-peer")))
	})
}

func TestSyncManager_Lifecycle(t *testing.T) {
	tSettings := createBaseTestSettings()
	tSettings.ChainCfgParams = &chaincfg.MainNetParams

	t.Run("start_and_stop", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		sm := NewSyncManager(ulogger.New("test"), tSettings)

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

	t.Run("delays_initial_sync_peer_selection", func(t *testing.T) {
		sm := NewSyncManager(ulogger.New("test"), tSettings)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sm.Start(ctx)

		// Set up callbacks
		sm.SetPeerHeightCallback(func(p peer.ID) int32 {
			return 150
		})
		sm.SetLocalHeightCallback(func() uint32 {
			return 100
		})

		// Add peers as candidates
		peer1 := peer.ID("peer1")
		peer2 := peer.ID("peer2")
		sm.peerStates.AddPeer(peer1, true)
		sm.peerStates.AddPeer(peer2, true)

		// Try to update peer height immediately - should not select sync peer
		selected := sm.UpdatePeerHeight(peer1, 150)
		assert.False(t, selected, "Should not select sync peer during grace period")
		assert.Equal(t, peer.ID(""), sm.GetSyncPeer(), "No sync peer should be selected yet")

		// Wait for grace period to pass
		time.Sleep(6 * time.Second)

		// After the timer, the sync peer should have been selected
		assert.NotEqual(t, peer.ID(""), sm.GetSyncPeer(), "Sync peer should be selected by timer")

		// Verify initial selection is complete
		assert.True(t, sm.IsInitialSyncComplete(), "Initial sync should be complete")
	})

	t.Run("requires_minimum_peers", func(t *testing.T) {
		sm := NewSyncManager(ulogger.New("test"), tSettings)
		sm.Start(context.Background())
		sm.startTime = time.Now().Add(-10 * time.Second) // Bypass time delay but not max wait

		// Set up callbacks
		sm.SetPeerHeightCallback(func(p peer.ID) int32 {
			return 150
		})
		sm.SetLocalHeightCallback(func() uint32 {
			return 100
		})

		// Add only one peer (less than minimum)
		peer1 := peer.ID("peer1")
		sm.peerStates.AddPeer(peer1, true)

		// Should not select sync peer with insufficient peers
		selected := sm.UpdatePeerHeight(peer1, 150)
		assert.False(t, selected, "Should not select sync peer with insufficient peers")

		// Add second peer to meet minimum
		peer2 := peer.ID("peer2")
		sm.peerStates.AddPeer(peer2, true)

		// Now should allow selection
		selected = sm.UpdatePeerHeight(peer2, 150)
		assert.True(t, selected, "Should select sync peer with minimum peers")
	})

	t.Run("proceeds_after_max_wait_time", func(t *testing.T) {
		sm := NewSyncManager(ulogger.New("test"), tSettings)
		sm.Start(context.Background())
		// Set start time to exceed max wait but with only 1 peer
		sm.startTime = time.Now().Add(-25 * time.Second) // Exceeds maxWaitForMinPeers

		// Set up callbacks
		sm.SetPeerHeightCallback(func(p peer.ID) int32 {
			return 150
		})
		sm.SetLocalHeightCallback(func() uint32 {
			return 100
		})

		// Add only one peer (less than minimum)
		peer1 := peer.ID("peer1")
		sm.peerStates.AddPeer(peer1, true)

		// Should now select sync peer despite insufficient peers due to max wait exceeded
		selected := sm.UpdatePeerHeight(peer1, 150)
		assert.True(t, selected, "Should select sync peer after max wait time even with insufficient peers")
		assert.Equal(t, peer1, sm.GetSyncPeer(), "Peer1 should be selected as sync peer")
	})
}

func TestSyncManager_ClearSyncPeer(t *testing.T) {
	tSettings := createBaseTestSettings()
	tSettings.ChainCfgParams = &chaincfg.MainNetParams

	t.Run("clears_existing_sync_peer", func(t *testing.T) {
		sm := NewSyncManager(ulogger.New("test"), tSettings)

		// Set up a sync peer
		sm.syncPeer = peer.ID("test-peer")
		sm.syncPeerState = &syncPeerState{
			lastBlockTime: time.Now(),
		}

		// Clear sync peer
		sm.ClearSyncPeer()

		// Should be cleared
		assert.Equal(t, peer.ID(""), sm.syncPeer)
		assert.Nil(t, sm.syncPeerState)
	})

	t.Run("handles_no_sync_peer", func(t *testing.T) {
		sm := NewSyncManager(ulogger.New("test"), tSettings)

		// No sync peer set
		assert.Equal(t, peer.ID(""), sm.syncPeer)

		// Should not panic when clearing non-existent sync peer
		sm.ClearSyncPeer()

		// Should still be empty
		assert.Equal(t, peer.ID(""), sm.syncPeer)
		assert.Nil(t, sm.syncPeerState)
	})
}

func TestSyncManager_BlockAnnouncementBuffering(t *testing.T) {
	tSettings := createBaseTestSettings()
	tSettings.ChainCfgParams = &chaincfg.MainNetParams

	t.Run("buffers_announcements_during_initial_period", func(t *testing.T) {
		sm := NewSyncManager(ulogger.New("test"), tSettings)

		// Create test announcement
		announcement := &BlockAnnouncement{
			Hash:       "test-hash-1",
			Height:     100,
			DataHubURL: "http://test.com",
			PeerID:     "peer1",
			From:       "from1",
			Timestamp:  time.Now(),
		}

		// Should buffer during initial period
		buffered := sm.BufferBlockAnnouncement(announcement)
		assert.True(t, buffered, "Should buffer announcement during initial period")

		// Verify announcement was buffered
		announcements := sm.GetBufferedAnnouncements()
		assert.Len(t, announcements, 1)
		assert.Equal(t, "test-hash-1", announcements[0].Hash)
	})

	t.Run("does_not_buffer_after_initial_period", func(t *testing.T) {
		sm := NewSyncManager(ulogger.New("test"), tSettings)

		// Mark initial selection as done
		sm.initialSelectionDone = true

		// Create test announcement
		announcement := &BlockAnnouncement{
			Hash:       "test-hash-2",
			Height:     100,
			DataHubURL: "http://test.com",
			PeerID:     "peer2",
			From:       "from2",
			Timestamp:  time.Now(),
		}

		// Should not buffer after initial period
		buffered := sm.BufferBlockAnnouncement(announcement)
		assert.False(t, buffered, "Should not buffer announcement after initial period")

		// Verify nothing was buffered
		announcements := sm.GetBufferedAnnouncements()
		assert.Len(t, announcements, 0)
	})

	t.Run("clears_buffer_when_getting_announcements", func(t *testing.T) {
		sm := NewSyncManager(ulogger.New("test"), tSettings)

		// Buffer multiple announcements
		for i := 0; i < 5; i++ {
			announcement := &BlockAnnouncement{
				Hash:       fmt.Sprintf("test-hash-%d", i),
				Height:     uint32(100 + i),
				DataHubURL: "http://test.com",
				PeerID:     fmt.Sprintf("peer%d", i),
				From:       fmt.Sprintf("from%d", i),
				Timestamp:  time.Now(),
			}
			sm.BufferBlockAnnouncement(announcement)
		}

		// Get buffered announcements
		announcements := sm.GetBufferedAnnouncements()
		assert.Len(t, announcements, 5)

		// Buffer should be cleared
		announcements2 := sm.GetBufferedAnnouncements()
		assert.Len(t, announcements2, 0)
	})

	t.Run("correctly_reports_initial_sync_status", func(t *testing.T) {
		sm := NewSyncManager(ulogger.New("test"), tSettings)

		// Initially not complete
		assert.False(t, sm.IsInitialSyncComplete())

		// Mark as complete
		sm.initialSelectionDone = true
		assert.True(t, sm.IsInitialSyncComplete())
	})

	t.Run("correctly_reports_sync_peer_need", func(t *testing.T) {
		sm := NewSyncManager(ulogger.New("test"), tSettings)

		// No sync peer initially
		assert.False(t, sm.NeedsSyncPeer())

		// Set a sync peer
		sm.syncPeer = peer.ID("test-peer")
		assert.True(t, sm.NeedsSyncPeer())

		// Clear sync peer
		sm.syncPeer = ""
		assert.False(t, sm.NeedsSyncPeer())
	})
}

func TestSyncManager_InitialSyncBufferingIntegration(t *testing.T) {
	tSettings := createBaseTestSettings()
	tSettings.ChainCfgParams = &chaincfg.MainNetParams

	t.Run("buffers_multiple_announcements_until_sync_complete", func(t *testing.T) {
		sm := NewSyncManager(ulogger.New("test"), tSettings)

		// Initial sync should not be complete
		assert.False(t, sm.IsInitialSyncComplete())
		assert.False(t, sm.NeedsSyncPeer())

		// Buffer multiple announcements from different peers
		announcements := make([]*BlockAnnouncement, 0)
		for i := 0; i < 10; i++ {
			announcement := &BlockAnnouncement{
				Hash:       fmt.Sprintf("hash-%d", i),
				Height:     uint32(100 + i),
				DataHubURL: fmt.Sprintf("http://peer%d.com", i),
				PeerID:     fmt.Sprintf("peer%d", i),
				From:       fmt.Sprintf("from%d", i),
				Timestamp:  time.Now(),
			}
			announcements = append(announcements, announcement)

			// Should buffer during initial period
			buffered := sm.BufferBlockAnnouncement(announcement)
			assert.True(t, buffered, "Should buffer announcement %d during initial period", i)
		}

		// Mark initial sync as complete
		sm.initialSelectionDone = true
		assert.True(t, sm.IsInitialSyncComplete())

		// Get buffered announcements
		retrieved := sm.GetBufferedAnnouncements()
		assert.Len(t, retrieved, 10, "Should retrieve all 10 buffered announcements")

		// Verify announcements are in order
		for i, ann := range retrieved {
			assert.Equal(t, fmt.Sprintf("hash-%d", i), ann.Hash)
			assert.Equal(t, uint32(100+i), ann.Height)
		}

		// Buffer should be cleared after retrieval
		retrieved2 := sm.GetBufferedAnnouncements()
		assert.Len(t, retrieved2, 0, "Buffer should be empty after retrieval")

		// New announcements after initial sync shouldn't be buffered
		newAnn := &BlockAnnouncement{
			Hash:       "new-hash",
			Height:     200,
			DataHubURL: "http://new.com",
			PeerID:     "new-peer",
			From:       "new-from",
			Timestamp:  time.Now(),
		}
		buffered := sm.BufferBlockAnnouncement(newAnn)
		assert.False(t, buffered, "Should not buffer after initial sync complete")
	})

	t.Run("handles_sync_peer_selection_with_buffering", func(t *testing.T) {
		sm := NewSyncManager(ulogger.New("test"), tSettings)
		sm.Start(context.Background())

		// Set up callbacks
		sm.SetPeerHeightCallback(func(p peer.ID) int32 {
			switch p {
			case peer.ID("peer1"):
				return 100
			case peer.ID("peer2"):
				return 150 // Highest
			case peer.ID("peer3"):
				return 120
			default:
				return 0
			}
		})
		sm.SetLocalHeightCallback(func() uint32 {
			return 90
		})

		// Add peers
		sm.peerStates.AddPeer(peer.ID("peer1"), true)
		sm.peerStates.AddPeer(peer.ID("peer2"), true)
		sm.peerStates.AddPeer(peer.ID("peer3"), true)

		// Buffer some announcements during initial period
		for i := 0; i < 5; i++ {
			announcement := &BlockAnnouncement{
				Hash:       fmt.Sprintf("hash-%d", i),
				Height:     uint32(100 + i),
				DataHubURL: "http://test.com",
				PeerID:     "peer1",
				From:       "from1",
				Timestamp:  time.Now(),
			}
			buffered := sm.BufferBlockAnnouncement(announcement)
			assert.True(t, buffered, "Should buffer during initial period")
		}

		// Force initial sync completion
		sm.startTime = time.Now().Add(-25 * time.Second) // Exceed max wait
		sm.UpdatePeerHeight(peer.ID("peer2"), 150)

		// Should have selected peer2 as sync peer (highest)
		assert.Equal(t, peer.ID("peer2"), sm.GetSyncPeer())
		assert.True(t, sm.NeedsSyncPeer())

		// Get buffered announcements - they should be discarded if we need sync
		announcements := sm.GetBufferedAnnouncements()
		assert.Len(t, announcements, 5, "Should still have buffered announcements")
	})
}

func TestSyncManager_UpdateCallbacks(t *testing.T) {
	tSettings := createBaseTestSettings()
	tSettings.ChainCfgParams = &chaincfg.MainNetParams

	t.Run("update_peer_height_triggers_sync", func(t *testing.T) {
		sm := NewSyncManager(ulogger.New("test"), tSettings)

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
		sm := NewSyncManager(ulogger.New("test"), tSettings)

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
		sm := NewSyncManager(ulogger.New("test"), tSettings)

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

func TestSyncManager_ForcedSyncPeer(t *testing.T) {
	t.Run("set_forced_sync_peer_valid", func(t *testing.T) {
		tSettings := createBaseTestSettings()
		sm := NewSyncManager(ulogger.New("test"), tSettings)

		// Valid peer ID
		validPeerID := "12D3KooWG6aCkDmi5tqx4G4AvVDTQdSVvTSzzQvk1vh9CtSR8KEW"

		// Set forced sync peer
		err := sm.SetForceSyncPeer(validPeerID)
		require.NoError(t, err)

		// Verify it was set
		expectedPeer, _ := peer.Decode(validPeerID)
		assert.Equal(t, expectedPeer, sm.forceSyncPeer)
	})

	t.Run("set_forced_sync_peer_invalid", func(t *testing.T) {
		tSettings := createBaseTestSettings()
		sm := NewSyncManager(ulogger.New("test"), tSettings)

		// Invalid peer ID
		invalidPeerID := "invalid-peer-id"

		// Try to set forced sync peer
		err := sm.SetForceSyncPeer(invalidPeerID)
		assert.Error(t, err)

		// Verify it was not set
		assert.Equal(t, peer.ID(""), sm.forceSyncPeer)
	})

	t.Run("clear_forced_sync_peer", func(t *testing.T) {
		tSettings := createBaseTestSettings()
		sm := NewSyncManager(ulogger.New("test"), tSettings)

		// Set a forced sync peer first
		validPeerID := "12D3KooWG6aCkDmi5tqx4G4AvVDTQdSVvTSzzQvk1vh9CtSR8KEW"
		err := sm.SetForceSyncPeer(validPeerID)
		require.NoError(t, err)

		// Clear it
		err = sm.SetForceSyncPeer("")
		require.NoError(t, err)

		// Verify it was cleared
		assert.Equal(t, peer.ID(""), sm.forceSyncPeer)
	})

	t.Run("forced_peer_overrides_selection", func(t *testing.T) {
		tSettings := createBaseTestSettings()
		sm := NewSyncManager(ulogger.New("test"), tSettings)

		forcedPeerID := peer.ID("forced-peer")
		higherPeerID := peer.ID("higher-peer")

		// Set up peer heights
		peerHeights := map[peer.ID]int32{
			forcedPeerID: 100, // Lower height
			higherPeerID: 150, // Higher height
		}

		sm.SetPeerHeightCallback(func(p peer.ID) int32 {
			return peerHeights[p]
		})
		sm.SetLocalHeightCallback(func() uint32 {
			return 90
		})

		// Add both peers
		sm.peerStates.AddPeer(forcedPeerID, true)
		sm.peerStates.AddPeer(higherPeerID, true)

		// Set forced sync peer
		sm.forceSyncPeer = forcedPeerID

		// Select sync peer - should return forced peer even though higher peer exists
		selected := sm.selectSyncPeer()
		assert.Equal(t, forcedPeerID, selected)
	})

	t.Run("forced_peer_not_connected_waits", func(t *testing.T) {
		tSettings := createBaseTestSettings()
		sm := NewSyncManager(ulogger.New("test"), tSettings)

		forcedPeerID := peer.ID("forced-peer")
		availablePeerID := peer.ID("available-peer")

		// Set up peer heights
		peerHeights := map[peer.ID]int32{
			availablePeerID: 120,
		}

		sm.SetPeerHeightCallback(func(p peer.ID) int32 {
			return peerHeights[p]
		})
		sm.SetLocalHeightCallback(func() uint32 {
			return 90
		})

		// Only add available peer (forced peer not connected)
		sm.peerStates.AddPeer(availablePeerID, true)

		// Set forced sync peer that's not connected
		sm.forceSyncPeer = forcedPeerID

		// Select sync peer - should return empty (waiting for forced peer)
		selected := sm.selectSyncPeer()
		assert.Equal(t, peer.ID(""), selected, "Should not select any peer when waiting for forced peer")

		// Verify sync peer was not set in selectSyncPeerLocked either
		wasSet := sm.selectSyncPeerLocked()
		assert.False(t, wasSet, "Should not set sync peer when forced peer is not available")
		assert.Equal(t, peer.ID(""), sm.syncPeer, "Sync peer should remain empty")
	})

	t.Run("forced_peer_reconnects", func(t *testing.T) {
		tSettings := createBaseTestSettings()
		sm := NewSyncManager(ulogger.New("test"), tSettings)

		forcedPeerID := peer.ID("forced-peer")
		sm.forceSyncPeer = forcedPeerID

		sm.SetLocalHeightCallback(func() uint32 {
			return 90
		})

		// Add forced peer (simulating reconnection)
		sm.peerStates.AddPeer(forcedPeerID, true)

		// Update height - should trigger selection of forced peer
		wasSelected := sm.UpdatePeerHeight(forcedPeerID, 100)
		assert.True(t, wasSelected)
		assert.Equal(t, forcedPeerID, sm.syncPeer)
	})

	t.Run("forced_peer_health_check_lenient", func(t *testing.T) {
		tSettings := createBaseTestSettings()
		sm := NewSyncManager(ulogger.New("test"), tSettings)

		forcedPeerID := peer.ID("forced-peer")
		sm.forceSyncPeer = forcedPeerID
		sm.syncPeer = forcedPeerID
		sm.syncPeerState = &syncPeerState{
			lastBlockTime: time.Now().Add(-5 * time.Minute), // Past normal timeout
			violations:    5,                                // Past normal violation limit
			ticks:         1,
		}

		// Run health check
		sm.checkSyncPeer()

		// Should keep the forced peer despite violations
		assert.Equal(t, forcedPeerID, sm.syncPeer)
		assert.NotNil(t, sm.syncPeerState)
	})

	t.Run("does_not_select_other_peers_when_waiting_for_forced", func(t *testing.T) {
		tSettings := createBaseTestSettings()
		sm := NewSyncManager(ulogger.New("test"), tSettings)

		forcedPeerID := peer.ID("forced-peer")
		otherPeer1 := peer.ID("other-peer-1")
		otherPeer2 := peer.ID("other-peer-2")

		// Set forced sync peer
		sm.forceSyncPeer = forcedPeerID

		// Set up peer heights - other peers are ahead
		peerHeights := map[peer.ID]int32{
			otherPeer1: 150,
			otherPeer2: 160,
		}

		sm.SetPeerHeightCallback(func(p peer.ID) int32 {
			return peerHeights[p]
		})
		sm.SetLocalHeightCallback(func() uint32 {
			return 100
		})

		// Add other peers (but not the forced peer)
		sm.peerStates.AddPeer(otherPeer1, true)
		sm.peerStates.AddPeer(otherPeer2, true)

		// Try to select a sync peer - should not select any
		selected := sm.selectSyncPeer()
		assert.Equal(t, peer.ID(""), selected, "Should not select any peer when waiting for forced peer")

		// Try using selectSyncPeerLocked - should also not select
		wasSet := sm.selectSyncPeerLocked()
		assert.False(t, wasSet)
		assert.Equal(t, peer.ID(""), sm.syncPeer, "Should not have any sync peer")

		// Now add the forced peer
		sm.peerStates.AddPeer(forcedPeerID, true)
		peerHeights[forcedPeerID] = 105 // Lower than other peers but still ahead

		// Now selection should work and choose the forced peer
		selected = sm.selectSyncPeer()
		assert.Equal(t, forcedPeerID, selected, "Should select forced peer once available")
	})
}
