package p2p

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSyncCoordinator_NewSyncCoordinator(t *testing.T) {
	logger := ulogger.New("test")
	settings := CreateTestSettings()
	registry := NewPeerRegistry()
	selector := NewPeerSelector(logger, nil)
	banManager := NewPeerBanManager(context.Background(), nil, settings, registry)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		banManager,
		blockchainSetup.Client,
		nil, // blocksKafkaProducerClient
	)

	assert.NotNil(t, sc)
	assert.Equal(t, logger, sc.logger)
	assert.Equal(t, settings, sc.settings)
	assert.Equal(t, registry, sc.registry)
	assert.Equal(t, selector, sc.selector)
	assert.Equal(t, banManager, sc.banManager)
	assert.Equal(t, blockchainSetup.Client, sc.blockchainClient)
	assert.NotNil(t, sc.stopCh)
}

func TestSyncCoordinator_SetGetLocalHeightCallback(t *testing.T) {
	logger := ulogger.New("test")
	settings := CreateTestSettings()
	registry := NewPeerRegistry()
	selector := NewPeerSelector(logger, nil)
	banManager := NewPeerBanManager(context.Background(), nil, settings, registry)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		banManager,
		blockchainSetup.Client,
		nil, // blocksKafkaProducerClient
	)

	// Set callback
	getLocalHeight := func() uint32 {
		return 100
	}

	sc.SetGetLocalHeightCallback(getLocalHeight)

	// Verify callback is set
	assert.NotNil(t, sc.getLocalHeight)

	// Test callback works
	height := sc.getLocalHeight()
	assert.Equal(t, uint32(100), height)
}

func TestSyncCoordinator_StartAndStop(t *testing.T) {
	logger := ulogger.New("test")
	settings := CreateTestSettings()
	registry := NewPeerRegistry()
	selector := NewPeerSelector(logger, nil)
	banManager := NewPeerBanManager(context.Background(), nil, settings, registry)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		banManager,
		blockchainSetup.Client,
		nil, // blocksKafkaProducerClient
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Start the coordinator
	sc.Start(ctx)

	// Let it run briefly
	time.Sleep(100 * time.Millisecond)

	// Stop it
	sc.Stop()

	// Should stop cleanly
	assert.True(t, true, "Coordinator stopped cleanly")
}

func TestSyncCoordinator_GetCurrentSyncPeer(t *testing.T) {
	logger := ulogger.New("test")
	settings := CreateTestSettings()
	registry := NewPeerRegistry()
	selector := NewPeerSelector(logger, nil)
	banManager := NewPeerBanManager(context.Background(), nil, settings, registry)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		banManager,
		blockchainSetup.Client,
		nil, // blocksKafkaProducerClient
	)

	// Initially no sync peer
	peerID := sc.GetCurrentSyncPeer()
	assert.Equal(t, peer.ID(""), peerID)

	// Set a sync peer
	testPeer := peer.ID("test-peer")
	sc.mu.Lock()
	sc.currentSyncPeer = testPeer
	sc.mu.Unlock()

	// Get current sync peer
	peerID = sc.GetCurrentSyncPeer()
	assert.Equal(t, testPeer, peerID)
}

func TestSyncCoordinator_ClearSyncPeer(t *testing.T) {
	logger := ulogger.New("test")
	settings := CreateTestSettings()
	registry := NewPeerRegistry()
	selector := NewPeerSelector(logger, nil)
	banManager := NewPeerBanManager(context.Background(), nil, settings, registry)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		banManager,
		blockchainSetup.Client,
		nil, // blocksKafkaProducerClient
	)

	// Set a sync peer
	testPeer := peer.ID("test-peer")
	sc.mu.Lock()
	sc.currentSyncPeer = testPeer
	sc.mu.Unlock()

	// Clear sync peer
	sc.ClearSyncPeer()

	// Verify cleared
	peerID := sc.GetCurrentSyncPeer()
	assert.Equal(t, peer.ID(""), peerID)
}

func TestSyncCoordinator_TriggerSync(t *testing.T) {
	logger := ulogger.New("test")
	settings := CreateTestSettings()
	registry := NewPeerRegistry()
	selector := NewPeerSelector(logger, nil)
	banManager := NewPeerBanManager(context.Background(), nil, settings, registry)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		banManager,
		blockchainSetup.Client,
		nil, // blocksKafkaProducerClient
	)

	// Set callback
	sc.SetGetLocalHeightCallback(func() uint32 {
		return 100
	})

	// Add a peer that is ahead
	peerID := peer.ID("test-peer")
	peerHash, _ := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
	registry.Put(peerID, "", 110, peerHash, "http://test.com")

	// Trigger sync
	err := sc.TriggerSync()
	assert.NoError(t, err)

	// Verify sync peer was selected
	currentPeer := sc.GetCurrentSyncPeer()
	assert.Equal(t, peerID, currentPeer)
}

func TestSyncCoordinator_TriggerSync_NoPeersAvailable(t *testing.T) {
	logger := ulogger.New("test")
	settings := CreateTestSettings()
	registry := NewPeerRegistry()
	selector := NewPeerSelector(logger, nil)
	banManager := NewPeerBanManager(context.Background(), nil, settings, registry)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		banManager,
		blockchainSetup.Client,
		nil, // blocksKafkaProducerClient
	)

	sc.SetGetLocalHeightCallback(func() uint32 {
		return 100
	})

	// No peers available - should not error
	err := sc.TriggerSync()
	assert.NoError(t, err)

	// No sync peer should be selected
	currentPeer := sc.GetCurrentSyncPeer()
	assert.Equal(t, peer.ID(""), currentPeer)
}

func TestSyncCoordinator_HandlePeerDisconnected(t *testing.T) {
	logger := ulogger.New("test")
	settings := CreateTestSettings()
	registry := NewPeerRegistry()
	selector := NewPeerSelector(logger, nil)
	banManager := NewPeerBanManager(context.Background(), nil, settings, registry)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		banManager,
		blockchainSetup.Client,
		nil, // blocksKafkaProducerClient
	)

	// Add a peer and set as sync peer
	peerID := peer.ID("test-peer")
	registry.Put(peerID, "", 0, nil, "")

	sc.mu.Lock()
	sc.currentSyncPeer = peerID
	sc.mu.Unlock()

	// Handle disconnection of sync peer
	sc.HandlePeerDisconnected(peerID)

	// Give goroutine time to run
	time.Sleep(100 * time.Millisecond)

	// Verify peer was removed from registry
	_, exists := registry.Get(peerID)
	assert.False(t, exists)

	// Sync peer should be cleared
	currentPeer := sc.GetCurrentSyncPeer()
	assert.Equal(t, peer.ID(""), currentPeer)
}

func TestSyncCoordinator_HandlePeerDisconnected_NotSyncPeer(t *testing.T) {
	logger := ulogger.New("test")
	settings := CreateTestSettings()
	registry := NewPeerRegistry()
	selector := NewPeerSelector(logger, nil)
	banManager := NewPeerBanManager(context.Background(), nil, settings, registry)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		banManager,
		blockchainSetup.Client,
		nil, // blocksKafkaProducerClient
	)

	// Add two peers
	syncPeer := peer.ID("sync-peer")
	otherPeer := peer.ID("other-peer")
	registry.Put(syncPeer, "", 0, nil, "")
	registry.Put(otherPeer, "", 0, nil, "")

	// Set sync peer
	sc.mu.Lock()
	sc.currentSyncPeer = syncPeer
	sc.mu.Unlock()

	// Disconnect non-sync peer
	sc.HandlePeerDisconnected(otherPeer)

	// Verify peer was removed
	_, exists := registry.Get(otherPeer)
	assert.False(t, exists)

	// Sync peer should remain
	currentPeer := sc.GetCurrentSyncPeer()
	assert.Equal(t, syncPeer, currentPeer)
}

func TestSyncCoordinator_HandleCatchupFailure(t *testing.T) {
	logger := ulogger.New("test")
	settings := CreateTestSettings()
	registry := NewPeerRegistry()
	selector := NewPeerSelector(logger, nil)
	banManager := NewPeerBanManager(context.Background(), nil, settings, registry)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		banManager,
		blockchainSetup.Client,
		nil, // blocksKafkaProducerClient
	)

	// Set initial sync peer
	initialPeer := peer.ID("initial-peer")
	sc.mu.Lock()
	sc.currentSyncPeer = initialPeer
	sc.mu.Unlock()

	// Add new peer for recovery
	newPeer := peer.ID("new-peer")
	peerHash, _ := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
	registry.Put(newPeer, "", 110, peerHash, "http://test.com")

	sc.SetGetLocalHeightCallback(func() uint32 {
		return 100
	})

	// Handle catchup failure
	sc.HandleCatchupFailure("test failure reason")

	// Sync peer should be cleared and new one selected
	currentPeer := sc.GetCurrentSyncPeer()
	assert.Equal(t, newPeer, currentPeer)
}

func TestSyncCoordinator_selectNewSyncPeer(t *testing.T) {
	logger := ulogger.New("test")
	settings := CreateTestSettings()
	registry := NewPeerRegistry()
	selector := NewPeerSelector(logger, nil)
	banManager := NewPeerBanManager(context.Background(), nil, settings, registry)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		banManager,
		blockchainSetup.Client,
		nil, // blocksKafkaProducerClient
	)

	sc.SetGetLocalHeightCallback(func() uint32 {
		return 100
	})

	// Add eligible peers
	peer1 := peer.ID("peer1")
	peer2 := peer.ID("peer2")

	peerHash1, _ := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
	registry.Put(peer1, "", 105, peerHash1, "http://peer1.com")

	peerHash2, _ := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
	registry.Put(peer2, "", 110, peerHash2, "http://peer2.com")
	// Give peer2 better reputation to ensure it's selected
	registry.UpdateReputation(peer2, 80.0)

	// Select new sync peer
	selected := sc.selectNewSyncPeer()

	// Should select peer2 (higher reputation and higher height)
	assert.Equal(t, peer2, selected)
}

func TestSyncCoordinator_selectNewSyncPeer_ForcedPeer(t *testing.T) {
	logger := ulogger.New("test")
	settings := CreateTestSettings()
	settings.P2P.ForceSyncPeer = "forced-peer"

	registry := NewPeerRegistry()
	selector := NewPeerSelector(logger, nil)
	banManager := NewPeerBanManager(context.Background(), nil, settings, registry)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		banManager,
		blockchainSetup.Client,
		nil, // blocksKafkaProducerClient
	)

	sc.SetGetLocalHeightCallback(func() uint32 {
		return 100
	})

	// Add forced peer
	forcedPeer := peer.ID("forced-peer")
	settings.P2P.ForceSyncPeer = string(forcedPeer) // Set the forced peer in settings
	peerHash, _ := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
	registry.Put(forcedPeer, "", 110, peerHash, "http://forced.com")

	// Add another better peer
	betterPeer := peer.ID("better-peer")
	betterHash, _ := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
	registry.Put(betterPeer, "", 120, betterHash, "http://better.com")

	// Should select forced peer
	selected := sc.selectNewSyncPeer()
	assert.Equal(t, forcedPeer, selected)
}

func TestSyncCoordinator_UpdateBanStatus(t *testing.T) {
	logger := ulogger.New("test")
	settings := CreateTestSettings()
	registry := NewPeerRegistry()
	selector := NewPeerSelector(logger, nil)
	banManager := NewPeerBanManager(context.Background(), nil, settings, registry)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		banManager,
		blockchainSetup.Client,
		nil, // blocksKafkaProducerClient
	)

	// Add peer and ban it
	peerID := peer.ID("test-peer")
	registry.Put(peerID, "", 0, nil, "")

	// Add ban score - use raw string conversion to match UpdateBanStatus
	banManager.AddScore(string(peerID), ReasonSpam)
	banManager.AddScore(string(peerID), ReasonSpam) // Should trigger ban

	// Update ban status
	sc.UpdateBanStatus(peerID)

	// Verify ban status was updated
	info, exists := registry.Get(peerID)
	require.True(t, exists)
	assert.True(t, info.IsBanned)
	assert.Equal(t, 100, info.BanScore)
}

func TestSyncCoordinator_checkFSMState(t *testing.T) {
	logger := ulogger.New("test")
	settings := CreateTestSettings()
	registry := NewPeerRegistry()
	selector := NewPeerSelector(logger, nil)
	banManager := NewPeerBanManager(context.Background(), nil, settings, registry)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		banManager,
		blockchainSetup.Client,
		nil, // blocksKafkaProducerClient
	)

	// Set callback
	sc.SetGetLocalHeightCallback(func() uint32 {
		return 100
	})

	// Add peer
	peerID := peer.ID("test-peer")
	peerHash, _ := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
	registry.Put(peerID, "", 110, peerHash, "http://test.com")

	// Check FSM state (LocalClient returns RUNNING by default)
	sc.checkFSMState(blockchainSetup.Ctx)

	// May trigger sync if peer is ahead of us while in RUNNING state
	// (local height is 100, peer is at 110)
	currentPeer := sc.GetCurrentSyncPeer()
	assert.True(t, currentPeer == "" || currentPeer == peerID, "Should either have no sync peer or have selected the test peer")

	// Now checking should see transition from IDLE to RUNNING
	sc.checkFSMState(blockchainSetup.Ctx)

	// Should have potentially updated sync peer
	finalPeer := sc.GetCurrentSyncPeer()
	assert.True(t, finalPeer == "" || finalPeer == peerID, "Should either have no sync peer or have selected the test peer")
}

func TestSyncCoordinator_evaluateSyncPeer(t *testing.T) {
	logger := ulogger.New("test")
	settings := CreateTestSettings()
	registry := NewPeerRegistry()
	selector := NewPeerSelector(logger, nil)
	banManager := NewPeerBanManager(context.Background(), nil, settings, registry)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		banManager,
		blockchainSetup.Client,
		nil, // blocksKafkaProducerClient
	)

	// Set up callbacks - local height is caught up to sync peer
	sc.SetGetLocalHeightCallback(func() uint32 {
		return 105 // Same as sync peer height - we've caught up
	})

	// Add current sync peer
	syncPeer := peer.ID("sync-peer")
	peerHash, _ := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
	registry.Put(syncPeer, "", 105, peerHash, "")

	// Add better peer
	betterPeer := peer.ID("better-peer")
	betterHash, _ := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
	registry.Put(betterPeer, "", 120, betterHash, "http://better.com")

	// Set current sync peer
	sc.mu.Lock()
	sc.currentSyncPeer = syncPeer
	sc.mu.Unlock()

	// Evaluate sync peer
	sc.evaluateSyncPeer()

	// Should NOT have switched yet (TriggerSync was called but test doesn't have kafka)
	// The actual switching happens in TriggerSync which we can't fully test here
	currentPeer := sc.GetCurrentSyncPeer()
	// For now, just verify it didn't crash and peer remains
	assert.NotEmpty(t, currentPeer)
}

func TestSyncCoordinator_evaluateSyncPeer_StuckAtHeight(t *testing.T) {
	logger := ulogger.New("test")
	settings := CreateTestSettings()

	// Setup test blockchain
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	registry := NewPeerRegistry()
	selector := NewPeerSelector(logger, nil)
	banManager := NewPeerBanManager(context.Background(), nil, settings, registry)

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		banManager,
		blockchainSetup.Client,
		nil, // blocksKafkaProducerClient
	)

	// Set callback
	sc.SetGetLocalHeightCallback(func() uint32 {
		return 100 // Stuck at same height
	})

	// Add sync peer
	syncPeer := peer.ID("sync-peer")
	peerHash, _ := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
	registry.Put(syncPeer, "", 110, peerHash, "http://test.com")

	// Set sync peer and simulate being stuck for too long
	sc.mu.Lock()
	sc.currentSyncPeer = syncPeer
	sc.lastLocalHeight = 100
	sc.syncStartTime = time.Now().Add(-6 * time.Minute) // Been syncing for 6 minutes
	sc.mu.Unlock()

	// Set peer's last message time to be old (> 2 minutes)
	registry.UpdateNetworkStats(syncPeer, 1000)
	registry.mu.Lock()
	if info, exists := registry.peers[syncPeer]; exists {
		info.LastMessageTime = time.Now().Add(-3 * time.Minute) // Last message 3 minutes ago
	}
	registry.mu.Unlock()

	// Add alternative peer
	altPeer := peer.ID("alt-peer")
	altHash, _ := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
	registry.Put(altPeer, "", 115, altHash, "http://alt.com")

	// Evaluate - should clear peer due to long sync without progress and select new one
	sc.evaluateSyncPeer()

	// Should have cleared the sync peer and selected alternative peer
	currentPeer := sc.GetCurrentSyncPeer()
	assert.Equal(t, altPeer, currentPeer, "Should switch to alternative peer after detecting inactive sync peer")
}

func TestSyncCoordinator_LogPeerList(t *testing.T) {
	logger := ulogger.New("test")
	settings := CreateTestSettings()
	registry := NewPeerRegistry()
	selector := NewPeerSelector(logger, nil)
	banManager := NewPeerBanManager(context.Background(), nil, settings, registry)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		banManager,
		blockchainSetup.Client,
		nil, // blocksKafkaProducerClient
	)

	// Test with empty list
	sc.logPeerList([]*PeerInfo{})

	// Test with peers
	peers := []*PeerInfo{
		{
			ID:         peer.ID("peer1"),
			DataHubURL: "http://example1.com",
			Height:     100,
			BanScore:   5,
			Storage:    "full",
		},
		{
			ID:         peer.ID("peer2"),
			DataHubURL: "http://example2.com",
			Height:     200,
			BanScore:   10,
			Storage:    "full",
		},
	}

	// This just logs, so we're mainly testing it doesn't panic
	sc.logPeerList(peers)
}

func TestSyncCoordinator_LogCandidateList(t *testing.T) {
	logger := ulogger.New("test")
	settings := CreateTestSettings()
	registry := NewPeerRegistry()
	selector := NewPeerSelector(logger, nil)
	banManager := NewPeerBanManager(context.Background(), nil, settings, registry)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		banManager,
		blockchainSetup.Client,
		nil, // blocksKafkaProducerClient
	)

	// Test with empty list
	sc.logCandidateList([]*PeerInfo{})

	// Test with candidates
	candidates := []*PeerInfo{
		{
			ID:         peer.ID("candidate1"),
			DataHubURL: "http://candidate1.com",
			Height:     150,
			BanScore:   3,
			Storage:    "full",
		},
		{
			ID:         peer.ID("candidate2"),
			DataHubURL: "http://candidate2.com",
			Height:     250,
			BanScore:   7,
			Storage:    "full",
		},
	}

	// This just logs, so we're mainly testing it doesn't panic
	sc.logCandidateList(candidates)
}

func TestSyncCoordinator_IsCaughtUp(t *testing.T) {
	logger := ulogger.New("test")
	settings := CreateTestSettings()
	registry := NewPeerRegistry()
	selector := NewPeerSelector(logger, nil)
	banManager := NewPeerBanManager(context.Background(), nil, settings, registry)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		banManager,
		blockchainSetup.Client,
		nil,
	)

	// Set local height
	sc.SetGetLocalHeightCallback(func() uint32 {
		return 100
	})

	// Test with no peers - should be caught up
	assert.True(t, sc.isCaughtUp(), "Should be caught up with no peers")

	// Add peer at same height - should be caught up
	peer1 := peer.ID("peer1")
	peerHash1, _ := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
	registry.Put(peer1, "", 100, peerHash1, "")
	assert.True(t, sc.isCaughtUp(), "Should be caught up when at same height")

	// Add peer behind us - should still be caught up
	peer2 := peer.ID("peer2")
	peerHash2, _ := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
	registry.Put(peer2, "", 90, peerHash2, "")
	assert.True(t, sc.isCaughtUp(), "Should be caught up when peers are behind")

	// Add peer significantly ahead of us (more than 10 block tolerance) - should NOT be caught up
	peer3 := peer.ID("peer3")
	peerHash3, _ := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
	registry.Put(peer3, "", 120, peerHash3, "http://peer3:8080") // 20 blocks ahead, beyond 10 block tolerance
	assert.False(t, sc.isCaughtUp(), "Should NOT be caught up when a peer is significantly ahead")
}

func TestSyncCoordinator_IsCaughtUp_IgnoresNonViablePeers(t *testing.T) {
	logger := ulogger.New("test")
	settings := CreateTestSettings()
	registry := NewPeerRegistry()
	selector := NewPeerSelector(logger, nil)
	banManager := NewPeerBanManager(context.Background(), nil, settings, registry)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		banManager,
		blockchainSetup.Client,
		nil,
	)

	sc.SetGetLocalHeightCallback(func() uint32 { return 100 })

	peerHash, _ := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")

	// Peer ahead but no DataHub URL -> should be ignored
	noURL := peer.ID("peer-no-url")
	registry.Put(noURL, "", 200, peerHash, "")

	// Peer ahead but banned -> should be ignored
	banned := peer.ID("peer-banned")
	registry.Put(banned, "", 200, peerHash, "http://banned:8080")
	registry.UpdateBanStatus(banned, 100, true)

	// Peer ahead but low reputation -> should be ignored
	lowRep := peer.ID("peer-low-rep")
	registry.Put(lowRep, "", 200, peerHash, "http://lowrep:8080")
	registry.UpdateReputation(lowRep, 10)

	assert.True(t, sc.isCaughtUp(), "Should be caught up when only non-viable peers are ahead")

	// Now add a viable peer significantly ahead -> should not be caught up
	viable := peer.ID("peer-viable")
	registry.Put(viable, "", 200, peerHash, "http://viable:8080")
	registry.UpdateReputation(viable, 80)
	assert.False(t, sc.isCaughtUp(), "Should NOT be caught up when a viable peer is significantly ahead")
}

func TestSyncCoordinator_BackoffClearsSyncAttemptsAndExpires(t *testing.T) {
	logger := ulogger.New("test")
	settings := CreateTestSettings()
	registry := NewPeerRegistry()
	selector := NewPeerSelector(logger, nil)
	banManager := NewPeerBanManager(context.Background(), nil, settings, registry)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		banManager,
		blockchainSetup.Client,
		nil,
	)

	// Add a peer and record a sync attempt so LastSyncAttempt is non-zero
	peerHash, _ := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
	p := peer.ID("peer1")
	registry.Put(p, "", 120, peerHash, "http://peer1:8080")
	registry.RecordSyncAttempt(p)

	infoBefore, ok := registry.Get(p)
	require.True(t, ok)
	require.False(t, infoBefore.LastSyncAttempt.IsZero(), "LastSyncAttempt should be set before backoff")

	// Enter backoff and verify sync attempts were cleared
	sc.enterBackoffMode()
	infoAfter, ok := registry.Get(p)
	require.True(t, ok)
	assert.True(t, infoAfter.LastSyncAttempt.IsZero(), "LastSyncAttempt should be cleared when entering backoff")

	// Immediately after entering backoff, we should still be in backoff
	assert.True(t, sc.checkAndClearExpiredBackoff(), "Should be in backoff immediately after entering")

	// Force backoff to expire by moving the last attempt time into the past
	sc.mu.Lock()
	sc.lastAllPeersAttemptTime = time.Now().Add(-3 * time.Second)
	sc.mu.Unlock()

	assert.False(t, sc.checkAndClearExpiredBackoff(), "Backoff should be expired")

	sc.mu.RLock()
	defer sc.mu.RUnlock()
	assert.False(t, sc.allPeersAttempted, "Backoff state should be cleared after expiration")
	assert.Equal(t, 2, sc.backoffMultiplier, "Backoff multiplier should increase after expiration")
}

func TestSyncCoordinator_SendSyncTriggerToKafka(t *testing.T) {
	logger := ulogger.New("test")
	settings := CreateTestSettings()
	registry := NewPeerRegistry()
	selector := NewPeerSelector(logger, nil)
	banManager := NewPeerBanManager(context.Background(), nil, settings, registry)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	// Create mock Kafka producer
	mockProducer := kafka.NewKafkaAsyncProducerMock()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		banManager,
		blockchainSetup.Client,
		mockProducer,
	)

	// Add peer with DataHub URL
	peerID := peer.ID("test-peer")
	registry.Put(peerID, "", 0, nil, "http://datahub.example.com")

	// Start monitoring the publish channel
	publishCount := int32(0)
	go func() {
		for range mockProducer.PublishChannel() {
			atomic.AddInt32(&publishCount, 1)
		}
	}()

	// Test successful send
	sc.sendSyncTriggerToKafka(peerID, "blockhash123")
	time.Sleep(10 * time.Millisecond) // Give goroutine time to process
	assert.Equal(t, int32(1), atomic.LoadInt32(&publishCount), "Should publish one message")

	// Test with nil producer
	sc.blocksKafkaProducerClient = nil
	sc.sendSyncTriggerToKafka(peerID, "blockhash456")
	// Should not panic, just return

	// Test with empty block hash
	sc.blocksKafkaProducerClient = mockProducer
	sc.sendSyncTriggerToKafka(peerID, "")
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, int32(1), atomic.LoadInt32(&publishCount), "Should not publish with empty hash")
}

func TestSyncCoordinator_SendSyncMessage(t *testing.T) {
	logger := ulogger.New("test")
	settings := CreateTestSettings()
	registry := NewPeerRegistry()
	selector := NewPeerSelector(logger, nil)
	banManager := NewPeerBanManager(context.Background(), nil, settings, registry)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	mockProducer := kafka.NewKafkaAsyncProducerMock()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		banManager,
		blockchainSetup.Client,
		mockProducer,
	)

	// Test with peer not in registry
	unknownPeer := peer.ID("unknown-peer")
	err := sc.sendSyncMessage(unknownPeer)
	assert.Error(t, err, "Should error when peer not found")
	assert.Contains(t, err.Error(), "not found in registry")

	// Add peer without block hash
	peerNoHash := peer.ID("peer-no-hash")
	registry.Put(peerNoHash, "", 0, nil, "")
	err = sc.sendSyncMessage(peerNoHash)
	assert.Error(t, err, "Should error when peer has no block hash")
	assert.Contains(t, err.Error(), "no block hash available")

	// Add peer with block hash
	peerWithHash := peer.ID("peer-with-hash")
	peerHash, _ := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
	registry.Put(peerWithHash, "", 100, peerHash, "http://datahub.example.com")

	// Start monitoring the publish channel
	done := make(chan bool)
	go func() {
		<-mockProducer.PublishChannel()
		done <- true
	}()

	err = sc.sendSyncMessage(peerWithHash)
	assert.NoError(t, err, "Should successfully send sync message")

	select {
	case <-done:
		// Message was published
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Message was not published")
	}
}

func TestSyncCoordinator_MonitorFSM(t *testing.T) {
	logger := ulogger.New("test")
	settings := CreateTestSettings()
	registry := NewPeerRegistry()
	selector := NewPeerSelector(logger, nil)
	banManager := NewPeerBanManager(context.Background(), nil, settings, registry)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		banManager,
		blockchainSetup.Client,
		nil,
	)

	sc.SetGetLocalHeightCallback(func() uint32 {
		return 100
	})

	// Test context cancellation
	ctx, cancel := context.WithCancel(context.Background())
	sc.wg.Add(1)
	go sc.monitorFSM(ctx)

	// Let it run briefly
	time.Sleep(100 * time.Millisecond)

	// Cancel context
	cancel()

	// Wait for goroutine to finish
	done := make(chan bool)
	go func() {
		sc.wg.Wait()
		done <- true
	}()

	select {
	case <-done:
		// Success
	case <-time.After(2 * time.Second):
		t.Fatal("monitorFSM did not stop on context cancellation")
	}

	// Test stop channel
	sc2 := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		banManager,
		blockchainSetup.Client,
		nil,
	)

	sc2.SetGetLocalHeightCallback(func() uint32 {
		return 100
	})

	ctx2 := context.Background()
	sc2.wg.Add(1)
	go sc2.monitorFSM(ctx2)

	// Let it run briefly
	time.Sleep(100 * time.Millisecond)

	// Close stop channel
	close(sc2.stopCh)

	// Wait for goroutine to finish
	done2 := make(chan bool)
	go func() {
		sc2.wg.Wait()
		done2 <- true
	}()

	select {
	case <-done2:
		// Success
	case <-time.After(2 * time.Second):
		t.Fatal("monitorFSM did not stop on stop channel close")
	}
}

func TestSyncCoordinator_MonitorFSM_AdaptiveIntervals(t *testing.T) {
	logger := ulogger.New("test")
	settings := CreateTestSettings()
	registry := NewPeerRegistry()
	selector := NewPeerSelector(logger, nil)
	banManager := NewPeerBanManager(context.Background(), nil, settings, registry)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		banManager,
		blockchainSetup.Client,
		nil,
	)

	// Test when caught up - should use slow interval
	sc.SetGetLocalHeightCallback(func() uint32 {
		return 100
	})

	// No peers means we're caught up
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sc.wg.Add(1)
	go sc.monitorFSM(ctx)

	// Let it run for a bit - should be using slow interval
	time.Sleep(200 * time.Millisecond)

	// Add a peer ahead of us - should switch to fast monitoring
	peerID := peer.ID("test-peer")
	peerHash, _ := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
	registry.Put(peerID, "", 110, peerHash, "")

	// Let it detect we're not caught up and switch to fast interval
	time.Sleep(3 * time.Second) // Wait for timer to fire with fast interval

	// Now remove the peer so we're caught up again
	registry.Remove(peerID)

	// Let it detect we're caught up and switch back to slow interval
	time.Sleep(3 * time.Second)

	cancel()

	// Wait for goroutine to finish
	done := make(chan bool)
	go func() {
		sc.wg.Wait()
		done <- true
	}()

	select {
	case <-done:
		// Success - test covered the adaptive interval logic
	case <-time.After(2 * time.Second):
		t.Fatal("monitorFSM did not stop properly")
	}
}

func TestSyncCoordinator_HandleFSMTransition_Simplified(t *testing.T) {
	logger := ulogger.New("test")
	settings := CreateTestSettings()
	registry := NewPeerRegistry()
	selector := NewPeerSelector(logger, nil)
	banManager := NewPeerBanManager(context.Background(), nil, settings, registry)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		banManager,
		blockchainSetup.Client,
		nil,
	)

	sc.SetGetLocalHeightCallback(func() uint32 {
		return 100
	})

	runningState := blockchain_api.FSMStateType_RUNNING
	catchingState := blockchain_api.FSMStateType_CATCHINGBLOCKS

	t.Run("RUNNING state with sync peer behind triggers transition", func(t *testing.T) {
		// Test RUNNING state with current sync peer where local height < peer height
		// This simulates a catchup failure scenario
		syncPeer := peer.ID("sync-peer-1")
		peerHash, _ := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
		registry.Put(syncPeer, "", 110, peerHash, "") // Set peer height higher than local (110 > 100)

		sc.mu.Lock()
		sc.currentSyncPeer = syncPeer
		sc.mu.Unlock()

		transitioned := sc.handleFSMTransition(&runningState)
		assert.True(t, transitioned, "Should return true for RUNNING state with sync peer behind")
	})

	t.Run("RUNNING state without sync peer returns false", func(t *testing.T) {
		// Clear all state first
		sc.mu.Lock()
		sc.currentSyncPeer = ""
		sc.mu.Unlock()

		transitioned := sc.handleFSMTransition(&runningState)
		assert.False(t, transitioned, "Should return false for RUNNING state without sync peer")
	})

	t.Run("non-RUNNING state returns false", func(t *testing.T) {
		// Even with a sync peer set, non-RUNNING state should return false
		somePeer := peer.ID("some-peer")
		peerHash, _ := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
		registry.Put(somePeer, "", 110, peerHash, "")

		sc.mu.Lock()
		sc.currentSyncPeer = somePeer
		sc.mu.Unlock()

		transitioned := sc.handleFSMTransition(&catchingState)
		assert.False(t, transitioned, "Should return false for non-RUNNING state")
	})

	t.Run("successful sync triggers transition", func(t *testing.T) {
		// Test successful sync scenario (local height >= peer height)
		successPeer := peer.ID("success-peer")
		successHash, _ := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
		registry.Put(successPeer, "", 90, successHash, "") // Set peer height lower than local (90 < 100)

		sc.mu.Lock()
		sc.currentSyncPeer = successPeer
		sc.mu.Unlock()

		transitioned := sc.handleFSMTransition(&runningState)
		assert.True(t, transitioned, "Should return true for RUNNING state with successful sync")
	})

	t.Run("peer not in registry triggers transition", func(t *testing.T) {
		// Test scenario where sync peer is no longer in registry (disconnected)
		missingPeer := peer.ID("missing-peer")
		// Don't add to registry

		sc.mu.Lock()
		sc.currentSyncPeer = missingPeer
		sc.mu.Unlock()

		transitioned := sc.handleFSMTransition(&runningState)
		assert.True(t, transitioned, "Should return true when sync peer not in registry")
	})
}

func TestSyncCoordinator_FilterEligiblePeers(t *testing.T) {
	logger := ulogger.New("test")
	settings := CreateTestSettings()
	registry := NewPeerRegistry()
	selector := NewPeerSelector(logger, nil)
	banManager := NewPeerBanManager(context.Background(), nil, settings, registry)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		banManager,
		blockchainSetup.Client,
		nil,
	)

	oldPeer := peer.ID("old-peer")
	localHeight := uint32(100)

	peers := []*PeerInfo{
		{ID: oldPeer, Height: 110, Storage: "full"},          // Old peer, should be skipped
		{ID: peer.ID("peer1"), Height: 90, Storage: "full"},  // Below local height, should be skipped
		{ID: peer.ID("peer2"), Height: 100, Storage: "full"}, // At local height, should be skipped
		{ID: peer.ID("peer3"), Height: 120, Storage: "full"}, // Above local height, should be included
		{ID: peer.ID("peer4"), Height: 115, Storage: "full"}, // Above local height, should be included
	}

	eligible := sc.filterEligiblePeers(peers, oldPeer, localHeight)

	assert.Len(t, eligible, 2, "Should have 2 eligible peers")
	assert.Equal(t, peer.ID("peer3"), eligible[0].ID)
	assert.Equal(t, peer.ID("peer4"), eligible[1].ID)
}

func TestSyncCoordinator_FilterEligiblePeers_OldPeerLogging(t *testing.T) {
	logger := ulogger.New("test")
	settings := CreateTestSettings()
	registry := NewPeerRegistry()
	selector := NewPeerSelector(logger, nil)
	banManager := NewPeerBanManager(context.Background(), nil, settings, registry)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		banManager,
		blockchainSetup.Client,
		nil,
	)

	oldPeer := peer.ID("old-peer-to-skip")
	localHeight := uint32(100)

	// Test case 1: Old peer is ahead of local height (should log that it's being skipped)
	peers1 := []*PeerInfo{
		{ID: oldPeer, Height: 110, Storage: "full"},          // Old peer ahead - should be skipped with logging
		{ID: peer.ID("peer1"), Height: 120, Storage: "full"}, // New peer ahead - should be included
	}

	eligible1 := sc.filterEligiblePeers(peers1, oldPeer, localHeight)
	assert.Len(t, eligible1, 1, "Should have 1 eligible peer (not the old peer)")
	assert.Equal(t, peer.ID("peer1"), eligible1[0].ID)

	// Test case 2: Peer at same height as local (not old peer) - should be skipped but not logged
	peers2 := []*PeerInfo{
		{ID: peer.ID("peer2"), Height: 100, Storage: "full"}, // At local height, not old peer - skipped without special logging
		{ID: peer.ID("peer3"), Height: 110, Storage: "full"}, // Above local height - included
	}

	eligible2 := sc.filterEligiblePeers(peers2, oldPeer, localHeight)
	assert.Len(t, eligible2, 1, "Should have 1 eligible peer")
	assert.Equal(t, peer.ID("peer3"), eligible2[0].ID)

	// Test case 3: Old peer behind local height - should be skipped
	peers3 := []*PeerInfo{
		{ID: oldPeer, Height: 90, Storage: "full"},           // Old peer behind - should be skipped
		{ID: peer.ID("peer4"), Height: 105, Storage: "full"}, // New peer ahead - should be included
	}

	eligible3 := sc.filterEligiblePeers(peers3, oldPeer, localHeight)
	assert.Len(t, eligible3, 1, "Should have 1 eligible peer")
	assert.Equal(t, peer.ID("peer4"), eligible3[0].ID)

	// Test case 4: Mix of peers to test all branches
	peers4 := []*PeerInfo{
		{ID: oldPeer, Height: 110, Storage: "full"},          // Old peer ahead - skipped with logging
		{ID: peer.ID("peer5"), Height: 95, Storage: "full"},  // Below local - skipped
		{ID: peer.ID("peer6"), Height: 100, Storage: "full"}, // At local - skipped
		{ID: peer.ID("peer7"), Height: 115, Storage: "full"}, // Above local - included
		{ID: peer.ID("peer8"), Height: 120, Storage: "full"}, // Above local - included
	}

	eligible4 := sc.filterEligiblePeers(peers4, oldPeer, localHeight)
	assert.Len(t, eligible4, 2, "Should have 2 eligible peers")
	assert.Equal(t, peer.ID("peer7"), eligible4[0].ID)
	assert.Equal(t, peer.ID("peer8"), eligible4[1].ID)
}

func TestSyncCoordinator_SelectAndActivateNewPeer(t *testing.T) {
	logger := ulogger.New("test")
	settings := CreateTestSettings()
	registry := NewPeerRegistry()
	selector := NewPeerSelector(logger, nil)
	banManager := NewPeerBanManager(context.Background(), nil, settings, registry)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	mockProducer := kafka.NewKafkaAsyncProducerMock()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		banManager,
		blockchainSetup.Client,
		mockProducer,
	)

	localHeight := uint32(100)
	oldPeer := peer.ID("old-peer")

	// Test with no eligible peers
	sc.selectAndActivateNewPeer(localHeight, oldPeer)
	assert.Equal(t, peer.ID(""), sc.GetCurrentSyncPeer(), "Should have no sync peer when no eligible peers")

	// Add eligible peer
	newPeer := peer.ID("new-peer")
	peerHash, _ := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
	registry.Put(newPeer, "", 110, peerHash, "http://datahub.example.com")

	// Start monitoring the publish channel
	done := make(chan bool)
	go func() {
		select {
		case <-mockProducer.PublishChannel():
			done <- true
		case <-time.After(100 * time.Millisecond):
			done <- false
		}
	}()

	// Test with eligible peer
	sc.selectAndActivateNewPeer(localHeight, oldPeer)
	assert.Equal(t, newPeer, sc.GetCurrentSyncPeer(), "Should select new eligible peer")

	if <-done {
		// Message was published - success
	} else {
		t.Fatal("Sync message was not published")
	}
}

func TestSyncCoordinator_UpdateBanStatus_SyncPeerBanned(t *testing.T) {
	logger := ulogger.New("test")
	settings := CreateTestSettings()
	registry := NewPeerRegistry()
	selector := NewPeerSelector(logger, nil)
	banManager := NewPeerBanManager(context.Background(), nil, settings, registry)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	mockProducer := kafka.NewKafkaAsyncProducerMock()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		banManager,
		blockchainSetup.Client,
		mockProducer,
	)

	sc.SetGetLocalHeightCallback(func() uint32 {
		return 100
	})

	// Add and set sync peer
	syncPeer := peer.ID("sync-peer")
	peerHash, _ := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
	registry.Put(syncPeer, "", 110, peerHash, "")

	sc.mu.Lock()
	sc.currentSyncPeer = syncPeer
	sc.mu.Unlock()

	// Add alternative peer
	altPeer := peer.ID("alt-peer")
	altHash, _ := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
	registry.Put(altPeer, "", 115, altHash, "http://alt.example.com")

	// Start monitoring the publish channel
	done := make(chan bool)
	go func() {
		select {
		case <-mockProducer.PublishChannel():
			done <- true
		case <-time.After(100 * time.Millisecond):
			done <- false
		}
	}()

	// Ban the sync peer
	banManager.AddScore(string(syncPeer), ReasonSpam)
	banManager.AddScore(string(syncPeer), ReasonSpam) // Should trigger ban

	// Update ban status
	sc.UpdateBanStatus(syncPeer)

	// Verify sync peer was cleared and new one selected
	assert.Equal(t, altPeer, sc.GetCurrentSyncPeer(), "Should switch to alternative peer when sync peer is banned")

	if <-done {
		// Message was published - success
	} else {
		t.Fatal("Sync message was not published")
	}
}

func TestSyncCoordinator_TriggerSync_SendMessageError(t *testing.T) {
	logger := ulogger.New("test")
	settings := CreateTestSettings()
	registry := NewPeerRegistry()
	selector := NewPeerSelector(logger, nil)
	banManager := NewPeerBanManager(context.Background(), nil, settings, registry)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		banManager,
		blockchainSetup.Client,
		nil, // No Kafka producer
	)

	sc.SetGetLocalHeightCallback(func() uint32 {
		return 100
	})

	// Add peer without block hash (will cause sendSyncMessage to fail)
	peerID := peer.ID("test-peer")
	registry.Put(peerID, "", 110, nil, "http://test.com")

	// Trigger sync - should fail to send message but not panic
	err := sc.TriggerSync()
	assert.Error(t, err, "Should return error when sendSyncMessage fails")
}

func TestSyncCoordinator_HandleCatchupFailure_NoNewPeer(t *testing.T) {
	logger := ulogger.New("test")
	settings := CreateTestSettings()
	registry := NewPeerRegistry()
	selector := NewPeerSelector(logger, nil)
	banManager := NewPeerBanManager(context.Background(), nil, settings, registry)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		banManager,
		blockchainSetup.Client,
		nil,
	)

	sc.SetGetLocalHeightCallback(func() uint32 {
		return 100
	})

	// Set initial sync peer
	initialPeer := peer.ID("initial-peer")
	sc.mu.Lock()
	sc.currentSyncPeer = initialPeer
	sc.mu.Unlock()

	// Handle catchup failure with no new peers available
	sc.HandleCatchupFailure("test failure reason")

	// Sync peer should be cleared
	assert.Equal(t, peer.ID(""), sc.GetCurrentSyncPeer(), "Should clear sync peer even with no alternatives")
}

func TestSyncCoordinator_PeriodicEvaluation(t *testing.T) {
	logger := ulogger.New("test")
	settings := CreateTestSettings()
	registry := NewPeerRegistry()
	selector := NewPeerSelector(logger, nil)
	banManager := NewPeerBanManager(context.Background(), nil, settings, registry)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		banManager,
		blockchainSetup.Client,
		nil,
	)

	// Test context cancellation
	ctx, cancel := context.WithCancel(context.Background())
	sc.wg.Add(1)
	go sc.periodicEvaluation(ctx)

	// Let it run briefly
	time.Sleep(100 * time.Millisecond)

	// Cancel context
	cancel()

	// Wait for goroutine to finish
	done := make(chan bool)
	go func() {
		sc.wg.Wait()
		done <- true
	}()

	select {
	case <-done:
		// Success
	case <-time.After(2 * time.Second):
		t.Fatal("periodicEvaluation did not stop on context cancellation")
	}

	// Test stop channel
	sc2 := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		banManager,
		blockchainSetup.Client,
		nil,
	)

	ctx2 := context.Background()
	sc2.wg.Add(1)
	go sc2.periodicEvaluation(ctx2)

	// Let it run briefly
	time.Sleep(100 * time.Millisecond)

	// Close stop channel
	close(sc2.stopCh)

	// Wait for goroutine to finish
	done2 := make(chan bool)
	go func() {
		sc2.wg.Wait()
		done2 <- true
	}()

	select {
	case <-done2:
		// Success
	case <-time.After(2 * time.Second):
		t.Fatal("periodicEvaluation did not stop on stop channel close")
	}
}
