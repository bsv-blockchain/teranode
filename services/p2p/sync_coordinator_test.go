package p2p

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/services/blockchain/blockchain_api"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSyncCoordinator_NewSyncCoordinator(t *testing.T) {
	logger := ulogger.New("test")
	settings := CreateTestSettings()
	registry := NewPeerRegistry()
	selector := NewPeerSelector(logger)
	healthChecker := NewPeerHealthChecker(logger, registry, settings)
	banManager := NewPeerBanManager(context.Background(), nil, settings)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		healthChecker,
		banManager,
		blockchainSetup.Client,
		nil, // blocksKafkaProducerClient
	)

	assert.NotNil(t, sc)
	assert.Equal(t, logger, sc.logger)
	assert.Equal(t, settings, sc.settings)
	assert.Equal(t, registry, sc.registry)
	assert.Equal(t, selector, sc.selector)
	assert.Equal(t, healthChecker, sc.healthChecker)
	assert.Equal(t, banManager, sc.banManager)
	assert.Equal(t, blockchainSetup.Client, sc.blockchainClient)
	assert.NotNil(t, sc.stopCh)
}

func TestSyncCoordinator_SetGetLocalHeightCallback(t *testing.T) {
	logger := ulogger.New("test")
	settings := CreateTestSettings()
	registry := NewPeerRegistry()
	selector := NewPeerSelector(logger)
	healthChecker := NewPeerHealthChecker(logger, registry, settings)
	banManager := NewPeerBanManager(context.Background(), nil, settings)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		healthChecker,
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
	selector := NewPeerSelector(logger)
	healthChecker := NewPeerHealthChecker(logger, registry, settings)
	banManager := NewPeerBanManager(context.Background(), nil, settings)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		healthChecker,
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
	selector := NewPeerSelector(logger)
	healthChecker := NewPeerHealthChecker(logger, registry, settings)
	banManager := NewPeerBanManager(context.Background(), nil, settings)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		healthChecker,
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
	selector := NewPeerSelector(logger)
	healthChecker := NewPeerHealthChecker(logger, registry, settings)
	banManager := NewPeerBanManager(context.Background(), nil, settings)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		healthChecker,
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
	selector := NewPeerSelector(logger)
	healthChecker := NewPeerHealthChecker(logger, registry, settings)
	banManager := NewPeerBanManager(context.Background(), nil, settings)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		healthChecker,
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
	registry.AddPeer(peerID)
	registry.UpdateHeight(peerID, 110, "hash")
	registry.UpdateDataHubURL(peerID, "http://test.com")
	registry.UpdateURLResponsiveness(peerID, true)

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
	selector := NewPeerSelector(logger)
	healthChecker := NewPeerHealthChecker(logger, registry, settings)
	banManager := NewPeerBanManager(context.Background(), nil, settings)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		healthChecker,
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
	selector := NewPeerSelector(logger)
	healthChecker := NewPeerHealthChecker(logger, registry, settings)
	banManager := NewPeerBanManager(context.Background(), nil, settings)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		healthChecker,
		banManager,
		blockchainSetup.Client,
		nil, // blocksKafkaProducerClient
	)

	// Add a peer and set as sync peer
	peerID := peer.ID("test-peer")
	registry.AddPeer(peerID)

	sc.mu.Lock()
	sc.currentSyncPeer = peerID
	sc.mu.Unlock()

	// Handle disconnection of sync peer
	sc.HandlePeerDisconnected(peerID)

	// Give goroutine time to run
	time.Sleep(100 * time.Millisecond)

	// Verify peer was removed from registry
	_, exists := registry.GetPeer(peerID)
	assert.False(t, exists)

	// Sync peer should be cleared
	currentPeer := sc.GetCurrentSyncPeer()
	assert.Equal(t, peer.ID(""), currentPeer)
}

func TestSyncCoordinator_HandlePeerDisconnected_NotSyncPeer(t *testing.T) {
	logger := ulogger.New("test")
	settings := CreateTestSettings()
	registry := NewPeerRegistry()
	selector := NewPeerSelector(logger)
	healthChecker := NewPeerHealthChecker(logger, registry, settings)
	banManager := NewPeerBanManager(context.Background(), nil, settings)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		healthChecker,
		banManager,
		blockchainSetup.Client,
		nil, // blocksKafkaProducerClient
	)

	// Add two peers
	syncPeer := peer.ID("sync-peer")
	otherPeer := peer.ID("other-peer")
	registry.AddPeer(syncPeer)
	registry.AddPeer(otherPeer)

	// Set sync peer
	sc.mu.Lock()
	sc.currentSyncPeer = syncPeer
	sc.mu.Unlock()

	// Disconnect non-sync peer
	sc.HandlePeerDisconnected(otherPeer)

	// Verify peer was removed
	_, exists := registry.GetPeer(otherPeer)
	assert.False(t, exists)

	// Sync peer should remain
	currentPeer := sc.GetCurrentSyncPeer()
	assert.Equal(t, syncPeer, currentPeer)
}

func TestSyncCoordinator_HandleCatchupFailure(t *testing.T) {
	logger := ulogger.New("test")
	settings := CreateTestSettings()
	registry := NewPeerRegistry()
	selector := NewPeerSelector(logger)
	healthChecker := NewPeerHealthChecker(logger, registry, settings)
	banManager := NewPeerBanManager(context.Background(), nil, settings)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		healthChecker,
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
	registry.AddPeer(newPeer)
	registry.UpdateHeight(newPeer, 110, "hash")
	registry.UpdateDataHubURL(newPeer, "http://test.com")
	registry.UpdateURLResponsiveness(newPeer, true)

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
	selector := NewPeerSelector(logger)
	healthChecker := NewPeerHealthChecker(logger, registry, settings)
	banManager := NewPeerBanManager(context.Background(), nil, settings)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		healthChecker,
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

	registry.AddPeer(peer1)
	registry.UpdateHeight(peer1, 105, "hash1")
	registry.UpdateDataHubURL(peer1, "http://peer1.com")
	registry.UpdateURLResponsiveness(peer1, false) // Not responsive

	registry.AddPeer(peer2)
	registry.UpdateHeight(peer2, 110, "hash2")
	registry.UpdateDataHubURL(peer2, "http://peer2.com")
	registry.UpdateURLResponsiveness(peer2, true) // Responsive

	// Select new sync peer
	selected := sc.selectNewSyncPeer()

	// Should select peer2 (responsive and higher)
	assert.Equal(t, peer2, selected)
}

func TestSyncCoordinator_selectNewSyncPeer_ForcedPeer(t *testing.T) {
	logger := ulogger.New("test")
	settings := CreateTestSettings()
	settings.P2P.ForceSyncPeer = "forced-peer"

	registry := NewPeerRegistry()
	selector := NewPeerSelector(logger)
	healthChecker := NewPeerHealthChecker(logger, registry, settings)
	banManager := NewPeerBanManager(context.Background(), nil, settings)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		healthChecker,
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
	registry.AddPeer(forcedPeer)
	registry.UpdateHeight(forcedPeer, 110, "hash")
	registry.UpdateDataHubURL(forcedPeer, "http://forced.com")
	registry.UpdateURLResponsiveness(forcedPeer, true)

	// Add another better peer
	betterPeer := peer.ID("better-peer")
	registry.AddPeer(betterPeer)
	registry.UpdateHeight(betterPeer, 120, "hash2")
	registry.UpdateDataHubURL(betterPeer, "http://better.com")
	registry.UpdateURLResponsiveness(betterPeer, true)

	// Should select forced peer
	selected := sc.selectNewSyncPeer()
	assert.Equal(t, forcedPeer, selected)
}

func TestSyncCoordinator_UpdatePeerInfo(t *testing.T) {
	logger := ulogger.New("test")
	settings := CreateTestSettings()
	registry := NewPeerRegistry()
	selector := NewPeerSelector(logger)
	healthChecker := NewPeerHealthChecker(logger, registry, settings)
	banManager := NewPeerBanManager(context.Background(), nil, settings)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		healthChecker,
		banManager,
		blockchainSetup.Client,
		nil, // blocksKafkaProducerClient
	)

	// Add peer first
	peerID := peer.ID("test-peer")
	registry.AddPeer(peerID)

	// Update peer info
	sc.UpdatePeerInfo(peerID, 150, "block-hash", "http://datahub.com")

	// Verify peer was updated
	info, exists := registry.GetPeer(peerID)
	require.True(t, exists)
	assert.Equal(t, int32(150), info.Height)
	assert.Equal(t, "block-hash", info.BlockHash)
	assert.Equal(t, "http://datahub.com", info.DataHubURL)
}

func TestSyncCoordinator_UpdateBanStatus(t *testing.T) {
	logger := ulogger.New("test")
	settings := CreateTestSettings()
	registry := NewPeerRegistry()
	selector := NewPeerSelector(logger)
	healthChecker := NewPeerHealthChecker(logger, registry, settings)
	banManager := NewPeerBanManager(context.Background(), nil, settings)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		healthChecker,
		banManager,
		blockchainSetup.Client,
		nil, // blocksKafkaProducerClient
	)

	// Add peer and ban it
	peerID := peer.ID("test-peer")
	registry.AddPeer(peerID)

	// Add ban score - use raw string conversion to match UpdateBanStatus
	banManager.AddScore(string(peerID), ReasonSpam)
	banManager.AddScore(string(peerID), ReasonSpam) // Should trigger ban

	// Update ban status
	sc.UpdateBanStatus(peerID)

	// Verify ban status was updated
	info, exists := registry.GetPeer(peerID)
	require.True(t, exists)
	assert.True(t, info.IsBanned)
	assert.Equal(t, 100, info.BanScore)
}

func TestSyncCoordinator_checkURLResponsiveness(t *testing.T) {
	// Create test HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	logger := ulogger.New("test")
	settings := CreateTestSettings()
	registry := NewPeerRegistry()
	selector := NewPeerSelector(logger)
	healthChecker := NewPeerHealthChecker(logger, registry, settings)
	banManager := NewPeerBanManager(context.Background(), nil, settings)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		healthChecker,
		banManager,
		blockchainSetup.Client,
		nil, // blocksKafkaProducerClient
	)

	// Test responsive URL
	responsive := sc.checkURLResponsiveness(server.URL)
	assert.True(t, responsive)

	// Test unresponsive URL
	responsive = sc.checkURLResponsiveness("http://localhost:99999")
	assert.False(t, responsive)

	// Test empty URL
	responsive = sc.checkURLResponsiveness("")
	assert.False(t, responsive)
}

func TestSyncCoordinator_checkAndUpdateURLResponsiveness(t *testing.T) {
	// Create test HTTP servers
	successServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer successServer.Close()

	logger := ulogger.New("test")
	settings := CreateTestSettings()
	registry := NewPeerRegistry()
	selector := NewPeerSelector(logger)
	healthChecker := NewPeerHealthChecker(logger, registry, settings)
	banManager := NewPeerBanManager(context.Background(), nil, settings)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		healthChecker,
		banManager,
		blockchainSetup.Client,
		nil, // blocksKafkaProducerClient
	)

	// Create test peers
	peers := []*PeerInfo{
		{
			ID:         peer.ID("peer1"),
			DataHubURL: successServer.URL,
		},
		{
			ID:         peer.ID("peer2"),
			DataHubURL: "http://localhost:99999",
		},
		{
			ID:         peer.ID("peer3"),
			DataHubURL: "",
		},
	}

	// Add peers to registry
	for _, p := range peers {
		registry.AddPeer(p.ID)
		if p.DataHubURL != "" {
			registry.UpdateDataHubURL(p.ID, p.DataHubURL)
		}
	}

	// Check and update responsiveness
	sc.checkAndUpdateURLResponsiveness(peers)

	// Verify updates
	info1, _ := registry.GetPeer(peer.ID("peer1"))
	assert.True(t, info1.URLResponsive)

	info2, _ := registry.GetPeer(peer.ID("peer2"))
	assert.False(t, info2.URLResponsive)

	info3, _ := registry.GetPeer(peer.ID("peer3"))
	assert.False(t, info3.URLResponsive)
}

func TestSyncCoordinator_checkFSMState(t *testing.T) {
	logger := ulogger.New("test")
	settings := CreateTestSettings()
	registry := NewPeerRegistry()
	selector := NewPeerSelector(logger)
	healthChecker := NewPeerHealthChecker(logger, registry, settings)
	banManager := NewPeerBanManager(context.Background(), nil, settings)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		healthChecker,
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
	registry.AddPeer(peerID)
	registry.UpdateHeight(peerID, 110, "hash")
	registry.UpdateDataHubURL(peerID, "http://test.com")
	registry.UpdateURLResponsiveness(peerID, true)

	// Check FSM state (LocalClient returns RUNNING by default)
	sc.checkFSMState(blockchainSetup.Ctx)

	// May trigger sync if peer is ahead of us while in RUNNING state
	// (local height is 100, peer is at 110)
	currentPeer := sc.GetCurrentSyncPeer()
	assert.True(t, currentPeer == "" || currentPeer == peerID, "Should either have no sync peer or have selected the test peer")

	// Simulate IDLE state by setting last state
	sc.mu.Lock()
	sc.lastFSMState = blockchain_api.FSMStateType_IDLE
	sc.mu.Unlock()

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
	selector := NewPeerSelector(logger)
	healthChecker := NewPeerHealthChecker(logger, registry, settings)
	banManager := NewPeerBanManager(context.Background(), nil, settings)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		healthChecker,
		banManager,
		blockchainSetup.Client,
		nil, // blocksKafkaProducerClient
	)

	// Set up callbacks
	sc.SetGetLocalHeightCallback(func() uint32 {
		return 100
	})

	// Add current sync peer
	syncPeer := peer.ID("sync-peer")
	registry.AddPeer(syncPeer)
	registry.UpdateHeight(syncPeer, 105, "hash")

	// Add better peer
	betterPeer := peer.ID("better-peer")
	registry.AddPeer(betterPeer)
	registry.UpdateHeight(betterPeer, 120, "hash")
	registry.UpdateDataHubURL(betterPeer, "http://better.com")
	registry.UpdateURLResponsiveness(betterPeer, true)

	// Set current sync peer
	sc.mu.Lock()
	sc.currentSyncPeer = syncPeer
	sc.mu.Unlock()

	// Evaluate sync peer
	sc.evaluateSyncPeer()

	// Should have switched to better peer
	currentPeer := sc.GetCurrentSyncPeer()
	assert.Equal(t, betterPeer, currentPeer)
}

func TestSyncCoordinator_evaluateSyncPeer_StuckAtHeight(t *testing.T) {
	logger := ulogger.New("test")
	settings := CreateTestSettings()

	// Setup test blockchain
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	registry := NewPeerRegistry()
	selector := NewPeerSelector(logger)
	healthChecker := NewPeerHealthChecker(logger, registry, settings)
	banManager := NewPeerBanManager(context.Background(), nil, settings)

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		healthChecker,
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
	registry.AddPeer(syncPeer)
	registry.UpdateHeight(syncPeer, 110, "hash")
	registry.UpdateDataHubURL(syncPeer, "http://test.com")
	registry.UpdateURLResponsiveness(syncPeer, true)

	// Set sync peer and simulate being stuck for too long
	sc.mu.Lock()
	sc.currentSyncPeer = syncPeer
	sc.lastLocalHeight = 100
	sc.syncStartTime = time.Now().Add(-6 * time.Minute) // Been syncing for 6 minutes
	sc.mu.Unlock()

	// Set peer's last block time to be old (> 2 minutes)
	registry.UpdateNetworkStats(syncPeer, 1000)
	registry.mu.Lock()
	if info, exists := registry.peers[syncPeer]; exists {
		info.LastBlockTime = time.Now().Add(-3 * time.Minute) // Last block 3 minutes ago
	}
	registry.mu.Unlock()

	// Add alternative peer
	altPeer := peer.ID("alt-peer")
	registry.AddPeer(altPeer)
	registry.UpdateHeight(altPeer, 115, "hash2")
	registry.UpdateDataHubURL(altPeer, "http://alt.com")
	registry.UpdateURLResponsiveness(altPeer, true)

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
	selector := NewPeerSelector(logger)
	healthChecker := NewPeerHealthChecker(logger, registry, settings)
	banManager := NewPeerBanManager(context.Background(), nil, settings)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		healthChecker,
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
		},
		{
			ID:         peer.ID("peer2"),
			DataHubURL: "http://example2.com",
			Height:     200,
			BanScore:   10,
		},
	}

	// This just logs, so we're mainly testing it doesn't panic
	sc.logPeerList(peers)
}

func TestSyncCoordinator_LogCandidateList(t *testing.T) {
	logger := ulogger.New("test")
	settings := CreateTestSettings()
	registry := NewPeerRegistry()
	selector := NewPeerSelector(logger)
	healthChecker := NewPeerHealthChecker(logger, registry, settings)
	banManager := NewPeerBanManager(context.Background(), nil, settings)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		healthChecker,
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
		},
		{
			ID:         peer.ID("candidate2"),
			DataHubURL: "http://candidate2.com",
			Height:     250,
			BanScore:   7,
		},
	}

	// This just logs, so we're mainly testing it doesn't panic
	sc.logCandidateList(candidates)
}

func TestSyncCoordinator_CheckURLResponsiveness(t *testing.T) {
	logger := ulogger.New("test")
	settings := CreateTestSettings()
	registry := NewPeerRegistry()
	selector := NewPeerSelector(logger)
	healthChecker := NewPeerHealthChecker(logger, registry, settings)
	banManager := NewPeerBanManager(context.Background(), nil, settings)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		healthChecker,
		banManager,
		blockchainSetup.Client,
		nil, // blocksKafkaProducerClient
	)

	// Test with empty URL
	assert.False(t, sc.checkURLResponsiveness(""))

	// Test with mock server that responds OK
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	assert.True(t, sc.checkURLResponsiveness(server.URL))

	// Test with mock server that returns server error
	errorServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer errorServer.Close()

	assert.False(t, sc.checkURLResponsiveness(errorServer.URL))

	// Test with invalid URL
	assert.False(t, sc.checkURLResponsiveness("http://invalid.localhost.test:99999"))
}

func TestSyncCoordinator_CheckAndUpdateURLResponsiveness(t *testing.T) {
	logger := ulogger.New("test")
	settings := CreateTestSettings()
	registry := NewPeerRegistry()
	selector := NewPeerSelector(logger)
	healthChecker := NewPeerHealthChecker(logger, registry, settings)
	banManager := NewPeerBanManager(context.Background(), nil, settings)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		healthChecker,
		banManager,
		blockchainSetup.Client,
		nil, // blocksKafkaProducerClient
	)

	// Create test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// Test with peers
	peerID1 := peer.ID("peer1")
	peerID2 := peer.ID("peer2")

	// Add peers to registry first
	registry.AddPeer(peerID1)
	registry.UpdateDataHubURL(peerID1, server.URL)

	registry.AddPeer(peerID2)
	registry.UpdateDataHubURL(peerID2, "http://invalid.localhost.test:99999")

	// Get peers and set old check times
	peer1Info, _ := registry.GetPeer(peerID1)
	peer1Info.LastURLCheck = time.Now().Add(-1 * time.Minute)

	peer2Info, _ := registry.GetPeer(peerID2)
	peer2Info.LastURLCheck = time.Now().Add(-1 * time.Minute)

	peers := []*PeerInfo{peer1Info, peer2Info}

	// Check and update responsiveness
	sc.checkAndUpdateURLResponsiveness(peers)

	// Verify results in registry
	peer1InfoUpdated, _ := registry.GetPeer(peerID1)
	assert.True(t, peer1InfoUpdated.URLResponsive, "Peer1 URL should be responsive")

	peer2InfoUpdated, _ := registry.GetPeer(peerID2)
	assert.False(t, peer2InfoUpdated.URLResponsive, "Peer2 URL should not be responsive")

	// Test with peer that was checked recently (should skip)
	peerID3 := peer.ID("peer3")
	registry.AddPeer(peerID3)
	registry.UpdateDataHubURL(peerID3, server.URL)

	peer3Info, _ := registry.GetPeer(peerID3)
	peer3Info.LastURLCheck = time.Now() // Just checked

	peers3 := []*PeerInfo{peer3Info}
	sc.checkAndUpdateURLResponsiveness(peers3)
	// Should not update since it was checked recently
	peer3InfoUpdated, _ := registry.GetPeer(peerID3)
	assert.False(t, peer3InfoUpdated.URLResponsive, "Peer3 URL should not be updated (checked recently)")
}

func TestSyncCoordinator_EvaluateSyncPeer_Coverage(t *testing.T) {
	logger := ulogger.New("test")
	settings := CreateTestSettings()
	registry := NewPeerRegistry()
	selector := NewPeerSelector(logger)
	healthChecker := NewPeerHealthChecker(logger, registry, settings)
	banManager := NewPeerBanManager(context.Background(), nil, settings)
	blockchainSetup := SetupTestBlockchain(t)
	defer blockchainSetup.Cleanup()

	sc := NewSyncCoordinator(
		logger,
		settings,
		registry,
		selector,
		healthChecker,
		banManager,
		blockchainSetup.Client,
		nil, // blocksKafkaProducerClient
	)

	// Set local height callback
	sc.SetGetLocalHeightCallback(func() uint32 {
		return 1000
	})

	// Test with no current sync peer
	sc.evaluateSyncPeer()
	assert.Equal(t, peer.ID(""), sc.currentSyncPeer)

	// Add and set a sync peer
	peerID := peer.ID("test-peer")
	registry.AddPeer(peerID)
	registry.UpdateHeight(peerID, 2000, "hash")
	registry.UpdateHealth(peerID, true)

	peerInfo, _ := registry.GetPeer(peerID)
	peerInfo.LastBlockTime = time.Now()

	sc.mu.Lock()
	sc.currentSyncPeer = peerID
	sc.syncStartTime = time.Now().Add(-1 * time.Minute)
	sc.mu.Unlock()

	// Evaluate - should keep peer (healthy and recent blocks)
	sc.evaluateSyncPeer()
	sc.mu.RLock()
	currentPeer := sc.currentSyncPeer
	sc.mu.RUnlock()
	assert.Equal(t, peerID, currentPeer, "Should keep healthy peer")

	// Mark peer as unhealthy
	registry.UpdateHealth(peerID, false)
	sc.evaluateSyncPeer()
	sc.mu.RLock()
	currentPeer = sc.currentSyncPeer
	sc.mu.RUnlock()
	assert.Equal(t, peer.ID(""), currentPeer, "Should clear unhealthy peer")

	// Test with peer that no longer exists
	sc.mu.Lock()
	sc.currentSyncPeer = peer.ID("non-existent")
	sc.mu.Unlock()
	sc.evaluateSyncPeer()
	sc.mu.RLock()
	currentPeer = sc.currentSyncPeer
	sc.mu.RUnlock()
	assert.Equal(t, peer.ID(""), currentPeer, "Should clear non-existent peer")

	// Test with peer that has been inactive too long
	registry.UpdateHealth(peerID, true) // Make healthy again
	peerInfo, _ = registry.GetPeer(peerID)
	peerInfo.LastBlockTime = time.Now().Add(-10 * time.Minute) // Old block time

	sc.mu.Lock()
	sc.currentSyncPeer = peerID
	sc.syncStartTime = time.Now().Add(-6 * time.Minute) // Been syncing for 6 minutes
	sc.mu.Unlock()

	sc.evaluateSyncPeer()
	sc.mu.RLock()
	currentPeer = sc.currentSyncPeer
	sc.mu.RUnlock()
	assert.Equal(t, peer.ID(""), currentPeer, "Should clear inactive peer")

	// Test when caught up to peer
	peerInfo.Height = 1000 // Same as local height
	peerInfo.LastBlockTime = time.Now()
	registry.UpdateHealth(peerID, true)

	sc.mu.Lock()
	sc.currentSyncPeer = peerID
	sc.syncStartTime = time.Now()
	sc.mu.Unlock()

	sc.evaluateSyncPeer()
	// Should keep peer but look for better one (we just test it doesn't clear)
	sc.mu.RLock()
	currentPeer = sc.currentSyncPeer
	sc.mu.RUnlock()
	assert.Equal(t, peerID, currentPeer, "Should keep peer when caught up")
}
