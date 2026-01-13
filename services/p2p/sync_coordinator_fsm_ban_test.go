package p2p

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSyncCoordinator_FSMTransitionBansPeerAndUpdatesRegistry(t *testing.T) {
	t.Skip("State transition banning temporarily disabled")

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

	// Add a peer that will fail during catchup
	failingPeer := peer.ID("failing-peer")
	registry.AddPeer(failingPeer, "")
	registry.UpdateHeight(failingPeer, 200, "hash200")
	registry.UpdateDataHubURL(failingPeer, "http://failing.test")
	registry.UpdateReputation(failingPeer, 80.0)
	registry.UpdateURLResponsiveness(failingPeer, true)
	registry.UpdateStorage(failingPeer, "full")

	// Add an alternative peer
	goodPeer := peer.ID("good-peer")
	registry.AddPeer(goodPeer, "")
	registry.UpdateHeight(goodPeer, 190, "hash190")
	registry.UpdateDataHubURL(goodPeer, "http://good.test")
	registry.UpdateReputation(goodPeer, 80.0)
	registry.UpdateURLResponsiveness(goodPeer, true)
	registry.UpdateStorage(goodPeer, "full")

	// Set the failing peer as current sync peer
	sc.mu.Lock()
	sc.currentSyncPeer = failingPeer
	sc.mu.Unlock()

	// Simulate FSM transitioning from CATCHINGBLOCKS to RUNNING
	// (This indicates catchup failure)
	runningState := blockchain_api.FSMStateType_RUNNING
	sc.handleFSMTransition(&runningState)

	// Check that the peer's ban status was updated in the registry
	peerInfo, exists := registry.GetPeer(failingPeer)
	require.True(t, exists, "Failing peer should still exist in registry")
	assert.True(t, peerInfo.BanScore > 0, "Peer should have non-zero ban score after catchup failure")

	// Check that after enough failures, the peer is actually banned
	// Simulate multiple failures to exceed ban threshold
	for i := 0; i < 10; i++ {
		sc.mu.Lock()
		sc.currentSyncPeer = failingPeer
		sc.mu.Unlock()
		sc.handleFSMTransition(&runningState)
	}

	// Now the peer should be banned in the registry
	peerInfo, exists = registry.GetPeer(failingPeer)
	require.True(t, exists, "Failing peer should still exist in registry")
	assert.True(t, peerInfo.IsBanned, "Peer should be banned after multiple failures")
	assert.True(t, peerInfo.BanScore >= 100, "Peer should have ban score >= 100")

	// Verify that the banned peer is not selected as sync peer
	sc.SetGetLocalHeightCallback(func() uint32 { return 100 })
	newSyncPeer := sc.selectNewSyncPeer()
	assert.NotEqual(t, failingPeer, newSyncPeer, "Banned peer should not be selected")
	assert.Equal(t, goodPeer, newSyncPeer, "Good peer should be selected instead")
}

func TestSyncCoordinator_BannedPeerNotReselected(t *testing.T) {
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

	// Add a peer with highest height but it's banned
	bannedPeer := peer.ID("banned-peer")
	registry.AddPeer(bannedPeer, "")
	registry.UpdateHeight(bannedPeer, 300, "hash300")
	registry.UpdateDataHubURL(bannedPeer, "http://banned.test")
	registry.UpdateReputation(bannedPeer, 80.0)
	registry.UpdateURLResponsiveness(bannedPeer, true)
	registry.UpdateStorage(bannedPeer, "full")

	// Ban the peer
	score, banned := banManager.AddScore(string(bannedPeer), ReasonInvalidBlock)
	for !banned && score < 100 {
		score, banned = banManager.AddScore(string(bannedPeer), ReasonInvalidBlock)
	}
	registry.UpdateBanStatus(bannedPeer, score, banned)

	// Add other peers with lower height
	peer1 := peer.ID("peer1")
	registry.AddPeer(peer1, "")
	registry.UpdateHeight(peer1, 250, "hash250")
	registry.UpdateDataHubURL(peer1, "http://peer1.test")
	registry.UpdateReputation(peer1, 80.0)
	registry.UpdateURLResponsiveness(peer1, true)
	registry.UpdateStorage(peer1, "full")

	peer2 := peer.ID("peer2")
	registry.AddPeer(peer2, "")
	registry.UpdateHeight(peer2, 240, "hash240")
	registry.UpdateDataHubURL(peer2, "http://peer2.test")
	registry.UpdateReputation(peer2, 80.0)
	registry.UpdateURLResponsiveness(peer2, true)
	registry.UpdateStorage(peer2, "full")

	// Set local height
	sc.SetGetLocalHeightCallback(func() uint32 { return 200 })

	// Select peer - should NOT select the banned peer even though it has highest height
	selectedPeer := sc.selectNewSyncPeer()
	assert.NotEqual(t, bannedPeer, selectedPeer, "Should not select banned peer")
	assert.Equal(t, peer1, selectedPeer, "Should select peer1 with next highest height")

	// Verify the banned peer is marked as banned in registry
	bannedInfo, exists := registry.GetPeer(bannedPeer)
	require.True(t, exists)
	assert.True(t, bannedInfo.IsBanned, "Peer should be marked as banned in registry")
}
