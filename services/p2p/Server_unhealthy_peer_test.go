package p2p

import (
	"context"
	"fmt"
	"testing"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// TestShouldSkipUnhealthyPeer tests the shouldSkipUnhealthyPeer helper function
func TestShouldSkipUnhealthyPeer(t *testing.T) {
	t.Run("skip_unhealthy_peer", func(t *testing.T) {
		// Create peer registry with an unhealthy CONNECTED peer
		peerRegistry := NewPeerRegistry()
		unhealthyPeerID, _ := peer.Decode("12D3KooWEyX7hgdXy8zUjCs9CqvMGpB5dKVFj9MX2nUBLwajdSZH")
		peerRegistry.AddPeer(unhealthyPeerID)
		peerRegistry.UpdateConnectionState(unhealthyPeerID, true) // Mark as connected
		peerRegistry.UpdateHealth(unhealthyPeerID, false)         // Mark as unhealthy

		server := &Server{
			peerRegistry: peerRegistry,
			logger:       ulogger.New("test-server"),
		}

		// Should return true for unhealthy connected peer
		result := server.shouldSkipUnhealthyPeer(unhealthyPeerID.String(), "test")
		assert.True(t, result, "Should skip unhealthy connected peer")
	})

	t.Run("allow_healthy_peer", func(t *testing.T) {
		// Create peer registry with a healthy peer
		peerRegistry := NewPeerRegistry()
		healthyPeerID, _ := peer.Decode("12D3KooWEyX7hgdXy8zUjCs9CqvMGpB5dKVFj9MX2nUBLwajdSZH")
		peerRegistry.AddPeer(healthyPeerID)
		peerRegistry.UpdateHealth(healthyPeerID, true) // Mark as healthy

		server := &Server{
			peerRegistry: peerRegistry,
			logger:       ulogger.New("test-server"),
		}

		// Should return false for healthy peer
		result := server.shouldSkipUnhealthyPeer(healthyPeerID.String(), "test")
		assert.False(t, result, "Should not skip healthy peer")
	})

	t.Run("allow_peer_not_in_registry", func(t *testing.T) {
		// Create empty peer registry
		peerRegistry := NewPeerRegistry()
		newPeerID, _ := peer.Decode("12D3KooWEyX7hgdXy8zUjCs9CqvMGpB5dKVFj9MX2nUBLwajdSZH")

		server := &Server{
			peerRegistry: peerRegistry,
			logger:       ulogger.New("test-server"),
		}

		// Should return false for peer not in registry (allow new peers)
		result := server.shouldSkipUnhealthyPeer(newPeerID.String(), "test")
		assert.False(t, result, "Should not skip peer not in registry")
	})

	t.Run("allow_invalid_peer_id_gossiped_messages", func(t *testing.T) {
		peerRegistry := NewPeerRegistry()

		server := &Server{
			peerRegistry: peerRegistry,
			logger:       ulogger.New("test-server"),
		}

		// Should return false for invalid peer ID (e.g., hostname from gossiped messages)
		// We can't determine health status without a valid peer ID, so we allow the message
		result := server.shouldSkipUnhealthyPeer("teranode.space", "test")
		assert.False(t, result, "Should allow messages with invalid peer ID (gossiped messages)")
	})
}

// TestHandleBlockTopic_UnhealthyPeer tests block topic handling with unhealthy peers
func TestHandleBlockTopic_UnhealthyPeer(t *testing.T) {
	ctx := context.Background()

	t.Run("ignore_block_from_unhealthy_peer", func(t *testing.T) {
		// Create mock P2PClient
		mockP2PNode := new(MockServerP2PClient)
		selfPeerID, _ := peer.Decode("12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ")
		unhealthyPeerID, _ := peer.Decode("12D3KooWEyX7hgdXy8zUjCs9CqvMGpB5dKVFj9MX2nUBLwajdSZH")

		mockP2PNode.On("GetID").Return(selfPeerID)

		// Create mock ban manager
		mockBanManager := new(MockPeerBanManager)
		mockBanManager.On("IsBanned", unhealthyPeerID.String()).Return(false)

		// Create peer registry with unhealthy CONNECTED peer
		peerRegistry := NewPeerRegistry()
		peerRegistry.AddPeer(unhealthyPeerID)
		peerRegistry.UpdateConnectionState(unhealthyPeerID, true) // Mark as connected
		peerRegistry.UpdateHealth(unhealthyPeerID, false)         // Mark as unhealthy

		// Create mock kafka producer (should NOT be called)
		mockKafkaProducer := new(MockKafkaProducer)

		// Create server with registry
		server := &Server{
			P2PClient:                 mockP2PNode,
			peerRegistry:              peerRegistry,
			banManager:                mockBanManager,
			notificationCh:            make(chan *notificationMsg, 10),
			blocksKafkaProducerClient: mockKafkaProducer,
			logger:                    ulogger.New("test-server"),
		}

		// Call handler with message from unhealthy peer
		blockMsg := fmt.Sprintf(`{"Hash":"000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f","Height":1,"DataHubURL":"http://example.com","PeerID":"%s"}`, unhealthyPeerID.String())
		server.handleBlockTopic(ctx, []byte(blockMsg), unhealthyPeerID.String())

		// Verify notification was still sent (happens before health check)
		select {
		case notification := <-server.notificationCh:
			assert.Equal(t, "block", notification.Type)
		default:
			t.Fatal("Expected notification message but none received")
		}

		// Verify Kafka producer was NOT called (message was ignored)
		mockKafkaProducer.AssertNotCalled(t, "Publish", mock.Anything)
	})

	t.Run("allow_block_from_healthy_peer", func(t *testing.T) {
		// Create mock P2PClient
		mockP2PNode := new(MockServerP2PClient)
		selfPeerID, _ := peer.Decode("12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ")
		healthyPeerID, _ := peer.Decode("12D3KooWEyX7hgdXy8zUjCs9CqvMGpB5dKVFj9MX2nUBLwajdSZH")
		originatorPeerIDStr := "12D3KooWQYVQJfrw4RZnNHgRxGFLXoXswE5wuoUBgWpeJYeGDjvA"

		mockP2PNode.On("GetID").Return(selfPeerID)

		// Create mock ban manager
		mockBanManager := new(MockPeerBanManager)
		mockBanManager.On("IsBanned", originatorPeerIDStr).Return(false)

		// Create peer registry with healthy peer
		peerRegistry := NewPeerRegistry()
		peerRegistry.AddPeer(healthyPeerID)
		peerRegistry.UpdateHealth(healthyPeerID, true) // Mark as healthy

		// Create mock kafka producer (SHOULD be called)
		mockKafkaProducer := new(MockKafkaProducer)
		mockKafkaProducer.On("Publish", mock.Anything).Return()

		// Create server with registry
		server := &Server{
			P2PClient:                 mockP2PNode,
			peerRegistry:              peerRegistry,
			banManager:                mockBanManager,
			notificationCh:            make(chan *notificationMsg, 10),
			blocksKafkaProducerClient: mockKafkaProducer,
			logger:                    ulogger.New("test-server"),
		}

		// Call handler with message from healthy peer
		blockMsg := fmt.Sprintf(`{"Hash":"000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f","Height":1,"DataHubURL":"http://example.com","PeerID":"%s"}`, originatorPeerIDStr)
		server.handleBlockTopic(ctx, []byte(blockMsg), healthyPeerID.String())

		// Verify notification was sent
		select {
		case notification := <-server.notificationCh:
			assert.Equal(t, "block", notification.Type)
		default:
			t.Fatal("Expected notification message but none received")
		}

		// Verify Kafka producer WAS called (message was processed)
		mockKafkaProducer.AssertCalled(t, "Publish", mock.Anything)
	})
}

// TestHandleSubtreeTopic_UnhealthyPeer tests subtree topic handling with unhealthy peers
func TestHandleSubtreeTopic_UnhealthyPeer(t *testing.T) {
	ctx := context.Background()

	t.Run("ignore_subtree_from_unhealthy_peer", func(t *testing.T) {
		// Create mock P2PClient
		mockP2PNode := new(MockServerP2PClient)
		selfPeerID, _ := peer.Decode("12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ")
		unhealthyPeerID, _ := peer.Decode("12D3KooWEyX7hgdXy8zUjCs9CqvMGpB5dKVFj9MX2nUBLwajdSZH")

		mockP2PNode.On("GetID").Return(selfPeerID)

		// Create mock ban manager
		mockBanManager := new(MockPeerBanManager)
		mockBanManager.On("IsBanned", unhealthyPeerID.String()).Return(false)

		// Create peer registry with unhealthy CONNECTED peer
		peerRegistry := NewPeerRegistry()
		peerRegistry.AddPeer(unhealthyPeerID)
		peerRegistry.UpdateConnectionState(unhealthyPeerID, true) // Mark as connected
		peerRegistry.UpdateHealth(unhealthyPeerID, false)         // Mark as unhealthy

		// Create mock kafka producer (should NOT be called)
		mockKafkaProducer := new(MockKafkaProducer)

		// Create settings
		tSettings := createBaseTestSettings()

		// Create server with registry
		server := &Server{
			P2PClient:                  mockP2PNode,
			peerRegistry:               peerRegistry,
			banManager:                 mockBanManager,
			notificationCh:             make(chan *notificationMsg, 10),
			subtreeKafkaProducerClient: mockKafkaProducer,
			settings:                   tSettings,
			logger:                     ulogger.New("test-server"),
		}

		// Call handler with message from unhealthy connected peer
		subtreeMsg := `{"Hash":"000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f","DataHubURL":"http://example.com","PeerID":"QmcqHnEQuFdvxoRax8V9qjvHnqF2TpJ8nt8PNGJRRsKKg5"}`
		server.handleSubtreeTopic(ctx, []byte(subtreeMsg), unhealthyPeerID.String())

		// Verify notification was still sent (happens before health check)
		select {
		case notification := <-server.notificationCh:
			assert.Equal(t, "subtree", notification.Type)
		default:
			t.Fatal("Expected notification message but none received")
		}

		// Verify Kafka producer was NOT called (message was ignored)
		mockKafkaProducer.AssertNotCalled(t, "Publish", mock.Anything)
	})

	t.Run("allow_subtree_from_healthy_peer", func(t *testing.T) {
		// Create mock P2PClient
		mockP2PNode := new(MockServerP2PClient)
		selfPeerID, _ := peer.Decode("12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ")
		healthyPeerID, _ := peer.Decode("12D3KooWEyX7hgdXy8zUjCs9CqvMGpB5dKVFj9MX2nUBLwajdSZH")

		mockP2PNode.On("GetID").Return(selfPeerID)

		// Create mock ban manager
		mockBanManager := new(MockPeerBanManager)
		mockBanManager.On("IsBanned", healthyPeerID.String()).Return(false)

		// Create peer registry with healthy peer
		peerRegistry := NewPeerRegistry()
		peerRegistry.AddPeer(healthyPeerID)
		peerRegistry.UpdateHealth(healthyPeerID, true) // Mark as healthy

		// Create mock kafka producer (SHOULD be called)
		mockKafkaProducer := new(MockKafkaProducer)
		mockKafkaProducer.On("Publish", mock.Anything).Return()

		// Create settings
		tSettings := createBaseTestSettings()

		// Create server with registry
		server := &Server{
			P2PClient:                  mockP2PNode,
			peerRegistry:               peerRegistry,
			banManager:                 mockBanManager,
			notificationCh:             make(chan *notificationMsg, 10),
			subtreeKafkaProducerClient: mockKafkaProducer,
			settings:                   tSettings,
			logger:                     ulogger.New("test-server"),
		}

		// Call handler with message from healthy peer
		subtreeMsg := `{"Hash":"000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f","DataHubURL":"http://example.com","PeerID":"QmcqHnEQuFdvxoRax8V9qjvHnqF2TpJ8nt8PNGJRRsKKg5"}`
		server.handleSubtreeTopic(ctx, []byte(subtreeMsg), healthyPeerID.String())

		// Verify notification was sent
		select {
		case notification := <-server.notificationCh:
			assert.Equal(t, "subtree", notification.Type)
		default:
			t.Fatal("Expected notification message but none received")
		}

		// Verify Kafka producer WAS called (message was processed)
		mockKafkaProducer.AssertCalled(t, "Publish", mock.Anything)
	})
}

// TestHandleNodeStatusTopic_UnhealthyPeer tests node status topic handling with unhealthy peers
func TestHandleNodeStatusTopic_UnhealthyPeer(t *testing.T) {
	t.Run("skip_peer_updates_from_unhealthy_peer", func(t *testing.T) {
		// Create mock P2PClient
		mockP2PNode := new(MockServerP2PClient)
		selfPeerID, _ := peer.Decode("12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ")
		unhealthyPeerID, _ := peer.Decode("12D3KooWEyX7hgdXy8zUjCs9CqvMGpB5dKVFj9MX2nUBLwajdSZH")

		mockP2PNode.On("GetID").Return(selfPeerID)

		// Create peer registry with unhealthy CONNECTED peer
		peerRegistry := NewPeerRegistry()
		peerRegistry.AddPeer(unhealthyPeerID)
		peerRegistry.UpdateConnectionState(unhealthyPeerID, true) // Mark as connected
		peerRegistry.UpdateHealth(unhealthyPeerID, false)         // Mark as unhealthy

		// Create server with registry
		server := &Server{
			P2PClient:      mockP2PNode,
			peerRegistry:   peerRegistry,
			notificationCh: make(chan *notificationMsg, 10),
			logger:         ulogger.New("test-server"),
		}

		// Store initial peer state
		peerBefore, _ := peerRegistry.GetPeer(unhealthyPeerID)

		// Call handler with message from unhealthy connected peer
		nodeStatusMsg := fmt.Sprintf(`{"type":"node_status","base_url":"http://example.com","peer_id":"%s","version":"1.0","best_block_hash":"000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f","best_height":100}`, unhealthyPeerID.String())
		server.handleNodeStatusTopic(context.Background(), []byte(nodeStatusMsg), unhealthyPeerID.String())

		// Verify notification was still sent to WebSocket (for monitoring)
		select {
		case notification := <-server.notificationCh:
			assert.Equal(t, "node_status", notification.Type)
		default:
			t.Fatal("Expected notification message but none received")
		}

		// Verify peer data was NOT updated (height should remain the same)
		peerAfter, _ := peerRegistry.GetPeer(unhealthyPeerID)
		assert.Equal(t, peerBefore.Height, peerAfter.Height, "Height should not be updated from unhealthy connected peer")
		assert.Equal(t, peerBefore.BlockHash, peerAfter.BlockHash, "BlockHash should not be updated from unhealthy connected peer")
	})

	t.Run("allow_peer_updates_from_healthy_peer", func(t *testing.T) {
		// Create mock P2PClient
		mockP2PNode := new(MockServerP2PClient)
		selfPeerID, _ := peer.Decode("12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ")
		healthyPeerID, _ := peer.Decode("12D3KooWEyX7hgdXy8zUjCs9CqvMGpB5dKVFj9MX2nUBLwajdSZH")

		mockP2PNode.On("GetID").Return(selfPeerID)

		// Create peer registry with healthy peer
		peerRegistry := NewPeerRegistry()
		peerRegistry.AddPeer(healthyPeerID)
		peerRegistry.UpdateHealth(healthyPeerID, true) // Mark as healthy

		// Create server with registry
		server := &Server{
			P2PClient:      mockP2PNode,
			peerRegistry:   peerRegistry,
			notificationCh: make(chan *notificationMsg, 10),
			logger:         ulogger.New("test-server"),
		}

		// Store initial peer state
		peerBefore, _ := peerRegistry.GetPeer(healthyPeerID)
		assert.Equal(t, int32(0), peerBefore.Height, "Initial height should be 0")

		// Call handler with message from healthy peer
		nodeStatusMsg := fmt.Sprintf(`{"type":"node_status","base_url":"http://example.com","peer_id":"%s","version":"1.0","best_block_hash":"000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f","best_height":100}`, healthyPeerID.String())
		server.handleNodeStatusTopic(context.Background(), []byte(nodeStatusMsg), healthyPeerID.String())

		// Verify notification was sent
		select {
		case notification := <-server.notificationCh:
			assert.Equal(t, "node_status", notification.Type)
		default:
			t.Fatal("Expected notification message but none received")
		}

		// Verify peer data WAS updated
		peerAfter, _ := peerRegistry.GetPeer(healthyPeerID)
		assert.Equal(t, int32(100), peerAfter.Height, "Height should be updated from healthy peer")
		assert.Equal(t, "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f", peerAfter.BlockHash, "BlockHash should be updated from healthy peer")
	})
}

// TestConnectedVsGossipedPeers tests the distinction between connected and gossiped peers
func TestConnectedVsGossipedPeers(t *testing.T) {
	t.Run("connected_peer_marked_as_connected", func(t *testing.T) {
		peerRegistry := NewPeerRegistry()
		connectedPeerID, _ := peer.Decode("12D3KooWEyX7hgdXy8zUjCs9CqvMGpB5dKVFj9MX2nUBLwajdSZH")

		// Add as connected peer
		peerRegistry.AddPeer(connectedPeerID)
		peerRegistry.UpdateConnectionState(connectedPeerID, true)

		// Verify it's marked as connected
		peerInfo, exists := peerRegistry.GetPeer(connectedPeerID)
		assert.True(t, exists)
		assert.True(t, peerInfo.IsConnected, "Peer should be marked as connected")

		// Verify GetConnectedPeers returns it
		connectedPeers := peerRegistry.GetConnectedPeers()
		assert.Len(t, connectedPeers, 1)
		assert.Equal(t, connectedPeerID, connectedPeers[0].ID)
	})

	t.Run("gossiped_peer_not_marked_as_connected", func(t *testing.T) {
		peerRegistry := NewPeerRegistry()
		gossipedPeerID, _ := peer.Decode("12D3KooWEyX7hgdXy8zUjCs9CqvMGpB5dKVFj9MX2nUBLwajdSZH")

		// Add as gossiped peer (default IsConnected = false)
		peerRegistry.AddPeer(gossipedPeerID)

		// Verify it's not marked as connected
		peerInfo, exists := peerRegistry.GetPeer(gossipedPeerID)
		assert.True(t, exists)
		assert.False(t, peerInfo.IsConnected, "Gossiped peer should not be marked as connected")

		// Verify GetConnectedPeers doesn't return it
		connectedPeers := peerRegistry.GetConnectedPeers()
		assert.Len(t, connectedPeers, 0, "Gossiped peer should not appear in connected peers list")
	})

	t.Run("health_checker_only_checks_connected_peers", func(t *testing.T) {
		peerRegistry := NewPeerRegistry()

		// Add one connected peer and one gossiped peer
		connectedPeerID, _ := peer.Decode("12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ")
		gossipedPeerID, _ := peer.Decode("12D3KooWEyX7hgdXy8zUjCs9CqvMGpB5dKVFj9MX2nUBLwajdSZH")

		peerRegistry.AddPeer(connectedPeerID)
		peerRegistry.UpdateConnectionState(connectedPeerID, true)
		peerRegistry.UpdateDataHubURL(connectedPeerID, "http://connected.test")

		peerRegistry.AddPeer(gossipedPeerID)
		// Don't mark as connected
		peerRegistry.UpdateDataHubURL(gossipedPeerID, "http://gossiped.test")

		// Verify registry state
		allPeers := peerRegistry.GetAllPeers()
		assert.Len(t, allPeers, 2, "Should have 2 peers total")

		connectedPeers := peerRegistry.GetConnectedPeers()
		assert.Len(t, connectedPeers, 1, "Should have 1 connected peer")
		assert.Equal(t, connectedPeerID, connectedPeers[0].ID)
	})

	t.Run("unhealthy_connected_peer_filtered", func(t *testing.T) {
		// Create mock P2PClient
		mockP2PNode := new(MockServerP2PClient)
		selfPeerID, _ := peer.Decode("12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ")
		unhealthyConnectedPeerID, _ := peer.Decode("12D3KooWEyX7hgdXy8zUjCs9CqvMGpB5dKVFj9MX2nUBLwajdSZH")

		mockP2PNode.On("GetID").Return(selfPeerID)

		// Create mock ban manager
		mockBanManager := new(MockPeerBanManager)
		mockBanManager.On("IsBanned", unhealthyConnectedPeerID.String()).Return(false)

		// Create peer registry with unhealthy CONNECTED peer
		peerRegistry := NewPeerRegistry()
		peerRegistry.AddPeer(unhealthyConnectedPeerID)
		peerRegistry.UpdateConnectionState(unhealthyConnectedPeerID, true) // Mark as connected
		peerRegistry.UpdateHealth(unhealthyConnectedPeerID, false)         // Mark as unhealthy

		// Create mock kafka producer (should NOT be called)
		mockKafkaProducer := new(MockKafkaProducer)

		// Create server
		server := &Server{
			P2PClient:                 mockP2PNode,
			peerRegistry:              peerRegistry,
			banManager:                mockBanManager,
			notificationCh:            make(chan *notificationMsg, 10),
			blocksKafkaProducerClient: mockKafkaProducer,
			logger:                    ulogger.New("test-server"),
		}

		// Call handler with message from unhealthy connected peer
		blockMsg := `{"Hash":"000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f","Height":1,"DataHubURL":"http://example.com","PeerID":"12D3KooWEyX7hgdXy8zUjCs9CqvMGpB5dKVFj9MX2nUBLwajdSZH"}`
		server.handleBlockTopic(context.Background(), []byte(blockMsg), unhealthyConnectedPeerID.String())

		// Verify Kafka producer was NOT called (message was filtered)
		mockKafkaProducer.AssertNotCalled(t, "Publish", mock.Anything)
	})

	t.Run("unhealthy_gossiped_peer_not_filtered", func(t *testing.T) {
		// Create mock P2PClient
		mockP2PNode := new(MockServerP2PClient)
		selfPeerID, _ := peer.Decode("12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ")
		unhealthyGossipedPeerID, _ := peer.Decode("12D3KooWEyX7hgdXy8zUjCs9CqvMGpB5dKVFj9MX2nUBLwajdSZH")

		mockP2PNode.On("GetID").Return(selfPeerID)

		// Create mock ban manager
		mockBanManager := new(MockPeerBanManager)
		mockBanManager.On("IsBanned", "12D3KooWQYVQJfrw4RZnNHgRxGFLXoXswE5wuoUBgWpeJYeGDjvA").Return(false)

		// Create peer registry with unhealthy GOSSIPED peer (not connected)
		peerRegistry := NewPeerRegistry()
		peerRegistry.AddPeer(unhealthyGossipedPeerID)
		// Don't mark as connected (IsConnected = false by default)
		peerRegistry.UpdateHealth(unhealthyGossipedPeerID, false) // Mark as unhealthy

		// Create mock kafka producer (SHOULD be called since gossiped peers aren't filtered)
		mockKafkaProducer := new(MockKafkaProducer)
		mockKafkaProducer.On("Publish", mock.Anything).Return()

		// Create server
		server := &Server{
			P2PClient:                 mockP2PNode,
			peerRegistry:              peerRegistry,
			banManager:                mockBanManager,
			notificationCh:            make(chan *notificationMsg, 10),
			blocksKafkaProducerClient: mockKafkaProducer,
			logger:                    ulogger.New("test-server"),
		}

		// Call handler with message from unhealthy gossiped peer
		// Gossiped peers aren't health-checked, so their health status doesn't matter
		blockMsg := `{"Hash":"000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f","Height":1,"DataHubURL":"http://example.com","PeerID":"12D3KooWQYVQJfrw4RZnNHgRxGFLXoXswE5wuoUBgWpeJYeGDjvA"}`
		server.handleBlockTopic(context.Background(), []byte(blockMsg), unhealthyGossipedPeerID.String())

		// Verify Kafka producer WAS called (gossiped peers not filtered based on health)
		mockKafkaProducer.AssertCalled(t, "Publish", mock.Anything)
	})
}

// TestHandleRejectedTxTopic_UnhealthyPeer tests rejected tx topic handling with unhealthy peers
func TestHandleRejectedTxTopic_UnhealthyPeer(t *testing.T) {
	ctx := context.Background()

	t.Run("ignore_rejected_tx_from_unhealthy_peer", func(t *testing.T) {
		// Create mock P2PClient
		mockP2PNode := new(MockServerP2PClient)
		selfPeerID, _ := peer.Decode("12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ")
		unhealthyPeerID, _ := peer.Decode("12D3KooWEyX7hgdXy8zUjCs9CqvMGpB5dKVFj9MX2nUBLwajdSZH")

		mockP2PNode.On("GetID").Return(selfPeerID)

		// Create mock ban manager
		mockBanManager := new(MockPeerBanManager)
		mockBanManager.On("IsBanned", unhealthyPeerID.String()).Return(false)

		// Create peer registry with unhealthy CONNECTED peer
		peerRegistry := NewPeerRegistry()
		peerRegistry.AddPeer(unhealthyPeerID)
		peerRegistry.UpdateConnectionState(unhealthyPeerID, true) // Mark as connected
		peerRegistry.UpdateHealth(unhealthyPeerID, false)         // Mark as unhealthy

		// Create server with registry
		server := &Server{
			P2PClient:    mockP2PNode,
			peerRegistry: peerRegistry,
			banManager:   mockBanManager,
			logger:       ulogger.New("test-server"),
		}

		// Call handler with message from unhealthy connected peer - should return early without error
		rejectedTxMsg := `{"TxID":"deadbeef","Reason":"double-spend","PeerID":"` + unhealthyPeerID.String() + `"}`
		server.handleRejectedTxTopic(ctx, []byte(rejectedTxMsg), unhealthyPeerID.String())

		// Test passes if no panic occurs and function returns early
	})

	t.Run("allow_rejected_tx_from_healthy_peer", func(t *testing.T) {
		// Create mock P2PClient
		mockP2PNode := new(MockServerP2PClient)
		selfPeerID, _ := peer.Decode("12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ")
		healthyPeerID, _ := peer.Decode("12D3KooWEyX7hgdXy8zUjCs9CqvMGpB5dKVFj9MX2nUBLwajdSZH")

		mockP2PNode.On("GetID").Return(selfPeerID)

		// Create mock ban manager
		mockBanManager := new(MockPeerBanManager)
		mockBanManager.On("IsBanned", healthyPeerID.String()).Return(false)

		// Create peer registry with healthy peer
		peerRegistry := NewPeerRegistry()
		peerRegistry.AddPeer(healthyPeerID)
		peerRegistry.UpdateHealth(healthyPeerID, true) // Mark as healthy

		// Create server with registry
		server := &Server{
			P2PClient:    mockP2PNode,
			peerRegistry: peerRegistry,
			banManager:   mockBanManager,
			logger:       ulogger.New("test-server"),
		}

		// Call handler with message from healthy peer - should process normally
		rejectedTxMsg := `{"TxID":"deadbeef","Reason":"double-spend","PeerID":"` + healthyPeerID.String() + `"}`
		server.handleRejectedTxTopic(ctx, []byte(rejectedTxMsg), healthyPeerID.String())

		// Test passes if function processes without error
	})
}
