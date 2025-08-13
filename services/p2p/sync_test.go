package p2p

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/services/blockchain/blockchain_api"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/kafka"
	kafkamessage "github.com/bitcoin-sv/teranode/util/kafka/kafka_message"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-p2p"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// TestCurrentSyncBehavior tests the sync manager behavior of only syncing from the selected sync peer
// This test documents the improved behavior where only the sync peer's blocks are sent to Kafka
func TestCurrentSyncBehavior(t *testing.T) {
	t.Run("sends_only_sync_peer_block_to_kafka_when_higher", func(t *testing.T) {
		// Setup
		ctx := context.Background()
		tSettings := createBaseTestSettings()

		blockchainClient := new(blockchain.Mock)
		fsmState := blockchain_api.FSMStateType_RUNNING
		blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&fsmState, nil)

		mockBlocksProducer := kafka.NewKafkaAsyncProducerMock()
		publishCh := mockBlocksProducer.PublishChannel()

		mockP2PNode := new(MockServerP2PNode)
		mockP2PNode.On("UpdatePeerHeight", mock.Anything, mock.Anything).Return()
		mockP2PNode.On("GetPeerStartingHeight", mock.Anything).Return(int32(0), false)
		mockP2PNode.On("SetPeerStartingHeight", mock.Anything, mock.Anything).Return()

		// Create sync manager with initial setup
		syncManager := NewSyncManager(ulogger.New("test-sync"), &chaincfg.MainNetParams)

		server := &Server{
			settings:                  tSettings,
			logger:                    ulogger.New("test-server"),
			blockchainClient:          blockchainClient,
			blocksKafkaProducerClient: mockBlocksProducer,
			P2PNode:                   mockP2PNode,
			gCtx:                      ctx,
			syncManager:               syncManager,
			peerBlockHashes:           sync.Map{},
		}

		localHeight := uint32(100)

		// Simulate 3 peers with different heights
		// Using valid 64-character hex strings for hashes
		peer1 := p2p.HandshakeMessage{
			PeerID:     "12D3KooWEyoppNCUx8Yx66oV9fJnriXwCcXwDDUA2kj6vnc6iDEp",
			BestHeight: 105,
			BestHash:   "0000000000000000000000000000000000000000000000000000000000000105",
			DataHubURL: "http://peer1",
		}

		peer2 := p2p.HandshakeMessage{
			PeerID:     "12D3KooWL3XJ9EMCyZvmmGXL2LMiVBtrVa2BuESsJiXkSj7333Jw",
			BestHeight: 110, // Highest
			BestHash:   "0000000000000000000000000000000000000000000000000000000000000110",
			DataHubURL: "http://peer2",
		}

		peer3 := p2p.HandshakeMessage{
			PeerID:     "12D3KooWDpJ7As7BWAwRMfu1VU2WCqNjvq387JEYKDBj4kx6nXTN",
			BestHeight: 103,
			BestHash:   "0000000000000000000000000000000000000000000000000000000000000103",
			DataHubURL: "http://peer3",
		}

		// New behavior: Only sync peer sends blocks to Kafka
		// Since syncManager has no sync peer selected initially, no messages should be sent
		server.checkAndTriggerSync(peer1, localHeight)
		server.checkAndTriggerSync(peer2, localHeight)
		server.checkAndTriggerSync(peer3, localHeight)

		// Verify that NO messages were sent to Kafka (since no sync peer is selected)
		select {
		case msg := <-publishCh:
			t.Fatalf("Expected no messages to be sent without a sync peer, but got: %v", msg)
		case <-time.After(100 * time.Millisecond):
			// Expected: no messages sent
		}
	})

	t.Run("skips_peers_at_or_below_our_height", func(t *testing.T) {
		// Setup
		ctx := context.Background()
		tSettings := createBaseTestSettings()

		blockchainClient := new(blockchain.Mock)
		fsmState := blockchain_api.FSMStateType_RUNNING
		blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&fsmState, nil)

		mockBlocksProducer := kafka.NewKafkaAsyncProducerMock()
		publishCh := mockBlocksProducer.PublishChannel()

		mockP2PNode := new(MockServerP2PNode)
		mockP2PNode.On("UpdatePeerHeight", mock.Anything, mock.Anything).Return()
		mockP2PNode.On("GetPeerStartingHeight", mock.Anything).Return(int32(0), false)
		mockP2PNode.On("SetPeerStartingHeight", mock.Anything, mock.Anything).Return()

		server := &Server{
			settings:                  tSettings,
			logger:                    ulogger.New("test-server"),
			blockchainClient:          blockchainClient,
			blocksKafkaProducerClient: mockBlocksProducer,
			P2PNode:                   mockP2PNode,
			gCtx:                      ctx,
			syncManager:               NewSyncManager(ulogger.New("test-sync"), &chaincfg.MainNetParams),
		}

		localHeight := uint32(100)

		// Peer at same height - should not send
		samePeer := p2p.HandshakeMessage{
			PeerID:     "12D3KooWQYV9dGMFoRzNStwpXztXaBUjtPqi6aU76ZgUriHhKust",
			BestHeight: 100,
			BestHash:   "0000000000000000000000000000000000000000000000000000000000000100",
		}

		// Peer below our height - should not send
		behindPeer := p2p.HandshakeMessage{
			PeerID:     "12D3KooWJBMZxRLnMQSeNhTkiPXAHFSVznvymNayL6Caqq9vv4Qv",
			BestHeight: 95,
			BestHash:   "0000000000000000000000000000000000000000000000000000000000000095",
		}

		server.checkAndTriggerSync(samePeer, localHeight)
		server.checkAndTriggerSync(behindPeer, localHeight)

		// Verify no messages sent
		select {
		case msg := <-publishCh:
			t.Errorf("Expected no messages, but got: %v", msg)
		case <-time.After(100 * time.Millisecond):
			// Expected timeout - no messages
		}
	})

	t.Run("skips_sync_when_blockchain_is_syncing", func(t *testing.T) {
		// Setup
		ctx := context.Background()
		tSettings := createBaseTestSettings()

		blockchainClient := new(blockchain.Mock)
		fsmState := blockchain_api.FSMStateType_CATCHINGBLOCKS
		blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&fsmState, nil)

		mockBlocksProducer := kafka.NewKafkaAsyncProducerMock()
		publishCh := mockBlocksProducer.PublishChannel()

		mockP2PNode := new(MockServerP2PNode)
		mockP2PNode.On("UpdatePeerHeight", mock.Anything, mock.Anything).Return()
		mockP2PNode.On("GetPeerStartingHeight", mock.Anything).Return(int32(0), false)
		mockP2PNode.On("SetPeerStartingHeight", mock.Anything, mock.Anything).Return()

		server := &Server{
			settings:                  tSettings,
			logger:                    ulogger.New("test-server"),
			blockchainClient:          blockchainClient,
			blocksKafkaProducerClient: mockBlocksProducer,
			P2PNode:                   mockP2PNode,
			gCtx:                      ctx,
			syncManager:               NewSyncManager(ulogger.New("test-sync"), &chaincfg.MainNetParams),
		}

		localHeight := uint32(100)
		higherPeer := p2p.HandshakeMessage{
			PeerID:     "12D3KooWBHvsNmhLgRkKDpWnLaUvdR3STHHU3zW2CAJFf1gGJWY5",
			BestHeight: 110,
			BestHash:   "0000000000000000000000000000000000000000000000000000000000000110",
		}

		server.checkAndTriggerSync(higherPeer, localHeight)

		// Verify no message sent when syncing
		select {
		case msg := <-publishCh:
			t.Errorf("Expected no message during sync, but got: %v", msg)
		case <-time.After(100 * time.Millisecond):
			// Expected - no messages during sync
		}
	})
}

// TestPeerSelectionLogic tests the logic for selecting the best peer
// These tests will initially fail until we implement the feature
func TestPeerSelectionLogic(t *testing.T) {
	t.Run("should_only_send_highest_peer_block_to_kafka", func(t *testing.T) {
		// This test will fail with current implementation
		// It documents the desired behavior we want to achieve
		t.Skip("Not yet implemented - will be implemented after peer selection is added")

		ctx := context.Background()
		tSettings := createBaseTestSettings()

		blockchainClient := new(blockchain.Mock)
		fsmState := blockchain_api.FSMStateType_RUNNING
		blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&fsmState, nil)

		mockBlocksProducer := kafka.NewKafkaAsyncProducerMock()
		publishCh := mockBlocksProducer.PublishChannel()

		mockP2PNode := new(MockServerP2PNode)
		mockP2PNode.On("UpdatePeerHeight", mock.Anything, mock.Anything).Return()
		mockP2PNode.On("GetPeerStartingHeight", mock.Anything).Return(int32(0), false)
		mockP2PNode.On("SetPeerStartingHeight", mock.Anything, mock.Anything).Return()

		server := &Server{
			settings:                  tSettings,
			logger:                    ulogger.New("test-server"),
			blockchainClient:          blockchainClient,
			blocksKafkaProducerClient: mockBlocksProducer,
			P2PNode:                   mockP2PNode,
			gCtx:                      ctx,
			syncManager:               NewSyncManager(ulogger.New("test-sync"), &chaincfg.MainNetParams),
		}

		localHeight := uint32(100)

		// Three peers with different heights
		peer1 := p2p.HandshakeMessage{
			PeerID:     "12D3KooWEyoppNCUx8Yx66oV9fJnriXwCcXwDDUA2kj6vnc6iDEp",
			BestHeight: 105,
			BestHash:   "0000000000000000000000000000000000000000000000000000000000000105",
			DataHubURL: "http://peer1",
		}

		peer2 := p2p.HandshakeMessage{
			PeerID:     "12D3KooWL3XJ9EMCyZvmmGXL2LMiVBtrVa2BuESsJiXkSj7333Jw",
			BestHeight: 110, // Highest - should be selected
			BestHash:   "0000000000000000000000000000000000000000000000000000000000000110",
			DataHubURL: "http://peer2",
		}

		peer3 := p2p.HandshakeMessage{
			PeerID:     "12D3KooWDpJ7As7BWAwRMfu1VU2WCqNjvq387JEYKDBj4kx6nXTN",
			BestHeight: 103,
			BestHash:   "0000000000000000000000000000000000000000000000000000000000000103",
			DataHubURL: "http://peer3",
		}

		// DESIRED BEHAVIOR: Only the highest peer's block should be sent
		server.checkAndTriggerSync(peer1, localHeight)
		server.checkAndTriggerSync(peer2, localHeight)
		server.checkAndTriggerSync(peer3, localHeight)

		// Should receive only ONE message (from peer2 with height 110)
		select {
		case msg := <-publishCh:
			require.NotNil(t, msg)

			var kafkaMsg kafkamessage.KafkaBlockTopicMessage
			err := proto.Unmarshal(msg.Value, &kafkaMsg)
			require.NoError(t, err)

			// Should be the highest peer's block
			assert.Equal(t, "0000000000000000000000000000000000000000000000000000000000000110", kafkaMsg.Hash)
			assert.Equal(t, "http://peer2", kafkaMsg.URL)
		case <-time.After(100 * time.Millisecond):
			t.Fatal("Expected 1 message from highest peer")
		}

		// Should NOT receive any more messages
		select {
		case msg := <-publishCh:
			t.Errorf("Expected only 1 message, but got another: %v", msg)
		case <-time.After(100 * time.Millisecond):
			// Good - no more messages
		}
	})

	t.Run("should_handle_peers_at_same_height", func(t *testing.T) {
		t.Skip("Not yet implemented - will be implemented in phase 4")

		// When all peers are at the same height as us,
		// randomly select one (okPeers behavior)
	})

	t.Run("should_ignore_peers_behind_us", func(t *testing.T) {
		t.Skip("Not yet implemented - will be implemented in phase 4")

		// Peers with lower height than ours should not be considered
	})

	t.Run("should_switch_sync_peer_on_stall", func(t *testing.T) {
		t.Skip("Not yet implemented - will be implemented in phase 6")

		// If sync peer hasn't sent a block in 3 minutes, switch to another
	})
}

// TestSyncPeerManagement tests managing a single sync peer
func TestSyncPeerManagement(t *testing.T) {
	t.Run("only_one_sync_peer_at_a_time", func(t *testing.T) {
		t.Skip("Not yet implemented - will be implemented in phase 5")

		// Ensure we only sync from one peer at a time
		// When we have a healthy sync peer, don't switch
	})

	t.Run("periodic_sync_peer_evaluation", func(t *testing.T) {
		t.Skip("Not yet implemented - will be implemented in phase 6")

		// Every 30 seconds, evaluate sync peer health
		// Check network speed and last block time
	})
}

// TestRunningStateSync tests sync behavior when node is RUNNING (not just IBD)
func TestRunningStateSync(t *testing.T) {
	t.Run("continues_sync_when_running", func(t *testing.T) {
		t.Skip("Not yet implemented - will be implemented in phase 11")

		// When FSM is RUNNING, still monitor for new blocks
		// Handle chain tip updates properly
	})

	t.Run("handles_chain_reorganization", func(t *testing.T) {
		t.Skip("Not yet implemented - will be implemented in phase 12")

		// When a peer announces a longer valid chain, switch to it
	})
}

// TestNetworkMonitoring tests network speed and health monitoring
func TestNetworkMonitoring(t *testing.T) {
	t.Run("tracks_bytes_received_per_peer", func(t *testing.T) {
		t.Skip("Not yet implemented - will be implemented in phase 7")

		// Track network speed for each peer
	})

	t.Run("detects_slow_peers", func(t *testing.T) {
		t.Skip("Not yet implemented - will be implemented in phase 7")

		// Detect when peer network speed falls below threshold
	})

	t.Run("counts_violations", func(t *testing.T) {
		t.Skip("Not yet implemented - will be implemented in phase 7")

		// Count network speed violations
		// After 3 violations, mark peer as unhealthy
	})
}
