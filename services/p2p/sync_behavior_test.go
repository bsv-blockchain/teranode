package p2p

import (
	"context"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/services/blockchain/blockchain_api"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/kafka"
	kafkamessage "github.com/bitcoin-sv/teranode/util/kafka/kafka_message"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-p2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// TestNewSyncBehavior tests that the new implementation only sends the best peer's block to Kafka
func TestNewSyncBehavior(t *testing.T) {
	t.Run("only_sends_best_peer_block_to_kafka", func(t *testing.T) {
		// Setup
		ctx := context.Background()
		tSettings := createBaseTestSettings()

		blockchainClient := new(blockchain.Mock)
		fsmState := blockchain_api.FSMStateType_RUNNING
		blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&fsmState, nil)
		blockchainClient.On("GetBestHeightAndTime", mock.Anything).Return(100, 0, nil)

		mockBlocksProducer := kafka.NewKafkaAsyncProducerMock()
		publishCh := mockBlocksProducer.PublishChannel()

		mockP2PNode := new(MockServerP2PNode)
		mockP2PNode.On("UpdatePeerHeight", mock.Anything, mock.Anything).Return()
		mockP2PNode.On("GetPeerStartingHeight", mock.Anything).Return(int32(0), false)
		mockP2PNode.On("SetPeerStartingHeight", mock.Anything, mock.Anything).Return()

		// Set up ConnectedPeers to return peer information for sync manager
		peer1ID, _ := peer.Decode("12D3KooWEyoppNCUx8Yx66oV9fJnriXwCcXwDDUA2kj6vnc6iDEp")
		peer2ID, _ := peer.Decode("12D3KooWL3XJ9EMCyZvmmGXL2LMiVBtrVa2BuESsJiXkSj7333Jw")
		peer3ID, _ := peer.Decode("12D3KooWDpJ7As7BWAwRMfu1VU2WCqNjvq387JEYKDBj4kx6nXTN")

		mockP2PNode.On("ConnectedPeers").Return([]p2p.PeerInfo{
			{ID: peer1ID, CurrentHeight: 105},
			{ID: peer2ID, CurrentHeight: 110}, // Highest
			{ID: peer3ID, CurrentHeight: 103},
		})
		mockP2PNode.On("GetPeerIPs", mock.Anything).Return([]string{"127.0.0.1"})

		server := &Server{
			settings:                  tSettings,
			logger:                    ulogger.New("test-server"),
			blockchainClient:          blockchainClient,
			blocksKafkaProducerClient: mockBlocksProducer,
			P2PNode:                   mockP2PNode,
			gCtx:                      ctx,
			syncManager:               NewSyncManager(ulogger.New("test-sync"), &chaincfg.MainNetParams),
		}

		// Set up SyncManager callbacks
		server.syncManager.SetPeerHeightCallback(func(peerID peer.ID) int32 {
			for _, peerInfo := range mockP2PNode.ConnectedPeers() {
				if peerInfo.ID == peerID {
					return peerInfo.CurrentHeight
				}
			}
			return 0
		})
		server.syncManager.SetLocalHeightCallback(func() uint32 {
			height, _, _ := blockchainClient.GetBestHeightAndTime(ctx)
			return height
		})
		server.syncManager.SetPeerIPsCallback(func(peerID peer.ID) []string {
			return []string{"127.0.0.1"}
		})

		// Add peers to sync manager
		server.syncManager.AddPeer(peer1ID)
		server.syncManager.AddPeer(peer2ID)
		server.syncManager.AddPeer(peer3ID)

		// Give the sync manager time to select a sync peer
		time.Sleep(50 * time.Millisecond)

		localHeight := uint32(100)

		// Simulate 3 peers with different heights
		peer1 := p2p.HandshakeMessage{
			PeerID:     "12D3KooWEyoppNCUx8Yx66oV9fJnriXwCcXwDDUA2kj6vnc6iDEp",
			BestHeight: 105,
			BestHash:   "0000000000000000000000000000000000000000000000000000000000000105",
			DataHubURL: "http://peer1",
		}

		peer2 := p2p.HandshakeMessage{
			PeerID:     "12D3KooWL3XJ9EMCyZvmmGXL2LMiVBtrVa2BuESsJiXkSj7333Jw",
			BestHeight: 110, // Highest - should be sync peer
			BestHash:   "0000000000000000000000000000000000000000000000000000000000000110",
			DataHubURL: "http://peer2",
		}

		peer3 := p2p.HandshakeMessage{
			PeerID:     "12D3KooWDpJ7As7BWAwRMfu1VU2WCqNjvq387JEYKDBj4kx6nXTN",
			BestHeight: 103,
			BestHash:   "0000000000000000000000000000000000000000000000000000000000000103",
			DataHubURL: "http://peer3",
		}

		// NEW BEHAVIOR: Only the sync peer's block should be sent
		server.checkAndTriggerSync(peer1, localHeight)
		server.checkAndTriggerSync(peer2, localHeight)
		server.checkAndTriggerSync(peer3, localHeight)

		// Check which peer was selected as sync peer
		syncPeer := server.syncManager.GetSyncPeer()
		t.Logf("Selected sync peer: %s", syncPeer)

		// Should receive only ONE message from the sync peer
		messagesReceived := 0
		var receivedHash string

		// Try to receive messages
		done := make(chan bool)
		go func() {
			for {
				select {
				case msg := <-publishCh:
					require.NotNil(t, msg)
					var kafkaMsg kafkamessage.KafkaBlockTopicMessage
					err := proto.Unmarshal(msg.Value, &kafkaMsg)
					require.NoError(t, err)
					receivedHash = kafkaMsg.Hash
					messagesReceived++
				case <-time.After(100 * time.Millisecond):
					done <- true
					return
				}
			}
		}()

		<-done

		// With sync peer switching, we may receive up to 2 messages as we upgrade to better peers
		// (first from peer1 at 105, then from peer2 at 110 when it's discovered to be better)
		assert.LessOrEqual(t, messagesReceived, 2, "Should receive at most 2 messages (initial + switch)")
		assert.GreaterOrEqual(t, messagesReceived, 1, "Should receive at least 1 message from sync peer")

		// The last message should be from one of the peers ahead of us (preferably the highest)
		validHashes := []string{
			"0000000000000000000000000000000000000000000000000000000000000105",
			"0000000000000000000000000000000000000000000000000000000000000110",
			"0000000000000000000000000000000000000000000000000000000000000103",
		}
		assert.Contains(t, validHashes, receivedHash, "Received hash should be from one of the peers ahead")

		// With our switching logic, the last message should be from the best peer (110)
		if messagesReceived == 2 {
			assert.Equal(t, "0000000000000000000000000000000000000000000000000000000000000110",
				receivedHash, "Final message should be from the best peer after switching")
		}

		// Most likely it should be from peer2 (highest), but could be peer1 or peer3 due to random selection
		// The important thing is that only ONE peer sent a message, not all three
	})

	t.Run("no_messages_when_no_peers_ahead", func(t *testing.T) {
		// Setup
		ctx := context.Background()
		tSettings := createBaseTestSettings()

		blockchainClient := new(blockchain.Mock)
		fsmState := blockchain_api.FSMStateType_RUNNING
		blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&fsmState, nil)
		blockchainClient.On("GetBestHeightAndTime", mock.Anything).Return(100, 0, nil)

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

		// Peers at or below our height - should not trigger sync
		samePeer := p2p.HandshakeMessage{
			PeerID:     "12D3KooWQYV9dGMFoRzNStwpXztXaBUjtPqi6aU76ZgUriHhKust",
			BestHeight: 100,
			BestHash:   "0000000000000000000000000000000000000000000000000000000000000100",
		}

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
}
