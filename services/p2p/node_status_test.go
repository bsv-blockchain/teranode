package p2p

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/services/blockchain/blockchain_api"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-p2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestNodeStatusWithSyncPeer tests that the node status message includes sync peer information
func TestNodeStatusWithSyncPeer(t *testing.T) {
	t.Run("includes_sync_peer_information", func(t *testing.T) {
		// Setup
		ctx := context.Background()
		tSettings := createBaseTestSettings()
		tSettings.Version = "1.0.0"
		tSettings.Commit = "abc123"
		tSettings.P2P.ListenMode = "both"
		tSettings.Coinbase.ArbitraryText = "TestMiner"

		// Mock blockchain client
		blockchainClient := new(blockchain.Mock)
		fsmState := blockchain_api.FSMStateType_RUNNING
		blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&fsmState, nil)
		blockchainClient.On("GetBestHeightAndTime", mock.Anything).Return(100, 0, nil)

		// Mock best block header - create a proper header with a hash
		bestBlockHeader := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(time.Now().Unix()),
			Bits:           model.NBit{0xff, 0xff, 0x7f, 0x20},
			Nonce:          0,
		}
		bestBlockMeta := &model.BlockHeaderMeta{Height: 100}
		blockchainClient.On("GetBestBlockHeader", mock.Anything).Return(bestBlockHeader, bestBlockMeta, nil)

		// Mock P2P node
		mockP2PNode := new(MockServerP2PNode)
		hostID, _ := peer.Decode("12D3KooWLocalNodeIDExample1234567890abcdefghijk")
		mockP2PNode.On("HostID").Return(hostID)

		// Set up peer information for sync manager
		syncPeerID, _ := peer.Decode("12D3KooWL3XJ9EMCyZvmmGXL2LMiVBtrVa2BuESsJiXkSj7333Jw")
		mockP2PNode.On("ConnectedPeers").Return([]p2p.PeerInfo{
			{ID: syncPeerID, CurrentHeight: 110},
		})

		// Mock topic publish
		publishedMessages := make(chan []byte, 1)
		mockP2PNode.On("Publish", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			msgBytes := args.Get(2).([]byte)
			publishedMessages <- msgBytes
		}).Return(nil)

		// Create server with sync manager
		server := &Server{
			settings:            tSettings,
			logger:              ulogger.New("test-server"),
			blockchainClient:    blockchainClient,
			P2PNode:             mockP2PNode,
			gCtx:                ctx,
			syncManager:         NewSyncManager(ulogger.New("test-sync"), &chaincfg.MainNetParams),
			startTime:           time.Now(),
			notificationCh:      make(chan *notificationMsg, 1),
			nodeStatusTopicName: "test/node_status",
			AssetHTTPAddressURL: "http://localhost:8090",
		}

		// Set up sync manager callbacks
		server.syncManager.SetPeerHeightCallback(func(peerID peer.ID) int32 {
			for _, peerInfo := range mockP2PNode.ConnectedPeers() {
				if peerInfo.ID == peerID {
					return peerInfo.CurrentHeight
				}
			}
			return 0
		})
		server.syncManager.SetLocalHeightCallback(func() uint32 {
			return 100
		})
		server.syncManager.SetPeerIPsCallback(func(peerID peer.ID) []string {
			return []string{"127.0.0.1"}
		})

		// Add the sync peer
		server.syncManager.AddPeer(syncPeerID)

		// Update peer height - this will trigger sync peer selection
		// The peer at height 110 is ahead of local height 100, so it will be selected
		server.syncManager.UpdatePeerHeight(syncPeerID, 110)

		// Store the peer's block hash
		server.peerBlockHashes.Store(syncPeerID, "0000000000000000000000000000000000000000000000000000000000000110")

		// Send node status
		err := server.handleNodeStatusNotification(ctx)
		require.NoError(t, err)

		// Get the published message
		select {
		case msgBytes := <-publishedMessages:
			var nodeStatus NodeStatusMessage
			err := json.Unmarshal(msgBytes, &nodeStatus)
			require.NoError(t, err)

			// Verify basic fields
			assert.Equal(t, "node_status", nodeStatus.Type)
			assert.Equal(t, "1.0.0", nodeStatus.Version)
			assert.Equal(t, "abc123", nodeStatus.CommitHash)
			assert.Equal(t, uint32(100), nodeStatus.BestHeight)

			// Verify sync peer information
			assert.Equal(t, syncPeerID.String(), nodeStatus.SyncPeerID, "Should include sync peer ID")
			assert.Equal(t, int32(110), nodeStatus.SyncPeerHeight, "Should include sync peer height")
			assert.Equal(t, "0000000000000000000000000000000000000000000000000000000000000110",
				nodeStatus.SyncPeerBlockHash, "Should include sync peer block hash")

		case <-time.After(100 * time.Millisecond):
			t.Fatal("No message published")
		}
	})

	t.Run("omits_sync_peer_when_none_selected", func(t *testing.T) {
		// Setup
		ctx := context.Background()
		tSettings := createBaseTestSettings()

		// Mock blockchain client
		blockchainClient := new(blockchain.Mock)
		fsmState := blockchain_api.FSMStateType_RUNNING
		blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&fsmState, nil)
		bestBlockHeader := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      uint32(time.Now().Unix()),
			Bits:           model.NBit{0xff, 0xff, 0x7f, 0x20},
			Nonce:          0,
		}
		blockchainClient.On("GetBestBlockHeader", mock.Anything).Return(bestBlockHeader, &model.BlockHeaderMeta{Height: 100}, nil)

		// Mock P2P node
		mockP2PNode := new(MockServerP2PNode)
		hostID, _ := peer.Decode("12D3KooWLocalNodeIDExample1234567890abcdefghijk")
		mockP2PNode.On("HostID").Return(hostID)
		mockP2PNode.On("ConnectedPeers").Return([]p2p.PeerInfo{})

		// Mock topic publish
		publishedMessages := make(chan []byte, 1)
		mockP2PNode.On("Publish", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			msgBytes := args.Get(2).([]byte)
			publishedMessages <- msgBytes
		}).Return(nil)

		// Create server without sync peer
		server := &Server{
			settings:            tSettings,
			logger:              ulogger.New("test-server"),
			blockchainClient:    blockchainClient,
			P2PNode:             mockP2PNode,
			gCtx:                ctx,
			syncManager:         NewSyncManager(ulogger.New("test-sync"), &chaincfg.MainNetParams),
			startTime:           time.Now(),
			notificationCh:      make(chan *notificationMsg, 1),
			nodeStatusTopicName: "test/node_status",
		}

		// Send node status
		err := server.handleNodeStatusNotification(ctx)
		require.NoError(t, err)

		// Get the published message
		select {
		case msgBytes := <-publishedMessages:
			var nodeStatus NodeStatusMessage
			err := json.Unmarshal(msgBytes, &nodeStatus)
			require.NoError(t, err)

			// Verify sync peer fields are empty
			assert.Empty(t, nodeStatus.SyncPeerID, "Should have empty sync peer ID when no sync peer")
			assert.Equal(t, int32(0), nodeStatus.SyncPeerHeight, "Should have zero sync peer height")
			assert.Empty(t, nodeStatus.SyncPeerBlockHash, "Should have empty sync peer block hash")

		case <-time.After(100 * time.Millisecond):
			t.Fatal("No message published")
		}
	})
}
