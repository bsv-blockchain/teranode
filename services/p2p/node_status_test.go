package p2p

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockassembly"
	"github.com/bitcoin-sv/teranode/services/blockassembly/blockassembly_api"
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
		tSettings.ChainCfgParams = &chaincfg.RegressionNetParams

		// Mock blockchain client
		blockchainClient := new(blockchain.Mock)
		blockAssemblyClient := new(blockassembly.Mock)
		fsmState := blockchain_api.FSMStateType_RUNNING
		basmState := &blockassembly_api.StateMessage{
			BlockAssemblyState: "running",
			CurrentHash:        "00000000ec43df22050847b5a5992d5484458fd49a77d775411f28b64198d518",
			CurrentHeight:      110,
			TxCount:            1000,
			SubtreeCount:       3,
			Subtrees: []string{
				"d433534730f3d24bda1ecec478fb44411fd596b056bce5513f40036a1863edbb",
				"6f00bb8845234c1db7d932b9cfdd7ac0b00751fe42022a7698a1b4401ccf5bf8",
				"6bd64f75737fceb324076c27550a54761c6ea68b5d2d4d4a0278e7f05e1e7d7d",
			},
		}

		blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&fsmState, nil)
		blockchainClient.On("GetBestHeightAndTime", mock.Anything).Return(100, 0, nil)
		blockAssemblyClient.On("GetBlockAssemblyState", mock.Anything).Return(basmState, nil)

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
			blockAssemblyClient: blockAssemblyClient,
			P2PNode:             mockP2PNode,
			gCtx:                ctx,
			syncManager:         NewSyncManager(ulogger.New("test-sync"), tSettings),
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

			// Verify block assembly state
			assert.Equal(t, uint32(110), nodeStatus.BlockAssemblyDetails.CurrentHeight, "Should include current height")
			assert.Equal(t, "running", nodeStatus.BlockAssemblyDetails.BlockAssemblyState, "Should include block assembly state")
			assert.Equal(t, uint64(1000), nodeStatus.BlockAssemblyDetails.TxCount, "Should include transaction count")
			assert.Equal(t, uint32(3), nodeStatus.BlockAssemblyDetails.SubtreeCount, "Should include subtree count")
			assert.Equal(t, 3, len(nodeStatus.BlockAssemblyDetails.Subtrees), "Should include 3 subtrees")
			assert.Equal(t, "d433534730f3d24bda1ecec478fb44411fd596b056bce5513f40036a1863edbb",
				nodeStatus.BlockAssemblyDetails.Subtrees[0], "Should include first subtree hash")
			assert.Equal(t, "6f00bb8845234c1db7d932b9cfdd7ac0b00751fe42022a7698a1b4401ccf5bf8",
				nodeStatus.BlockAssemblyDetails.Subtrees[1], "Should include second subtree hash")
			assert.Equal(t, "6bd64f75737fceb324076c27550a54761c6ea68b5d2d4d4a0278e7f05e1e7d7d",
				nodeStatus.BlockAssemblyDetails.Subtrees[2], "Should include third subtree hash")

		case <-time.After(100 * time.Millisecond):
			t.Fatal("No message published")
		}
	})

	t.Run("omits_sync_peer_when_none_selected", func(t *testing.T) {
		// Setup
		ctx := context.Background()
		tSettings := createBaseTestSettings()
		tSettings.ChainCfgParams = &chaincfg.RegressionNetParams

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
			syncManager:         NewSyncManager(ulogger.New("test-sync"), tSettings),
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
