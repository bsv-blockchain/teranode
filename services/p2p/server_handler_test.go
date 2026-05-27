package p2p

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	p2pMessageBus "github.com/bsv-blockchain/go-p2p-message-bus"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/services/p2p/p2p_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
)

// --- handleRejectedTxTopic tests ---

func TestHandleRejectedTxTopic(t *testing.T) {
	t.Run("valid message from matching peer", func(t *testing.T) {
		_, pub, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
		require.NoError(t, err)
		selfPeerID, err := peer.IDFromPublicKey(pub)
		require.NoError(t, err)

		_, pub2, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
		require.NoError(t, err)
		remotePeerID, err := peer.IDFromPublicKey(pub2)
		require.NoError(t, err)

		mockP2P := new(MockServerP2PClient)
		mockP2P.peerID = selfPeerID

		registry := NewPeerRegistry()
		registry.Put(remotePeerID, "", 0, nil, "")

		tSettings := createBaseTestSettings()
		tSettings.P2P.ListenMode = settings.ListenModeFull

		server := &Server{
			logger:         ulogger.New("test"),
			P2PClient:      mockP2P,
			peerRegistry:   registry,
			banManager:     NewPeerBanManager(context.Background(), nil, tSettings, registry),
			settings:       tSettings,
			notificationCh: make(chan *notificationMsg, 10),
		}

		msg := RejectedTxMessage{
			PeerID: remotePeerID.String(),
			TxID:   "abc123",
			Reason: "invalid",
		}
		msgBytes, err := json.Marshal(msg)
		require.NoError(t, err)

		server.handleRejectedTxTopic(context.Background(), msgBytes, remotePeerID.String())

		// Verify peer last message time was updated
		peerInfo, exists := registry.Get(remotePeerID)
		require.True(t, exists)
		assert.False(t, peerInfo.LastMessageTime.IsZero(), "last message time should be updated")
	})

	t.Run("invalid json returns early", func(t *testing.T) {
		server := &Server{
			logger: ulogger.New("test"),
		}

		// Should not panic on invalid JSON
		server.handleRejectedTxTopic(context.Background(), []byte("not-json"), "peer1")
	})

	t.Run("mismatched fromID and peerID returns early", func(t *testing.T) {
		mockP2P := new(MockServerP2PClient)
		mockP2P.On("GetID").Return(peer.ID("self-peer"))

		registry := NewPeerRegistry()
		tSettings := createBaseTestSettings()

		server := &Server{
			logger:     ulogger.New("test"),
			P2PClient:  mockP2P,
			banManager: NewPeerBanManager(context.Background(), nil, tSettings, registry),
		}

		msg := RejectedTxMessage{
			PeerID: "peer-A",
			TxID:   "tx1",
			Reason: "test",
		}
		msgBytes, _ := json.Marshal(msg)

		// fromID is "peer-B" but message says peer-A: should log error and return
		server.handleRejectedTxTopic(context.Background(), msgBytes, "peer-B")
	})

	t.Run("own message is ignored", func(t *testing.T) {
		selfID := peer.ID("self-peer")
		mockP2P := new(MockServerP2PClient)
		mockP2P.peerID = selfID

		server := &Server{
			logger:    ulogger.New("test"),
			P2PClient: mockP2P,
		}

		msg := RejectedTxMessage{
			PeerID: selfID.String(),
			TxID:   "tx1",
			Reason: "test",
		}
		msgBytes, _ := json.Marshal(msg)

		// Should return early without updating anything
		server.handleRejectedTxTopic(context.Background(), msgBytes, selfID.String())
	})
}

// --- handlePeerFailureNotification tests ---

func TestHandlePeerFailureNotification(t *testing.T) {
	t.Run("nil metadata returns nil", func(t *testing.T) {
		server := &Server{
			logger: ulogger.New("test"),
		}

		notification := &blockchain.Notification{
			Metadata: nil,
		}

		err := server.handlePeerFailureNotification(context.Background(), notification)
		require.NoError(t, err)
	})

	t.Run("nil metadata map returns nil", func(t *testing.T) {
		server := &Server{
			logger: ulogger.New("test"),
		}

		notification := &blockchain.Notification{
			Metadata: &blockchain.NotificationMetadata{
				Metadata: nil,
			},
		}

		err := server.handlePeerFailureNotification(context.Background(), notification)
		require.NoError(t, err)
	})

	t.Run("catchup failure triggers sync coordinator", func(t *testing.T) {
		registry := NewPeerRegistry()
		tSettings := createBaseTestSettings()

		mockBC := new(blockchain.Mock)
		mockP2P := new(MockServerP2PClient)
		mockP2P.On("GetID").Return(peer.ID("self")).Maybe()

		peerSelector := NewPeerSelector(ulogger.New("test"), tSettings)

		banManager := NewPeerBanManager(context.Background(), nil, tSettings, registry)

		mockKafkaProducer := new(MockKafkaProducer)
		mockKafkaProducer.On("Publish", mock.Anything).Return().Maybe()

		syncCoord := NewSyncCoordinator(
			ulogger.New("test"),
			tSettings,
			registry,
			peerSelector,
			banManager,
			mockBC,
			mockKafkaProducer,
		)

		server := &Server{
			logger:          ulogger.New("test"),
			syncCoordinator: syncCoord,
		}

		notification := &blockchain.Notification{
			Metadata: &blockchain.NotificationMetadata{
				Metadata: map[string]string{
					"peer_id":      "12D3KooWTest",
					"failure_type": "catchup",
					"reason":       "timeout downloading block",
				},
			},
		}

		err := server.handlePeerFailureNotification(context.Background(), notification)
		require.NoError(t, err)
	})

	t.Run("non-catchup failure does not trigger sync coordinator", func(t *testing.T) {
		server := &Server{
			logger: ulogger.New("test"),
		}

		notification := &blockchain.Notification{
			Metadata: &blockchain.NotificationMetadata{
				Metadata: map[string]string{
					"peer_id":      "12D3KooWTest",
					"failure_type": "validation",
					"reason":       "invalid block",
				},
			},
		}

		err := server.handlePeerFailureNotification(context.Background(), notification)
		require.NoError(t, err)
	})
}

// --- processBlockchainNotification PeerFailure tests ---

func TestProcessBlockchainNotificationPeerFailure(t *testing.T) {
	hash := &chainhash.Hash{0x1}
	hashBytes := hash.CloneBytes()

	notification := &blockchain.Notification{
		Type: model.NotificationType_PeerFailure,
		Hash: hashBytes[:],
		Metadata: &blockchain.NotificationMetadata{
			Metadata: map[string]string{
				"peer_id":      "test-peer",
				"failure_type": "catchup",
				"reason":       "timeout",
			},
		},
	}

	server := &Server{
		logger: ulogger.New("test"),
	}

	err := server.processBlockchainNotification(context.Background(), notification)
	require.NoError(t, err)
}

// --- handleNodeStatusNotification tests ---

func TestHandleNodeStatusNotification(t *testing.T) {
	t.Run("successful publish", func(t *testing.T) {
		_, pub, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
		require.NoError(t, err)
		myPeerID, err := peer.IDFromPublicKey(pub)
		require.NoError(t, err)

		header := model.GenesisBlockHeader
		meta := &model.BlockHeaderMeta{Height: 100, ChainWork: []byte{0x01, 0x02}}

		mockP2P := new(MockServerP2PClient)
		mockP2P.peerID = myPeerID
		mockP2P.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		mockBC := new(blockchain.Mock)
		mockBC.On("GetBestBlockHeader", mock.Anything).Return(header, meta, nil)
		fsmState := blockchain_api.FSMStateType_RUNNING
		mockBC.On("GetFSMCurrentState", mock.Anything).Return(&fsmState, nil)

		blockPersisterData := make([]byte, 4)
		binary.LittleEndian.PutUint32(blockPersisterData, 0)
		mockBC.On("GetState", mock.Anything, "BlockPersisterHeight").Return(blockPersisterData, nil)

		tSettings := createBaseTestSettings()
		tSettings.P2P.ListenMode = settings.ListenModeFull
		tSettings.Version = "v1.0.0"
		tSettings.Commit = "abc123"
		tSettings.ClientName = "test-node"

		server := &Server{
			logger:              ulogger.New("test"),
			P2PClient:           mockP2P,
			blockchainClient:    mockBC,
			settings:            tSettings,
			startTime:           time.Now(),
			syncConnectionTimes: sync.Map{},
			notificationCh:      make(chan *notificationMsg, 10),
			nodeStatusTopicName: "test-node-status",
			AssetHTTPAddressURL: testAssetURL,
			peerRegistry:        NewPeerRegistry(),
		}

		err = server.handleNodeStatusNotification(context.Background())
		require.NoError(t, err)

		// Verify notification was sent to channel
		select {
		case msg := <-server.notificationCh:
			assert.Equal(t, "node_status", msg.Type)
			assert.Equal(t, myPeerID.String(), msg.PeerID)
		default:
			t.Fatal("expected notification in channel")
		}

		mockP2P.AssertCalled(t, "Publish", mock.Anything, "test-node-status", mock.Anything)
	})

	t.Run("publish error returns error", func(t *testing.T) {
		_, pub, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
		require.NoError(t, err)
		errPeerID, err := peer.IDFromPublicKey(pub)
		require.NoError(t, err)

		mockP2P := new(MockServerP2PClient)
		mockP2P.peerID = errPeerID
		mockP2P.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(assert.AnError)

		mockBC := new(blockchain.Mock)
		mockBC.On("GetBestBlockHeader", mock.Anything).Return(model.GenesisBlockHeader, model.GenesisBlockHeaderMeta, nil)
		fsmState := blockchain_api.FSMStateType_RUNNING
		mockBC.On("GetFSMCurrentState", mock.Anything).Return(&fsmState, nil)

		blockPersisterData := make([]byte, 4)
		mockBC.On("GetState", mock.Anything, "BlockPersisterHeight").Return(blockPersisterData, nil)

		tSettings := createBaseTestSettings()
		tSettings.P2P.ListenMode = settings.ListenModeFull

		server := &Server{
			logger:              ulogger.New("test"),
			P2PClient:           mockP2P,
			blockchainClient:    mockBC,
			settings:            tSettings,
			startTime:           time.Now(),
			syncConnectionTimes: sync.Map{},
			notificationCh:      make(chan *notificationMsg, 10),
			nodeStatusTopicName: "node-status",
			peerRegistry:        NewPeerRegistry(),
		}

		err = server.handleNodeStatusNotification(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "publish error")
	})
}

// --- getNodeStatusMessage tests ---

func TestGetNodeStatusMessage(t *testing.T) {
	t.Run("with blockchain client returning best block", func(t *testing.T) {
		_, pub, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
		require.NoError(t, err)
		myPeerID, err := peer.IDFromPublicKey(pub)
		require.NoError(t, err)

		header := model.GenesisBlockHeader
		meta := &model.BlockHeaderMeta{
			Height:    42,
			Miner:     "TestMiner",
			ChainWork: []byte{0x00, 0x01},
		}

		mockP2P := new(MockServerP2PClient)
		mockP2P.peerID = myPeerID

		mockBC := new(blockchain.Mock)
		mockBC.On("GetBestBlockHeader", mock.Anything).Return(header, meta, nil)
		fsmState := blockchain_api.FSMStateType_RUNNING
		mockBC.On("GetFSMCurrentState", mock.Anything).Return(&fsmState, nil)

		blockPersisterData := make([]byte, 4)
		binary.LittleEndian.PutUint32(blockPersisterData, 40)
		mockBC.On("GetState", mock.Anything, "BlockPersisterHeight").Return(blockPersisterData, nil)

		tSettings := createBaseTestSettings()
		tSettings.P2P.ListenMode = settings.ListenModeFull
		tSettings.Version = "v2.0.0"
		tSettings.Commit = "def456"
		tSettings.ClientName = "my-client"

		server := &Server{
			logger:              ulogger.New("test"),
			P2PClient:           mockP2P,
			blockchainClient:    mockBC,
			settings:            tSettings,
			startTime:           time.Now().Add(-10 * time.Minute),
			syncConnectionTimes: sync.Map{},
			AssetHTTPAddressURL: testAssetURL,
			PropagationURL:      testPropagationURL,
			peerRegistry:        NewPeerRegistry(),
		}

		msg := server.getNodeStatusMessage(context.Background())
		require.NotNil(t, msg)
		assert.Equal(t, "node_status", msg.Type)
		assert.Equal(t, myPeerID.String(), msg.PeerID)
		assert.Equal(t, uint32(42), msg.BestHeight)
		assert.Equal(t, "v2.0.0", msg.Version)
		assert.Equal(t, "def456", msg.CommitHash)
		assert.Equal(t, "my-client", msg.ClientName)
		assert.Equal(t, "TestMiner", msg.MinerName)
		assert.Equal(t, "RUNNING", msg.FSMState)
		assert.Equal(t, testAssetURL, msg.BaseURL)
		assert.Equal(t, testPropagationURL, msg.PropagationURL)
		assert.Greater(t, msg.Uptime, float64(0))
	})

	t.Run("nil blockchain client uses genesis fallback", func(t *testing.T) {
		_, pub, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
		require.NoError(t, err)
		testPeerID, err := peer.IDFromPublicKey(pub)
		require.NoError(t, err)

		mockP2P := new(MockServerP2PClient)
		mockP2P.peerID = testPeerID

		tSettings := createBaseTestSettings()
		tSettings.P2P.ListenMode = settings.ListenModeFull

		server := &Server{
			logger:              ulogger.New("test"),
			P2PClient:           mockP2P,
			blockchainClient:    nil,
			settings:            tSettings,
			startTime:           time.Now(),
			syncConnectionTimes: sync.Map{},
			peerRegistry:        NewPeerRegistry(),
		}

		msg := server.getNodeStatusMessage(context.Background())
		require.NotNil(t, msg)
		assert.Equal(t, model.GenesisBlockHeaderMeta.Height, msg.BestHeight)
	})

	t.Run("listen only mode clears baseURL and propagationURL", func(t *testing.T) {
		_, pub, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
		require.NoError(t, err)
		testPeerID, err := peer.IDFromPublicKey(pub)
		require.NoError(t, err)

		mockP2P := new(MockServerP2PClient)
		mockP2P.peerID = testPeerID

		mockBC := new(blockchain.Mock)
		mockBC.On("GetBestBlockHeader", mock.Anything).Return(model.GenesisBlockHeader, model.GenesisBlockHeaderMeta, nil)
		fsmState := blockchain_api.FSMStateType_RUNNING
		mockBC.On("GetFSMCurrentState", mock.Anything).Return(&fsmState, nil)

		blockPersisterData := make([]byte, 4)
		mockBC.On("GetState", mock.Anything, "BlockPersisterHeight").Return(blockPersisterData, nil)

		tSettings := createBaseTestSettings()
		tSettings.P2P.ListenMode = settings.ListenModeListenOnly

		server := &Server{
			logger:              ulogger.New("test"),
			P2PClient:           mockP2P,
			blockchainClient:    mockBC,
			settings:            tSettings,
			startTime:           time.Now(),
			syncConnectionTimes: sync.Map{},
			AssetHTTPAddressURL: testAssetURL,
			PropagationURL:      testPropagationURL,
			peerRegistry:        NewPeerRegistry(),
		}

		msg := server.getNodeStatusMessage(context.Background())
		require.NotNil(t, msg)
		assert.Empty(t, msg.BaseURL)
		assert.Empty(t, msg.PropagationURL)
	})

	t.Run("with connected peers counted", func(t *testing.T) {
		_, pub, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
		require.NoError(t, err)
		testPeerID, err := peer.IDFromPublicKey(pub)
		require.NoError(t, err)

		_, pub1, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
		require.NoError(t, err)
		countPeer1, err := peer.IDFromPublicKey(pub1)
		require.NoError(t, err)

		_, pub2, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
		require.NoError(t, err)
		countPeer2, err := peer.IDFromPublicKey(pub2)
		require.NoError(t, err)

		mockP2P := new(MockServerP2PClient)
		mockP2P.peerID = testPeerID

		mockBC := new(blockchain.Mock)
		mockBC.On("GetBestBlockHeader", mock.Anything).Return(model.GenesisBlockHeader, model.GenesisBlockHeaderMeta, nil)
		fsmState := blockchain_api.FSMStateType_RUNNING
		mockBC.On("GetFSMCurrentState", mock.Anything).Return(&fsmState, nil)

		blockPersisterData := make([]byte, 4)
		mockBC.On("GetState", mock.Anything, "BlockPersisterHeight").Return(blockPersisterData, nil)

		tSettings := createBaseTestSettings()
		tSettings.P2P.ListenMode = settings.ListenModeFull

		registry := NewPeerRegistry()
		registry.Put(countPeer1, "", 100, nil, "")
		registry.Put(countPeer2, "", 200, nil, "")

		server := &Server{
			logger:              ulogger.New("test"),
			P2PClient:           mockP2P,
			blockchainClient:    mockBC,
			settings:            tSettings,
			startTime:           time.Now(),
			syncConnectionTimes: sync.Map{},
			peerRegistry:        registry,
		}

		msg := server.getNodeStatusMessage(context.Background())
		require.NotNil(t, msg)
		assert.Equal(t, 2, msg.ConnectedPeersCount)
	})

	t.Run("populates FeePolicy from policy settings", func(t *testing.T) {
		_, pub, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
		require.NoError(t, err)
		testPeerID, err := peer.IDFromPublicKey(pub)
		require.NoError(t, err)

		mockP2P := new(MockServerP2PClient)
		mockP2P.peerID = testPeerID

		mockBC := new(blockchain.Mock)
		mockBC.On("GetBestBlockHeader", mock.Anything).Return(model.GenesisBlockHeader, model.GenesisBlockHeaderMeta, nil)
		fsmState := blockchain_api.FSMStateType_RUNNING
		mockBC.On("GetFSMCurrentState", mock.Anything).Return(&fsmState, nil)
		blockPersisterData := make([]byte, 4)
		mockBC.On("GetState", mock.Anything, "BlockPersisterHeight").Return(blockPersisterData, nil)

		tSettings := createBaseTestSettings()
		tSettings.P2P.ListenMode = settings.ListenModeFull
		tSettings.Policy.MinMiningTxFee = 0.000005 // 500 sat/kB
		tSettings.Policy.MaxScriptSizePolicy = 500_000
		tSettings.Policy.MaxTxSizePolicy = 10_485_760
		tSettings.Policy.MaxTxSigopsCountsPolicy = 4000

		server := &Server{
			logger:              ulogger.New("test"),
			P2PClient:           mockP2P,
			blockchainClient:    mockBC,
			settings:            tSettings,
			startTime:           time.Now(),
			syncConnectionTimes: sync.Map{},
			peerRegistry:        NewPeerRegistry(),
		}

		msg := server.getNodeStatusMessage(context.Background())
		require.NotNil(t, msg)
		require.NotNil(t, msg.FeePolicy)
		assert.Equal(t, uint64(500), msg.FeePolicy.MiningFee.Satoshis)
		assert.Equal(t, uint64(1000), msg.FeePolicy.MiningFee.Bytes)
		assert.Equal(t, uint64(500_000), msg.FeePolicy.MaxScriptSizePolicy)
		assert.Equal(t, uint64(10_485_760), msg.FeePolicy.MaxTxSizePolicy)
		assert.Equal(t, uint64(4000), msg.FeePolicy.MaxTxSigopsCountsPolicy)
	})

	t.Run("invalid policy omits both FeePolicy and MinMiningTxFee", func(t *testing.T) {
		_, pub, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
		require.NoError(t, err)
		testPeerID, err := peer.IDFromPublicKey(pub)
		require.NoError(t, err)

		mockP2P := new(MockServerP2PClient)
		mockP2P.peerID = testPeerID

		mockBC := new(blockchain.Mock)
		mockBC.On("GetBestBlockHeader", mock.Anything).Return(model.GenesisBlockHeader, model.GenesisBlockHeaderMeta, nil)
		fsmState := blockchain_api.FSMStateType_RUNNING
		mockBC.On("GetFSMCurrentState", mock.Anything).Return(&fsmState, nil)
		blockPersisterData := make([]byte, 4)
		mockBC.On("GetState", mock.Anything, "BlockPersisterHeight").Return(blockPersisterData, nil)

		tSettings := createBaseTestSettings()
		tSettings.P2P.ListenMode = settings.ListenModeFull
		// Negative MaxTxSizePolicy makes policyFromSettings reject the policy.
		tSettings.Policy.MaxTxSizePolicy = -1

		server := &Server{
			logger:              ulogger.New("test"),
			P2PClient:           mockP2P,
			blockchainClient:    mockBC,
			settings:            tSettings,
			startTime:           time.Now(),
			syncConnectionTimes: sync.Map{},
			peerRegistry:        NewPeerRegistry(),
		}

		// Call twice to exercise the sync.Once warn gate — both calls must
		// still omit the fee fields, only the first call should log Warnf.
		msg := server.getNodeStatusMessage(context.Background())
		require.NotNil(t, msg)
		// Legacy and new fields must agree: both omitted when policy is invalid.
		assert.Nil(t, msg.FeePolicy)
		assert.Nil(t, msg.MinMiningTxFee)

		msg2 := server.getNodeStatusMessage(context.Background())
		require.NotNil(t, msg2)
		assert.Nil(t, msg2.FeePolicy)
		assert.Nil(t, msg2.MinMiningTxFee)
	})

	t.Run("FeePolicy is nil when policy settings are absent", func(t *testing.T) {
		_, pub, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
		require.NoError(t, err)
		testPeerID, err := peer.IDFromPublicKey(pub)
		require.NoError(t, err)

		mockP2P := new(MockServerP2PClient)
		mockP2P.peerID = testPeerID

		mockBC := new(blockchain.Mock)
		mockBC.On("GetBestBlockHeader", mock.Anything).Return(model.GenesisBlockHeader, model.GenesisBlockHeaderMeta, nil)
		fsmState := blockchain_api.FSMStateType_RUNNING
		mockBC.On("GetFSMCurrentState", mock.Anything).Return(&fsmState, nil)
		blockPersisterData := make([]byte, 4)
		mockBC.On("GetState", mock.Anything, "BlockPersisterHeight").Return(blockPersisterData, nil)

		tSettings := createBaseTestSettings()
		tSettings.P2P.ListenMode = settings.ListenModeFull
		tSettings.Policy = nil // simulate misconfigured / missing policy

		server := &Server{
			logger:              ulogger.New("test"),
			P2PClient:           mockP2P,
			blockchainClient:    mockBC,
			settings:            tSettings,
			startTime:           time.Now(),
			syncConnectionTimes: sync.Map{},
			peerRegistry:        NewPeerRegistry(),
		}

		msg := server.getNodeStatusMessage(context.Background())
		require.NotNil(t, msg)
		assert.Nil(t, msg.FeePolicy)
	})
}

// --- updatePeerLastMessageTime tests ---

func TestUpdatePeerLastMessageTime(t *testing.T) {
	t.Run("nil registry does not panic", func(t *testing.T) {
		server := &Server{
			peerRegistry: nil,
		}
		// Should not panic
		server.updatePeerLastMessageTime("peer1", "peer2")
	})

	t.Run("updates sender last message time", func(t *testing.T) {
		_, pub, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
		require.NoError(t, err)
		senderPeerID, err := peer.IDFromPublicKey(pub)
		require.NoError(t, err)

		registry := NewPeerRegistry()
		mockP2P := new(MockServerP2PClient)
		mockP2P.On("GetID").Return(peer.ID("self")).Maybe()

		server := &Server{
			logger:       ulogger.New("test"),
			peerRegistry: registry,
			P2PClient:    mockP2P,
		}

		server.updatePeerLastMessageTime(senderPeerID.String(), senderPeerID.String())

		// Sender should be added and have updated last message time
		peerInfo, exists := registry.Get(senderPeerID)
		require.True(t, exists)
		assert.False(t, peerInfo.LastMessageTime.IsZero())
	})

	t.Run("updates both sender and originator", func(t *testing.T) {
		_, pub1, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
		require.NoError(t, err)
		senderPeerID, err := peer.IDFromPublicKey(pub1)
		require.NoError(t, err)

		_, pub2, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
		require.NoError(t, err)
		originatorPeerID, err := peer.IDFromPublicKey(pub2)
		require.NoError(t, err)

		registry := NewPeerRegistry()
		mockP2P := new(MockServerP2PClient)
		mockP2P.On("GetID").Return(peer.ID("self")).Maybe()

		server := &Server{
			logger:       ulogger.New("test"),
			peerRegistry: registry,
			P2PClient:    mockP2P,
		}

		server.updatePeerLastMessageTime(senderPeerID.String(), originatorPeerID.String())

		// Both should be in registry
		senderInfo, exists := registry.Get(senderPeerID)
		require.True(t, exists)
		assert.False(t, senderInfo.LastMessageTime.IsZero())

		originatorInfo, exists := registry.Get(originatorPeerID)
		require.True(t, exists)
		assert.False(t, originatorInfo.LastMessageTime.IsZero())
	})

	t.Run("skips self as originator", func(t *testing.T) {
		_, pub, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
		require.NoError(t, err)
		senderPeerID, err := peer.IDFromPublicKey(pub)
		require.NoError(t, err)

		selfPeerID := peer.ID("self-peer")

		registry := NewPeerRegistry()
		mockP2P := new(MockServerP2PClient)
		mockP2P.peerID = selfPeerID

		server := &Server{
			logger:       ulogger.New("test"),
			peerRegistry: registry,
			P2PClient:    mockP2P,
		}

		// Originator is self - should skip adding self to registry
		server.updatePeerLastMessageTime(senderPeerID.String(), selfPeerID.String())

		// Sender should be added
		_, exists := registry.Get(senderPeerID)
		assert.True(t, exists)

		// Self should NOT be added as a peer
		_, exists = registry.Get(selfPeerID)
		assert.False(t, exists)
	})

	t.Run("invalid sender peer ID logs error", func(t *testing.T) {
		registry := NewPeerRegistry()

		server := &Server{
			logger:       ulogger.New("test"),
			peerRegistry: registry,
		}

		// Invalid base58 peer ID - should log error and return
		server.updatePeerLastMessageTime("not-a-valid-peer-id", "")
	})
}

// --- updateBytesReceived tests ---

func TestUpdateBytesReceived(t *testing.T) {
	t.Run("nil registry does not panic", func(t *testing.T) {
		server := &Server{
			peerRegistry: nil,
		}
		server.updateBytesReceived("peer1", "peer2", 1024)
	})

	t.Run("updates sender bytes", func(t *testing.T) {
		_, pub, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
		require.NoError(t, err)
		senderPeerID, err := peer.IDFromPublicKey(pub)
		require.NoError(t, err)

		registry := NewPeerRegistry()
		registry.Put(senderPeerID, "", 0, nil, "")

		server := &Server{
			logger:       ulogger.New("test"),
			peerRegistry: registry,
		}

		server.updateBytesReceived(senderPeerID.String(), "", 1024)

		peerInfo, exists := registry.Get(senderPeerID)
		require.True(t, exists)
		assert.Equal(t, uint64(1024), peerInfo.BytesReceived)
	})

	t.Run("updates both sender and originator bytes", func(t *testing.T) {
		_, pub1, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
		require.NoError(t, err)
		senderPeerID, err := peer.IDFromPublicKey(pub1)
		require.NoError(t, err)

		_, pub2, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
		require.NoError(t, err)
		originatorPeerID, err := peer.IDFromPublicKey(pub2)
		require.NoError(t, err)

		registry := NewPeerRegistry()
		registry.Put(senderPeerID, "", 0, nil, "")
		registry.Put(originatorPeerID, "", 0, nil, "")

		server := &Server{
			logger:       ulogger.New("test"),
			peerRegistry: registry,
		}

		server.updateBytesReceived(senderPeerID.String(), originatorPeerID.String(), 2048)

		senderInfo, exists := registry.Get(senderPeerID)
		require.True(t, exists)
		assert.Equal(t, uint64(2048), senderInfo.BytesReceived)

		originatorInfo, exists := registry.Get(originatorPeerID)
		require.True(t, exists)
		assert.Equal(t, uint64(2048), originatorInfo.BytesReceived)
	})

	t.Run("accumulates bytes over multiple calls", func(t *testing.T) {
		_, pub, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
		require.NoError(t, err)
		senderPeerID, err := peer.IDFromPublicKey(pub)
		require.NoError(t, err)

		registry := NewPeerRegistry()
		registry.Put(senderPeerID, "", 0, nil, "")

		server := &Server{
			logger:       ulogger.New("test"),
			peerRegistry: registry,
		}

		server.updateBytesReceived(senderPeerID.String(), "", 100)
		server.updateBytesReceived(senderPeerID.String(), "", 200)

		peerInfo, exists := registry.Get(senderPeerID)
		require.True(t, exists)
		assert.Equal(t, uint64(300), peerInfo.BytesReceived)
	})

	t.Run("invalid sender ID logs error", func(t *testing.T) {
		registry := NewPeerRegistry()
		server := &Server{
			logger:       ulogger.New("test"),
			peerRegistry: registry,
		}

		// Should not panic on invalid peer ID
		server.updateBytesReceived("invalid-peer", "", 100)
	})
}

// Constants for handler tests
const (
	testPeerIDStr1     = "peer-1"
	testPeerIDStr2     = "peer-2"
	testAssetURL       = "http://example.com"
	testPropagationURL = "http://propagation.example.com"
)

// --- RecordBytesDownloaded gRPC tests ---

func TestRecordBytesDownloaded(t *testing.T) {
	t.Run("successful recording", func(t *testing.T) {
		_, pub, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
		require.NoError(t, err)
		testPeerID, err := peer.IDFromPublicKey(pub)
		require.NoError(t, err)

		registry := NewPeerRegistry()
		registry.Put(testPeerID, "", 0, nil, "")

		server := &Server{
			logger:       ulogger.New("test"),
			peerRegistry: registry,
		}

		req := &p2p_api.RecordBytesDownloadedRequest{
			PeerId:          testPeerID.String(),
			BytesDownloaded: 5000,
		}

		resp, err := server.RecordBytesDownloaded(context.Background(), req)
		require.NoError(t, err)
		assert.True(t, resp.Ok)

		peerInfo, exists := registry.Get(testPeerID)
		require.True(t, exists)
		assert.Equal(t, uint64(5000), peerInfo.BytesReceived)
	})

	t.Run("peer not in registry still returns ok", func(t *testing.T) {
		_, pub, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
		require.NoError(t, err)
		testPeerID, err := peer.IDFromPublicKey(pub)
		require.NoError(t, err)

		registry := NewPeerRegistry()

		server := &Server{
			logger:       ulogger.New("test"),
			peerRegistry: registry,
		}

		req := &p2p_api.RecordBytesDownloadedRequest{
			PeerId:          testPeerID.String(),
			BytesDownloaded: 5000,
		}

		resp, err := server.RecordBytesDownloaded(context.Background(), req)
		require.NoError(t, err)
		assert.True(t, resp.Ok)
	})

	t.Run("invalid peer ID returns error", func(t *testing.T) {
		registry := NewPeerRegistry()

		server := &Server{
			logger:       ulogger.New("test"),
			peerRegistry: registry,
		}

		req := &p2p_api.RecordBytesDownloadedRequest{
			PeerId:          "invalid-peer-id",
			BytesDownloaded: 1000,
		}

		resp, err := server.RecordBytesDownloaded(context.Background(), req)
		require.Error(t, err)
		assert.False(t, resp.Ok)
	})

	t.Run("accumulates bytes from multiple downloads", func(t *testing.T) {
		_, pub, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
		require.NoError(t, err)
		testPeerID, err := peer.IDFromPublicKey(pub)
		require.NoError(t, err)

		registry := NewPeerRegistry()
		registry.Put(testPeerID, "", 0, nil, "")

		server := &Server{
			logger:       ulogger.New("test"),
			peerRegistry: registry,
		}

		for i := 0; i < 3; i++ {
			req := &p2p_api.RecordBytesDownloadedRequest{
				PeerId:          testPeerID.String(),
				BytesDownloaded: 1000,
			}
			resp, err := server.RecordBytesDownloaded(context.Background(), req)
			require.NoError(t, err)
			assert.True(t, resp.Ok)
		}

		peerInfo, exists := registry.Get(testPeerID)
		require.True(t, exists)
		assert.Equal(t, uint64(3000), peerInfo.BytesReceived)
	})
}

// --- ResetReputation gRPC tests ---

func TestResetReputation(t *testing.T) {
	t.Run("reset all peers", func(t *testing.T) {
		registry := NewPeerRegistry()
		registry.Put(peer.ID(testPeerIDStr1), "", 100, nil, "")
		registry.Put(peer.ID(testPeerIDStr2), "", 200, nil, "")

		// Record some failures to create reputation data
		registry.RecordInteractionFailure(peer.ID(testPeerIDStr1))
		registry.RecordInteractionFailure(peer.ID(testPeerIDStr2))

		server := &Server{
			logger:       ulogger.New("test"),
			peerRegistry: registry,
		}

		req := &p2p_api.ResetReputationRequest{
			PeerId: "", // empty = all peers
		}

		resp, err := server.ResetReputation(context.Background(), req)
		require.NoError(t, err)
		assert.True(t, resp.Ok)
		assert.Equal(t, int32(2), resp.PeersReset)
	})

	t.Run("reset specific peer", func(t *testing.T) {
		registry := NewPeerRegistry()
		registry.Put(peer.ID(testPeerIDStr1), "", 100, nil, "")
		registry.RecordInteractionFailure(peer.ID(testPeerIDStr1))

		server := &Server{
			logger:       ulogger.New("test"),
			peerRegistry: registry,
		}

		req := &p2p_api.ResetReputationRequest{
			PeerId: testPeerIDStr1,
		}

		resp, err := server.ResetReputation(context.Background(), req)
		require.NoError(t, err)
		assert.True(t, resp.Ok)
	})

	t.Run("reset nonexistent peer", func(t *testing.T) {
		registry := NewPeerRegistry()

		server := &Server{
			logger:       ulogger.New("test"),
			peerRegistry: registry,
		}

		req := &p2p_api.ResetReputationRequest{
			PeerId: "nonexistent-peer",
		}

		resp, err := server.ResetReputation(context.Background(), req)
		require.NoError(t, err)
		assert.True(t, resp.Ok)
		assert.Equal(t, int32(0), resp.PeersReset)
	})
}

// --- GetPeerRegistry gRPC tests ---

func TestGetPeerRegistry(t *testing.T) {
	t.Run("nil registry returns empty list", func(t *testing.T) {
		server := &Server{
			logger:       ulogger.New("test"),
			peerRegistry: nil,
		}

		resp, err := server.GetPeerRegistry(context.Background(), &emptypb.Empty{})
		require.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Empty(t, resp.Peers)
	})

	t.Run("returns all peers with full metadata", func(t *testing.T) {
		_, pub1, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
		require.NoError(t, err)
		peerID1, err := peer.IDFromPublicKey(pub1)
		require.NoError(t, err)

		_, pub2, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
		require.NoError(t, err)
		peerID2, err := peer.IDFromPublicKey(pub2)
		require.NoError(t, err)

		registry := NewPeerRegistry()

		blockHash, _ := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
		registry.Put(peerID1, "client-a", 100, blockHash, "http://peer1:8080")
		registry.UpdateConnectionState(peerID1, true)
		registry.RecordInteractionSuccess(peerID1, 50*time.Millisecond)
		registry.UpdateStorage(peerID1, "full")

		registry.Put(peerID2, "client-b", 200, nil, "http://peer2:8080")
		registry.RecordInteractionFailure(peerID2)
		registry.RecordMaliciousInteraction(peerID2)

		server := &Server{
			logger:       ulogger.New("test"),
			peerRegistry: registry,
		}

		resp, err := server.GetPeerRegistry(context.Background(), &emptypb.Empty{})
		require.NoError(t, err)
		require.Len(t, resp.Peers, 2)

		// Find peers in response by ID
		var peer1Info, peer2Info *p2p_api.PeerRegistryInfo
		for _, p := range resp.Peers {
			if p.Id == peerID1.String() {
				peer1Info = p
			}
			if p.Id == peerID2.String() {
				peer2Info = p
			}
		}

		require.NotNil(t, peer1Info)
		assert.Equal(t, uint32(100), peer1Info.Height)
		assert.Equal(t, blockHash.String(), peer1Info.BlockHash)
		assert.Equal(t, "http://peer1:8080", peer1Info.DataHubUrl)
		assert.True(t, peer1Info.IsConnected)
		assert.Equal(t, int64(1), peer1Info.InteractionSuccesses)
		assert.Equal(t, "full", peer1Info.Storage)
		assert.Equal(t, "client-a", peer1Info.ClientName)

		require.NotNil(t, peer2Info)
		assert.Equal(t, uint32(200), peer2Info.Height)
		assert.Empty(t, peer2Info.BlockHash)
		// RecordInteractionFailure + RecordMaliciousInteraction both increment failures
		assert.Equal(t, int64(2), peer2Info.InteractionFailures)
		assert.Equal(t, int64(1), peer2Info.MaliciousCount)
	})

	t.Run("empty registry returns empty list", func(t *testing.T) {
		registry := NewPeerRegistry()
		server := &Server{
			logger:       ulogger.New("test"),
			peerRegistry: registry,
		}

		resp, err := server.GetPeerRegistry(context.Background(), &emptypb.Empty{})
		require.NoError(t, err)
		assert.Empty(t, resp.Peers)
	})
}

// --- GetPeer gRPC endpoint tests ---

func TestGetPeerGRPC(t *testing.T) {
	t.Run("nil registry returns not found", func(t *testing.T) {
		server := &Server{
			logger:       ulogger.New("test"),
			peerRegistry: nil,
		}

		req := &p2p_api.GetPeerRequest{PeerId: "some-peer"}
		resp, err := server.GetPeer(context.Background(), req)
		require.NoError(t, err)
		assert.False(t, resp.Found)
	})

	t.Run("invalid peer ID returns not found", func(t *testing.T) {
		registry := NewPeerRegistry()
		server := &Server{
			logger:       ulogger.New("test"),
			peerRegistry: registry,
		}

		req := &p2p_api.GetPeerRequest{PeerId: "not-a-valid-peer-id"}
		resp, err := server.GetPeer(context.Background(), req)
		require.NoError(t, err)
		assert.False(t, resp.Found)
	})

	t.Run("peer not in registry returns not found", func(t *testing.T) {
		_, pub, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
		require.NoError(t, err)
		testPeerID, err := peer.IDFromPublicKey(pub)
		require.NoError(t, err)

		registry := NewPeerRegistry()
		server := &Server{
			logger:       ulogger.New("test"),
			peerRegistry: registry,
		}

		req := &p2p_api.GetPeerRequest{PeerId: testPeerID.String()}
		resp, err := server.GetPeer(context.Background(), req)
		require.NoError(t, err)
		assert.False(t, resp.Found)
	})

	t.Run("existing peer returns full info", func(t *testing.T) {
		_, pub, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
		require.NoError(t, err)
		testPeerID, err := peer.IDFromPublicKey(pub)
		require.NoError(t, err)

		blockHash, _ := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")

		registry := NewPeerRegistry()
		registry.Put(testPeerID, "my-client", 500, blockHash, "http://test:8080")
		registry.UpdateConnectionState(testPeerID, true)
		registry.RecordInteractionSuccess(testPeerID, 100*time.Millisecond)
		registry.UpdateStorage(testPeerID, "pruned")

		server := &Server{
			logger:       ulogger.New("test"),
			peerRegistry: registry,
		}

		req := &p2p_api.GetPeerRequest{PeerId: testPeerID.String()}
		resp, err := server.GetPeer(context.Background(), req)
		require.NoError(t, err)
		require.True(t, resp.Found)
		require.NotNil(t, resp.Peer)

		assert.Equal(t, testPeerID.String(), resp.Peer.Id)
		assert.Equal(t, uint32(500), resp.Peer.Height)
		assert.Equal(t, blockHash.String(), resp.Peer.BlockHash)
		assert.Equal(t, "http://test:8080", resp.Peer.DataHubUrl)
		assert.True(t, resp.Peer.IsConnected)
		assert.Equal(t, int64(1), resp.Peer.InteractionSuccesses)
		assert.Equal(t, "pruned", resp.Peer.Storage)
		assert.Equal(t, "my-client", resp.Peer.ClientName)
	})
}

// --- AddBanScore gRPC tests ---

func TestAddBanScoreGRPC(t *testing.T) {
	t.Run("maps known reason strings", func(t *testing.T) {
		registry := NewPeerRegistry()
		tSettings := createBaseTestSettings()

		server := &Server{
			logger:       ulogger.New("test"),
			peerRegistry: registry,
			banManager:   NewPeerBanManager(context.Background(), nil, tSettings, registry),
		}

		reasons := []string{"invalid_subtree", "protocol_violation", "spam", "invalid_block", "unknown_reason"}
		for _, reason := range reasons {
			req := &p2p_api.AddBanScoreRequest{
				PeerId: "test-peer",
				Reason: reason,
			}
			resp, err := server.AddBanScore(context.Background(), req)
			require.NoError(t, err)
			assert.True(t, resp.Ok)
		}
	})

	t.Run("updates sync coordinator ban status", func(t *testing.T) {
		_, pub, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
		require.NoError(t, err)
		testPeerID, err := peer.IDFromPublicKey(pub)
		require.NoError(t, err)

		registry := NewPeerRegistry()
		registry.Put(testPeerID, "", 100, nil, "")

		tSettings := createBaseTestSettings()
		mockBC := new(blockchain.Mock)
		mockP2P := new(MockServerP2PClient)
		mockP2P.On("GetID").Return(peer.ID("self")).Maybe()

		peerSelector := NewPeerSelector(ulogger.New("test"), tSettings)
		banManager := NewPeerBanManager(context.Background(), nil, tSettings, registry)

		mockKafkaProducer := new(MockKafkaProducer)
		mockKafkaProducer.On("Publish", mock.Anything).Return().Maybe()

		syncCoord := NewSyncCoordinator(
			ulogger.New("test"),
			tSettings,
			registry,
			peerSelector,
			banManager,
			mockBC,
			mockKafkaProducer,
		)

		server := &Server{
			logger:          ulogger.New("test"),
			peerRegistry:    registry,
			banManager:      banManager,
			syncCoordinator: syncCoord,
		}

		req := &p2p_api.AddBanScoreRequest{
			PeerId: testPeerID.String(),
			Reason: "invalid_block",
		}
		resp, err := server.AddBanScore(context.Background(), req)
		require.NoError(t, err)
		assert.True(t, resp.Ok)
	})
}

// --- per-topic size limit tests ---
//
// Each topic handler enforces a tighter cap than the global maxP2PMessageSize.
// We confirm that an oversized payload short-circuits before any peer-registry
// side effect: if the handler had run past the size guard, LastMessageTime
// would be updated.

func newSizeLimitTestServer(t *testing.T) (*Server, peer.ID, *PeerRegistry) {
	t.Helper()

	_, pub, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
	require.NoError(t, err)
	selfPeerID, err := peer.IDFromPublicKey(pub)
	require.NoError(t, err)

	_, pub2, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
	require.NoError(t, err)
	remotePeerID, err := peer.IDFromPublicKey(pub2)
	require.NoError(t, err)

	mockP2P := new(MockServerP2PClient)
	mockP2P.peerID = selfPeerID

	registry := NewPeerRegistry()
	registry.Put(remotePeerID, "", 0, nil, "")
	// Force LastMessageTime to a known sentinel so we can detect updates.
	registry.peers[remotePeerID].LastMessageTime = time.Time{}

	tSettings := createBaseTestSettings()
	tSettings.P2P.ListenMode = settings.ListenModeFull

	server := &Server{
		logger:         ulogger.New("test"),
		P2PClient:      mockP2P,
		peerRegistry:   registry,
		banManager:     NewPeerBanManager(context.Background(), nil, tSettings, registry),
		settings:       tSettings,
		notificationCh: make(chan *notificationMsg, 10),
	}

	return server, remotePeerID, registry
}

func TestHandleRejectedTxTopic_OversizedDropped(t *testing.T) {
	server, remotePeerID, registry := newSizeLimitTestServer(t)

	// Build a syntactically valid RejectedTxMessage but pad the reason so the
	// final JSON exceeds the per-topic limit. Without the size guard, the
	// handler would update peer state.
	padding := make([]byte, maxRejectedTxMessageSize+1)
	for i := range padding {
		padding[i] = 'x'
	}
	msgBytes, err := json.Marshal(RejectedTxMessage{
		PeerID: remotePeerID.String(),
		TxID:   "abc",
		Reason: string(padding),
	})
	require.NoError(t, err)
	require.Greater(t, len(msgBytes), maxRejectedTxMessageSize)

	server.handleRejectedTxTopic(context.Background(), msgBytes, remotePeerID.String())

	info, exists := registry.Get(remotePeerID)
	require.True(t, exists)
	assert.True(t, info.LastMessageTime.IsZero(), "oversized rejected_tx must not advance LastMessageTime")
}

func TestHandleNodeStatusTopic_OversizedDropped(t *testing.T) {
	server, remotePeerID, registry := newSizeLimitTestServer(t)

	padding := make([]byte, maxNodeStatusMessageSize+1)
	for i := range padding {
		padding[i] = 'x'
	}
	// ClientName isn't bounded by anything in the message itself, so it's a
	// convenient field to inflate without changing semantics.
	msgBytes, err := json.Marshal(NodeStatusMessage{
		PeerID:     remotePeerID.String(),
		ClientName: string(padding),
	})
	require.NoError(t, err)
	require.Greater(t, len(msgBytes), maxNodeStatusMessageSize)

	server.handleNodeStatusTopic(context.Background(), msgBytes, remotePeerID.String())

	info, exists := registry.Get(remotePeerID)
	require.True(t, exists)
	assert.True(t, info.LastMessageTime.IsZero(), "oversized node_status must not advance LastMessageTime")
}

func TestHandleBlockTopic_OversizedDropped(t *testing.T) {
	server, remotePeerID, registry := newSizeLimitTestServer(t)

	padding := make([]byte, maxBlockMessageSize+1)
	for i := range padding {
		padding[i] = 'x'
	}
	msgBytes, err := json.Marshal(BlockMessage{
		PeerID:     remotePeerID.String(),
		Hash:       "deadbeef",
		ClientName: string(padding),
	})
	require.NoError(t, err)
	require.Greater(t, len(msgBytes), maxBlockMessageSize)

	server.handleBlockTopic(context.Background(), msgBytes, remotePeerID.String())

	info, exists := registry.Get(remotePeerID)
	require.True(t, exists)
	assert.True(t, info.LastMessageTime.IsZero(), "oversized block must not advance LastMessageTime")
}

func TestHandleSubtreeTopic_OversizedDropped(t *testing.T) {
	server, remotePeerID, registry := newSizeLimitTestServer(t)

	padding := make([]byte, maxSubtreeMessageSize+1)
	for i := range padding {
		padding[i] = 'x'
	}
	msgBytes, err := json.Marshal(SubtreeMessage{
		PeerID:     remotePeerID.String(),
		Hash:       "deadbeef",
		ClientName: string(padding),
	})
	require.NoError(t, err)
	require.Greater(t, len(msgBytes), maxSubtreeMessageSize)

	server.handleSubtreeTopic(context.Background(), msgBytes, remotePeerID.String())

	info, exists := registry.Get(remotePeerID)
	require.True(t, exists)
	assert.True(t, info.LastMessageTime.IsZero(), "oversized subtree must not advance LastMessageTime")
}

// --- handleNodeStatusTopic branch coverage ---
//
// TestServerHandleNodeStatusTopic in Server_test.go covers self/remote happy
// paths; TestHandleNodeStatusTopic_OversizedDropped covers the size guard.
// These tests fill in the validation/error branches: bad JSON, peer ID
// spoofing, SSRF rejection, invalid block-hash, full notification channel,
// and the storage-update side effect.

func TestHandleNodeStatusTopic_BadJSON(t *testing.T) {
	server, remotePeerID, registry := newSizeLimitTestServer(t)

	// Garbage that passes the size guard but fails json.Unmarshal.
	server.handleNodeStatusTopic(context.Background(), []byte("{not-json"), remotePeerID.String())

	select {
	case msg := <-server.notificationCh:
		t.Fatalf("bad JSON must not produce a notification, got %+v", msg)
	default:
	}
	info, exists := registry.Get(remotePeerID)
	require.True(t, exists)
	assert.True(t, info.LastMessageTime.IsZero(), "bad JSON must not advance LastMessageTime")
}

func TestHandleNodeStatusTopic_PeerIDSpoofing(t *testing.T) {
	server, remotePeerID, _ := newSizeLimitTestServer(t)

	// Make claimed peer differ from the gossip sender.
	_, pub, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
	require.NoError(t, err)
	otherPeerID, err := peer.IDFromPublicKey(pub)
	require.NoError(t, err)

	msgBytes, err := json.Marshal(NodeStatusMessage{
		PeerID:        otherPeerID.String(),
		BestBlockHash: "0000000000000000000000000000000000000000000000000000000000000001",
		BestHeight:    1,
	})
	require.NoError(t, err)

	server.handleNodeStatusTopic(context.Background(), msgBytes, remotePeerID.String())

	score, _, _ := server.banManager.GetBanScore(remotePeerID.String())
	assert.Equal(t, 20, score, "spoofing should add ReasonProtocolViolation (20) to sender score")

	select {
	case msg := <-server.notificationCh:
		t.Fatalf("spoofing must short-circuit before notification, got %+v", msg)
	default:
	}
}

func TestHandleNodeStatusTopic_InvalidBaseURL(t *testing.T) {
	server, remotePeerID, _ := newSizeLimitTestServer(t)
	// AllowPrivateIPs is false by default in createBaseTestSettings, so a
	// loopback URL is rejected by validateDataHubURL.

	msgBytes, err := json.Marshal(NodeStatusMessage{
		PeerID:  remotePeerID.String(),
		BaseURL: "http://127.0.0.1:8080",
	})
	require.NoError(t, err)

	server.handleNodeStatusTopic(context.Background(), msgBytes, remotePeerID.String())

	score, _, _ := server.banManager.GetBanScore(remotePeerID.String())
	assert.Equal(t, 20, score, "invalid BaseURL should add ReasonProtocolViolation (20) to sender score")

	select {
	case msg := <-server.notificationCh:
		t.Fatalf("SSRF rejection must short-circuit before notification, got %+v", msg)
	default:
	}
}

func TestHandleNodeStatusTopic_InvalidBestBlockHash(t *testing.T) {
	server, remotePeerID, registry := newSizeLimitTestServer(t)

	// Hash that fails chainhash.NewHashFromStr — but height > 0 to enter the
	// branch in the first place. The notification fires before the hash
	// parse, so it must still arrive.
	msgBytes, err := json.Marshal(NodeStatusMessage{
		PeerID:        remotePeerID.String(),
		BestHeight:    42,
		BestBlockHash: "not-a-real-hex-hash",
	})
	require.NoError(t, err)

	server.handleNodeStatusTopic(context.Background(), msgBytes, remotePeerID.String())

	select {
	case msg := <-server.notificationCh:
		assert.Equal(t, remotePeerID.String(), msg.PeerID)
	default:
		t.Fatal("notification should fire before invalid block-hash check")
	}

	info, exists := registry.Get(remotePeerID)
	require.True(t, exists)
	assert.Equal(t, uint32(0), info.Height, "invalid block hash must abort before peer height update")
}

func TestHandleNodeStatusTopic_NotificationChannelFull(t *testing.T) {
	server, remotePeerID, _ := newSizeLimitTestServer(t)
	// Replace the buffered channel with an unbuffered one so the non-blocking
	// send hits the default branch.
	server.notificationCh = make(chan *notificationMsg)

	msgBytes, err := json.Marshal(NodeStatusMessage{
		PeerID: remotePeerID.String(),
	})
	require.NoError(t, err)

	// Must not panic or deadlock.
	server.handleNodeStatusTopic(context.Background(), msgBytes, remotePeerID.String())
}

func TestHandleNodeStatusTopic_StorageUpdate(t *testing.T) {
	server, remotePeerID, registry := newSizeLimitTestServer(t)

	validHash := "0000000000000000000000000000000000000000000000000000000000000001"
	msgBytes, err := json.Marshal(NodeStatusMessage{
		PeerID:        remotePeerID.String(),
		BestHeight:    7,
		BestBlockHash: validHash,
		Storage:       "full",
	})
	require.NoError(t, err)

	server.handleNodeStatusTopic(context.Background(), msgBytes, remotePeerID.String())

	info, exists := registry.Get(remotePeerID)
	require.True(t, exists)
	assert.Equal(t, "full", info.Storage, "storage mode should be propagated to the registry")
	assert.Equal(t, uint32(7), info.Height)
}

// --- shouldSkipDuringSync branch coverage ---
//
// shouldSkipDuringSync gates announcement processing while we're catching up
// from a designated sync peer. The function has six exit paths; these tests
// cover each one. Tests build a SyncCoordinator and write directly to its
// currentSyncPeer field (matching the pattern in sync_coordinator_test.go).

func newSyncSkipTestServer(t *testing.T, fsm blockchain_api.FSMStateType, syncPeer peer.ID, syncPeerHeight uint32) *Server {
	t.Helper()

	tSettings := createBaseTestSettings()
	registry := NewPeerRegistry()
	if syncPeer != "" && syncPeerHeight > 0 {
		registry.Put(syncPeer, "", syncPeerHeight, nil, "")
	}

	mockBC := new(blockchain.Mock)
	state := fsm
	mockBC.On("GetFSMCurrentState", mock.Anything).Return(&state, nil).Maybe()

	selector := NewPeerSelector(ulogger.New("test"), tSettings)
	banManager := NewPeerBanManager(context.Background(), nil, tSettings, registry)

	sc := NewSyncCoordinator(
		ulogger.New("test"),
		tSettings,
		registry,
		selector,
		banManager,
		mockBC,
		nil,
	)
	if syncPeer != "" {
		sc.mu.Lock()
		sc.currentSyncPeer = syncPeer
		sc.mu.Unlock()
	}

	return &Server{
		logger:           ulogger.New("test"),
		settings:         tSettings,
		blockchainClient: mockBC,
		peerRegistry:     registry,
		syncCoordinator:  sc,
		gCtx:             context.Background(),
	}
}

func TestShouldSkipDuringSync_NoSyncPeer(t *testing.T) {
	// syncCoordinator nil → getSyncPeer returns "" → function exits before
	// touching the blockchain client.
	server := &Server{
		logger:           ulogger.New("test"),
		settings:         createBaseTestSettings(),
		blockchainClient: nil,
		peerRegistry:     NewPeerRegistry(),
		gCtx:             context.Background(),
	}

	skip := server.shouldSkipDuringSync("from", "originator", 100, "block")
	assert.False(t, skip, "no sync peer must not skip")
}

func TestShouldSkipDuringSync_NotSyncing(t *testing.T) {
	// Sync peer is set but FSM reports RUNNING — we're caught up, so the
	// announcement should pass through.
	syncPeer := peer.ID("sync-peer")
	server := newSyncSkipTestServer(t, blockchain_api.FSMStateType_RUNNING, syncPeer, 10)

	skip := server.shouldSkipDuringSync("from", "originator", 100, "block")
	assert.False(t, skip, "RUNNING FSM must not skip")
}

func TestShouldSkipDuringSync_BelowSyncPeerHeight(t *testing.T) {
	// Syncing and announcement is older than where the sync peer already is.
	syncPeer := peer.ID("sync-peer-1")
	server := newSyncSkipTestServer(t, blockchain_api.FSMStateType_CATCHINGBLOCKS, syncPeer, 100)

	skip := server.shouldSkipDuringSync("from", syncPeer.String(), 50, "block")
	assert.True(t, skip, "announcement below sync peer height must skip")
}

func TestShouldSkipDuringSync_NotFromSyncPeer(t *testing.T) {
	// Syncing, height ok, but originator is not the sync peer.
	syncPeer := peer.ID("sync-peer-2")
	server := newSyncSkipTestServer(t, blockchain_api.FSMStateType_CATCHINGBLOCKS, syncPeer, 10)

	_, pub, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
	require.NoError(t, err)
	otherPeer, err := peer.IDFromPublicKey(pub)
	require.NoError(t, err)

	skip := server.shouldSkipDuringSync("from", otherPeer.String(), 100, "block")
	assert.True(t, skip, "announcement from non-sync peer must skip")
}

func TestShouldSkipDuringSync_InvalidOriginator(t *testing.T) {
	// Syncing, height ok, but originatorPeerID doesn't decode. Falls into the
	// same "not from sync peer" branch via the err != nil short-circuit.
	syncPeer := peer.ID("sync-peer-3")
	server := newSyncSkipTestServer(t, blockchain_api.FSMStateType_LEGACYSYNCING, syncPeer, 10)

	skip := server.shouldSkipDuringSync("from", "not-a-valid-peer-id", 100, "block")
	assert.True(t, skip, "undecodable originator must skip")
}

func TestShouldSkipDuringSync_FromSyncPeer(t *testing.T) {
	// All gates pass: sync peer set, FSM CATCHINGBLOCKS, height ok, originator
	// is the sync peer. Announcement is allowed through.
	_, pub, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
	require.NoError(t, err)
	syncPeer, err := peer.IDFromPublicKey(pub)
	require.NoError(t, err)

	server := newSyncSkipTestServer(t, blockchain_api.FSMStateType_CATCHINGBLOCKS, syncPeer, 10)

	skip := server.shouldSkipDuringSync("from", syncPeer.String(), 100, "block")
	assert.False(t, skip, "announcement from sync peer at higher height must not skip")
}

// --- handleBanEvent / disconnectBannedPeerByID branch coverage ---
//
// handleBanEvent dispatches BanEvents from the BanList; only "add" actions
// with a non-empty PeerID lead to a disconnect. disconnectBannedPeerByID
// iterates the connected-peers list and removes the peer if found.

func TestHandleBanEvent_NonAddAction(t *testing.T) {
	// "remove" / unset / anything other than banActionAdd should return
	// before parsing the PeerID — so even an empty event is a no-op.
	server := &Server{logger: ulogger.New("test")}

	server.handleBanEvent(context.Background(), BanEvent{Action: "remove", PeerID: "anything"})
	// Reaching this point without a panic confirms the early return.
}

func TestHandleBanEvent_EmptyPeerID(t *testing.T) {
	// PeerID-only banning: an "add" event without a PeerID is logged and
	// dropped, never reaching peer.Decode.
	server := &Server{logger: ulogger.New("test")}

	server.handleBanEvent(context.Background(), BanEvent{Action: banActionAdd, PeerID: ""})
}

func TestHandleBanEvent_InvalidPeerID(t *testing.T) {
	// peer.Decode failure short-circuits before disconnectBannedPeerByID,
	// so P2PClient can be nil — if disconnect ran, GetPeers would panic.
	server := &Server{logger: ulogger.New("test")}

	server.handleBanEvent(context.Background(), BanEvent{
		Action: banActionAdd,
		PeerID: "not-a-real-peer-id",
		Reason: "test",
	})
}

func TestHandleBanEvent_ValidPeerIDDispatchesDisconnect(t *testing.T) {
	// End-to-end happy path: handleBanEvent decodes the PeerID and reaches
	// disconnectBannedPeerByID, which finds the peer in GetPeers and removes
	// it from the registry.
	_, pub, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
	require.NoError(t, err)
	bannedPeer, err := peer.IDFromPublicKey(pub)
	require.NoError(t, err)

	mockP2P := new(MockServerP2PClient)
	mockP2P.peers = []p2pMessageBus.PeerInfo{{ID: bannedPeer.String()}}

	registry := NewPeerRegistry()
	registry.Put(bannedPeer, "", 0, nil, "")

	server := &Server{
		logger:       ulogger.New("test"),
		P2PClient:    mockP2P,
		peerRegistry: registry,
	}

	server.handleBanEvent(context.Background(), BanEvent{
		Action: banActionAdd,
		PeerID: bannedPeer.String(),
		Reason: "spam",
	})

	_, exists := registry.Get(bannedPeer)
	assert.False(t, exists, "banned peer should be removed from the registry after dispatch")
}

func TestDisconnectBannedPeerByID_PeerFound(t *testing.T) {
	// Direct test of the "peer in connected list" path: the registry entry
	// should be cleared.
	_, pub, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
	require.NoError(t, err)
	bannedPeer, err := peer.IDFromPublicKey(pub)
	require.NoError(t, err)

	mockP2P := new(MockServerP2PClient)
	mockP2P.peers = []p2pMessageBus.PeerInfo{
		{ID: "some-other-peer"},
		{ID: bannedPeer.String()},
	}

	registry := NewPeerRegistry()
	registry.Put(bannedPeer, "", 0, nil, "")

	server := &Server{
		logger:       ulogger.New("test"),
		P2PClient:    mockP2P,
		peerRegistry: registry,
	}

	server.disconnectBannedPeerByID(context.Background(), bannedPeer, "manual")

	_, exists := registry.Get(bannedPeer)
	assert.False(t, exists, "found peer must be removed from registry")
}

func TestDisconnectBannedPeerByID_PeerNotFound(t *testing.T) {
	// Peer is not in the connected list → debug log, no registry mutation.
	_, pub, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
	require.NoError(t, err)
	bannedPeer, err := peer.IDFromPublicKey(pub)
	require.NoError(t, err)

	mockP2P := new(MockServerP2PClient)
	mockP2P.peers = []p2pMessageBus.PeerInfo{{ID: "unrelated-peer"}}

	registry := NewPeerRegistry()
	registry.Put(bannedPeer, "", 0, nil, "")

	server := &Server{
		logger:       ulogger.New("test"),
		P2PClient:    mockP2P,
		peerRegistry: registry,
	}

	server.disconnectBannedPeerByID(context.Background(), bannedPeer, "manual")

	_, exists := registry.Get(bannedPeer)
	assert.True(t, exists, "untracked peer must not affect registry")
}

// --- startPeerRegistryCleanup tests ---

func TestStartPeerRegistryCleanup_NilRegistryReturnsEarly(t *testing.T) {
	server := &Server{
		logger:   ulogger.New("test"),
		settings: createBaseTestSettings(),
	}

	server.startPeerRegistryCleanup(context.Background())

	// No timer should be created when peerRegistry is nil — otherwise we would
	// leak a ticker on every test setup that omits the registry.
	assert.Nil(t, server.peerRegistryCleanupTimer)
}

func TestStartPeerRegistryCleanup_TickEvictsStalePeer(t *testing.T) {
	registry := NewPeerRegistry()

	stale := peer.ID("stale-peer-1")
	registry.Put(stale, "", 0, nil, "")
	registry.peers[stale].LastMessageTime = time.Now().Add(-2 * time.Hour)

	tSettings := createBaseTestSettings()
	tSettings.P2P.PeerRegistryCleanupInterval = 10 * time.Millisecond
	tSettings.P2P.PeerRegistryTTL = time.Hour
	tSettings.P2P.PeerRegistryMaxSize = 100

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server := &Server{
		logger:       ulogger.New("test"),
		settings:     tSettings,
		peerRegistry: registry,
	}
	server.startPeerRegistryCleanup(ctx)
	require.NotNil(t, server.peerRegistryCleanupTimer)
	defer server.peerRegistryCleanupTimer.Stop()

	require.Eventually(t, func() bool {
		return registry.PeerCount() == 0
	}, 2*time.Second, 10*time.Millisecond, "stale peer should be evicted by ticker")
}

func TestStartPeerRegistryCleanup_DefaultsAppliedWhenSettingsZero(t *testing.T) {
	registry := NewPeerRegistry()

	tSettings := createBaseTestSettings()
	// Both interval and ttl left at zero so the default fill-in branches run.
	tSettings.P2P.PeerRegistryCleanupInterval = 0
	tSettings.P2P.PeerRegistryTTL = 0
	tSettings.P2P.PeerRegistryMaxSize = 0

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server := &Server{
		logger:       ulogger.New("test"),
		settings:     tSettings,
		peerRegistry: registry,
	}
	server.startPeerRegistryCleanup(ctx)
	require.NotNil(t, server.peerRegistryCleanupTimer)
	server.peerRegistryCleanupTimer.Stop()
}

func TestStartPeerRegistryCleanup_CancelStopsGoroutine(t *testing.T) {
	registry := NewPeerRegistry()

	tSettings := createBaseTestSettings()
	tSettings.P2P.PeerRegistryCleanupInterval = 10 * time.Millisecond
	tSettings.P2P.PeerRegistryTTL = time.Hour
	tSettings.P2P.PeerRegistryMaxSize = 100

	ctx, cancel := context.WithCancel(context.Background())

	server := &Server{
		logger:       ulogger.New("test"),
		settings:     tSettings,
		peerRegistry: registry,
	}
	server.startPeerRegistryCleanup(ctx)
	require.NotNil(t, server.peerRegistryCleanupTimer)

	// Let the ticker fire at least once before cancelling, so we exercise both
	// select cases in the goroutine.
	time.Sleep(30 * time.Millisecond)

	cancel()
	server.peerRegistryCleanupTimer.Stop()

	// Goroutine exit isn't directly observable, but `go test -race` will surface
	// any ordering bug when paired with the explicit ticker stop.
	time.Sleep(20 * time.Millisecond)
}

func TestStartPeerRegistryCleanup_ExemptSaturationLogged(t *testing.T) {
	// Drives the post-Cleanup Warn path: exempt count alone exceeds maxSize so
	// the registry stays over-cap after a tick. We can't intercept the logger
	// without a custom shim, so we just confirm: (a) the ticker fires, (b) no
	// non-exempt entries remain, (c) the exempt entries are still there. The
	// Warn statement runs as a side effect of (b) + (c).
	registry := NewPeerRegistry()

	exempt := GenerateTestPeerIDs(3)
	for _, id := range exempt {
		registry.Put(id, "", 0, nil, "")
		registry.UpdateConnectionState(id, true)
	}
	stale := peer.ID("stale-peer")
	registry.Put(stale, "", 0, nil, "")
	registry.peers[stale].LastMessageTime = time.Now()

	tSettings := createBaseTestSettings()
	tSettings.P2P.PeerRegistryCleanupInterval = 10 * time.Millisecond
	tSettings.P2P.PeerRegistryTTL = time.Hour
	tSettings.P2P.PeerRegistryMaxSize = 2 // below exempt count of 3

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server := &Server{
		logger:       ulogger.New("test"),
		settings:     tSettings,
		peerRegistry: registry,
	}
	server.startPeerRegistryCleanup(ctx)
	require.NotNil(t, server.peerRegistryCleanupTimer)
	defer server.peerRegistryCleanupTimer.Stop()

	require.Eventually(t, func() bool {
		// Stale evicted, all three exempts retained, registry sits at 3 (> 2).
		return registry.PeerCount() == 3
	}, 2*time.Second, 10*time.Millisecond, "exempt-only registry should hold at exempt count")

	for _, id := range exempt {
		_, ok := registry.Get(id)
		assert.True(t, ok, "exempt peer must remain")
	}
	_, ok := registry.Get(stale)
	assert.False(t, ok, "stale non-exempt peer must be evicted")
}
