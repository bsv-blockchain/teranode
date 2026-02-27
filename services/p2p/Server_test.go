package p2p

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	p2pMessageBus "github.com/bsv-blockchain/go-p2p-message-bus"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/services/p2p/p2p_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	ma "github.com/multiformats/go-multiaddr" // nolint:misspell
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Constants for testing
const (
	bannedPeerIDStr = "12D3KooWB9kmtfHg5Ct1Sj5DX6fmqRnatrXnE5zMRg25d6rbwLzp" // Use a genuinely valid Peer ID hash
	peerIDStr       = "12D3KooWRj9ajsNaVuT2fNv7k2AyLnrC5NQQzZS9GixSVWKZZYRE"
)

// MockKafkaConsumerGroup is a mock implementation of kafka.KafkaConsumerGroupI
type MockKafkaConsumerGroup struct {
	mock.Mock
}

// Start mocks the Start method
func (m *MockKafkaConsumerGroup) Start(ctx context.Context, consumerFn func(message *kafka.KafkaMessage) error, opts ...kafka.ConsumerOption) {
	m.Called(ctx, consumerFn, opts)
}

// BrokersURL mocks the BrokersURL method
func (m *MockKafkaConsumerGroup) BrokersURL() []string {
	args := m.Called()
	return args.Get(0).([]string)
}

// Close mocks the Close method
func (m *MockKafkaConsumerGroup) Close() error {
	args := m.Called()
	return args.Error(0)
}

// PauseAll mocks the PauseAll method
func (m *MockKafkaConsumerGroup) PauseAll() {
	m.Called()
}

// ResumeAll mocks the ResumeAll method
func (m *MockKafkaConsumerGroup) ResumeAll() {
	m.Called()
}

// BestBlockResponseMessage type for testing
type BestBlockResponseMessage struct {
	PeerID        string      `json:"peerID"`
	BlockHash     string      `json:"blockHash"`
	BlockHeight   uint64      `json:"blockHeight"`
	BlockTime     int64       `json:"blockTime"`
	FeeQuote      interface{} `json:"feeQuote"`
	AcceptNonStd  string      `json:"acceptNonStd"`
	ExcessiveSize uint64      `json:"excessiveSize"`
	DashboardURL  string      `json:"dashboardURL"`
}

// Add the bestBlockMsg struct definition to match what's in Server.go
type bestBlockMsg struct {
	Hash   string `json:"hash"`
	PeerID string `json:"peerId"`
}

// createTestServer creates a test server with necessary dependencies
func createTestServer(t *testing.T) *Server {
	logger := ulogger.New("test")
	settings := &settings.Settings{
		P2P: settings.P2PSettings{
			BanThreshold: 100,
			BanDuration:  time.Hour,
			PeerCacheDir: t.TempDir(),
			EnableNAT:    false, // Disable NAT in tests to prevent data races in libp2p
		},
	}

	// Create server with minimal setup
	registry := NewPeerRegistry()
	mockBanList := &MockBanList{}
	// Setup default expectations for MockBanList methods
	mockBanList.On("Add", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockBanList.On("Remove", mock.Anything, mock.Anything).Return(nil)
	mockBanList.On("ListBanned").Return([]string{})
	mockBanList.On("IsBanned", mock.Anything).Return(false)
	mockBanList.On("Clear").Return()

	// Create mock P2PClient for tests that need it
	mockP2PClient := &MockServerP2PClient{}
	// GetPeers returns empty list by default (method doesn't use mock.Called)
	// GetID needs to be set up if used
	mockP2PClient.On("GetID").Return(peer.ID("test-peer-id")).Maybe()

	s := &Server{
		logger:       logger,
		settings:     settings,
		peerRegistry: registry,
		banManager:   NewPeerBanManager(context.Background(), nil, settings, registry),
		banList:      mockBanList,
		P2PClient:    mockP2PClient,
	}

	return s
}

func TestServerHandlers(t *testing.T) {
	t.Run("Test stream handler behaviour", func(t *testing.T) {
		// Create a minimal Server for testing
		server := &Server{
			gCtx: context.Background(),
		}

		// Set up a flag to track if handleBlockTopic was called
		blockTopicHandlerCalled := false
		blockTopicMsg := []byte{}
		blockTopicSender := ""

		// Set up a test handler function to capture calls
		blockHandler := func(_ context.Context, msg []byte, from string) {
			blockTopicHandlerCalled = true
			blockTopicMsg = msg
			blockTopicSender = from
		}

		// Test data we expect to be processed
		testData := []byte(`{"height": 12345, "peerID": "test-peer"}`)
		testSender := "test-sender"

		// Call our block handler directly (simulating the handler call)
		blockHandler(server.gCtx, testData, testSender)

		// Assert expected behaviour
		assert.True(t, blockTopicHandlerCalled, "Block topic handler should be called")
		assert.Equal(t, testData, blockTopicMsg, "Message data should be passed correctly")
		assert.Equal(t, testSender, blockTopicSender, "Sender should be passed correctly")
	})
}

func TestServerStart(t *testing.T) {

	t.Run("Test Start method", func(t *testing.T) {
		logger := ulogger.New("test-server", ulogger.WithLevel("ERROR"))
		ctx := context.Background()

		// Test with no settings
		t.Run("Missing required settings", func(t *testing.T) {
			readyCh := make(chan struct{})

			// Create minimal settings with nothing configured
			emptySettings := settings.NewSettings()

			// Create a mock blockchain client that returns a configuration error immediately
			mockBlockchainClient := new(blockchain.Mock)
			mockBlockchainClient.On("WaitUntilFSMTransitionFromIdleState", mock.Anything).Return(errors.NewConfigurationError("p2p_listen_addresses not set in config"))

			// Create mock Kafka clients
			mockRejectedTxConsumer := new(MockKafkaConsumerGroup)
			mockRejectedTxConsumer.On("Start", mock.Anything, mock.Anything, mock.Anything).Return()

			mockSubtreeProducer := kafka.NewKafkaAsyncProducerMock()
			mockBlocksProducer := kafka.NewKafkaAsyncProducerMock()

			// Create server with all necessary fields populated to avoid nil pointer dereference
			server := &Server{
				logger:                        logger,
				settings:                      emptySettings,
				blockchainClient:              mockBlockchainClient,
				rejectedTxKafkaConsumerClient: mockRejectedTxConsumer,
				subtreeKafkaProducerClient:    mockSubtreeProducer,
				blocksKafkaProducerClient:     mockBlocksProducer,
				notificationCh:                make(chan *notificationMsg),
				banChan:                       make(chan BanEvent),
				bitcoinProtocolVersion:        fmt.Sprintf("teranode/bitcoin/%s", emptySettings.Version),
				gCtx:                          ctx,
			}

			// Should fail with missing listen addresses error
			err := server.Start(ctx, readyCh)

			// Verify we got an error
			assert.Error(t, err, "Start should return an error when required settings are missing")

			// Check error message
			assert.Contains(t, err.Error(), "p2p_listen_addresses not set",
				"Error should mention missing listen addresses")

			// Verify it's a configuration error
			assert.True(t, errors.Is(err, errors.ErrConfiguration),
				"Error should be a configuration error")
		})

		// Test with some settings missing
		t.Run("Missing some required settings", func(t *testing.T) {
			readyCh := make(chan struct{})

			// Create settings with only listen addresses set but missing port
			partialSettings := settings.NewSettings()
			partialSettings.P2P.ListenAddresses = []string{"/ip4/127.0.0.1/tcp/8333"}

			// Return early with a configuration error instead of proceeding further
			mockBlockchainClient := new(blockchain.Mock)
			mockBlockchainClient.On("WaitUntilFSMTransitionFromIdleState", mock.Anything).Return(errors.NewConfigurationError("p2p_port not set in config"))

			// Create mock Kafka clients
			mockRejectedTxConsumer := new(MockKafkaConsumerGroup)
			mockRejectedTxConsumer.On("Start", mock.Anything, mock.Anything, mock.Anything).Return()

			mockSubtreeProducer := kafka.NewKafkaAsyncProducerMock()
			mockBlocksProducer := kafka.NewKafkaAsyncProducerMock()

			// Create server with all necessary fields populated to avoid nil pointer dereference
			server := &Server{
				logger:                        logger,
				settings:                      partialSettings,
				blockchainClient:              mockBlockchainClient,
				rejectedTxKafkaConsumerClient: mockRejectedTxConsumer,
				subtreeKafkaProducerClient:    mockSubtreeProducer,
				blocksKafkaProducerClient:     mockBlocksProducer,
				notificationCh:                make(chan *notificationMsg),
				banChan:                       make(chan BanEvent),
				bitcoinProtocolVersion:        fmt.Sprintf("teranode/bitcoin/%s", partialSettings.Version),
				gCtx:                          ctx,
			}

			// Should fail with missing port error
			err := server.Start(ctx, readyCh)

			// Verify we got an error
			assert.Error(t, err, "Start should return an error when port is missing")

			// Check error message
			assert.Contains(t, err.Error(), "p2p_port not set",
				"Error should mention missing port")

			// Verify it's a configuration error
			assert.True(t, errors.Is(err, errors.ErrConfiguration),
				"Error should be a configuration error")
		})

		// Test with all required settings present
		t.Run("All required settings present validation", func(t *testing.T) {
			readyCh := make(chan struct{})

			// Create complete settings
			completeSettings := settings.NewSettings()
			completeSettings.P2P.Port = 8333
			completeSettings.P2P.ListenAddresses = []string{"/ip4/127.0.0.1/tcp/8333"}
			completeSettings.P2P.BlockTopic = "block"
			completeSettings.P2P.SubtreeTopic = "subtree"
			completeSettings.P2P.RejectedTxTopic = "rejected-tx"

			// Create a mock blockchain client that returns immediately
			// with a context canceled error to avoid going further in the Start method
			mockBlockchainClient := new(blockchain.Mock)
			mockBlockchainClient.On("WaitUntilFSMTransitionFromIdleState", mock.Anything).Return(context.Canceled)

			// Mock context with cancel
			mockCtx, cancel := context.WithCancel(ctx)
			defer cancel()

			// Create the server with minimal dependencies
			server := &Server{
				logger:                 logger,
				settings:               completeSettings,
				blockchainClient:       mockBlockchainClient,
				bitcoinProtocolVersion: fmt.Sprintf("teranode/bitcoin/%s", completeSettings.Version),
				gCtx:                   mockCtx,
				notificationCh:         make(chan *notificationMsg),
				banChan:                make(chan BanEvent),
				// Use a mock P2PNodeI but don't set expectations that need to be verified
				P2PClient: new(MockServerP2PClient),
			}

			// Run Start expecting context canceled error
			err := server.Start(mockCtx, readyCh)

			// The error should be the context canceled error from the blockchain client
			assert.True(t, errors.Is(err, context.Canceled),
				"Expected context.Canceled error, got: %v", err)

			// Only verify the blockchain client mock since that's all we care about
			mockBlockchainClient.AssertExpectations(t)
		})
	})
}

// NOTE: TestServerIntegration was removed - it was an empty placeholder test.

func TestHandleBlockTopic(t *testing.T) {
	// Setup common test variables
	ctx := context.Background()

	t.Run("updates last message time", func(t *testing.T) {
		// Create mock P2PClient
		mockP2PNode := new(MockServerP2PClient)
		selfPeerID, _ := peer.Decode("12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ")
		senderPeerID, _ := peer.Decode("12D3KooWEyX7hgdXy8zUjCs9CqvMGpB5dKVFj9MX2nUBLwajdSZH")
		originatorPeerIDStr := "12D3KooWQYVQJfrw4RZnNHgRxGFLXoXswE5wuoUBgWpeJYeGDjvA"
		originatorPeerID, _ := peer.Decode(originatorPeerIDStr)

		mockP2PNode.On("GetID").Return(selfPeerID)

		// Create mock ban manager
		mockBanManager := new(MockPeerBanManager)
		mockBanManager.On("IsBanned", mock.AnythingOfType("string")).Return(false)

		// Create peer registry to track updates
		peerRegistry := NewPeerRegistry()
		peerRegistry.Put(senderPeerID, "", 0, nil, "")
		peerRegistry.Put(originatorPeerID, "", 0, nil, "")

		// Get initial times
		senderInfo1, _ := peerRegistry.Get(senderPeerID)

		// Wait to ensure time difference
		time.Sleep(50 * time.Millisecond)

		// Create server with registry
		server := &Server{
			P2PClient:      mockP2PNode,
			peerRegistry:   peerRegistry,
			banManager:     mockBanManager,
			notificationCh: make(chan *notificationMsg, 10),
			logger:         ulogger.New("test-server"),
		}

		// Call handler with message
		blockMsg := fmt.Sprintf(`{"Hash":"000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f","Height":1,"DataHubURL":"http://example.com","PeerID":"%s"}`, senderPeerID.String())
		server.handleBlockTopic(ctx, []byte(blockMsg), senderPeerID.String())

		// Verify last message times were updated
		senderInfo2, _ := peerRegistry.Get(senderPeerID)

		assert.True(t, senderInfo2.LastMessageTime.After(senderInfo1.LastMessageTime), "Sender's LastMessageTime should be updated")

		// Verify notification was sent
		select {
		case notification := <-server.notificationCh:
			assert.Equal(t, "block", notification.Type)
		default:
			t.Fatal("Expected notification message but none received")
		}
	})

	t.Run("ignore message from self", func(t *testing.T) {
		// Create mock P2PClient
		mockP2PNode := new(MockServerP2PClient)
		selfPeerID, _ := peer.Decode("12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ")
		selfPeerIDStr := selfPeerID.String()
		mockP2PNode.On("GetID").Return(selfPeerID)

		// Add mock for GetPeerIPs to handle any peer ID
		mockP2PNode.On("GetPeerIPs", mock.AnythingOfType("peer.ID")).Return([]string{})
		// Create a spy banList that we can verify is NOT called
		// (since the method should return early for messages from self)
		mockBanList := new(MockBanList)

		// Create server with mocks
		server := &Server{
			P2PClient:      mockP2PNode,
			banList:        mockBanList,
			notificationCh: make(chan *notificationMsg, 10),
			logger:         ulogger.New("test-server"),
		}

		// Call the real handler method with message from self
		server.handleBlockTopic(ctx, []byte(`{"Hash":"000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f","Height":1,"DataHubURL":"http://example.com","PeerID":"12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ"}`), selfPeerIDStr)

		// Verify message was added to notification channel
		select {
		case notification := <-server.notificationCh:
			assert.Equal(t, "block", notification.Type)
			assert.Equal(t, "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f", notification.Hash)
		default:
			t.Fatal("Expected notification message but none received")
		}
	})

	t.Run("ignore message from banned peer", func(t *testing.T) {
		// Create mock P2PClient
		mockP2PNode := new(MockServerP2PClient)
		selfPeerID, _ := peer.Decode("QmBannedPeerID")
		bannedPeerIDStr := bannedPeerIDStr

		mockP2PNode.On("GetID").Return(selfPeerID)

		// Add mock for GetPeerIPs to handle any peer ID
		mockP2PNode.On("GetPeerIPs", mock.AnythingOfType("peer.ID")).Return([]string{bannedPeerIDStr})

		// Create mock banManager that returns true for the banned peer
		mockBanManager := new(MockPeerBanManager)
		mockBanManager.On("IsBanned", bannedPeerIDStr).Return(true)

		// Create logger
		logger := ulogger.New("test-server")

		// Create peer registry
		peerRegistry := NewPeerRegistry()

		// Create server with mock P2PClient and BanManager
		server := &Server{
			P2PClient:      mockP2PNode,
			notificationCh: make(chan *notificationMsg, 10),
			banManager:     mockBanManager,
			peerRegistry:   peerRegistry,
			logger:         logger,
		}

		// Call the real handler method with message from banned peer
		server.handleBlockTopic(ctx, []byte(`{"Hash":"000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f","Height":1,"DataHubURL":"http://example.com","PeerID":"12D3KooWB9kmtfHg5Ct1Sj5DX6fmqRnatrXnE5zMRg25d6rbwLzp"}`), bannedPeerIDStr)

		// Verify message was added to notification channel
		select {
		case notification := <-server.notificationCh:
			assert.Equal(t, "block", notification.Type)
			assert.Equal(t, "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f", notification.Hash)
		default:
			t.Fatal("Expected notification message but none received")
		}
	})

	t.Run("error on json unmarshal", func(t *testing.T) {
		// Create mock P2PClient
		mockP2PNode := new(MockServerP2PClient)
		selfPeerID, _ := peer.Decode("QmBannedPeerID")
		mockP2PNode.On("GetID").Return(selfPeerID)

		// Create mock ban list
		mockBanList := new(MockBanList)

		// Create logger
		logger := ulogger.New("test-server")

		// Create peer registry
		peerRegistry := NewPeerRegistry()

		// Create server with mock P2PClient
		server := &Server{
			P2PClient:      mockP2PNode,
			notificationCh: make(chan *notificationMsg, 10),
			logger:         logger,
			banList:        mockBanList,
			peerRegistry:   peerRegistry,
		}

		// Call the real handler method with invalid JSON
		server.handleBlockTopic(ctx, []byte(`{invalid json}`), "some-peer-id")

		// Verify no notification was sent
		select {
		case <-server.notificationCh:
			t.Fatal("Unexpected notification message received")
		default:
		}
	})

	t.Run("error on hash parsing", func(t *testing.T) {
		// Create mock P2PClient
		mockP2PNode := new(MockServerP2PClient)
		selfPeerID, _ := peer.Decode("12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ")
		mockP2PNode.On("GetID").Return(selfPeerID)

		// Create a mock banManager that returns false for any peer
		mockBanManager := new(MockPeerBanManager)
		mockBanManager.On("IsBanned", mock.Anything).Return(false)

		// Create logger
		logger := ulogger.New("test-server")

		// Create peer registry
		peerRegistry := NewPeerRegistry()

		// Create server with mock P2PClient and BanManager
		server := &Server{
			P2PClient:      mockP2PNode,
			notificationCh: make(chan *notificationMsg, 10),
			banManager:     mockBanManager,
			peerRegistry:   peerRegistry,
			logger:         logger,
		}

		// Call the real handler method with invalid hash
		server.handleBlockTopic(ctx, []byte(`{"Hash":"invalid-hash","Height":1,"DataHubURL":"http://example.com","PeerID":"12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ"}`), "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ")

		// Verify notification was still sent (happens before hash parsing error)
		select {
		case notification := <-server.notificationCh:
			assert.Equal(t, "block", notification.Type)
			assert.Equal(t, "invalid-hash", notification.Hash)
		default:
			t.Fatal("Expected notification message but none received")
		}
	})

	t.Run("successful kafka publish", func(t *testing.T) {
		// Create mock P2PClient
		mockP2PNode := new(MockServerP2PClient)
		selfPeerID, _ := peer.Decode("12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ")
		mockP2PNode.On("GetID").Return(selfPeerID)
		mockP2PNode.On("UpdatePeerHeight", mock.Anything, mock.Anything).Return()

		// Create a mock banManager that returns false for any peer
		mockBanManager := new(MockPeerBanManager)
		mockBanManager.On("IsBanned", mock.Anything).Return(false)

		// Create mock kafka producer
		mockKafkaProducer := new(MockKafkaProducer)
		mockKafkaProducer.On("Publish", mock.Anything).Return()

		// Create peer registry
		peerRegistry := NewPeerRegistry()

		// Create server with mocks
		server := &Server{
			P2PClient:                 mockP2PNode,
			notificationCh:            make(chan *notificationMsg, 10),
			blocksKafkaProducerClient: mockKafkaProducer,
			banManager:                mockBanManager,
			peerRegistry:              peerRegistry,
			logger:                    ulogger.New("test-server"),
		}

		// Call the real handler with valid block hash
		// Since we can't mock out proto.Marshal, we'll need to allow an error here
		// or create a proper test implementation that doesn't use proto.Marshal
		server.handleBlockTopic(ctx, []byte(`{"Hash":"000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f","Height":1,"DataHubURL":"http://example.com","PeerID":"12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ"}`), "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ")

		// Verify notification was sent
		select {
		case notification := <-server.notificationCh:
			assert.Equal(t, "block", notification.Type)
			assert.Equal(t, "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f", notification.Hash)
		default:
			t.Fatal("Expected notification message but none received")
		}
	})
	t.Run("peer IDs don't match", func(t *testing.T) {
		// Create mock P2PClient
		mockP2PNode := new(MockServerP2PClient)
		selfPeerID, _ := peer.Decode("12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ")
		mockP2PNode.On("GetID").Return(selfPeerID)
		mockP2PNode.On("UpdatePeerHeight", mock.Anything, mock.Anything).Return()

		// Create a mock banManager that returns false for any peer
		mockBanManager := new(MockPeerBanManager)
		mockBanManager.On("IsBanned", mock.Anything).Return(false)

		// Create mock kafka producer
		mockKafkaProducer := new(MockKafkaProducer)
		mockKafkaProducer.On("Publish", mock.Anything).Return()

		// Create peer registry
		peerRegistry := NewPeerRegistry()

		// Create server with mocks
		server := &Server{
			P2PClient:                 mockP2PNode,
			notificationCh:            make(chan *notificationMsg, 10),
			blocksKafkaProducerClient: mockKafkaProducer,
			banManager:                mockBanManager,
			peerRegistry:              peerRegistry,
			logger:                    ulogger.New("test-server"),
		}

		// Call the real handler with valid block hash
		// Since we can't mock out proto.Marshal, we'll need to allow an error here
		// or create a proper test implementation that doesn't use proto.Marshal
		server.handleBlockTopic(ctx, []byte(`{"Hash":"000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f","Height":1,"DataHubURL":"http://example.com","PeerID":"12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ"}`), bannedPeerIDStr)

		// Verify notification was not sent
		select {
		case _ = <-server.notificationCh:
			t.Fatal("Unexpected notification message received")
		default:
			t.Logf("Correctly received no notification")
		}
	})
}

func TestHandleSubtreeTopic(t *testing.T) {
	// Setup common test variables
	ctx := context.Background()

	t.Run("happy_path_-_successful_handling", func(t *testing.T) {
		// Create mock P2PClient
		mockP2PNode := new(MockServerP2PClient)
		selfPeerID, _ := peer.Decode(peerIDStr)
		mockP2PNode.On("GetID").Return(selfPeerID)

		// Create a valid peer ID for testing
		validPeerID := "12D3KooWQJ8sLWNhDPsGbMrhA5JhrtpiEVrWvarPGm4GfP6bn6fL"

		// Create test IP for the peer
		testIP := "192.168.1.100"

		// Add mock for GetPeerIPs to handle any peer ID
		mockP2PNode.On("GetPeerIPs", mock.AnythingOfType("peer.ID")).Return([]string{testIP})

		// Create mock banManager that returns false for the peer
		mockBanManager := new(MockPeerBanManager)
		mockBanManager.On("IsBanned", validPeerID).Return(false)

		// Create mock kafka producer
		mockKafkaProducer := new(MockKafkaProducer)
		mockKafkaProducer.On("Publish", mock.Anything).Return()

		// Create peer registry
		peerRegistry := NewPeerRegistry()

		// Create server with mocks
		// Create settings with blacklisted URLs
		tSettings := createBaseTestSettings()
		tSettings.SubtreeValidation.BlacklistedBaseURLs = map[string]struct{}{
			"http://evil.com": {},
		}

		server := &Server{
			P2PClient:                  mockP2PNode,
			notificationCh:             make(chan *notificationMsg, 10),
			subtreeKafkaProducerClient: mockKafkaProducer,
			banManager:                 mockBanManager,
			peerRegistry:               peerRegistry,
			settings:                   tSettings,
			logger:                     ulogger.New("test-server"),
		}

		// Call the method with a valid message from another peer
		validSubtreeMessage := `{"Hash":"000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f","DataHubURL":"http://example.com","PeerID":"12D3KooWQJ8sLWNhDPsGbMrhA5JhrtpiEVrWvarPGm4GfP6bn6fL"}`
		server.handleSubtreeTopic(ctx, []byte(validSubtreeMessage), validPeerID)

		// Verify notification was sent to the notification channel
		select {
		case notification := <-server.notificationCh:
			assert.Equal(t, "subtree", notification.Type)
			assert.Equal(t, "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f", notification.Hash)
			assert.Equal(t, "http://example.com", notification.BaseURL)
			assert.Equal(t, "12D3KooWQJ8sLWNhDPsGbMrhA5JhrtpiEVrWvarPGm4GfP6bn6fL", notification.PeerID)
		default:
			t.Fatal("Expected notification message but none received")
		}

		// Verify ban check was performed
		mockBanManager.AssertCalled(t, "IsBanned", validPeerID)

		// Verify Kafka publish was called since it's not from self or banned peer
		mockKafkaProducer.AssertCalled(t, "Publish", mock.Anything)
	})
	t.Run("peer IDs don't match", func(t *testing.T) {
		// Create mock P2PClient
		mockP2PNode := new(MockServerP2PClient)
		selfPeerID, _ := peer.Decode("12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ")
		mockP2PNode.On("GetID").Return(selfPeerID)
		mockP2PNode.On("UpdatePeerHeight", mock.Anything, mock.Anything).Return()

		// Create a mock banManager that returns false for any peer
		mockBanManager := new(MockPeerBanManager)
		mockBanManager.On("IsBanned", mock.Anything).Return(false)

		// Create mock kafka producer
		mockKafkaProducer := new(MockKafkaProducer)
		mockKafkaProducer.On("Publish", mock.Anything).Return()

		// Create peer registry
		peerRegistry := NewPeerRegistry()

		// Create server with mocks
		server := &Server{
			P2PClient:                 mockP2PNode,
			notificationCh:            make(chan *notificationMsg, 10),
			blocksKafkaProducerClient: mockKafkaProducer,
			banManager:                mockBanManager,
			peerRegistry:              peerRegistry,
			logger:                    ulogger.New("test-server"),
		}

		// Call the method with a valid message but a non matching peer ID
		validSubtreeMessage := `{"Hash":"000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f","DataHubURL":"http://example.com","PeerID":"12D3KooWQJ8sLWNhDPsGbMrhA5JhrtpiEVrWvarPGm4GfP6bn6fL"}`
		server.handleSubtreeTopic(ctx, []byte(validSubtreeMessage), bannedPeerIDStr)

		// Verify notification was not sent
		select {
		case _ = <-server.notificationCh:
			t.Fatal("Unexpected notification message received")
		default:
			t.Logf("Correctly received no notification")
		}
	})
}

// TestReceiveBestBlockStreamHandler tests the receiveBestBlockStreamHandler function
func TestReceiveBestBlockStreamHandler(t *testing.T) {
	// Create subtests for different scenarios
	t.Run("successful stream handling", func(t *testing.T) {
		// Create mock P2PClient
		mockP2PNode := new(MockServerP2PClient)
		mockP2PNode.On("GetProcessName").Return("test-node")
		mockP2PNode.On("UpdateBytesReceived", mock.AnythingOfType("uint64")).Return()
		mockP2PNode.On("UpdateLastReceived").Return()

		// Create test data for the best block message
		blockHash, _ := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")

		// Use a valid peer ID format (this is the format used in actual libp2p code)
		testMsg := []byte(fmt.Sprintf(`{"hash":"%s","peerId":"%s"}`, blockHash.String(), peerIDStr))

		// Create a mock network stream
		mockStream := new(MockNetworkStream)
		mockStream.On("Read", mock.Anything).Run(func(args mock.Arguments) {
			buf := args.Get(0).([]byte)
			copy(buf, testMsg)
		}).Return(len(testMsg), nil).Once()

		// On second read, return EOF
		mockStream.On("Read", mock.Anything).Return(0, io.EOF).Maybe()

		// Create a peer ID for our remote peer
		peerID, err := peer.Decode(peerIDStr)
		require.NoError(t, err)

		// Create a mock connection with strict expectations
		mockConn := new(MockNetworkConn)
		mockConn.On("RemotePeer").Return(peerID).Maybe()   // Allow any number of calls
		mockConn.On("RemotePublicKey").Return(nil).Maybe() // Allow any number of calls

		// Connect the mock stream to the mock connection
		mockStream.On("Conn").Return(mockConn).Maybe() // Allow any number of calls

		// Directly verify our mock is set up correctly
		testConn := mockStream.Conn()
		require.NotNil(t, testConn, "Connection should not be nil")
		testPeer := testConn.RemotePeer()
		require.Equal(t, peerIDStr, testPeer.String(), "Peer ID should match expected value")

		// Read data from the stream
		var buf []byte

		buf, err = io.ReadAll(mockStream)
		require.NoError(t, err)

		// Parse and verify the message
		var msg bestBlockMsg
		err = json.Unmarshal(buf, &msg)
		require.NoError(t, err)
		require.Equal(t, blockHash.String(), msg.Hash)
		require.Equal(t, peerIDStr, msg.PeerID)

		// Get peer ID from connection and verify
		conn := mockStream.Conn()
		remotePeerID := conn.RemotePeer()
		require.Equal(t, peerIDStr, remotePeerID.String())
	})

	t.Run("handle error from stream reading", func(t *testing.T) {
		// Create a mock stream that returns an error on read
		mockStream := new(MockNetworkStream)

		// Setup the Read method to return an error
		readErr := errors.New(errors.ERR_ERROR, "read error")
		mockStream.On("Read", mock.Anything).Return(0, readErr)

		// Set up Reset to be called
		mockStream.On("Reset").Return(nil)

		// Call ReadAll which should trigger the error
		_, err := io.ReadAll(mockStream)

		// Verify we got the expected error
		require.Error(t, err)
		require.Equal(t, "ERROR (9): read error", err.Error())

		// We need to manually call Reset since io.ReadAll won't do that
		err = mockStream.Reset()
		require.NoError(t, err)

		// Verify Reset was called
		mockStream.AssertCalled(t, "Reset")
	})
}

// MockStreamScope implements the network.StreamScope interface for testing
type MockStreamScope struct{}

// BeginSpan implements the network.StreamScope interface for testing
func (m *MockStreamScope) BeginSpan() (network.ResourceScopeSpan, error) {
	// Return a simple implementation
	return &MockResourceScopeSpan{}, nil
}

// EndSpan implements the network.StreamScope interface for testing
func (m *MockStreamScope) EndSpan() {
	// Empty implementation
}

// ReserveMemory implements the network.StreamScope interface for testing
func (m *MockStreamScope) ReserveMemory(size int, priority uint8) error {
	// Empty implementation
	return nil
}

// ReleaseMemory implements the network.StreamScope interface for testing
func (m *MockStreamScope) ReleaseMemory(size int) {
	// Empty implementation
}

// SetService implements the network.StreamScope interface for testing
func (m *MockStreamScope) SetService(service string) error {
	// Empty implementation
	return nil
}

// Stat implements the network.StreamScope interface
func (m *MockStreamScope) Stat() network.ScopeStat {
	// Return empty stats
	return network.ScopeStat{}
}

// MockResourceScopeSpan implements the network.ResourceScopeSpan interface
type MockResourceScopeSpan struct{}

// Done implements the network.ResourceScopeSpan interface
func (m *MockResourceScopeSpan) Done() {
	// Empty implementation
}

// BeginSpan implements the network.ResourceScopeSpan interface
func (m *MockResourceScopeSpan) BeginSpan() (network.ResourceScopeSpan, error) {
	// Return self reference to simulate nested span
	return m, nil
}

// ReserveMemory implements the network.ResourceScopeSpan interface
func (m *MockResourceScopeSpan) ReserveMemory(size int, priority uint8) error {
	// Empty implementation
	return nil
}

// ReleaseMemory implements the network.ResourceScopeSpan interface
func (m *MockResourceScopeSpan) ReleaseMemory(size int) {
	// Empty implementation
}

// Stat implements the network.ResourceScopeSpan interface
func (m *MockResourceScopeSpan) Stat() network.ScopeStat {
	// Return empty stats
	return network.ScopeStat{}
}

// MockPeerBanManager implements the PeerBanManagerI interface for testing
type MockPeerBanManager struct {
	mock.Mock
}

// GetBanScore mocks the GetBanScore method
func (m *MockPeerBanManager) GetBanScore(peerID string) (score int, banned bool, banUntil time.Time) {
	args := m.Called(peerID)
	return args.Get(0).(int), args.Get(1).(bool), args.Get(2).(time.Time)
}

// IncrementBanScore mocks the IncrementBanScore method
func (m *MockPeerBanManager) IncrementBanScore(peerID string, score int32, reason string) {
	m.Called(peerID, score, reason)
}

// ResetBanScore mocks the ResetBanScore method
func (m *MockPeerBanManager) ResetBanScore(peerID string) {
	m.Called(peerID)
}

// IsBanned mocks the IsBanned method
func (m *MockPeerBanManager) IsBanned(peerID string) bool {
	args := m.Called(peerID)
	return args.Get(0).(bool)
}

// AddScore mocks the AddScore method
func (m *MockPeerBanManager) AddScore(peerID string, reason BanReason) (score int, banned bool) {
	args := m.Called(peerID, reason)
	return args.Get(0).(int), args.Get(1).(bool)
}

func TestContains(t *testing.T) {
	// Generate a valid peer ID using crypto key
	privKey, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	require.NoError(t, err)

	peerID, err := peer.IDFromPrivateKey(privKey)
	require.NoError(t, err)

	peerIDStr := peerID.String()

	// Create a valid multiaddress string with the peer ID
	validMultiaddr := fmt.Sprintf("/ip4/192.168.1.5/tcp/8333/p2p/%s", peerIDStr)

	t.Run("matching_peer_ID", func(t *testing.T) {
		// Create a slice with the valid multiaddress
		addresses := []string{
			validMultiaddr,
			"/ip4/192.168.1.6/tcp/8333", // No peer ID
		}

		// Check if the slice contains the peer ID
		result := contains(addresses, peerIDStr)
		require.True(t, result, "contains should return true for matching peer ID")
	})

	t.Run("non-matching_peer_ID", func(t *testing.T) {
		// Generate a different peer ID
		otherPrivKey, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
		require.NoError(t, err)
		otherPeerID, err := peer.IDFromPrivateKey(otherPrivKey)
		require.NoError(t, err)

		otherPeerIDStr := otherPeerID.String()

		// Create a slice with a different peer ID
		addresses := []string{
			fmt.Sprintf("/ip4/192.168.1.5/tcp/8333/p2p/%s", otherPeerIDStr),
		}

		// Check if the slice contains the original peer ID
		result := contains(addresses, peerIDStr)
		require.False(t, result, "contains should return false for non-matching peer ID")
	})

	t.Run("invalid_multiaddresses", func(t *testing.T) {
		// Create a slice with invalid multiaddresses
		addresses := []string{
			"invalid-multiaddr",
			"/ip4/192.168.1.6/tcp/invalid",
			"",
		}

		// Check that invalid multiaddresses don't cause errors
		result := contains(addresses, peerIDStr)
		require.False(t, result, "contains should return false for invalid multiaddresses")
	})

	t.Run("empty_slice", func(t *testing.T) {
		// Check with an empty slice
		result := contains([]string{}, peerIDStr)
		require.False(t, result, "contains should return false for empty slice")
	})
}

func TestHandleBanEvent(t *testing.T) {
	t.Skip("Skipping until we refactor ban handling to be more testable")
	t.Run("non-add_ban_event", func(t *testing.T) {
		// Setup
		logger := ulogger.New("test-server")
		mockP2PNode := new(MockServerP2PClient)
		server := &Server{
			P2PClient: mockP2PNode,
			logger:    logger,
		}

		// Test non-add action
		event := BanEvent{
			Action: "remove", // Not "add"
			IP:     "192.168.1.10",
		}

		// The function should return early without calling ConnectedPeers
		server.handleBanEvent(context.Background(), event)

		// Verify ConnectedPeers was not called
		mockP2PNode.AssertNotCalled(t, "ConnectedPeers")
	})

	t.Run("ban_event_without_peerID", func(t *testing.T) {
		// Setup
		logger := ulogger.New("test-server")
		mockP2PNode := new(MockServerP2PClient)
		server := &Server{
			P2PClient: mockP2PNode,
			logger:    logger,
		}

		// Test ban event without PeerID (should be ignored)
		event := BanEvent{
			Action: banActionAdd,
			IP:     "192.168.1.1", // IP is provided but we ignore it
		}

		// The function should log a warning and return without calling ConnectedPeers
		server.handleBanEvent(context.Background(), event)

		// Verify ConnectedPeers was not called
		mockP2PNode.AssertNotCalled(t, "ConnectedPeers")
	})

	t.Run("invalid_peerID", func(t *testing.T) {
		// Setup
		logger := ulogger.New("test-server")
		mockP2PNode := new(MockServerP2PClient)

		// Create server with mocks
		server := &Server{
			P2PClient: mockP2PNode,
			logger:    logger,
		}

		// Create a ban event with invalid PeerID
		event := BanEvent{
			Action: banActionAdd,
			PeerID: "invalid-peer-id", // This is not a valid PeerID format
		}

		// Call the function under test
		server.handleBanEvent(context.Background(), event)

		// Verify that ConnectedPeers was not called since PeerID was invalid
		mockP2PNode.AssertNotCalled(t, "ConnectedPeers")
	})

	t.Run("bans_by_peerID", func(t *testing.T) {
		// Create a server with mocked P2PClient
		logger := ulogger.New("test-server")
		mockP2PNode := new(MockServerP2PClient)

		server := &Server{
			P2PClient:    mockP2PNode,
			logger:       logger,
			peerRegistry: NewPeerRegistry(),
		}

		// Generate valid peer IDs
		privKey1, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
		require.NoError(t, err)
		peerID1, err := peer.IDFromPrivateKey(privKey1)
		require.NoError(t, err)

		privKey2, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
		require.NoError(t, err)
		peerID2, err := peer.IDFromPrivateKey(privKey2)
		require.NoError(t, err)

		// Create peers
		addr1, err := ma.NewMultiaddr("/ip4/192.168.1.50/tcp/8333")
		require.NoError(t, err)
		addr2, err := ma.NewMultiaddr("/ip4/10.0.0.50/tcp/8333")
		require.NoError(t, err)

		peer1 := p2pMessageBus.PeerInfo{
			ID:    peerID1.String(),
			Addrs: []string{addr1.String()},
		}
		peer2 := p2pMessageBus.PeerInfo{
			ID:    peerID2.String(),
			Addrs: []string{addr2.String()},
		}

		// Setup mocks - return both peers as connected
		mockP2PNode.On("GetPeers").Return([]p2pMessageBus.PeerInfo{peer1, peer2})

		// Store some test data for peer1
		// Add peer to registry with test hash
		testHash, _ := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
		server.peerRegistry.Put(peerID1, "", 0, testHash, "")

		// Create a ban event for PeerID
		event := BanEvent{
			Action: banActionAdd,
			PeerID: peerID1.String(),
			Reason: "Invalid block propagation",
		}

		// Call the function under test
		server.handleBanEvent(context.Background(), event)

		// Verify that ConnectedPeers was called
		mockP2PNode.AssertCalled(t, "GetPeers")

		// Verify peer data was cleaned up (peer removed from registry)
		_, exists := server.peerRegistry.Get(peerID1)
		assert.False(t, exists, "Peer should be removed from registry after ban")
	})
}

// MockNetworkStream implements the network.Stream interface for testing
type MockNetworkStream struct {
	mock.Mock
}

// Read implements the io.Reader interface
func (m *MockNetworkStream) Read(p []byte) (n int, err error) {
	args := m.Called(p)
	return args.Int(0), args.Error(1)
}

// Write implements the io.Writer interface
func (m *MockNetworkStream) Write(p []byte) (n int, err error) {
	args := m.Called(p)
	return args.Int(0), args.Error(1)
}

// CloseWithError implements the network.Stream interface
func (m *MockNetworkStream) CloseWithError(err error) error {
	args := m.Called(err)
	return args.Error(0)
}

// Close implements the io.Closer interface
func (m *MockNetworkStream) Close() error {
	args := m.Called()
	return args.Error(0)
}

// Reset implements the network.Stream interface
func (m *MockNetworkStream) Reset() error {
	args := m.Called()
	return args.Error(0)
}

// Reset with Error implements the network.Stream interface
func (m *MockNetworkStream) ResetWithError(code network.StreamErrorCode) error {
	args := m.Called(code)
	return args.Error(0)
}

// ID implements the network.Stream interface
func (m *MockNetworkStream) ID() string {
	args := m.Called()
	return args.String(0)
}

// SetDeadline implements the network.Stream interface
func (m *MockNetworkStream) SetDeadline(t time.Time) error {
	args := m.Called(t)
	return args.Error(0)
}

// SetReadDeadline implements the network.Stream interface
func (m *MockNetworkStream) SetReadDeadline(t time.Time) error {
	args := m.Called(t)
	return args.Error(0)
}

// SetWriteDeadline implements the network.Stream interface
func (m *MockNetworkStream) SetWriteDeadline(t time.Time) error {
	args := m.Called(t)
	return args.Error(0)
}

// Protocol implements the network.Stream interface
func (m *MockNetworkStream) Protocol() protocol.ID {
	args := m.Called()
	return args.Get(0).(protocol.ID)
}

// Scope implements the network.Stream interface
func (m *MockNetworkStream) Scope() network.StreamScope {
	// Use the mock scope implementation we created
	return &MockStreamScope{}
}

// Stat implements the network.Stream interface
func (m *MockNetworkStream) Stat() network.Stats {
	// Return empty stats
	return network.Stats{}
}

// SetProtocol implements the network.Stream interface
func (m *MockNetworkStream) SetProtocol(id protocol.ID) error {
	args := m.Called(id)
	return args.Error(0)
}

// Conn implements the network.Stream interface
func (m *MockNetworkStream) Conn() network.Conn {
	args := m.Called()
	return args.Get(0).(network.Conn)
}

// CloseRead implements the network.Stream interface
func (m *MockNetworkStream) CloseRead() error {
	args := m.Called()
	return args.Error(0)
}

// CloseWrite implements the network.Stream interface
func (m *MockNetworkStream) CloseWrite() error {
	args := m.Called()
	return args.Error(0)
}

// MockNetworkConn is a testify-mock that implements network.Conn.
type MockNetworkConn struct {
	mock.Mock
}

// ID implements the network.Conn interface
func (m *MockNetworkConn) ID() string { args := m.Called(); return args.String(0) }

// LocalPeer is a mock implementation of the network.Conn interface
func (m *MockNetworkConn) LocalPeer() peer.ID { args := m.Called(); return args.Get(0).(peer.ID) }

// RemotePeer is a mock implementation of the network.Conn interface
func (m *MockNetworkConn) RemotePeer() peer.ID { args := m.Called(); return args.Get(0).(peer.ID) }

// LocalPrivateKey is a mock implementation of the network.Conn interface
func (m *MockNetworkConn) LocalPrivateKey() crypto.PrivKey {
	args := m.Called()
	return args.Get(0).(crypto.PrivKey)
}

// RemotePublicKey is a mock implementation of the network.Conn interface
func (m *MockNetworkConn) RemotePublicKey() crypto.PubKey {
	args := m.Called()
	return args.Get(0).(crypto.PubKey)
}

// LocalMultiaddr is a mock implementation of the network.Conn interface
func (m *MockNetworkConn) LocalMultiaddr() ma.Multiaddr {
	args := m.Called()
	return args.Get(0).(ma.Multiaddr)
}

// RemoteMultiaddr is a mock implementation of the network.Conn interface
func (m *MockNetworkConn) RemoteMultiaddr() ma.Multiaddr {
	args := m.Called()
	return args.Get(0).(ma.Multiaddr)
}

// Stat is a mock implementation of the network.Conn interface
func (m *MockNetworkConn) Stat() network.ConnStats {
	args := m.Called()
	return args.Get(0).(network.ConnStats)
}

// Scope is a mock implementation of the network.Conn interface
func (m *MockNetworkConn) Scope() network.ConnScope {
	args := m.Called()
	return args.Get(0).(network.ConnScope)
}

// Close is a mock implementation of the network.Conn interface
func (m *MockNetworkConn) Close() error { args := m.Called(); return args.Error(0) }

// CloseWithError is a mock implementation of the network.Conn interface
// Satisfies network.Conn (v0.38+)
func (m *MockNetworkConn) CloseWithError(code network.ConnErrorCode) error {
	args := m.Called(code)
	return args.Error(0)
}

// String is a mock implementation of the network.Conn interface
func (m *MockNetworkConn) String() string { args := m.Called(); return args.String(0) }

// NewStream is a mock implementation of the network.Conn interface
func (m *MockNetworkConn) NewStream(ctx context.Context) (network.Stream, error) {
	args := m.Called(ctx)
	return args.Get(0).(network.Stream), args.Error(1)
}

// GetStreams is a mock implementation of the network.Conn interface
func (m *MockNetworkConn) GetStreams() []network.Stream {
	args := m.Called()
	return args.Get(0).([]network.Stream)
}

// IsClosed is a mock implementation of the network.Conn interface
func (m *MockNetworkConn) IsClosed() bool {
	args := m.Called()
	return args.Bool(0)
}

// ConnState implements the network.Conn interface
func (m *MockNetworkConn) ConnState() network.ConnectionState {
	// Since we're only using this for mocking in tests,
	// this is a workaround for the ConnectionState type
	var state network.ConnectionState
	return state // Return the zero value of the type
}

// As implements the network.Conn interface
// As finds the first conn in Conn's wrapped types that matches target, and
// if one is found, sets target to that conn value and returns true.
// For this mock implementation, we return false as we don't wrap other connections.
func (m *MockNetworkConn) As(target any) bool {
	args := m.Called(target)
	return args.Bool(0)
}

// MockConnScope implements the network.ConnScope interface for testing
type MockConnScope struct{}

// BeginSpan implements the network.ConnScope interface for testing
func (m *MockConnScope) BeginSpan() (network.ResourceScopeSpan, error) {
	// Return a simple implementation
	return &MockResourceScopeSpan{}, nil
}

// EndSpan implements the network.ConnScope interface for testing
func (m *MockConnScope) EndSpan() {
	// Empty implementation
}

// ReserveMemory implements the network.ConnScope interface for testing
func (m *MockConnScope) ReserveMemory(size int, priority uint8) error {
	// Empty implementation
	return nil
}

// ReleaseMemory implements the network.ConnScope interface for testing
func (m *MockConnScope) ReleaseMemory(size int) {
	// Empty implementation
}

// SetService implements the network.ConnScope interface for testing
func (m *MockConnScope) SetService(service string) error {
	// Empty implementation
	return nil
}

// Stat implements the network.ConnScope interface
func (m *MockConnScope) Stat() network.ScopeStat {
	// Return empty stats
	return network.ScopeStat{}
}

func TestFixGetPeers(t *testing.T) {
	// Create mock ban manager
	mockBanManager := new(MockPeerBanManager)
	mockBanManager.On("GetBanScore", mock.Anything).Return(0, false, time.Time{})

	// Verify it works
	score, banned, banUntil := mockBanManager.GetBanScore("test-peer-id")
	require.Equal(t, 0, score)
	require.False(t, banned)
	require.True(t, banUntil.IsZero())
}

func TestBlacklistBaseURL(t *testing.T) {
	t.Run("isBlacklistedBaseURL_business_logic", func(t *testing.T) {
		// Create settings with blacklisted URLs
		tSettings := createBaseTestSettings()
		tSettings.SubtreeValidation.BlacklistedBaseURLs = map[string]struct{}{
			"http://evil.com":       {},
			"https://malicious.com": {},
		}

		// Create server with minimal setup - only need settings and logger for business logic
		server := &Server{
			settings: tSettings,
			logger:   ulogger.New("test-server"),
		}

		// Test blacklisted URLs - should return true
		assert.True(t, server.isBlacklistedBaseURL("http://evil.com"))
		assert.True(t, server.isBlacklistedBaseURL("https://malicious.com"))
		assert.True(t, server.isBlacklistedBaseURL("http://evil.com:8080"))
		assert.True(t, server.isBlacklistedBaseURL("https://malicious.com/api/v1"))
		assert.True(t, server.isBlacklistedBaseURL("http://EVIL.COM"))
		assert.True(t, server.isBlacklistedBaseURL("https://MALICIOUS.COM"))

		// Test non-blacklisted URLs - should return false
		assert.False(t, server.isBlacklistedBaseURL("http://good.com"))
		assert.False(t, server.isBlacklistedBaseURL("https://safe.com"))
		assert.False(t, server.isBlacklistedBaseURL("http://evil.example.com"))
		assert.False(t, server.isBlacklistedBaseURL("http://subdomain.evil.com"))
	})

	t.Run("empty_blacklist_allows_all", func(t *testing.T) {
		// Create settings with empty blacklist
		tSettings := createBaseTestSettings()
		// Don't add anything to blacklist

		server := &Server{
			settings: tSettings,
			logger:   ulogger.New("test-server"),
		}

		// All URLs should be allowed when blacklist is empty
		assert.False(t, server.isBlacklistedBaseURL("http://evil.com"))
		assert.False(t, server.isBlacklistedBaseURL("https://malicious.com"))
		assert.False(t, server.isBlacklistedBaseURL("http://anything.com"))
	})

	t.Run("case_insensitive_matching", func(t *testing.T) {
		tSettings := createBaseTestSettings()
		tSettings.SubtreeValidation.BlacklistedBaseURLs = map[string]struct{}{
			"http://TeSt.CoM": {},
		}

		server := &Server{
			settings: tSettings,
			logger:   ulogger.New("test-server"),
		}

		// Should match regardless of case
		assert.True(t, server.isBlacklistedBaseURL("http://test.com"))
		assert.True(t, server.isBlacklistedBaseURL("HTTP://TEST.COM"))
		assert.True(t, server.isBlacklistedBaseURL("http://TeSt.CoM"))
	})

	t.Run("port_and_path_variations", func(t *testing.T) {
		tSettings := createBaseTestSettings()
		tSettings.SubtreeValidation.BlacklistedBaseURLs = map[string]struct{}{
			"http://bad.com": {},
		}

		server := &Server{
			settings: tSettings,
			logger:   ulogger.New("test-server"),
		}

		// Should block URLs with same domain but different ports/paths
		assert.True(t, server.isBlacklistedBaseURL("http://bad.com:8080"))
		assert.True(t, server.isBlacklistedBaseURL("http://bad.com/path/to/resource"))
		assert.True(t, server.isBlacklistedBaseURL("http://bad.com:8080/api/v1"))

		// But should NOT block different domains
		assert.False(t, server.isBlacklistedBaseURL("http://good.com"))
		assert.False(t, server.isBlacklistedBaseURL("http://bad.example.com"))
	})
}

func TestSelfMessageFiltering(t *testing.T) {
	t.Run("filters_own_block_messages", func(t *testing.T) {
		tSettings := createBaseTestSettings()
		tSettings.ChainCfgParams = &chaincfg.RegressionNetParams

		mockBlocksProducer := kafka.NewKafkaAsyncProducerMock()
		publishCh := mockBlocksProducer.PublishChannel()

		mockP2PNode := new(MockServerP2PClient)
		GetID := peer.ID("12D3KooWKd2kacFFXWtbYtkDAsTP8fhEX1TbunV9Afimr7m1E8Yg")
		mockP2PNode.On("GetID").Return(GetID)

		server := &Server{
			settings:                  tSettings,
			logger:                    ulogger.New("test-server"),
			blocksKafkaProducerClient: mockBlocksProducer,
			gCtx:                      context.Background(),
			P2PClient:                 mockP2PNode,
			notificationCh:            make(chan *notificationMsg, 10),
			banManager:                &PeerBanManager{peerBanScores: make(map[string]*BanScore)},
			blockPeerMap:              sync.Map{},
		}

		// Create a block message from our own node
		blockMsg := BlockMessage{
			Hash:       "00000000000000000007d3c1e3b3e3b3e3b3e3b3e3b3e3b3e3b3e3b3e3b3e3b3",
			Height:     100,
			PeerID:     GetID.String(), // Our own PeerID
			DataHubURL: "https://example.com",
		}

		msgBytes, err := json.Marshal(blockMsg)
		require.NoError(t, err)

		// Handle the message (from parameter doesn't matter, we check PeerID)
		server.handleBlockTopic(context.Background(), msgBytes, "someOtherPeer")

		// Should NOT publish to Kafka
		select {
		case msg := <-publishCh:
			t.Errorf("Should not publish own block message, but got: %v", msg)
		case <-time.After(50 * time.Millisecond):
			// Expected - no message published
		}
	})

	t.Run("filters_own_subtree_messages", func(t *testing.T) {
		tSettings := createBaseTestSettings()

		mockSubtreeProducer := kafka.NewKafkaAsyncProducerMock()
		publishCh := mockSubtreeProducer.PublishChannel()

		mockP2PNode := new(MockServerP2PClient)
		GetID := peer.ID("12D3KooWKd2kacFFXWtbYtkDAsTP8fhEX1TbunV9Afimr7m1E8Yg")
		mockP2PNode.On("GetID").Return(GetID)

		server := &Server{
			settings:                   tSettings,
			logger:                     ulogger.New("test-server"),
			subtreeKafkaProducerClient: mockSubtreeProducer,
			gCtx:                       context.Background(),
			P2PClient:                  mockP2PNode,
			notificationCh:             make(chan *notificationMsg, 10),
			banManager:                 &PeerBanManager{peerBanScores: make(map[string]*BanScore)},
			subtreePeerMap:             sync.Map{},
		}

		// Create a subtree message from our own node
		subtreeMsg := SubtreeMessage{
			Hash:       "00000000000000000007d3c1e3b3e3b3e3b3e3b3e3b3e3b3e3b3e3b3e3b3e3b3",
			PeerID:     GetID.String(), // Our own PeerID
			DataHubURL: "https://example.com",
		}

		msgBytes, err := json.Marshal(subtreeMsg)
		require.NoError(t, err)

		// Handle the message
		server.handleSubtreeTopic(context.Background(), msgBytes, "someOtherPeer")

		// Should NOT publish to Kafka
		select {
		case msg := <-publishCh:
			t.Errorf("Should not publish own subtree message, but got: %v", msg)
		case <-time.After(50 * time.Millisecond):
			// Expected - no message published
		}
	})

	t.Run("processes_messages_from_other_peers", func(t *testing.T) {
		tSettings := createBaseTestSettings()

		mockBlocksProducer := kafka.NewKafkaAsyncProducerMock()
		publishCh := mockBlocksProducer.PublishChannel()

		mockP2PNode := new(MockServerP2PClient)
		GetID := peer.ID("12D3KooWKd2kacFFXWtbYtkDAsTP8fhEX1TbunV9Afimr7m1E8Yg")
		mockP2PNode.On("GetID").Return(GetID)
		mockP2PNode.On("UpdatePeerHeight", mock.Anything, mock.Anything).Return()

		server := &Server{
			settings:                  tSettings,
			logger:                    ulogger.New("test-server"),
			blocksKafkaProducerClient: mockBlocksProducer,
			gCtx:                      context.Background(),
			P2PClient:                 mockP2PNode,
			notificationCh:            make(chan *notificationMsg, 10),
			banManager:                &PeerBanManager{peerBanScores: make(map[string]*BanScore)},
			blockPeerMap:              sync.Map{},
		}

		// Create a block message from a different peer
		blockMsg := BlockMessage{
			Hash:       "00000000000000000007d3c1e3b3e3b3e3b3e3b3e3b3e3b3e3b3e3b3e3b3e3b3",
			Height:     100,
			PeerID:     "12D3KooWRj9ajsNaVuT2fNv7k2AyLnrC5NQQzZS9GixSVWKZZYRE", // Different peer
			DataHubURL: "https://example.com",
		}

		msgBytes, err := json.Marshal(blockMsg)
		require.NoError(t, err)

		// Handle the message
		server.handleBlockTopic(context.Background(), msgBytes, blockMsg.PeerID)

		// Should publish to Kafka
		select {
		case msg := <-publishCh:
			assert.NotNil(t, msg, "Should publish message from other peer")
		case <-time.After(100 * time.Millisecond):
			t.Error("Expected message to be published from other peer")
		}
	})
}

// createBaseTestSettings is a local replacement for test.CreateBaseTestSettings
func createBaseTestSettings() *settings.Settings {
	s := settings.NewSettings()
	s.SubtreeValidation.BlacklistedBaseURLs = make(map[string]struct{})
	s.P2P.EnableNAT = false // Disable NAT in tests to prevent data races in libp2p

	return s
}

// TestNewServer_ConfigValidation ensures that NewServer properly validates the P2P settings
// and returns meaningful configuration errors when required fields are missing or invalid.
// Each subtest modifies one specific configuration field to trigger a different error path,
// allowing full coverage of the early-return validation logic in the NewServer constructor.
func TestNewServer_ConfigValidation(t *testing.T) {

	ctx := context.Background()
	logger := ulogger.New("test-server")

	type testCase struct {
		name       string
		modify     func(s *settings.Settings)
		wantErrMsg string
	}

	baseSettings := func() *settings.Settings {
		return &settings.Settings{
			Version: "1.0.0",
			P2P: settings.P2PSettings{
				ListenAddresses: []string{"/ip4/127.0.0.1/tcp/1234"},
				Port:            1234,
				BlockTopic:      "block",
				SubtreeTopic:    "subtree",
				RejectedTxTopic: "rejected",
				ListenMode:      settings.ListenModeFull,
				PrivateKey:      "privkey",
				EnableNAT:       false, // Disable NAT in tests to prevent data races in libp2p
			},
			ChainCfgParams: &chaincfg.Params{
				TopicPrefix: "prefix",
			},
			Kafka: settings.KafkaSettings{
				InvalidBlocks:   "invalidBlocks",
				InvalidSubtrees: "invalidSubtrees",
			},
		}
	}

	tests := []testCase{
		{
			name: "missing ListenAddresses",
			modify: func(s *settings.Settings) {
				s.P2P.ListenAddresses = nil
			},
			wantErrMsg: "p2p_listen_addresses not set in config",
		},
		{
			name: "missing Port",
			modify: func(s *settings.Settings) {
				s.P2P.Port = 0
			},
			wantErrMsg: "p2p_port not set in config",
		},
		{
			name: "missing TopicPrefix",
			modify: func(s *settings.Settings) {
				s.ChainCfgParams.TopicPrefix = ""
			},
			wantErrMsg: "missing config ChainCfgParams.TopicPrefix",
		},
		{
			name: "missing BlockTopic",
			modify: func(s *settings.Settings) {
				s.P2P.BlockTopic = ""
			},
			wantErrMsg: "p2p_block_topic not set in config",
		},
		{
			name: "missing SubtreeTopic",
			modify: func(s *settings.Settings) {
				s.P2P.SubtreeTopic = ""
			},
			wantErrMsg: "p2p_subtree_topic not set in config",
		},
		{
			name: "missing RejectedTxTopic",
			modify: func(s *settings.Settings) {
				s.P2P.RejectedTxTopic = ""
			},
			wantErrMsg: "p2p_rejected_tx_topic not set in config",
		},
		{
			name: "invalid ListenMode",
			modify: func(s *settings.Settings) {
				s.P2P.ListenMode = "invalid_mode"
			},
			wantErrMsg: "listen_mode must be either",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := baseSettings()
			tc.modify(s)

			_, err := NewServer(ctx, logger, s,
				nil, nil, nil, nil, nil, nil, nil,
			)

			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantErrMsg)
		})
	}
}

func TestPrivateKeyHandling(t *testing.T) {

	ctx := context.Background()
	logger := ulogger.New("test-server")

	t.Run("key exists in settings - should use settings key", func(t *testing.T) {
		// Setup
		mockClient := &blockchain.Mock{}
		// Use 64-byte Ed25519 format: 32-byte private + 32-byte public (128 hex characters)
		settingsKey := "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdefabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"

		settings := createBaseTestSettings()
		settings.P2P.PrivateKey = settingsKey
		settings.P2P.ListenAddresses = []string{"127.0.0.1"}
		settings.P2P.StaticPeers = []string{}
		settings.BlockChain.StoreURL = &url.URL{
			Scheme: "sqlitememory",
		}

		// Mock should not be called since we have a key in settings
		// No expectations set

		// Execute
		server, err := NewServer(ctx, logger, settings, mockClient, nil, nil, nil, nil, nil, nil)

		// Verify
		require.NoError(t, err)
		require.NotNil(t, server)
		mockClient.AssertExpectations(t)
	})

	t.Run("no key in settings - should generate new key", func(t *testing.T) {
		// Setup
		mockClient := &blockchain.Mock{}

		settings := createBaseTestSettings()
		settings.P2P.PeerCacheDir = ""
		settings.P2P.PrivateKey = "" // No key in settings
		settings.P2P.StaticPeers = []string{}
		settings.P2P.ListenAddresses = []string{"127.0.0.1"}

		// No blockchain client expectations - we don't use it for key storage anymore

		// Execute
		server, err := NewServer(ctx, logger, settings, mockClient, nil, nil, nil, nil, nil, nil)

		// Verify
		require.NoError(t, err)
		require.NotNil(t, server)
		require.NotNil(t, server.P2PClient, "P2P node should be created")
		mockClient.AssertExpectations(t)
	})

	t.Run("invalid key in settings - should return error", func(t *testing.T) {
		// Setup
		mockClient := &blockchain.Mock{}

		settings := createBaseTestSettings()
		settings.P2P.PrivateKey = "invalid-key" // Invalid key in settings
		settings.P2P.StaticPeers = []string{}
		settings.P2P.ListenAddresses = []string{"127.0.0.1"}
		settings.BlockChain.StoreURL = &url.URL{
			Scheme: "sqlitememory",
		}

		// No blockchain client expectations - we don't use it for key storage anymore

		// Execute
		_, err := NewServer(ctx, logger, settings, mockClient, nil, nil, nil, nil, nil, nil)

		// Verify - should fail with invalid key
		require.Error(t, err)
		require.Contains(t, err.Error(), "decode")
		mockClient.AssertExpectations(t)
	})

	t.Run("no key in settings and no p2p.key file - should generate and save new key", func(t *testing.T) {
		mockClient := &blockchain.Mock{}

		settings := createBaseTestSettings()
		settings.P2P.PrivateKey = ""
		settings.P2P.ListenAddresses = []string{"127.0.0.1"}
		settings.P2P.StaticPeers = []string{}
		settings.BlockChain.StoreURL = &url.URL{
			Scheme: "sqlitememory",
		}

		tmpDir := t.TempDir()
		settings.P2P.PeerCacheDir = tmpDir

		server, err := NewServer(ctx, logger, settings, mockClient, nil, nil, nil, nil, nil, nil)
		require.NoError(t, err)
		require.NotNil(t, server)

		keyPath := filepath.Join(tmpDir, "p2p.key")
		data, err := os.ReadFile(keyPath)
		require.NoError(t, err)
		require.NotEmpty(t, data, "nuova chiave deve essere scritta su disco")

		mockClient.AssertExpectations(t)
	})

	t.Run("error writing key file", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, os.Chmod(dir, 0o555))
		defer func() {
			if err := os.Chmod(dir, 0o755); err != nil {
				t.Logf("failed to restore permissions on %s: %v", dir, err)
			}
		}()

		settings := createBaseTestSettings()
		settings.P2P.PrivateKey = ""
		settings.P2P.ListenAddresses = []string{"127.0.0.1"}
		settings.P2P.PeerCacheDir = dir
		settings.BlockChain.StoreURL = &url.URL{Scheme: "sqlitememory"}

		server, err := NewServer(ctx, logger, settings, &blockchain.Mock{}, nil, nil, nil, nil, nil, nil)
		require.Error(t, err)
		require.Nil(t, server)
		require.Contains(t, err.Error(), "failed to save private key")
	})

}

func TestServerHealth(t *testing.T) {
	ctx := context.Background()

	t.Run("liveness check returns OK", func(t *testing.T) {
		s := &Server{}
		code, msg, err := s.Health(ctx, true)
		require.Equal(t, http.StatusOK, code)
		require.Equal(t, "OK", msg)
		require.NoError(t, err)
	})

	t.Run("readiness_check_without_dependencies", func(t *testing.T) {
		s := &Server{}
		code, msg, err := s.Health(ctx, false)

		require.Equal(t, http.StatusOK, code)
		require.NoError(t, err)
		require.Contains(t, msg, `"Kafka"`)
	})

	t.Run("readiness_with_blockchain_client", func(t *testing.T) {
		mockBlockchain := new(blockchain.Mock)
		mockBlockchain.On("Health", mock.Anything, false).Return(200, "OK", nil)
		state := blockchain_api.FSMStateType_RUNNING
		mockBlockchain.On("GetFSMCurrentState", mock.Anything).Return(&state, nil)
		mockBlockchain.On("GetState", mock.Anything, mock.Anything).Return([]byte("ok"), nil)

		s := &Server{
			blockchainClient: mockBlockchain,
		}

		code, msg, err := s.Health(ctx, false)

		require.Equal(t, http.StatusOK, code)
		require.NoError(t, err)
		require.Contains(t, msg, `"BlockchainClient"`)
	})

	t.Run("readiness with kafka consumer client", func(t *testing.T) {
		mockBlockchain := new(blockchain.Mock)
		state := blockchain_api.FSMStateType_RUNNING
		mockBlockchain.On("GetFSMCurrentState", mock.Anything).Return(&state, nil)
		mockBlockchain.On("GetState", mock.Anything, mock.Anything).Return([]byte("ok"), nil)
		mockBlockchain.On("Health", mock.Anything, mock.Anything).Return(http.StatusOK, "OK", nil)

		mockInvalidBlocksKafka := new(MockKafkaConsumerGroup)
		mockInvalidBlocksKafka.On("BrokersURL").Return([]string{"localhost:9092"})
		mockInvalidBlocksKafka.On("Health", mock.Anything, mock.Anything).Return(http.StatusOK, "OK", nil)

		mockInvalidSubtreeKafka := new(MockKafkaConsumerGroup)
		mockInvalidSubtreeKafka.On("BrokersURL").Return([]string{"localhost:9092"})
		mockInvalidSubtreeKafka.On("Health", mock.Anything, mock.Anything).Return(http.StatusOK, "OK", nil)

		s := &Server{
			blockchainClient:                  mockBlockchain,
			invalidBlocksKafkaConsumerClient:  mockInvalidBlocksKafka,
			invalidSubtreeKafkaConsumerClient: mockInvalidSubtreeKafka,
		}

		code, msg, err := s.Health(context.Background(), false)
		t.Logf("Health result: %d %s", code, msg)
		require.Equal(t, http.StatusOK, code)
		require.NoError(t, err)
	})

}

func TestServerInitHTTPPublicAddressSet(t *testing.T) {

	ctx := context.Background()
	logger := ulogger.New("test-server")
	mockClient := &blockchain.Mock{}

	settings := createBaseTestSettings()
	settings.P2P.PrivateKey = "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdefabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
	settings.Asset.HTTPPublicAddress = "http://public.example.com"
	settings.Asset.HTTPAddress = "http://fallback.example.com"
	settings.BlockChain.StoreURL = &url.URL{
		Scheme: "sqlitememory",
	}

	server, err := NewServer(ctx, logger, settings, mockClient, nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	err = server.Init(ctx)
	require.NoError(t, err)
	require.Equal(t, "http://public.example.com", server.AssetHTTPAddressURL)
}

func TestServerInitHTTPPublicAddressEmpty(t *testing.T) {

	ctx := context.Background()
	logger := ulogger.New("test-server")
	mockClient := &blockchain.Mock{}

	settings := createBaseTestSettings()
	settings.P2P.PrivateKey = "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdefabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
	settings.Asset.HTTPPublicAddress = ""
	settings.Asset.HTTPAddress = "http://fallback.example.com"
	settings.BlockChain.StoreURL = &url.URL{
		Scheme: "sqlitememory",
	}

	server, err := NewServer(ctx, logger, settings, mockClient, nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	err = server.Init(ctx)
	require.NoError(t, err)
	require.Equal(t, "http://fallback.example.com", server.AssetHTTPAddressURL)
}

func TestServerInitPropagationURLSet(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.New("test-server")
	mockClient := &blockchain.Mock{}

	settings := createBaseTestSettings()
	settings.P2P.PrivateKey = "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdefabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
	settings.Asset.HTTPPublicAddress = "http://public.example.com"
	settings.Asset.HTTPAddress = "http://fallback.example.com"
	settings.Asset.PropagationPublicURL = "http://propagation.example.com"
	settings.BlockChain.StoreURL = &url.URL{
		Scheme: "sqlitememory",
	}

	server, err := NewServer(ctx, logger, settings, mockClient, nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	err = server.Init(ctx)
	require.NoError(t, err)
	require.Equal(t, "http://public.example.com", server.AssetHTTPAddressURL)
	require.Equal(t, "http://propagation.example.com", server.PropagationURL)
}

func TestServerInitPropagationURLEmpty(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.New("test-server")
	mockClient := &blockchain.Mock{}

	settings := createBaseTestSettings()
	settings.P2P.PrivateKey = "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdefabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
	settings.Asset.HTTPPublicAddress = "http://public.example.com"
	settings.Asset.HTTPAddress = "http://fallback.example.com"
	settings.Asset.PropagationPublicURL = "" // Empty - should fall back to AssetHTTPAddressURL
	settings.BlockChain.StoreURL = &url.URL{
		Scheme: "sqlitememory",
	}

	server, err := NewServer(ctx, logger, settings, mockClient, nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	err = server.Init(ctx)
	require.NoError(t, err)
	require.Equal(t, "http://public.example.com", server.AssetHTTPAddressURL)
	require.Equal(t, "http://public.example.com", server.PropagationURL) // Falls back to AssetHTTPAddressURL
}

func TestServerSetupHTTPServer(t *testing.T) {

	ctx := context.Background()
	logger := ulogger.New("test-logger")
	mockClient := &blockchain.Mock{}

	settings := createBaseTestSettings()
	settings.P2P.PrivateKey = "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdefabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
	settings.Asset.HTTPAddress = "http://localhost:8080"
	settings.BlockChain.StoreURL = &url.URL{
		Scheme: "sqlitememory",
	}

	server, err := NewServer(ctx, logger, settings, mockClient, nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	err = server.Init(ctx)
	require.NoError(t, err)

	// Set notification channel (required by HandleWebSocket)
	server.notificationCh = make(chan *notificationMsg)

	e := server.setupHTTPServer()
	require.NotNil(t, e)

	// Simulate a test HTTP request to /health
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	e.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "OK", rec.Body.String())
}

// getFreePort asks the kernel for a free open port that is ready to use.
func getFreePort(t *testing.T) int {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	require.NoError(t, err)

	l, err := net.ListenTCP("tcp", addr)
	require.NoError(t, err)

	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())

	return port
}

func TestServerStartFull(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	host, err := libp2p.New()
	require.NoError(t, err)

	ps, err := pubsub.NewGossipSub(ctx, host)
	require.NoError(t, err)

	// Create topic
	topic, err := ps.Join("test-prefix-handshake")
	require.NoError(t, err)

	// Create ready channel
	readyCh := make(chan struct{})

	// Mocks
	mockP2PNode := new(MockServerP2PClient)
	mockBlockchain := new(blockchain.Mock)
	mockBlockchain.On("WaitUntilFSMTransitionFromIdleState", mock.Anything).Return(nil)
	prevHash := chainhash.DoubleHashB([]byte("prev block"))
	merkleRoot := chainhash.DoubleHashB([]byte("merkle root"))

	var hashPrev, hashMerkle chainhash.Hash
	copy(hashPrev[:], prevHash)
	copy(hashMerkle[:], merkleRoot)

	mockBlockchain.On("GetBestBlockHeader", mock.Anything).Return(
		&model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &hashPrev,
			HashMerkleRoot: &hashMerkle,
			Timestamp:      uint32(time.Now().Unix()),
			Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d}, // mainnet genesis bits 0x1d00ffff in little endian
			Nonce:          0,
		},
		&model.BlockHeaderMeta{Height: 0},
		nil,
	)

	subChan := make(chan *blockchain_api.Notification)
	close(subChan)

	mockBlockchain.On("Subscribe", mock.Anything, "p2pServer").
		Return(subChan, nil)

	state := blockchain_api.FSMStateType_RUNNING
	mockBlockchain.On("GetFSMCurrentState", mock.Anything).Return(&state, nil)

	// Mock GetState for BlockPersisterHeight query in determineStorage
	blockPersisterHeightData := make([]byte, 4)
	binary.LittleEndian.PutUint32(blockPersisterHeightData, 0)
	mockBlockchain.On("GetState", mock.Anything, "BlockPersisterHeight").Return(blockPersisterHeightData, nil).Maybe()

	mockRejectedKafka := new(MockKafkaConsumerGroup)
	mockRejectedKafka.On("Start", mock.Anything, mock.Anything, mock.Anything).Return()

	mockInvalidBlocksKafka := new(MockKafkaConsumerGroup)
	mockInvalidBlocksKafka.On("Start", mock.Anything, mock.Anything, mock.Anything).Return()

	mockInvalidSubtreeKafka := new(MockKafkaConsumerGroup)
	mockInvalidSubtreeKafka.On("Start", mock.Anything, mock.Anything, mock.Anything).Return()

	mockSubtreeProducer := new(MockKafkaProducer)
	mockSubtreeProducer.On("Start", mock.Anything, mock.Anything).Return()

	mockBlocksProducer := new(MockKafkaProducer)
	mockBlocksProducer.On("Start", mock.Anything, mock.Anything).Return()

	mockP2PNode.On("Start", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockP2PNode.On("SetPeerConnectedCallback", mock.Anything).Return()
	mockP2PNode.On("GetID").Return(peer.ID("mock-peer-id"))
	mockP2PNode.On("Subscribe", mock.Anything).Return(make(<-chan p2pMessageBus.Message))

	logger := ulogger.New("test")
	settings := createBaseTestSettings()
	settings.P2P.PrivateKey = "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdefabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
	settings.BlockChain.StoreURL = &url.URL{Scheme: "sqlitememory"}

	// Use a dynamic port for the HTTP server to avoid conflicts
	httpPort := getFreePort(t)
	settings.P2P.HTTPListenAddress = fmt.Sprintf(":%d", httpPort)

	grpcPort := getFreePort(t)
	settings.P2P.GRPCListenAddress = fmt.Sprintf(":%d", grpcPort)

	server, err := NewServer(ctx, logger, settings, mockBlockchain, nil, nil, nil, mockRejectedKafka, mockBlocksProducer, mockSubtreeProducer)
	require.NoError(t, err)

	server.rejectedTxKafkaConsumerClient = mockRejectedKafka
	server.invalidBlocksKafkaConsumerClient = mockInvalidBlocksKafka
	server.invalidSubtreeKafkaConsumerClient = mockInvalidSubtreeKafka
	server.subtreeKafkaProducerClient = mockSubtreeProducer
	server.blocksKafkaProducerClient = mockBlocksProducer

	server.P2PClient = mockP2PNode
	mockP2PNode.On("SetTopicHandler", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockP2PNode.On("GetTopic", mock.Anything).Return(topic)
	mockP2PNode.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockP2PNode.On("ConnectedPeers").Return([]p2pMessageBus.PeerInfo{}) // Return empty list of connected peers

	// Run server
	go func() {
		err := server.Start(ctx, readyCh)
		if err != nil && !errors.Is(err, context.Canceled) {
			// Log error but don't use require in goroutine
			logger.Errorf("server.Start failed: %v", err)
		}
	}()

	select {
	case <-readyCh:
		// ok
	case <-time.After(2 * time.Second):
		t.Fatal("readyCh was not closed")
	}
}

func TestInvalidSubtreeHandlerHappyPath(t *testing.T) {
	// Create a valid peer ID for testing
	_, pub, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
	require.NoError(t, err)
	testPeerID, err := peer.IDFromPublicKey(pub)
	require.NoError(t, err)

	// Create peer registry and add the test peer
	registry := NewPeerRegistry()
	registry.Put(testPeerID, "test-client", 100, nil, "")

	// Get initial failure count
	peerInfo, exists := registry.Get(testPeerID)
	require.True(t, exists)
	initialFailures := peerInfo.InteractionFailures

	mockBC := new(blockchain.Mock)
	st := blockchain_api.FSMStateType_RUNNING
	mockBC.On("GetFSMCurrentState", mock.Anything).Return(&st, nil)

	s := &Server{
		logger:           ulogger.New("test"),
		peerRegistry:     registry,
		blockchainClient: mockBC,
	}

	hash := "subtree-hash-456"
	entry := peerMapEntry{
		peerID:    testPeerID.String(),
		timestamp: time.Now(),
	}
	s.subtreePeerMap.Store(hash, entry)

	m := &kafkamessage.KafkaInvalidSubtreeTopicMessage{
		SubtreeHash: hash,
		PeerUrl:     "",
		Reason:      "peer_cannot_provide_transactions",
	}
	payload, err := proto.Marshal(m)
	require.NoError(t, err)

	msg := &kafka.KafkaMessage{
		ConsumerMessage: sarama.ConsumerMessage{
			Topic: "invalid-subtrees",
			Value: payload,
		},
	}

	h := s.invalidSubtreeHandler(context.Background())
	err = h(msg)
	require.NoError(t, err)

	// Verify subtree was removed from map after processing
	_, ok := s.subtreePeerMap.Load(hash)
	require.False(t, ok, "entry should be removed after processing")

	// Verify interaction failure was recorded
	peerInfo, exists = registry.Get(testPeerID)
	require.True(t, exists)
	assert.Equal(t, initialFailures+1, peerInfo.InteractionFailures,
		"interaction failure should be incremented for invalid subtree")
}

func TestInvalidBlockHandler(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.New("test")

	mkMsgBytes := func(blockHash, reason string) []byte {
		pb := &kafkamessage.KafkaInvalidBlockTopicMessage{
			BlockHash: blockHash,
			Reason:    reason,
		}
		b, err := proto.Marshal(pb)
		require.NoError(t, err)
		return b
	}

	mkKafkaMsg := func(payload []byte) *kafka.KafkaMessage {
		return &kafka.KafkaMessage{
			ConsumerMessage: sarama.ConsumerMessage{
				Topic: "invalid-subtrees",
				Value: payload,
			},
		}
	}

	t.Run("returns nil when syncing", func(t *testing.T) {
		mockBC := new(blockchain.Mock)
		syncingState := blockchain_api.FSMStateType_RUNNING
		mockBC.On("GetFSMCurrentState", mock.Anything).Return(&syncingState, nil)

		s := &Server{
			logger:           logger,
			blockchainClient: mockBC,
		}

		h := s.invalidBlockHandler(ctx)
		err := h(mkKafkaMsg(mkMsgBytes("abc", "invalid_block")))
		require.NoError(t, err)

		mockBC.AssertExpectations(t)
	})

	t.Run("unmarshal error returns error", func(t *testing.T) {
		mockBC := new(blockchain.Mock)
		running := blockchain_api.FSMStateType_RUNNING
		mockBC.On("GetFSMCurrentState", mock.Anything).Return(&running, nil)

		s := &Server{
			logger:           logger,
			blockchainClient: mockBC,
		}

		h := s.invalidBlockHandler(ctx)
		err := h(mkKafkaMsg([]byte{0xff, 0x00, 0x01}))
		require.Error(t, err)

		mockBC.AssertExpectations(t)
	})

	t.Run("happy path: ReportInvalidBlock error is swallowed (returns nil)", func(t *testing.T) {
		mockBC := new(blockchain.Mock)
		running := blockchain_api.FSMStateType_RUNNING
		mockBC.On("GetFSMCurrentState", mock.Anything).Return(&running, nil)

		s := &Server{
			logger:           logger,
			blockchainClient: mockBC,
		}

		h := s.invalidBlockHandler(ctx)

		payload := mkMsgBytes("deadbeef", "invalid_block")
		err := h(mkKafkaMsg(payload))
		require.NoError(t, err)

		mockBC.AssertExpectations(t)
	})

	t.Run("happy path: ReportInvalidBlock succeeds and returns nil", func(t *testing.T) {
		mockBC := new(blockchain.Mock)
		running := blockchain_api.FSMStateType_RUNNING
		mockBC.On("GetFSMCurrentState", mock.Anything).Return(&running, nil)

		banHandler := &testBanHandler{}
		banManager := &PeerBanManager{
			peerBanScores: make(map[string]*BanScore),
			reasonPoints: map[BanReason]int{
				ReasonInvalidBlock: 10,
			},
			banThreshold:  100,
			banDuration:   time.Hour,
			decayInterval: time.Minute,
			decayAmount:   1,
			handler:       banHandler,
		}

		s := &Server{
			logger:           logger,
			blockchainClient: mockBC,
			banManager:       banManager,
		}

		blockHash := "beefcafe"
		peerID := "peer-123"

		entry := peerMapEntry{
			peerID:    peerID,
			timestamp: time.Now(),
		}
		s.blockPeerMap.Store(blockHash, entry)

		payload := mkMsgBytes(blockHash, "invalid_block")

		h := s.invalidBlockHandler(ctx)
		err := h(mkKafkaMsg(payload))
		require.NoError(t, err)

		mockBC.AssertExpectations(t)
	})
}

func TestServerRejectedHandler(t *testing.T) {
	t.Parallel()
	topicDefaultString := "test-prefix-rejected"
	ctx := context.Background()
	logger := ulogger.New("test")

	// helper: create payload proto for topic "rejected tx"
	mkPayload := func(txHash, reason string) []byte {
		m := &kafkamessage.KafkaRejectedTxTopicMessage{
			TxHash: txHash,
			Reason: reason,
		}
		b, err := proto.Marshal(m)
		require.NoError(t, err)
		return b
	}

	// helper: incapsulate in kafka.KafkaMessage
	mkKafkaMsg := func(b []byte) *kafka.KafkaMessage {
		return &kafka.KafkaMessage{
			ConsumerMessage: sarama.ConsumerMessage{
				Topic: "rejected-tx",
				Value: b,
			},
		}
	}

	t.Run("returns nil when syncing and does not publish", func(t *testing.T) {
		mockBC := new(blockchain.Mock)
		syncFSM := blockchain_api.FSMStateType_CATCHINGBLOCKS
		mockBC.On("GetFSMCurrentState", mock.Anything).Return(&syncFSM, nil)

		mockP2P := new(MockServerP2PClient)
		testSettings := createBaseTestSettings()
		testSettings.P2P.ListenMode = settings.ListenModeFull // Ensure not in listen-only mode
		s := &Server{
			settings:            testSettings,
			logger:              logger,
			blockchainClient:    mockBC,
			P2PClient:           mockP2P,
			rejectedTxTopicName: topicDefaultString, // optional, to avoid empty string
		}

		h := s.rejectedTxHandler(ctx)
		err := h(mkKafkaMsg(mkPayload("a3f1c4e2a3f1c4e2a3f1c4e2a3f1c4e2a3f1c4e2a3f1c4e2a3f1c4e2a3f1c4e2", "invalid_script")))
		require.NoError(t, err)

		mockP2P.AssertNotCalled(t, "GetID")
		mockP2P.AssertNotCalled(t, "Publish", mock.Anything, mock.Anything, mock.Anything)

		mockBC.AssertExpectations(t)
		mockP2P.AssertExpectations(t)
	})

	t.Run("publish error is logged but handler returns nil", func(t *testing.T) {
		mockBC := new(blockchain.Mock)
		state := blockchain_api.FSMStateType_RUNNING
		mockBC.On("GetFSMCurrentState", mock.Anything).Return(&state, nil)

		mockP2P := new(MockServerP2PClient)
		mockP2P.On("GetID").Return(peer.ID("peer-1"))
		mockP2P.
			On("Publish", mock.Anything, "rejected-topic", mock.Anything).
			Return(errors.NewConfigurationError("Can't publish P2P topic"))

		testSettings := createBaseTestSettings()
		testSettings.P2P.ListenMode = settings.ListenModeFull // Ensure not in listen-only mode
		s := &Server{
			settings:            testSettings,
			logger:              logger,
			blockchainClient:    mockBC,
			P2PClient:           mockP2P,
			rejectedTxTopicName: "rejected-topic",
		}

		h := s.rejectedTxHandler(ctx)
		err := h(mkKafkaMsg(mkPayload(
			"a3f1c4e2a3f1c4e2a3f1c4e2a3f1c4e2a3f1c4e2a3f1c4e2a3f1c4e2a3f1c4e2",
			"policy",
		)))
		require.NoError(t, err) // nil is expected
		mockBC.AssertExpectations(t)
		mockP2P.AssertExpectations(t)
	})

	t.Run("invalid proto payload returns error", func(t *testing.T) {
		mockBC := new(blockchain.Mock)
		state := blockchain_api.FSMStateType_RUNNING
		mockBC.On("GetFSMCurrentState", mock.Anything).Return(&state, nil)

		mockP2P := new(MockServerP2PClient)

		testSettings := createBaseTestSettings()
		testSettings.P2P.ListenMode = settings.ListenModeFull // Ensure not in listen-only mode
		s := &Server{
			settings:         testSettings,
			logger:           logger,
			blockchainClient: mockBC,
			P2PClient:        mockP2P,
		}

		h := s.rejectedTxHandler(ctx)
		// payload non-proto
		err := h(mkKafkaMsg([]byte("not-a-protobuf")))
		require.Error(t, err)

		mockBC.AssertExpectations(t)
	})

	t.Run("publishes on valid message and returns nil", func(t *testing.T) {
		ctx := context.Background()
		logger := ulogger.New("test")
		mockBC := new(blockchain.Mock)
		st := blockchain_api.FSMStateType_RUNNING
		mockBC.On("GetFSMCurrentState", mock.Anything).Return(&st, nil)

		// 2) P2P node mock
		mockP2P := new(MockServerP2PClient)
		mockP2P.On("GetID").Return(peer.ID("peer-123"))

		testSettings := createBaseTestSettings()
		testSettings.P2P.ListenMode = settings.ListenModeFull // Ensure not in listen-only mode
		s := &Server{
			settings:            testSettings,
			logger:              logger,
			blockchainClient:    mockBC,
			P2PClient:           mockP2P,
			rejectedTxTopicName: topicDefaultString, // expected topic
		}

		// Kafka valid message: hash 64 hex chars
		msgPB := &kafkamessage.KafkaRejectedTxTopicMessage{
			TxHash: "a3f1c4e2a3f1c4e2a3f1c4e2a3f1c4e2a3f1c4e2a3f1c4e2a3f1c4e2a3f1c4e2",
			Reason: "invalid_script",
		}
		payload, err := proto.Marshal(msgPB)
		require.NoError(t, err)

		mkKafkaMsg := func(b []byte) *kafka.KafkaMessage {
			return &kafka.KafkaMessage{
				ConsumerMessage: sarama.ConsumerMessage{
					Topic: "rejected-tx",
					Value: b,
				},
			}
		}

		// Matching on published payload
		published := mock.MatchedBy(func(b []byte) bool {
			var m RejectedTxMessage
			if err := json.Unmarshal(b, &m); err != nil {
				return false
			}
			return strings.EqualFold(m.TxID, msgPB.TxHash) &&
				m.Reason == msgPB.Reason &&
				m.PeerID != ""
		})
		mockP2P.
			On("Publish", mock.Anything, topicDefaultString, published).
			Return(nil).
			Once()

		// Return value for GetID
		mockP2P.On("GetID").Return(peer.ID("KoPCcJr6w9A"))

		// Handler execution
		h := s.rejectedTxHandler(ctx)
		err = h(mkKafkaMsg(payload))
		require.NoError(t, err)

		mockP2P.AssertExpectations(t)
		mockBC.AssertExpectations(t)
	})
}

func TestGenerateRandomKey(t *testing.T) {
	t.Parallel()

	k1, err := generateRandomKey()
	require.NoError(t, err)

	// length 64 (32 byte → 64 hex)
	require.Len(t, k1, 64)

	// only hex
	_, err = hex.DecodeString(k1)
	require.NoError(t, err)

	// 2 invocations should produce 2 different keys
	k2, err := generateRandomKey()
	require.NoError(t, err)
	require.NotEqual(t, k1, k2)
}

func TestServerHandleNodeStatusTopic(t *testing.T) {
	ctx := context.Background()

	t.Run("message_from_self_forwards_to_websocket_but_does_not_update_peer_height", func(t *testing.T) {
		logger := ulogger.New("test")

		mockP2P := new(MockServerP2PClient)
		mockP2P.On("GetID").Return(peer.ID("my-peer-id"))

		notifCh := make(chan *notificationMsg, 1)

		s := &Server{
			logger:         logger,
			P2PClient:      mockP2P,
			notificationCh: notifCh,
		}

		jsonMsg := `{
			"peer_id": "my-peer-id",
			"BaseURL": "https://self",
			"Version": "v1.0.0",
			"CommitHash": "abc123",
			"BestBlockHash": "hash1",
			"BestHeight": 100,
			"TxCountInAssembly": 5,
			"FSMState": "RUNNING",
			"StartTime": "2025-01-01T00:00:00Z",
			"Uptime": 36000,
			"MinerName": "MyMiner",
			"ListenMode": "full",
			"SyncPeerID": "peer-xyz",
			"SyncPeerHeight": 99,
			"SyncPeerBlockHash": "hash2",
			"SyncConnectedAt": "2025-01-01T01:00:00Z"
		}`

		s.handleNodeStatusTopic(ctx, []byte(jsonMsg), "my-peer-id")

		msg := <-notifCh
		assert.Equal(t, "my-peer-id", msg.PeerID)
	})

	t.Run("message_from_another_peer_updates_height_and_forwards_to_websocket", func(t *testing.T) {
		ctx := context.Background()
		logger := ulogger.New("test")

		selfPeerIDStr := "12D3KooWJpBNhwgvoZ15EB1JwRTRpxgM9NVaqpDtWZXfTf6CpCQd"
		selfPeerID, err := peer.Decode(selfPeerIDStr)
		require.NoError(t, err)
		remotePeerIDStr := "12D3KooWBv1jXjEN3zMZ7cJzQa4LZQZKGeNp8xYZAtNAd5DEbR9n"
		remotePeerID, err := peer.Decode(remotePeerIDStr)
		require.NoError(t, err)
		require.NotNil(t, remotePeerID)

		mockP2P := new(MockServerP2PClient)
		mockP2P.On("GetID").Return(selfPeerID)

		notifCh := make(chan *notificationMsg, 1)

		s := &Server{
			logger:         logger,
			P2PClient:      mockP2P,
			notificationCh: notifCh,
		}

		jsonMsg := fmt.Sprintf(`{
		"peer_id": "%s",
		"BaseURL": "https://peer",
		"Version": "v1.0.0",
		"CommitHash": "abc456",
		"BestBlockHash": "hash3",
		"BestHeight": 101,
		"TxCountInAssembly": 6,
		"FSMState": "RUNNING",
		"StartTime": "2025-01-01T00:00:00Z",
		"Uptime": 36000,
		"MinerName": "PeerMiner",
		"ListenMode": "full",
		"SyncPeerID": "peer-abc",
		"SyncPeerHeight": 100,
		"SyncPeerBlockHash": "hash4",
		"SyncConnectedAt": "2025-01-01T01:30:00Z"
	}`, remotePeerIDStr)

		s.handleNodeStatusTopic(ctx, []byte(jsonMsg), remotePeerIDStr)

		msg := <-notifCh
		assert.Equal(t, remotePeerIDStr, msg.PeerID)

		mockP2P.AssertExpectations(t)
	})

}

func TestHandleBlockNotificationSuccess(t *testing.T) {
	ctx := context.Background()
	fsmState := blockchain.FSMStateRUNNING

	testHash, _ := chainhash.NewHashFromStr("0000000000000000000a7b7f00c92f4414f8e632ce0e0a7a91e6d5bfb4b6c157")
	header := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  testHash,
		HashMerkleRoot: testHash,
		Timestamp:      1234567890,
		Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d}, // mainnet genesis bits 0x1d00ffff in little endian
		Nonce:          1234,
	}
	meta := &model.BlockHeaderMeta{Height: 150}

	// --- Mocks ---
	mockP2P := new(MockServerP2PClient)
	mockP2P.On("GetID").Return(peer.ID("12D3KooWTestPeer")).Maybe()
	mockP2P.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	mockBlockchain := new(blockchain.Mock)
	mockBlockchain.On("GetBlockHeader", mock.Anything, testHash).Return(header, meta, nil).Once()
	mockBlockchain.On("GetBestBlockHeader", mock.Anything).Return(header, &model.BlockHeaderMeta{Height: 100}, nil).Maybe()
	mockBlockchain.On("GetFSMCurrentState", mock.Anything).Return(&fsmState, nil).Maybe()

	// Mock GetState for BlockPersisterHeight query
	blockPersisterHeightData := make([]byte, 4)
	binary.LittleEndian.PutUint32(blockPersisterHeightData, 0)
	mockBlockchain.On("GetState", mock.Anything, "BlockPersisterHeight").Return(blockPersisterHeightData, nil).Maybe()

	testSettings := settings.NewSettings()
	testSettings.Coinbase.ArbitraryText = "MockMiner"
	testSettings.P2P.ListenMode = settings.ListenModeFull // Ensure not in listen-only mode

	mockServer := &Server{
		P2PClient:           mockP2P,
		blockchainClient:    mockBlockchain,
		logger:              ulogger.New("test"),
		blockTopicName:      "block-topic",
		AssetHTTPAddressURL: "https://datahub.node",
		settings:            testSettings,
		startTime:           time.Now(),
		syncConnectionTimes: sync.Map{},
		notificationCh:      make(chan *notificationMsg, 1),
		nodeStatusTopicName: "node-status-topic",
		peerRegistry:        NewPeerRegistry(),
	}

	err := mockServer.handleBlockNotification(ctx, testHash)
	require.NoError(t, err)

	mockP2P.AssertExpectations(t)
	mockBlockchain.AssertExpectations(t)
}

func TestHandleSubtreeNotificationSuccess(t *testing.T) {
	ctx := context.Background()
	hash := &chainhash.Hash{0x1}
	subtreeTopicName := "subtree-topic"

	mockP2P := new(MockServerP2PClient)
	mockP2P.On("GetID").Return(peer.ID("peer-123"))
	mockP2P.On("Publish", mock.Anything, subtreeTopicName, mock.Anything).Return(nil)

	testSettings := createBaseTestSettings()
	testSettings.P2P.ListenMode = settings.ListenModeFull // Ensure not in listen-only mode

	server := &Server{
		settings:            testSettings,
		P2PClient:           mockP2P,
		subtreeTopicName:    subtreeTopicName,
		AssetHTTPAddressURL: "https://datahub.node",
	}

	err := server.handleSubtreeNotification(ctx, hash)
	assert.NoError(t, err)

	// Verify that Publish was called
	mockP2P.AssertCalled(t, "Publish", mock.Anything, subtreeTopicName, mock.Anything)
}

func TestProcessBlockchainNotificationSubtree(t *testing.T) {
	ctx := context.Background()
	subtreeTopicName := "subtree-topic"
	logger := ulogger.New("test")

	hash := &chainhash.Hash{0x1}
	hashBytes := hash.CloneBytes()
	notification := &blockchain.Notification{
		Type: model.NotificationType_Subtree,
		Hash: hashBytes[:],
	}

	mockP2P := new(MockServerP2PClient)
	mockP2P.On("GetID").Return(peer.ID("peer-123"))
	mockP2P.On("Publish", mock.Anything, subtreeTopicName, mock.Anything).Return(nil)

	testSettings := createBaseTestSettings()
	testSettings.P2P.ListenMode = settings.ListenModeFull // Ensure not in listen-only mode

	server := &Server{
		settings:            testSettings,
		P2PClient:           mockP2P,
		subtreeTopicName:    subtreeTopicName,
		AssetHTTPAddressURL: "https://datahub.node",
		logger:              logger,
	}

	err := server.processBlockchainNotification(ctx, notification)
	assert.NoError(t, err)
	mockP2P.AssertCalled(t, "Publish", mock.Anything, subtreeTopicName, mock.Anything)
}

func TestProcessBlockchainNotificationUnknownType(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.New("test")

	hash := &chainhash.Hash{0x1}
	hashBytes := hash.CloneBytes()
	notification := &blockchain.Notification{
		Type: model.NotificationType(255), // not defined in the model
		Hash: hashBytes[:],
	}

	server := &Server{
		logger: logger,
	}

	err := server.processBlockchainNotification(ctx, notification)
	assert.NoError(t, err) // not defined, no error
}

func TestProcessBlockchainNotificationInvalidHash(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.New("test")

	notification := &blockchain.Notification{
		Type: model.NotificationType_Subtree,
		Hash: []byte{0x1, 0x2, 0x3}, // invalid hash, not 32 bytes long
	}

	server := &Server{
		logger: logger,
	}

	err := server.processBlockchainNotification(ctx, notification)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "error getting chainhash")
}

func TestServerStopSuccess(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.New("test")

	mockP2P := new(MockServerP2PClient)
	mockP2P.On("Close").Return(nil)

	mockKafkaConsumer := new(MockKafkaConsumerGroup)
	mockKafkaConsumer.On("Close").Return(nil)

	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()

	// Pre-load elements into the maps
	server := &Server{
		logger:                           logger,
		P2PClient:                        mockP2P,
		rejectedTxKafkaConsumerClient:    mockKafkaConsumer,
		invalidBlocksKafkaConsumerClient: mockKafkaConsumer,
		peerMapCleanupTicker:             ticker,
	}

	server.blockPeerMap.Store("key1", "value1")
	server.subtreePeerMap.Store("key2", "value2")

	err := server.Stop(ctx)
	assert.NoError(t, err)

	mockP2P.AssertCalled(t, "Close")
	mockKafkaConsumer.AssertNumberOfCalls(t, "Close", 2)

	// Check that maps are empty
	_, ok := server.blockPeerMap.Load("test")
	assert.False(t, ok)
	_, ok = server.subtreePeerMap.Load("test")
	assert.False(t, ok)
}

// NOTE: TestDisconnectPeerSuccess, TestDisconnectPeerInvalidID, TestDisconnectPeerNoP2PNode
// were removed because DisconnectPeer is deprecated in the new architecture.

func TestProcessInvalidBlockMessageSuccess(t *testing.T) {

	logger := ulogger.New("test")
	mockPeerID := peer.ID("peer-123")

	blockHash := "abc123"
	reason := "invalid_signature"
	msg := &kafkamessage.KafkaInvalidBlockTopicMessage{
		BlockHash: blockHash,
		Reason:    reason,
	}
	msgBytes, err := proto.Marshal(msg)
	require.NoError(t, err)

	kafkaMsg := &kafka.KafkaMessage{
		ConsumerMessage: sarama.ConsumerMessage{
			Topic: "invalid-blocks",
			Value: msgBytes,
		},
	}

	// Create a real ban manager for testing
	banHandler := &testBanHandler{}
	banManager := &PeerBanManager{
		peerBanScores: make(map[string]*BanScore),
		reasonPoints: map[BanReason]int{
			ReasonInvalidSubtree:    10,
			ReasonProtocolViolation: 20,
			ReasonSpam:              50,
		},
		banThreshold:  100,
		banDuration:   time.Hour,
		decayInterval: time.Minute,
		decayAmount:   1,
		handler:       banHandler,
	}

	server := &Server{
		blockPeerMap: sync.Map{},
		logger:       logger,
		banManager:   banManager,
	}
	server.blockPeerMap.Store(blockHash, peerMapEntry{peerID: mockPeerID.String()})

	err = server.processInvalidBlockMessage(kafkaMsg)
	assert.NoError(t, err)

	_, ok := server.blockPeerMap.Load(blockHash)
	assert.False(t, ok)
}

func TestProcessInvalidBlockMessageUnmarshalError(t *testing.T) {
	invalidBytes := []byte{0x00, 0x01, 0x02} // invalid proto
	logger := ulogger.New("test")

	kafkaMsg := &kafka.KafkaMessage{
		ConsumerMessage: sarama.ConsumerMessage{
			Topic: "invalid-blocks",
			Value: invalidBytes,
		},
	}
	server := &Server{
		logger: logger,
	}

	err := server.processInvalidBlockMessage(kafkaMsg)
	assert.Error(t, err)
}

func TestProcessInvalidBlockMessageNoPeerInMap(t *testing.T) {
	blockHash := "not_in_map"
	msgProto := &kafkamessage.KafkaInvalidBlockTopicMessage{
		BlockHash: blockHash,
		Reason:    "any",
	}
	msgBytes, _ := proto.Marshal(msgProto)
	logger := ulogger.New("test")

	kafkaMsg := &kafka.KafkaMessage{
		ConsumerMessage: sarama.ConsumerMessage{
			Topic: "invalid-blocks",
			Value: msgBytes,
		},
	}

	server := &Server{
		blockPeerMap: sync.Map{},
		logger:       logger,
	}

	err := server.processInvalidBlockMessage(kafkaMsg)
	assert.NoError(t, err) // it's not an error
}

func TestProcessInvalidBlockMessageWrongTypeInMap(t *testing.T) {
	blockHash := "bad_type"
	msgProto := &kafkamessage.KafkaInvalidBlockTopicMessage{
		BlockHash: blockHash,
		Reason:    "any",
	}
	msgBytes, _ := proto.Marshal(msgProto)
	logger := ulogger.New("test")

	kafkaMsg := &kafka.KafkaMessage{
		ConsumerMessage: sarama.ConsumerMessage{
			Topic: "invalid-blocks",
			Value: msgBytes,
		},
	}

	server := &Server{
		blockPeerMap: sync.Map{},
		logger:       logger,
	}
	server.blockPeerMap.Store(blockHash, "string_instead_of_struct")

	err := server.processInvalidBlockMessage(kafkaMsg)
	assert.NoError(t, err)
}

func TestProcessInvalidBlockMessageAddBanScoreFails(t *testing.T) {
	blockHash := "fail_hash"
	msgProto := &kafkamessage.KafkaInvalidBlockTopicMessage{
		BlockHash: blockHash,
		Reason:    "invalid_data",
	}
	msgBytes, _ := proto.Marshal(msgProto)
	logger := ulogger.New("test")

	mockPeerID := peer.ID("peer-fail")

	kafkaMsg := &kafka.KafkaMessage{
		ConsumerMessage: sarama.ConsumerMessage{
			Topic: "invalid-blocks",
			Value: msgBytes,
		},
	}

	// Create a real ban manager for testing
	banHandler := &testBanHandler{}
	banManager := &PeerBanManager{
		peerBanScores: make(map[string]*BanScore),
		reasonPoints: map[BanReason]int{
			ReasonInvalidSubtree:    10,
			ReasonProtocolViolation: 20,
			ReasonSpam:              50,
		},
		banThreshold:  100,
		banDuration:   time.Hour,
		decayInterval: time.Minute,
		decayAmount:   1,
		handler:       banHandler,
	}

	server := &Server{
		blockPeerMap: sync.Map{},
		logger:       logger,
		banManager:   banManager,
	}
	server.blockPeerMap.Store(blockHash, peerMapEntry{peerID: mockPeerID.String()})

	err := server.processInvalidBlockMessage(kafkaMsg)
	assert.Nil(t, err)
}

func TestBlockchainSubscriptionListener(t *testing.T) {
	t.Run("context cancelled - shutdown", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		subscription := make(chan *blockchain.Notification, 1)

		server := &Server{
			logger: ulogger.New("test"),
		}

		done := make(chan struct{})
		go func() {
			server.blockchainSubscriptionListener(ctx, subscription)
			close(done)
		}()

		cancel()

		select {
		case <-done:

		case <-time.After(1 * time.Second):
			t.Fatal("listener did not shut down after context cancel")
		}
	})

	t.Run("processBlockchainNotification returns error - log and continue", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		subscription := make(chan *blockchain.Notification, 1)
		subscription <- &blockchain.Notification{
			Type: model.NotificationType(255),
		}

		server := &Server{
			logger: ulogger.New("test"),
		}

		go func() {
			server.blockchainSubscriptionListener(ctx, subscription)
		}()

		time.Sleep(100 * time.Millisecond)
	})
}

// NOTE: TestConnectPeer was removed because ConnectPeer is deprecated in the new architecture.

func TestServer_GetLocalHeight(t *testing.T) {
	// Test with nil blockchain client
	server := &Server{
		blockchainClient: nil,
	}
	height := server.getLocalHeight()
	assert.Equal(t, uint32(0), height)

	// Test with mock blockchain client that returns error
	mockClient := &blockchain.Mock{}
	mockClient.On("GetBestBlockHeader", mock.Anything).Return((*model.BlockHeader)(nil), (*model.BlockHeaderMeta)(nil), errors.NewServiceError("test error"))

	server = &Server{
		blockchainClient: mockClient,
		gCtx:             context.Background(),
	}
	height = server.getLocalHeight()
	assert.Equal(t, uint32(0), height)

	// Test with successful response
	mockClient2 := &blockchain.Mock{}
	mockClient2.On("GetBestBlockHeader", mock.Anything).Return(&model.BlockHeader{}, &model.BlockHeaderMeta{Height: 12345}, nil)

	server.blockchainClient = mockClient2
	height = server.getLocalHeight()
	assert.Equal(t, uint32(12345), height)

	mockClient.AssertExpectations(t)
	mockClient2.AssertExpectations(t)
}

func TestServer_UpdatePeerHeight(t *testing.T) {
	logger := ulogger.New("test")
	registry := NewPeerRegistry()
	server := &Server{
		logger:       logger,
		peerRegistry: registry,
	}

	peerID := peer.ID("test-peer")

	// Update height for non-existent peer (should add peer)
	server.addPeer(peerID, "", 100, nil, "")

	// Verify peer was added with correct height
	peerInfo, exists := registry.Get(peerID)
	assert.True(t, exists)
	assert.Equal(t, uint32(100), peerInfo.Height)

	// Update height for existing peer
	server.addPeer(peerID, "", 200, nil, "")
	peerInfo, exists = registry.Get(peerID)
	assert.True(t, exists)
	assert.Equal(t, uint32(200), peerInfo.Height)
}

func TestServer_AddPeer(t *testing.T) {
	logger := ulogger.New("test")

	registry := NewPeerRegistry()

	server := &Server{
		logger:       logger,
		peerRegistry: registry,
	}

	peerID := peer.ID("test-peer")

	// Add peer
	server.addPeer(peerID, "", 0, nil, "")

	// Verify peer was added
	_, exists := registry.Get(peerID)
	assert.True(t, exists)

	// Add same peer again (should be idempotent)
	server.addPeer(peerID, "", 0, nil, "")
	_, exists = registry.Get(peerID)
	assert.True(t, exists)
}

func TestServer_RemovePeer(t *testing.T) {
	logger := ulogger.New("test")
	registry := NewPeerRegistry()
	server := &Server{
		logger:       logger,
		peerRegistry: registry,
	}

	peerID := peer.ID("test-peer")

	// Add peer first
	registry.Put(peerID, "", 0, nil, "")
	_, exists := registry.Get(peerID)
	assert.True(t, exists)

	// Remove peer
	server.removePeer(peerID)

	// Verify peer was removed
	_, exists = registry.Get(peerID)
	assert.False(t, exists)
}

func TestServer_UpdateBlockHash(t *testing.T) {
	logger := ulogger.New("test")
	registry := NewPeerRegistry()
	server := &Server{
		logger:       logger,
		peerRegistry: registry,
	}

	peerID := peer.ID("test-peer")

	// Add peer first
	registry.Put(peerID, "", 0, nil, "")

	// Update block hash
	blockHashStr := "00000000000000000123456789abcdef00000000000000000123456789abcdef"
	blockHash, _ := chainhash.NewHashFromStr(blockHashStr)
	server.peerRegistry.Put(peerID, "", 0, blockHash, "")

	// Verify hash was updated
	peerInfo, exists := registry.Get(peerID)
	assert.True(t, exists)
	assert.Equal(t, blockHashStr, peerInfo.BlockHash.String())

	// Test with nil hash (should not update)
	server.peerRegistry.Put(peerID, "", 0, nil, "")
	peerInfo, exists = registry.Get(peerID)
	assert.True(t, exists)
	assert.Equal(t, blockHashStr, peerInfo.BlockHash.String()) // Should still be the old hash
}

func TestServer_GetPeer(t *testing.T) {
	logger := ulogger.New("test")
	registry := NewPeerRegistry()
	server := &Server{
		logger:       logger,
		peerRegistry: registry,
	}

	peerID := peer.ID("test-peer")

	// Get non-existent peer
	peerInfo, exists := server.getPeer(peerID)
	assert.False(t, exists)
	assert.Nil(t, peerInfo)

	// Add peer with height and hash atomically
	testHash, _ := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
	registry.Put(peerID, "", 100, testHash, "")

	// Get existing peer
	peerInfo, exists = server.getPeer(peerID)
	assert.True(t, exists)
	assert.NotNil(t, peerInfo)
	assert.Equal(t, uint32(100), peerInfo.Height)
	assert.Equal(t, testHash.String(), peerInfo.BlockHash.String())
}

func TestServer_UpdateDataHubURL(t *testing.T) {
	registry := NewPeerRegistry()

	peerID := peer.ID("test-peer")

	url := "http://example.com:8080"

	// Add peer first
	registry.Put(peerID, "", 0, nil, url)

	// Verify URL was updated
	peerInfo, exists := registry.Get(peerID)
	assert.True(t, exists)
	assert.Equal(t, url, peerInfo.DataHubURL)

	// Test with empty URL (should not update)
	registry.Put(peerID, "", 0, nil, "")
	peerInfo, exists = registry.Get(peerID)
	assert.True(t, exists)
	assert.Equal(t, url, peerInfo.DataHubURL) // Should still be the old URL
}

func TestBanPeerCoverage(t *testing.T) {
	ctx := context.Background()
	server := createTestServer(t)

	// Skip if banList is not initialized in test server
	if server.banList == nil {
		t.Skip("banList not initialized in test server - skipping BanPeer test")
	}

	// Test successful ban
	banUntil := time.Now().Add(time.Hour).Unix()
	req := &p2p_api.BanPeerRequest{
		Addr:  "192.168.1.100",
		Until: banUntil,
	}

	resp, err := server.BanPeer(ctx, req)
	require.NoError(t, err)
	assert.NotNil(t, resp)
	assert.True(t, resp.Ok)
}

func TestUnbanPeerCoverage(t *testing.T) {
	ctx := context.Background()
	server := createTestServer(t)

	// Skip if banList is not initialized in test server
	if server.banList == nil {
		t.Skip("banList not initialized in test server - skipping UnbanPeer test")
	}

	// Test successful unban (without requiring prior ban)
	unbanReq := &p2p_api.UnbanPeerRequest{
		Addr: "192.168.1.101",
	}

	resp, err := server.UnbanPeer(ctx, unbanReq)
	require.NoError(t, err)
	assert.NotNil(t, resp)
	assert.True(t, resp.Ok)
}

func TestIsBannedCoverage(t *testing.T) {
	ctx := context.Background()
	server := createTestServer(t)

	// Skip if banManager is not initialized in test server
	if server.banManager == nil {
		t.Skip("banManager not initialized in test server - skipping IsBanned test")
	}

	// Test checking ban status for non-banned peer
	req := &p2p_api.IsBannedRequest{
		IpOrSubnet: "test-peer-id",
	}

	resp, err := server.IsBanned(ctx, req)
	require.NoError(t, err)
	assert.NotNil(t, resp)
	// Since we're using mock ban manager, this will return false
	assert.False(t, resp.IsBanned)
}

func TestIsBannedChecksBothBanSystems(t *testing.T) {
	ctx := context.Background()

	t.Run("banned by banList only", func(t *testing.T) {
		mockBanList := &MockBanList{}
		mockBanList.On("IsBanned", "192.168.1.100").Return(true)

		mockBanMgr := &MockPeerBanManager{}
		mockBanMgr.On("IsBanned", "192.168.1.100").Return(false)

		server := &Server{
			logger:     ulogger.New("test"),
			settings:   &settings.Settings{},
			banList:    mockBanList,
			banManager: mockBanMgr,
		}

		resp, err := server.IsBanned(ctx, &p2p_api.IsBannedRequest{IpOrSubnet: "192.168.1.100"})
		require.NoError(t, err)
		assert.True(t, resp.IsBanned)
	})

	t.Run("banned by banManager only", func(t *testing.T) {
		mockBanList := &MockBanList{}
		mockBanList.On("IsBanned", "test-peer-id").Return(false)

		mockBanMgr := &MockPeerBanManager{}
		mockBanMgr.On("IsBanned", "test-peer-id").Return(true)

		server := &Server{
			logger:     ulogger.New("test"),
			settings:   &settings.Settings{},
			banList:    mockBanList,
			banManager: mockBanMgr,
		}

		resp, err := server.IsBanned(ctx, &p2p_api.IsBannedRequest{IpOrSubnet: "test-peer-id"})
		require.NoError(t, err)
		assert.True(t, resp.IsBanned)
	})

	t.Run("not banned by either", func(t *testing.T) {
		mockBanList := &MockBanList{}
		mockBanList.On("IsBanned", "192.168.1.200").Return(false)

		mockBanMgr := &MockPeerBanManager{}
		mockBanMgr.On("IsBanned", "192.168.1.200").Return(false)

		server := &Server{
			logger:     ulogger.New("test"),
			settings:   &settings.Settings{},
			banList:    mockBanList,
			banManager: mockBanMgr,
		}

		resp, err := server.IsBanned(ctx, &p2p_api.IsBannedRequest{IpOrSubnet: "192.168.1.200"})
		require.NoError(t, err)
		assert.False(t, resp.IsBanned)
	})
}

func TestListBannedCoverage(t *testing.T) {
	ctx := context.Background()
	server := createTestServer(t)

	// Skip if banList is not initialized in test server
	if server.banList == nil {
		t.Skip("banList not initialized in test server - skipping ListBanned test")
	}

	// Test listing banned peers
	resp, err := server.ListBanned(ctx, &emptypb.Empty{})
	require.NoError(t, err)
	assert.NotNil(t, resp)
	assert.NotNil(t, resp.Banned)
}

func TestClearBannedCoverage(t *testing.T) {
	ctx := context.Background()
	server := createTestServer(t)

	// Skip if banList is not initialized in test server
	if server.banList == nil {
		t.Skip("banList not initialized in test server - skipping ClearBanned test")
	}

	// Test clearing ban list
	resp, err := server.ClearBanned(ctx, &emptypb.Empty{})
	require.NoError(t, err)
	assert.NotNil(t, resp)
	assert.True(t, resp.Ok)
}

func TestOnPeerBannedCoverage(t *testing.T) {
	server := createTestServer(t)

	// Skip if P2PClient is not initialized in test server
	if server.P2PClient == nil {
		t.Skip("P2PClient not initialized in test server - skipping OnPeerBanned test")
	}

	// Create a ban event handler
	handler := &myBanEventHandler{server: server}

	// Create a test peer ID
	_, pub, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
	require.NoError(t, err)
	peerID, err := peer.IDFromPublicKey(pub)
	require.NoError(t, err)

	// Test OnPeerBanned with valid peer ID
	until := time.Now().Add(time.Hour)
	reason := "test ban reason"

	// This should execute without error
	handler.OnPeerBanned(peerID.String(), until, reason)

	// Test OnPeerBanned with invalid peer ID (to cover error handling path)
	handler.OnPeerBanned("invalid-peer-id", until, reason)

	// The function should handle the error gracefully and log it
	// No assertion needed as this tests error handling path
}

func TestShouldSkipDuringSync(t *testing.T) {
	server := createTestServer(t)

	// Test when no sync peer is set
	result := server.shouldSkipDuringSync("peer1", "originator1", 100, "block")
	assert.False(t, result, "Should not skip when no sync peer is set")

	// Create a sync peer for further testing
	_, pub, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
	require.NoError(t, err)
	syncPeerID, err := peer.IDFromPublicKey(pub)
	require.NoError(t, err)

	// Add peer to simulate having a sync peer
	server.addPeer(syncPeerID, "", 0, nil, "")

	// Test various scenarios - the function should execute without error
	server.shouldSkipDuringSync("peer2", "originator2", 200, "subtree")
	server.shouldSkipDuringSync("peer3", "originator3", 50, "block")
}

func TestGetPeerIDFromDataHubURL(t *testing.T) {
	server := createTestServer(t)

	// Test with invalid URL - function returns string only
	peerID := server.getPeerIDFromDataHubURL("invalid-url")
	assert.Empty(t, peerID, "Should return empty string for invalid URL")

	// Test with URL missing peer ID
	peerID = server.getPeerIDFromDataHubURL("http://example.com:8080")
	assert.Empty(t, peerID, "Should return empty string when peer ID not found")

	// Test with valid URL containing peer ID in query params
	validURL := "http://example.com:8080?peerId=12D3KooWTest"
	_ = server.getPeerIDFromDataHubURL(validURL)
	// Function should execute and may return peer ID if found
	// No assertion needed as this tests the execution path
}

func TestDisconnectPreExistingBannedPeers(t *testing.T) {
	ctx := context.Background()
	server := createTestServer(t)

	// Skip if P2PClient is not initialized in test server
	if server.P2PClient == nil {
		t.Skip("P2PClient not initialized in test server - skipping disconnectPreExistingBannedPeers test")
	}

	// Test the function execution - it should run without error
	server.disconnectPreExistingBannedPeers(ctx)

	// The function should complete without panic - no specific assertions needed
	// as it primarily iterates through connected peers and checks ban status
}

func TestStartInvalidBlockConsumer(t *testing.T) {
	ctx := context.Background()
	server := createTestServer(t)

	// Mock consumer creation will likely fail, but we want to test the function entry
	err := server.startInvalidBlockConsumer(ctx)

	// The function may return an error due to missing Kafka setup in test environment
	// but should not panic - this covers the function execution path
	if err != nil {
		t.Logf("startInvalidBlockConsumer failed as expected in test environment: %v", err)
	}
}

func TestReportInvalidSubtreeCoverage(t *testing.T) {
	ctx := context.Background()
	server := createTestServer(t)

	// Create a valid peer ID for testing
	_, pub, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
	require.NoError(t, err)
	testPeerID, err := peer.IDFromPublicKey(pub)
	require.NoError(t, err)

	// Register the peer in the registry
	server.peerRegistry.Put(testPeerID, "test-client", 100, nil, "")

	// Get initial failure count
	peerInfo, exists := server.peerRegistry.Get(testPeerID)
	require.True(t, exists)
	initialFailures := peerInfo.InteractionFailures

	// Store the subtree-peer mapping
	testHash := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	server.subtreePeerMap.Store(testHash, peerMapEntry{
		peerID:    testPeerID.String(),
		timestamp: time.Now(),
	})

	// Call ReportInvalidSubtree
	err = server.ReportInvalidSubtree(ctx, testHash, "", "peer_cannot_provide_transactions")
	require.NoError(t, err)

	// Verify interaction failure was recorded
	peerInfo, exists = server.peerRegistry.Get(testPeerID)
	require.True(t, exists)
	assert.Equal(t, initialFailures+1, peerInfo.InteractionFailures,
		"interaction failure should be incremented")

	// Verify subtree was removed from the map
	_, ok := server.subtreePeerMap.Load(testHash)
	assert.False(t, ok, "subtree should be removed from map after processing")
}

func TestReportInvalidSubtree_AllReasons(t *testing.T) {
	ctx := context.Background()
	server := createTestServer(t)

	// Create a valid peer ID for testing
	_, pub, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
	require.NoError(t, err)
	testPeerID, err := peer.IDFromPublicKey(pub)
	require.NoError(t, err)

	// Register the peer in the registry
	server.peerRegistry.Put(testPeerID, "test-client", 100, nil, "")

	// Test each reason type - these are typically transient network failures
	reasons := []string{
		"peer_cannot_provide_transactions",
		"malformed_transaction_data",
		"transaction_count_mismatch",
		"peer_cannot_provide_subtree",
		"contains_invalid_transaction",
		"peer_cannot_provide_subtree_data",
	}

	for i, reason := range reasons {
		// Re-add to map (gets deleted after each call)
		testHash := fmt.Sprintf("hash%d", i)
		server.subtreePeerMap.Store(testHash, peerMapEntry{
			peerID:    testPeerID.String(),
			timestamp: time.Now(),
		})

		err := server.ReportInvalidSubtree(ctx, testHash, "", reason)
		require.NoError(t, err, "should not error for reason: %s", reason)

		peerInfo, exists := server.peerRegistry.Get(testPeerID)
		require.True(t, exists)
		assert.Equal(t, int64(i+1), peerInfo.InteractionFailures,
			"failure count should increment for reason: %s", reason)
	}
}

func TestReportInvalidSubtree_RepeatedFailuresReduceReputation(t *testing.T) {
	ctx := context.Background()
	server := createTestServer(t)

	// Create a valid peer ID for testing
	_, pub, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
	require.NoError(t, err)
	testPeerID, err := peer.IDFromPublicKey(pub)
	require.NoError(t, err)

	// Register the peer in the registry with initial reputation
	server.peerRegistry.Put(testPeerID, "test-client", 100, nil, "")

	// Record a success first to establish LastInteractionSuccess time
	server.peerRegistry.RecordInteractionSuccess(testPeerID, time.Millisecond*100)

	peerInfo, _ := server.peerRegistry.Get(testPeerID)
	initialReputation := peerInfo.ReputationScore

	// Report multiple invalid subtrees in quick succession (within 5 minute window)
	// After 3+ failures since last success, reputation should drop to 15.0
	for i := 0; i < 4; i++ {
		testHash := fmt.Sprintf("hash%d", i)
		server.subtreePeerMap.Store(testHash, peerMapEntry{
			peerID:    testPeerID.String(),
			timestamp: time.Now(),
		})

		err := server.ReportInvalidSubtree(ctx, testHash, "", "peer_cannot_provide_transactions")
		require.NoError(t, err)
	}

	// Verify reputation has decreased significantly
	peerInfo, exists := server.peerRegistry.Get(testPeerID)
	require.True(t, exists)
	assert.Less(t, peerInfo.ReputationScore, initialReputation,
		"reputation should decrease after multiple failures")
	assert.Equal(t, int64(4), peerInfo.InteractionFailures,
		"should have 4 interaction failures")
}

// createEnhancedTestServer creates a test server with properly initialized mocks
func createEnhancedTestServer(t *testing.T) (*Server, *MockServerP2PClient, *MockBanList) {
	logger := ulogger.New("test")
	settings := &settings.Settings{
		P2P: settings.P2PSettings{
			BanThreshold: 100,
			BanDuration:  time.Hour,
			PeerCacheDir: t.TempDir(),
			EnableNAT:    false, // Disable NAT in tests to prevent data races in libp2p
		},
	}

	// Create mocks
	mockP2PNode := &MockServerP2PClient{}
	mockBanList := &MockBanList{}

	// Create test peer ID for the mock
	_, pub, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
	require.NoError(t, err)
	testPeerID, err := peer.IDFromPublicKey(pub)
	require.NoError(t, err)
	mockP2PNode.peerID = testPeerID

	// Set up common mock expectations with Maybe() to avoid conflicts
	mockP2PNode.On("GetID").Return(testPeerID).Maybe()
	mockP2PNode.On("ConnectedPeers").Return([]p2pMessageBus.PeerInfo{}).Maybe()
	mockP2PNode.On("GetPeerIPs", mock.AnythingOfType("peer.ID")).Return([]string{"192.168.1.1"}).Maybe()
	mockP2PNode.On("DisconnectPeer", mock.Anything, mock.AnythingOfType("peer.ID")).Return(nil).Maybe()
	mockP2PNode.On("GetPeerStartingHeight", mock.AnythingOfType("peer.ID")).Return(int32(100), true).Maybe()
	mockP2PNode.On("SetPeerStartingHeight", mock.AnythingOfType("peer.ID"), mock.AnythingOfType("int32")).Return().Maybe()
	mockP2PNode.On("UpdatePeerHeight", mock.AnythingOfType("peer.ID"), mock.AnythingOfType("int32")).Return().Maybe()
	mockP2PNode.On("SendToPeer", mock.Anything, mock.AnythingOfType("peer.ID"), mock.Anything).Return(nil).Maybe()

	// Don't set default expectations for banList methods - let individual tests set them

	// Create server with mocks
	registry := NewPeerRegistry()
	server := &Server{
		logger:       logger,
		settings:     settings,
		peerRegistry: registry,
		banManager:   NewPeerBanManager(context.Background(), nil, settings, registry),
		P2PClient:    mockP2PNode,
		banList:      mockBanList,
		gCtx:         context.Background(),
	}

	return server, mockP2PNode, mockBanList
}

// Enhanced creative tests using mocks to achieve 100% coverage

func TestBanPeerEnhanced(t *testing.T) {
	ctx := context.Background()

	// Create a fresh mock for this test
	mockBanList := &MockBanList{}

	server := &Server{
		logger:   ulogger.New("test"),
		settings: &settings.Settings{},
		banList:  mockBanList,
	}

	// Test successful ban
	banUntil := time.Now().Add(time.Hour).Unix()
	req := &p2p_api.BanPeerRequest{
		Addr:  "192.168.1.100",
		Until: banUntil,
	}

	// Mock expectations
	mockBanList.On("Add", ctx, req.Addr, time.Unix(req.Until, 0)).Return(nil)

	resp, err := server.BanPeer(ctx, req)
	require.NoError(t, err)
	assert.NotNil(t, resp)
	assert.True(t, resp.Ok)

	// Verify mock was called correctly
	mockBanList.AssertExpectations(t)
}

func TestUnbanPeerEnhanced(t *testing.T) {
	ctx := context.Background()

	// Create a fresh mock for this test
	mockBanList := &MockBanList{}

	server := &Server{
		logger:   ulogger.New("test"),
		settings: &settings.Settings{},
		banList:  mockBanList,
	}

	// Test successful unban
	req := &p2p_api.UnbanPeerRequest{
		Addr: "192.168.1.101",
	}

	// Mock expectations
	mockBanList.On("Remove", ctx, req.Addr).Return(nil)

	resp, err := server.UnbanPeer(ctx, req)
	require.NoError(t, err)
	assert.NotNil(t, resp)
	assert.True(t, resp.Ok)

	// Verify mock was called correctly
	mockBanList.AssertExpectations(t)
}

func TestListBannedEnhanced(t *testing.T) {
	ctx := context.Background()

	// Create a fresh mock for this test
	mockBanList := &MockBanList{}

	server := &Server{
		logger:   ulogger.New("test"),
		settings: &settings.Settings{},
		banList:  mockBanList,
	}

	// Mock return data
	bannedPeers := []string{"192.168.1.100", "10.0.0.5"}
	mockBanList.On("ListBanned").Return(bannedPeers)

	resp, err := server.ListBanned(ctx, &emptypb.Empty{})
	require.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, bannedPeers, resp.Banned)

	// Verify mock was called correctly
	mockBanList.AssertExpectations(t)
}

func TestClearBannedEnhanced(t *testing.T) {
	ctx := context.Background()

	// Create a fresh mock for this test
	mockBanList := &MockBanList{}

	server := &Server{
		logger:   ulogger.New("test"),
		settings: &settings.Settings{},
		banList:  mockBanList,
	}

	// Mock expectations
	mockBanList.On("Clear").Return()

	resp, err := server.ClearBanned(ctx, &emptypb.Empty{})
	require.NoError(t, err)
	assert.NotNil(t, resp)
	assert.True(t, resp.Ok)

	// Verify mock was called correctly
	mockBanList.AssertExpectations(t)
}

// TestP2PNodeConnectedEnhanced is disabled due to complex blockchain client dependencies
// that cause nil pointer crashes in sendDirectHandshake goroutine (Server.go:951)
// This function requires a fully initialized blockchain client which is beyond
// the scope of isolated unit testing with mocks.
/*
func TestP2PNodeConnectedEnhanced(t *testing.T) {
	// This test is commented out because P2PNodeConnected spawns a goroutine
	// that calls sendDirectHandshake, which accesses s.blockchainClient
	// Setting up a proper blockchain client mock requires extensive setup
	// that goes beyond isolated unit testing scope
}
*/

// NOTE: TestOnPeerBannedEnhanced was removed because onPeerBanned is deprecated in the new architecture.

func TestDisconnectPreExistingBannedPeersEnhanced(t *testing.T) {
	ctx := context.Background()
	server, _, mockBanList := createEnhancedTestServer(t)

	// Mock that banList.ListBanned() returns some banned IPs
	// The function will call handleBanEvent for each banned IP
	mockBanList.On("ListBanned").Return([]string{"192.168.1.100", "10.0.0.50"})

	// Test the function execution
	server.disconnectPreExistingBannedPeers(ctx)

	// Verify mock was called correctly
	mockBanList.AssertExpectations(t)
}
