package p2p

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/services/blockchain/blockchain_api"
	"github.com/bitcoin-sv/teranode/services/blockvalidation"
	"github.com/bitcoin-sv/teranode/services/p2p/p2p_api"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/kafka"
	kafkamessage "github.com/bitcoin-sv/teranode/util/kafka/kafka_message"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-p2p"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	ma "github.com/multiformats/go-multiaddr" // nolint:misspell
	"github.com/ordishs/go-utils/expiringmap"
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

func TestGetIPFromMultiaddr(t *testing.T) {
	s := &Server{}
	ctx := context.Background()

	tests := []struct {
		name     string
		maddr    string
		expected string
		nilIP    bool
		error    bool
	}{
		{
			name:     "valid ip4 address",
			maddr:    "/ip4/127.0.0.1/tcp/8333",
			expected: "127.0.0.1",
			nilIP:    false,
			error:    false,
		},
		{
			name:     "valid ip6 address",
			maddr:    "/ip6/::1/tcp/8333",
			expected: "::1",
			nilIP:    false,
			error:    false,
		},
		{
			name:     "invalid multiaddress format",
			maddr:    "invalid",
			expected: "",
			nilIP:    true,
			error:    true,
		},
		{
			name:     "no ip in multiaddress",
			maddr:    "/tcp/8333",
			expected: "",
			nilIP:    true,
			error:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var (
				maddr ma.Multiaddr // nolint:misspell
				err   error
			)

			// Try to create a multiaddr - this might fail for invalid formats
			maddr, err = ma.NewMultiaddr(tt.maddr) // nolint:misspell
			if tt.error {
				require.Error(t, err, "Expected error creating multiaddr")

				return // Skip further testing as we can't create a valid multiaddr
			}

			require.NoError(t, err)

			ip, err := s.getIPFromMultiaddr(ctx, maddr)
			require.NoError(t, err, "getIPFromMultiaddr should not return an error")

			if tt.nilIP {
				assert.Nil(t, ip, "Expected nil IP for %s", tt.name)
			} else {
				assert.NotNil(t, ip, "Expected non-nil IP for %s", tt.name)
				assert.Equal(t, tt.expected, ip.String(), "IP string representation should match")
			}
		})
	}
}

func TestResolveDNS(t *testing.T) {
	// This is an integration test that requires network connectivity
	// Skip if we're in a CI environment or if explicitly requested
	if testing.Short() {
		t.Skip("Skipping DNS resolution test in short mode")
	}

	// Create a server instance
	logger := ulogger.New("test-server")
	server := &Server{
		logger: logger,
	}

	// Test cases
	testCases := []struct {
		name        string
		inputAddr   string
		expectError bool
	}{
		{
			name:        "valid domain with IPv4",
			inputAddr:   "/dns4/example.com/tcp/8333",
			expectError: false,
		},
		{
			name:        "invalid domain",
			inputAddr:   "/dns4/this-is-an-invalid-domain-that-does-not-exist.test/tcp/8333",
			expectError: true,
		},
		{
			name:        "non-DNS multiaddr",
			inputAddr:   "/tcp/8333",
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Parse the multiaddr
			maddr, err := ma.NewMultiaddr(tc.inputAddr)
			require.NoError(t, err, "Failed to create multiaddr")

			// Call the function under test
			ctx := context.Background()
			ip, err := server.resolveDNS(ctx, maddr)

			// Check results
			if tc.expectError {
				assert.Error(t, err, "Expected an error for %s", tc.inputAddr)
				assert.Nil(t, ip, "Expected nil IP when there's an error")
			} else {
				if err != nil {
					// Only fail the test if we have confirmed connectivity
					// This makes the test more resilient to network issues
					t.Logf("DNS resolution failed but we won't fail the test: %v", err)
					t.Skip("Skipping due to possible network connectivity issues")
				} else {
					assert.NotNil(t, ip, "Expected a valid IP address")
					t.Logf("Resolved %s to IP: %s", tc.inputAddr, ip.String())
				}
			}
		})
	}

	// Now test the specific error cases in the function
	t.Run("empty address list", func(t *testing.T) {
		// Create a test context
		ctx := context.Background()

		// We'll use a valid multiaddr but we'll replace the resolver.Resolve result
		// This is a manual test to verify error handling
		maddr, err := ma.NewMultiaddr("/dns4/example.com/tcp/8333")
		require.NoError(t, err)

		// This test depends on the internal behaviour of the server.resolveDNS method
		// which uses madns.DefaultResolver.Resolve under the hood
		result, err := server.resolveDNS(ctx, maddr)

		// If DNS resolution failed for whatever reason, skip this test
		if err != nil && !strings.Contains(err.Error(), "no addresses found") {
			t.Skip("DNS resolution failed, skipping specific error case test")
		}

		// If the test gets this far and the resolution succeeded, log it
		if err == nil {
			t.Logf("DNS resolution succeeded where we expected failure: %v", result)
		}
	})
}

func TestServerHandlers(t *testing.T) {
	t.Run("Test stream handler behaviour", func(t *testing.T) {
		// Create a minimal Server for testing
		server := &Server{
			logger: ulogger.New("test-server", ulogger.WithLevel("ERROR")),
			gCtx:   context.Background(),
		}

		// Set up a flag to track if handleBlockTopic was called
		blockTopicHandlerCalled := false
		blockTopicMsg := []byte{}
		blockTopicSender := ""

		// Set up a test handler function to capture calls
		blockHandler := func(ctx context.Context, msg []byte, from string) {
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

	t.Run("Test sendp2p.HandshakeMessage behaviour", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		mockP2PNode := new(MockServerP2PNode)
		mockBlockchainClient := new(blockchain.Mock)

		pid, _ := peer.Decode("QmTestPeerID")
		mockP2PNode.On("HostID").Return(pid)

		// Create a valid BlockHeader with initialized fields
		prevHash, _ := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
		merkleRoot, _ := chainhash.NewHashFromStr("4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b")

		validHeader := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  prevHash,
			HashMerkleRoot: merkleRoot,
			Timestamp:      1231006505,
			Bits:           model.NBit{0x1d, 0x00, 0xff, 0xff},
			Nonce:          2083236893,
		}

		meta := &model.BlockHeaderMeta{Height: 123}
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).Return(validHeader, meta, nil)

		called := make(chan struct{}, 1)

		var publishedMsg []byte

		mockP2PNode.On("Publish", mock.Anything, "test-handshake-topic", mock.Anything).
			Return(nil).
			Run(func(args mock.Arguments) {
				publishedMsg = args.Get(2).([]byte)
				called <- struct{}{}
			})

		server := &Server{
			P2PNode:            mockP2PNode,
			blockchainClient:   mockBlockchainClient,
			logger:             ulogger.New("test-server"),
			handshakeTopicName: "test-handshake-topic",
			bitcoinProtocolID:  "test-agent",
			gCtx:               ctx,
		}
		server.sendHandshake(ctx)
		select {
		case <-called:
		case <-time.After(time.Second):
			t.Fatal("Publish was not called")
		}

		var hs p2p.HandshakeMessage

		err := json.Unmarshal(publishedMsg, &hs)
		assert.NoError(t, err)
		assert.Equal(t, p2p.MessageType("version"), hs.Type)
		assert.Equal(t, pid.String(), hs.PeerID)
		assert.Equal(t, uint32(123), hs.BestHeight)
		assert.NotEmpty(t, hs.BestHash)
		assert.Equal(t, "test-agent", hs.UserAgent)
		assert.Equal(t, uint64(0), hs.Services)
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
				bitcoinProtocolID:             fmt.Sprintf("teranode/bitcoin/%s", emptySettings.Version),
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
				bitcoinProtocolID:             fmt.Sprintf("teranode/bitcoin/%s", partialSettings.Version),
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
			completeSettings.P2P.BestBlockTopic = "best-block"
			completeSettings.P2P.BlockTopic = "block"
			completeSettings.P2P.MiningOnTopic = "mining-on"
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
				logger:            logger,
				settings:          completeSettings,
				blockchainClient:  mockBlockchainClient,
				bitcoinProtocolID: fmt.Sprintf("teranode/bitcoin/%s", completeSettings.Version),
				gCtx:              mockCtx,
				notificationCh:    make(chan *notificationMsg),
				banChan:           make(chan BanEvent),
				// Use a mock P2PNodeI but don't set expectations that need to be verified
				P2PNode: new(MockServerP2PNode),
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

func TestServerIntegration(t *testing.T) {
	t.Skip("Integration tests require more comprehensive mocking of dependencies")
}

func TestHandleBlockTopic(t *testing.T) {
	// Setup common test variables
	ctx := context.Background()

	t.Run("ignore message from self", func(t *testing.T) {
		// Create mock P2PNode
		mockP2PNode := new(MockServerP2PNode)
		selfPeerID, _ := peer.Decode("QmBannedPeerID")
		selfPeerIDStr := selfPeerID.String()
		mockP2PNode.On("HostID").Return(selfPeerID)

		// Add mock for GetPeerIPs to handle any peer ID
		mockP2PNode.On("GetPeerIPs", mock.AnythingOfType("peer.ID")).Return([]string{})
		// Create a spy banList that we can verify is NOT called
		// (since the method should return early for messages from self)
		mockBanList := new(MockBanList)

		// Create server with mocks
		server := &Server{
			P2PNode:        mockP2PNode,
			banList:        mockBanList,
			notificationCh: make(chan *notificationMsg, 10),
			logger:         ulogger.New("test-server"),
		}

		// Call the real handler method with message from self
		server.handleBlockTopic(ctx, []byte(`{"Hash":"000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f","Height":1,"DataHubURL":"http://example.com","PeerID":"QmBannedPeerID"}`), selfPeerIDStr)

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
		// Create mock P2PNode
		mockP2PNode := new(MockServerP2PNode)
		selfPeerID, _ := peer.Decode("QmBannedPeerID")
		bannedPeerIDStr := bannedPeerIDStr

		mockP2PNode.On("HostID").Return(selfPeerID)

		// Add mock for GetPeerIPs to handle any peer ID
		mockP2PNode.On("GetPeerIPs", mock.AnythingOfType("peer.ID")).Return([]string{bannedPeerIDStr})

		// Create mock banManager that returns true for the banned peer
		mockBanManager := new(MockPeerBanManager)
		mockBanManager.On("IsBanned", bannedPeerIDStr).Return(true)

		// Create logger
		logger := ulogger.New("test-server")

		// Create server with mock P2PNode and BanManager
		server := &Server{
			P2PNode:        mockP2PNode,
			notificationCh: make(chan *notificationMsg, 10),
			banManager:     mockBanManager,
			logger:         logger,
		}

		// Call the real handler method with message from banned peer
		server.handleBlockTopic(ctx, []byte(`{"Hash":"000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f","Height":1,"DataHubURL":"http://example.com","PeerID":"QmValidPeerID"}`), bannedPeerIDStr)

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
		// Create mock P2PNode
		mockP2PNode := new(MockServerP2PNode)
		selfPeerID, _ := peer.Decode("QmBannedPeerID")
		mockP2PNode.On("HostID").Return(selfPeerID)

		// Create mock ban list
		mockBanList := new(MockBanList)

		// Create logger
		logger := ulogger.New("test-server")

		// Create server with mock P2PNode
		server := &Server{
			P2PNode:        mockP2PNode,
			notificationCh: make(chan *notificationMsg, 10),
			logger:         logger,
			banList:        mockBanList,
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
		// Create mock P2PNode
		mockP2PNode := new(MockServerP2PNode)
		selfPeerID, _ := peer.Decode("QmBannedPeerID")
		mockP2PNode.On("HostID").Return(selfPeerID)

		// Create a mock banManager that returns false for any peer
		mockBanManager := new(MockPeerBanManager)
		mockBanManager.On("IsBanned", mock.Anything).Return(false)

		// Create logger
		logger := ulogger.New("test-server")

		// Create server with mock P2PNode and BanManager
		server := &Server{
			P2PNode:        mockP2PNode,
			notificationCh: make(chan *notificationMsg, 10),
			banManager:     mockBanManager,
			logger:         logger,
		}

		// Call the real handler method with invalid hash
		server.handleBlockTopic(ctx, []byte(`{"Hash":"invalid-hash","Height":1,"DataHubURL":"http://example.com","PeerID":"QmValidPeerID"}`), "other-peer-id")

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
		// Create mock P2PNode
		mockP2PNode := new(MockServerP2PNode)
		selfPeerID, _ := peer.Decode("QmBannedPeerID")
		mockP2PNode.On("HostID").Return(selfPeerID)

		// Create a mock banManager that returns false for any peer
		mockBanManager := new(MockPeerBanManager)
		mockBanManager.On("IsBanned", mock.Anything).Return(false)

		// Create mock kafka producer
		mockKafkaProducer := new(MockKafkaProducer)
		mockKafkaProducer.On("Publish", mock.Anything).Return()

		// Create server with mocks
		server := &Server{
			P2PNode:                   mockP2PNode,
			notificationCh:            make(chan *notificationMsg, 10),
			blocksKafkaProducerClient: mockKafkaProducer,
			banManager:                mockBanManager,
			logger:                    ulogger.New("test-server"),
		}

		// Call the real handler with valid block hash
		// Since we can't mock out proto.Marshal, we'll need to allow an error here
		// or create a proper test implementation that doesn't use proto.Marshal
		server.handleBlockTopic(ctx, []byte(`{"Hash":"000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f","Height":1,"DataHubURL":"http://example.com","PeerID":"QmValidPeerID"}`), "other-peer-id")

		// Verify notification was sent
		select {
		case notification := <-server.notificationCh:
			assert.Equal(t, "block", notification.Type)
			assert.Equal(t, "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f", notification.Hash)
		default:
			t.Fatal("Expected notification message but none received")
		}
	})
}

func TestHandleSubtreeTopic(t *testing.T) {
	// Setup common test variables
	ctx := context.Background()

	t.Run("happy_path_-_successful_handling", func(t *testing.T) {
		// Create mock P2PNode
		mockP2PNode := new(MockServerP2PNode)
		selfPeerID, _ := peer.Decode("QmSelfPeerID")
		mockP2PNode.On("HostID").Return(selfPeerID)

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

		// Create server with mocks
		// Create settings with blacklisted URLs
		tSettings := createBaseTestSettings()
		tSettings.SubtreeValidation.BlacklistedBaseURLs = map[string]struct{}{
			"http://evil.com": {},
		}

		server := &Server{
			P2PNode:                    mockP2PNode,
			notificationCh:             make(chan *notificationMsg, 10),
			subtreeKafkaProducerClient: mockKafkaProducer,
			banManager:                 mockBanManager,
			settings:                   tSettings,
			logger:                     ulogger.New("test-server"),
		}

		// Call the method with a valid message from another peer
		validSubtreeMessage := `{"Hash":"000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f","DataHubURL":"http://example.com","PeerID":"QmcqHnEQuFdvxoRax8V9qjvHnqF2TpJ8nt8PNGJRRsKKg5"}`
		server.handleSubtreeTopic(ctx, []byte(validSubtreeMessage), validPeerID)

		// Verify notification was sent to the notification channel
		select {
		case notification := <-server.notificationCh:
			assert.Equal(t, "subtree", notification.Type)
			assert.Equal(t, "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f", notification.Hash)
			assert.Equal(t, "http://example.com", notification.BaseURL)
			assert.Equal(t, "QmcqHnEQuFdvxoRax8V9qjvHnqF2TpJ8nt8PNGJRRsKKg5", notification.PeerID)
		default:
			t.Fatal("Expected notification message but none received")
		}

		// Verify ban check was performed
		mockBanManager.AssertCalled(t, "IsBanned", validPeerID)

		// Verify Kafka publish was called since it's not from self or banned peer
		mockKafkaProducer.AssertCalled(t, "Publish", mock.Anything)
	})
}

// TestReceiveBestBlockStreamHandler tests the receiveBestBlockStreamHandler function
func TestReceiveBestBlockStreamHandler(t *testing.T) {
	// Create subtests for different scenarios
	t.Run("successful stream handling", func(t *testing.T) {
		// Create mock P2PNode
		mockP2PNode := new(MockServerP2PNode)
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
		mockStream.On("Conn").Return(network.Conn(mockConn)).Maybe()

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

func TestGetPeers(t *testing.T) {
	t.Run("returns_connected_peers_with_private_addresses_allowed", func(t *testing.T) {
		// Create mock dependencies
		logger := ulogger.New("test-server")
		mockP2PNode := new(MockServerP2PNode)

		// Generate a valid peer ID
		privKey, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
		require.NoError(t, err)

		peerID, err := peer.IDFromPrivateKey(privKey)
		require.NoError(t, err)

		// Create a peer with test IP (private IP)
		validIP := "192.168.1.20"
		addr, err := ma.NewMultiaddr(fmt.Sprintf("/ip4/%s/tcp/8333", validIP))
		require.NoError(t, err)

		peerInfo := p2p.PeerInfo{
			ID:    peerID,
			Addrs: []ma.Multiaddr{addr},
		}

		// Setup mock to return our test peer
		mockP2PNode.On("ConnectedPeers").Return([]p2p.PeerInfo{peerInfo})
		mockP2PNode.On("HostID").Return(peer.ID("QmServerID"))

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

		// Create server with SharePrivateAddresses set to true
		testSettings := &settings.Settings{}
		testSettings.P2P.SharePrivateAddresses = true // Allow private addresses

		server := &Server{
			P2PNode:    mockP2PNode,
			logger:     logger,
			settings:   testSettings,
			banManager: banManager,
		}

		// Call GetPeers
		resp, err := server.GetPeers(context.Background(), &emptypb.Empty{})

		// Verify
		require.NoError(t, err)
		require.NotNil(t, resp)

		// We should get exactly 1 peer in the response with private IP
		require.Len(t, resp.Peers, 1, "Should have one peer in the response")
		require.Contains(t, resp.Peers[0].Addr, validIP)

		// Verify calls to mocks
		mockP2PNode.AssertCalled(t, "ConnectedPeers")
	})

	t.Run("returns_all_peers_regardless_of_share_private_setting", func(t *testing.T) {
		// Create mock dependencies
		logger := ulogger.New("test-server")
		mockP2PNode := new(MockServerP2PNode)

		// Generate peer IDs
		privKey1, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
		require.NoError(t, err)
		peerID1, err := peer.IDFromPrivateKey(privKey1)
		require.NoError(t, err)

		privKey2, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
		require.NoError(t, err)
		peerID2, err := peer.IDFromPrivateKey(privKey2)
		require.NoError(t, err)

		// Create peers with private and public IPs
		privateAddr, err := ma.NewMultiaddr("/ip4/192.168.1.20/tcp/8333")
		require.NoError(t, err)
		publicAddr, err := ma.NewMultiaddr("/ip4/8.8.8.8/tcp/8333")
		require.NoError(t, err)

		peerInfo1 := p2p.PeerInfo{
			ID:    peerID1,
			Addrs: []ma.Multiaddr{privateAddr}, // Only private address
		}

		peerInfo2 := p2p.PeerInfo{
			ID:    peerID2,
			Addrs: []ma.Multiaddr{publicAddr}, // Public address
		}

		// Setup mock to return both peers
		mockP2PNode.On("ConnectedPeers").Return([]p2p.PeerInfo{peerInfo1, peerInfo2})
		mockP2PNode.On("HostID").Return(peer.ID("QmServerID"))

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

		// Create server with SharePrivateAddresses set to false
		testSettings := &settings.Settings{}
		testSettings.P2P.SharePrivateAddresses = false // Disable private addresses

		server := &Server{
			P2PNode:    mockP2PNode,
			logger:     logger,
			settings:   testSettings,
			banManager: banManager,
		}

		// Call GetPeers
		resp, err := server.GetPeers(context.Background(), &emptypb.Empty{})

		// Verify
		require.NoError(t, err)
		require.NotNil(t, resp)

		// We should get all connected peers regardless of SharePrivateAddresses setting
		// GetPeers always returns all connected peers for monitoring purposes
		require.Len(t, resp.Peers, 2, "Should have all connected peers")

		// Both peers should be present
		addrs := []string{resp.Peers[0].Addr, resp.Peers[1].Addr}
		assert.Contains(t, addrs, "/ip4/192.168.1.20/tcp/8333")
		assert.Contains(t, addrs, "/ip4/8.8.8.8/tcp/8333")

		// Verify calls to mocks
		mockP2PNode.AssertCalled(t, "ConnectedPeers")
	})

	t.Run("returns_first_address_when_peer_has_multiple", func(t *testing.T) {
		// Create mock dependencies
		logger := ulogger.New("test-server")
		mockP2PNode := new(MockServerP2PNode)

		// Generate a valid peer ID
		privKey, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
		require.NoError(t, err)

		peerID, err := peer.IDFromPrivateKey(privKey)
		require.NoError(t, err)

		// Create a peer with both private and public addresses
		privateAddr, err := ma.NewMultiaddr("/ip4/192.168.1.20/tcp/8333")
		require.NoError(t, err)
		publicAddr, err := ma.NewMultiaddr("/ip4/1.2.3.4/tcp/8333")
		require.NoError(t, err)

		peerInfo := p2p.PeerInfo{
			ID:    peerID,
			Addrs: []ma.Multiaddr{privateAddr, publicAddr},
		}

		// Setup mock to return our test peer
		mockP2PNode.On("ConnectedPeers").Return([]p2p.PeerInfo{peerInfo})
		mockP2PNode.On("HostID").Return(peer.ID("QmServerID"))

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

		// Create server with SharePrivateAddresses disabled
		testSettings := &settings.Settings{}
		testSettings.P2P.SharePrivateAddresses = false // Disable private addresses

		server := &Server{
			P2PNode:    mockP2PNode,
			logger:     logger,
			settings:   testSettings,
			banManager: banManager,
		}

		// Call GetPeers
		resp, err := server.GetPeers(context.Background(), &emptypb.Empty{})

		// Verify
		require.NoError(t, err)
		require.NotNil(t, resp)

		// GetPeers returns the first available address without preference
		require.Len(t, resp.Peers, 1, "Should have one peer in the response")
		// The first address in the list is returned (private in this case)
		require.Equal(t, "/ip4/192.168.1.20/tcp/8333", resp.Peers[0].Addr, "Should return first address")

		// Verify calls to mocks
		mockP2PNode.AssertCalled(t, "ConnectedPeers")
	})

	t.Run("handles_empty_peer_list", func(t *testing.T) {
		// Setup
		logger := ulogger.New("test-server")
		mockP2PNode := new(MockServerP2PNode)

		// Setup mock to return empty peer list
		mockP2PNode.On("ConnectedPeers").Return([]p2p.PeerInfo{})
		mockP2PNode.On("HostID").Return(peer.ID("QmServerID"))

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

		// Create server with mocks including ban manager
		server := &Server{
			P2PNode:    mockP2PNode,
			logger:     logger,
			settings:   &settings.Settings{},
			banManager: banManager,
		}

		// Call GetPeers with context and empty request
		ctx := context.Background()
		resp, err := server.GetPeers(ctx, &emptypb.Empty{})
		require.NoError(t, err)

		// Verify response
		require.NotNil(t, resp)
		require.Empty(t, resp.Peers, "Should have empty peer list")

		// Verify calls to mocks
		mockP2PNode.AssertCalled(t, "ConnectedPeers")
	})

	t.Run("handles_peer_with_no_addresses", func(t *testing.T) {
		// Setup
		logger := ulogger.New("test-server")
		mockP2PNode := new(MockServerP2PNode)

		// Generate a valid peer ID
		privKey, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
		require.NoError(t, err)
		peerID, err := peer.IDFromPrivateKey(privKey)
		require.NoError(t, err)

		// Create a peer with nil addresses instead of empty slice to match implementation behaviour
		peerInfo := p2p.PeerInfo{
			ID:    peerID,
			Addrs: nil, // Using nil instead of empty slice
		}

		// Setup mock to return our test peer
		mockP2PNode.On("ConnectedPeers").Return([]p2p.PeerInfo{peerInfo})
		mockP2PNode.On("HostID").Return(peer.ID("QmServerID"))

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

		// Create server with mocks including ban manager
		server := &Server{
			P2PNode:    mockP2PNode,
			logger:     logger,
			settings:   &settings.Settings{},
			banManager: banManager,
		}

		// Call GetPeers with context and empty request
		ctx := context.Background()
		resp, err := server.GetPeers(ctx, &emptypb.Empty{})
		require.NoError(t, err)

		// Verify response - peer with nil address list should be skipped
		require.NotNil(t, resp)
		require.Empty(t, resp.Peers, "Should have empty peer list since the peer has no addresses")

		// Verify calls to mocks
		mockP2PNode.AssertCalled(t, "ConnectedPeers")
	})

	t.Run("populates_connection_time", func(t *testing.T) {
		// Setup
		logger := ulogger.New("test-server")
		mockP2PNode := new(MockServerP2PNode)

		// Generate a valid peer ID
		privKey, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
		require.NoError(t, err)
		peerID, err := peer.IDFromPrivateKey(privKey)
		require.NoError(t, err)

		// Create a test connection time
		connTime := time.Now().Add(-5 * time.Minute) // Connected 5 minutes ago

		// Create a peer with test IP and connection time (use public IP for test)
		validIP := "8.8.8.8"
		addr, err := ma.NewMultiaddr(fmt.Sprintf("/ip4/%s/tcp/8333", validIP))
		require.NoError(t, err)

		peerInfo := p2p.PeerInfo{
			ID:            peerID,
			Addrs:         []ma.Multiaddr{addr},
			CurrentHeight: 12345,
			ConnTime:      &connTime,
		}

		// Setup mock to return our test peer
		mockP2PNode.On("ConnectedPeers").Return([]p2p.PeerInfo{peerInfo})
		mockP2PNode.On("HostID").Return(peer.ID("QmServerID"))

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

		// Create server with mocks including ban manager
		server := &Server{
			P2PNode:    mockP2PNode,
			logger:     logger,
			settings:   &settings.Settings{},
			banManager: banManager,
		}

		// Call GetPeers
		resp, err := server.GetPeers(context.Background(), &emptypb.Empty{})

		// Verify
		require.NoError(t, err)
		require.NotNil(t, resp)

		// We should get exactly 1 peer in the response
		require.Len(t, resp.Peers, 1, "Should have one peer in the response")

		// Verify the connection time was properly converted to Unix timestamp
		peer := resp.Peers[0]
		require.Equal(t, connTime.Unix(), peer.ConnTime, "Connection time should be converted to Unix timestamp")
		require.Equal(t, peerInfo.CurrentHeight, peer.CurrentHeight, "Height should match")
		require.Contains(t, peer.Addr, validIP, "Address should contain the IP")

		// Verify calls to mocks
		mockP2PNode.AssertCalled(t, "ConnectedPeers")
	})

	t.Run("handles_nil_connection_time", func(t *testing.T) {
		// Setup
		logger := ulogger.New("test-server")
		mockP2PNode := new(MockServerP2PNode)

		// Generate a valid peer ID
		privKey, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
		require.NoError(t, err)
		peerID, err := peer.IDFromPrivateKey(privKey)
		require.NoError(t, err)

		// Create a peer with nil connection time (use public IP for test)
		validIP := "1.2.3.4"
		addr, err := ma.NewMultiaddr(fmt.Sprintf("/ip4/%s/tcp/8333", validIP))
		require.NoError(t, err)

		peerInfo := p2p.PeerInfo{
			ID:            peerID,
			Addrs:         []ma.Multiaddr{addr},
			CurrentHeight: 12345,
			ConnTime:      nil, // No connection time set
		}

		// Setup mock to return our test peer
		mockP2PNode.On("ConnectedPeers").Return([]p2p.PeerInfo{peerInfo})
		mockP2PNode.On("HostID").Return(peer.ID("QmServerID"))

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

		// Create server with mocks including ban manager
		server := &Server{
			P2PNode:    mockP2PNode,
			logger:     logger,
			settings:   &settings.Settings{},
			banManager: banManager,
		}

		// Call GetPeers
		resp, err := server.GetPeers(context.Background(), &emptypb.Empty{})

		// Verify
		require.NoError(t, err)
		require.NotNil(t, resp)

		// We should get exactly 1 peer in the response
		require.Len(t, resp.Peers, 1, "Should have one peer in the response")

		// Verify the connection time is 0 when nil
		peer := resp.Peers[0]
		require.Equal(t, int64(0), peer.ConnTime, "Connection time should be 0 when nil")
		require.Equal(t, peerInfo.CurrentHeight, peer.CurrentHeight, "Height should match")
		require.Contains(t, peer.Addr, validIP, "Address should contain the IP")

		// Verify calls to mocks
		mockP2PNode.AssertCalled(t, "ConnectedPeers")
	})
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
	t.Run("non-add_ban_event", func(t *testing.T) {
		// Setup
		logger := ulogger.New("test-server")
		mockP2PNode := new(MockServerP2PNode)
		server := &Server{
			P2PNode: mockP2PNode,
			logger:  logger,
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
		mockP2PNode := new(MockServerP2PNode)
		server := &Server{
			P2PNode: mockP2PNode,
			logger:  logger,
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
		mockP2PNode := new(MockServerP2PNode)

		// Create server with mocks
		server := &Server{
			P2PNode: mockP2PNode,
			logger:  logger,
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
		// Create a server with mocked P2PNode
		logger := ulogger.New("test-server")
		mockP2PNode := new(MockServerP2PNode)

		server := &Server{
			P2PNode:         mockP2PNode,
			logger:          logger,
			peerBlockHashes: sync.Map{},
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

		peer1 := p2p.PeerInfo{
			ID:    peerID1,
			Addrs: []ma.Multiaddr{addr1},
		}
		peer2 := p2p.PeerInfo{
			ID:    peerID2,
			Addrs: []ma.Multiaddr{addr2},
		}

		// Setup mocks - return both peers as connected
		mockP2PNode.On("ConnectedPeers").Return([]p2p.PeerInfo{peer1, peer2})

		// Only peer1 should be disconnected
		mockP2PNode.On("DisconnectPeer", mock.Anything, peerID1).Return(nil)

		// Store some test data for peer1
		server.peerBlockHashes.Store(peerID1, "test-hash")

		// Create a ban event for PeerID
		event := BanEvent{
			Action: banActionAdd,
			PeerID: peerID1.String(),
			Reason: "Invalid block propagation",
		}

		// Call the function under test
		server.handleBanEvent(context.Background(), event)

		// Verify that ConnectedPeers was called
		mockP2PNode.AssertCalled(t, "ConnectedPeers")

		// Verify that DisconnectPeer was called only for peer1
		mockP2PNode.AssertCalled(t, "DisconnectPeer", mock.Anything, peerID1)
		mockP2PNode.AssertNotCalled(t, "DisconnectPeer", mock.Anything, peerID2)

		// Verify peer data was cleaned up
		_, exists := server.peerBlockHashes.Load(peerID1)
		assert.False(t, exists, "Peer block hash should be deleted after ban")
	})
}

func TestHandshakeFlow(t *testing.T) {
	// Create a context that can be canceled
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("handshake_version_to_verack_flow", func(t *testing.T) {
		// Create mock P2PNode
		mockP2PNode := new(MockServerP2PNode)

		// Setup self peer ID
		selfPeerIDStr := "12D3KooWJpBNhwgvoZ15EB1JwRTRpxgM9NVaqpDtWZXfTf6CpCQd"
		selfPeerID, err := peer.Decode(selfPeerIDStr)
		require.NoError(t, err)
		mockP2PNode.On("HostID").Return(selfPeerID)
		mockP2PNode.On("GetProcessName").Return("test-node")

		mockP2PNode.On("UpdatePeerHeight", mock.Anything, mock.Anything).Return()
		mockP2PNode.On("GetPeerStartingHeight", mock.Anything).Return(int32(0), false) // First time seeing peer
		mockP2PNode.On("SetPeerStartingHeight", mock.Anything, mock.Anything).Return()

		// Create a requester peer ID
		requesterPeerID, err := peer.Decode(peerIDStr)
		require.NoError(t, err)

		// Create mock ban list
		mockBanList := new(MockBanList)
		mockBanList.On("IsBanned", peerIDStr).Return(false)

		// Create mock blockchain client with expectations
		mockBlockchainClient := new(blockchain.Mock)
		nBit, _ := model.NewNBitFromString("1d00ffff")
		header := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  new(chainhash.Hash),
			HashMerkleRoot: new(chainhash.Hash),
			Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
			Bits:           *nBit,
			Nonce:          2083236893,
		}
		meta := &model.BlockHeaderMeta{
			ID:          1,
			Height:      123,
			TxCount:     0,
			SizeInBytes: 0,
			Miner:       "test-miner",
			BlockTime:   uint32(time.Now().Unix()), //nolint:gosec
			Timestamp:   uint32(time.Now().Unix()), //nolint:gosec
			ChainWork:   []byte{},
		}
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).Return(header, meta, nil)
		fsmState := blockchain.FSMStateRUNNING
		mockBlockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&fsmState, nil)

		// Create server with mocks
		server := &Server{
			P2PNode:          mockP2PNode,
			banList:          mockBanList,
			gCtx:             ctx,
			notificationCh:   make(chan *notificationMsg, 10),
			logger:           ulogger.New("test-server"),
			blockchainClient: mockBlockchainClient,
			settings:         settings.NewSettings(),
			topicPrefix:      "testnet", // Add topic prefix for validation
		}

		// Create a version handshake message
		versionMsg := p2p.HandshakeMessage{
			Type:        "version",
			PeerID:      peerIDStr,
			BestHeight:  100,
			UserAgent:   "test-agent",
			Services:    1,
			TopicPrefix: "testnet", // Use test topic prefix
		}
		versionMsgBytes, err := json.Marshal(versionMsg)
		require.NoError(t, err)

		// Setup expectation for SendToPeer - this is the key part we're testing
		// The server should respond to the version message with a verack message
		mockP2PNode.On("SendToPeer", mock.Anything, requesterPeerID, mock.Anything).Run(func(args mock.Arguments) {
			// Extract and verify the response message
			responseBytes := args.Get(2).([]byte)
			t.Logf("Response message: %s", string(responseBytes))

			var response p2p.HandshakeMessage
			err := json.Unmarshal(responseBytes, &response)
			require.NoError(t, err)

			// Log the response fields for debugging
			t.Logf("Response type: %s", response.Type)
			t.Logf("Response PeerID: %s", response.PeerID)
			t.Logf("Response BestHeight: %d", response.BestHeight)
			t.Logf("Response UserAgent: %s", response.UserAgent)

			// Verify it's a verack message with the correct fields
			assert.Equal(t, p2p.MessageType("verack"), response.Type)
			assert.Equal(t, selfPeerIDStr, response.PeerID)
			assert.NotZero(t, response.BestHeight)
			// assert.NotEmpty(t, response.UserAgent)
		}).Return(nil)

		// Call the handshake handler with the version message
		server.handleHandshakeTopic(ctx, versionMsgBytes, peerIDStr)

		// Verify SendToPeer was called with the expected peer ID
		mockP2PNode.AssertCalled(t, "SendToPeer", mock.Anything, requesterPeerID, mock.Anything)
	})

	t.Run("handshake_verack_handling", func(t *testing.T) {
		// Create mock P2PNode
		mockP2PNode := new(MockServerP2PNode)

		// Create mock blockchain client
		mockBlockchainClient := new(blockchain.Mock)

		// Setup mock blockchain client to return a valid response for GetBestBlockHeader
		prevHash, _ := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
		merkleRoot, _ := chainhash.NewHashFromStr("4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b")

		validHeader := &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  prevHash,
			HashMerkleRoot: merkleRoot,
			Timestamp:      1231006505,
			Bits:           model.NBit{0x1d, 0x00, 0xff, 0xff},
			Nonce:          2083236893,
		}

		meta := &model.BlockHeaderMeta{Height: 150} // Our height is lower than the peer's
		mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).Return(validHeader, meta, nil)
		fsmState := blockchain.FSMStateRUNNING
		mockBlockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&fsmState, nil)

		// Setup self peer ID
		selfPeerIDStr := "12D3KooWJpBNhwgvoZ15EB1JwRTRpxgM9NVaqpDtWZXfTf6CpCQd"
		selfPeerID, err := peer.Decode(selfPeerIDStr)
		require.NoError(t, err)
		mockP2PNode.On("HostID").Return(selfPeerID)

		// Create a peer ID for the verack sender
		senderPeerID, err := peer.Decode(peerIDStr)
		require.NoError(t, err)

		// Expect UpdatePeerHeight to be called with the height from the verack message
		mockP2PNode.On("UpdatePeerHeight", senderPeerID, int32(200)).Return()
		mockP2PNode.On("GetPeerStartingHeight", senderPeerID).Return(int32(0), false) // First time seeing peer
		mockP2PNode.On("SetPeerStartingHeight", senderPeerID, int32(200)).Return()

		// Create server with mocks
		server := &Server{
			P2PNode:          mockP2PNode,
			blockchainClient: mockBlockchainClient,
			gCtx:             ctx,
			notificationCh:   make(chan *notificationMsg, 10),
			logger:           ulogger.New("test-server"),
			topicPrefix:      "testnet", // Add topic prefix for validation
		}

		// Create a verack handshake message
		verackMsg := p2p.HandshakeMessage{
			Type:        "verack",
			PeerID:      peerIDStr,
			BestHeight:  200,
			UserAgent:   "test-agent",
			Services:    1,
			TopicPrefix: "testnet", // Use test topic prefix
		}
		verackMsgBytes, err := json.Marshal(verackMsg)
		require.NoError(t, err)

		// Call the handshake handler with the verack message
		server.handleHandshakeTopic(ctx, verackMsgBytes, peerIDStr)

		// Verify UpdatePeerHeight was called with the correct peer ID and height
		mockP2PNode.AssertCalled(t, "UpdatePeerHeight", senderPeerID, int32(200))
	})

	t.Run("Test handshake ignores peer with incompatible topic prefix", func(t *testing.T) {
		// Create a peer ID
		senderPeerID, _ := peer.Decode("QmSenderPeerID")
		senderPeerIDStr := senderPeerID.String()

		// Set up mock expectations
		mockP2PNode := new(MockServerP2PNode)
		mockP2PNode.On("HostID").Return(peer.ID("QmOurPeerID"))

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Create server with mocks
		server := &Server{
			P2PNode:        mockP2PNode,
			gCtx:           ctx,
			notificationCh: make(chan *notificationMsg, 10),
			logger:         ulogger.New("test-server"),
			topicPrefix:    "mainnet", // Our topic prefix
		}

		// Create a version handshake message with different topic prefix
		versionMsg := p2p.HandshakeMessage{
			Type:        "version",
			PeerID:      senderPeerIDStr,
			BestHeight:  100,
			UserAgent:   "test-agent",
			Services:    1,
			TopicPrefix: "testnet", // Different topic prefix - should be ignored
		}
		versionMsgBytes, err := json.Marshal(versionMsg)
		require.NoError(t, err)

		// Call the handler method directly - should be ignored due to topic prefix mismatch
		server.handleHandshakeTopic(ctx, versionMsgBytes, senderPeerIDStr)

		// Verify that no peer operations were called since the peer should be ignored
		mockP2PNode.AssertNotCalled(t, "UpdatePeerHeight")
		mockP2PNode.AssertNotCalled(t, "SetPeerStartingHeight")
		mockP2PNode.AssertNotCalled(t, "GetPeerStartingHeight")
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

func TestHandleMiningOnTopic(t *testing.T) {
	// Setup common test variables
	ctx := context.Background()

	t.Run("error on json unmarshal - should log and return", func(t *testing.T) {
		// Create mock P2PNode
		mockP2PNode := new(MockServerP2PNode)
		mockP2PNode.On("HostID").Return(peer.ID(""))

		// Create server with mock P2PNode
		server := &Server{
			P2PNode:        mockP2PNode,
			notificationCh: make(chan *notificationMsg, 10), // Initialize channel to prevent nil channel panic
			logger:         ulogger.New("test-server"),
		}

		// Call handler with invalid JSON
		server.handleMiningOnTopic(ctx, []byte(`{invalid json}`), "some-peer-id")
	})

	t.Run("message from self - should return early", func(t *testing.T) {
		// Create mock P2PNode
		mockP2PNode := new(MockServerP2PNode)
		selfPeerID, _ := peer.Decode("12D3KooWJpBNhwgvoZ15EB1JwRTRpxgM9NVaqpDtWZXfTf6CpCQd")
		selfPeerIDStr := selfPeerID.String()
		mockP2PNode.On("HostID").Return(selfPeerID)

		// Create a spy banManager that we can verify is NOT called
		// (since the method should return early for messages from self)
		mockBanManager := new(MockPeerBanManager)

		// Create server with mocks
		server := &Server{
			P2PNode:        mockP2PNode,
			banManager:     mockBanManager,
			notificationCh: make(chan *notificationMsg, 10), // Initialize channel to prevent nil channel panic
			logger:         ulogger.New("test-server"),
		}

		// Call handler with a message from self
		validMessage := `{
			"peerID":"QmBannedPeerID",
			"miningOn":true,
			"dataHubURL":"http://example.com",
			"hash":"000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f",
			"previousHash":"0000000000000000000000000000000000000000000000000000000000000000",
			"height":0,
			"miner":"Genesis",
			"sizeInBytes":285
		}`
		server.handleMiningOnTopic(ctx, []byte(validMessage), selfPeerIDStr)

		// Verify banManager.IsBanned was NOT called since the method returned early
		mockBanManager.AssertNotCalled(t, "IsBanned", mock.Anything)
	})

	t.Run("message from banned peer - should log and return", func(t *testing.T) {
		// Create mock P2PNode
		mockP2PNode := new(MockServerP2PNode)
		selfPeerID, _ := peer.Decode("12D3KooWJpBNhwgvoZ15EB1JwRTRpxgM9NVaqpDtWZXfTf6CpCQd")
		bannedPeerIDStr := bannedPeerIDStr

		mockP2PNode.On("HostID").Return(selfPeerID)

		// Create mock banManager that returns true for banned peer
		mockBanManager := new(MockPeerBanManager)
		mockBanManager.On("IsBanned", bannedPeerIDStr).Return(true)

		mockP2PNode.On("UpdatePeerHeight", mock.Anything, mock.Anything).Return()

		// Create server with mocks
		server := &Server{
			P2PNode:        mockP2PNode,
			banManager:     mockBanManager,
			notificationCh: make(chan *notificationMsg, 10), // Initialize channel to prevent nil channel panic
			logger:         ulogger.New("test-server"),
		}

		// Call handler with message from banned peer
		validMessage := `{
			"peerID":"QmcqHnEQuFdvxoRax8V9qjvHnqF2TpJ8nt8PNGJRRsKKg5",
			"miningOn":true,
			"dataHubURL":"http://example.com",
			"hash":"000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f",
			"previousHash":"0000000000000000000000000000000000000000000000000000000000000000",
			"height":0,
			"miner":"Genesis",
			"sizeInBytes":285
		}`
		server.handleMiningOnTopic(ctx, []byte(validMessage), bannedPeerIDStr)

		// Verify banManager.IsBanned was called
		mockBanManager.AssertCalled(t, "IsBanned", bannedPeerIDStr) // Verify ban check was performed
	})

	t.Run("happy path - successful handling from other peer", func(t *testing.T) {
		// Create mock P2PNode
		mockP2PNode := new(MockServerP2PNode)
		selfPeerID, _ := peer.Decode("12D3KooWJpBNhwgvoZ15EB1JwRTRpxgM9NVaqpDtWZXfTf6CpCQd")
		mockP2PNode.On("HostID").Return(selfPeerID)

		// Create a valid peer ID for testing
		validPeerID := "12D3KooWQJ8sLWNhDPsGbMrhA5JhrtpiEVrWvarPGm4GfP6bn6fL"

		mockP2PNode.On("UpdatePeerHeight", mock.Anything, mock.Anything).Return()

		// Create mock banManager that returns false for the peer
		mockBanManager := new(MockPeerBanManager)
		mockBanManager.On("IsBanned", validPeerID).Return(false)

		// Create server with mocks
		server := &Server{
			P2PNode:        mockP2PNode,
			banManager:     mockBanManager,
			notificationCh: make(chan *notificationMsg, 10), // Initialize channel with buffer
			logger:         ulogger.New("test-server"),
		}

		// Call handler with message from other peer
		validMessage := `{
			"peerID":"QmcqHnEQuFdvxoRax8V9qjvHnqF2TpJ8nt8PNGJRRsKKg5",
			"miningOn":true,
			"dataHubURL":"http://example.com",
			"hash":"000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f",
			"previousHash":"0000000000000000000000000000000000000000000000000000000000000000",
			"height":0,
			"miner":"Genesis",
			"sizeInBytes":285
		}`
		server.handleMiningOnTopic(ctx, []byte(validMessage), validPeerID)

		// Verify ban check was performed
		mockBanManager.AssertCalled(t, "IsBanned", validPeerID)

		// Verify notification was sent to channel
		select {
		case notification := <-server.notificationCh:
			assert.Equal(t, "mining_on", notification.Type)
			assert.Equal(t, "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f", notification.Hash)
			assert.Equal(t, "http://example.com", notification.BaseURL)
			assert.Equal(t, "QmcqHnEQuFdvxoRax8V9qjvHnqF2TpJ8nt8PNGJRRsKKg5", notification.PeerID)
			assert.Equal(t, "0000000000000000000000000000000000000000000000000000000000000000", notification.PreviousHash)
			assert.Equal(t, uint32(0), notification.Height)
			assert.Equal(t, "Genesis", notification.Miner)
			assert.Equal(t, uint64(285), notification.SizeInBytes)
		default:
			t.Fatal("Expected notification message but none received")
		}
	})
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

func TestServer_checkAndTriggerSync(t *testing.T) {
	tSettings := createBaseTestSettings()

	t.Run("sync in running sync mode", func(t *testing.T) {
		blockchainClient := new(blockchain.Mock)
		fsmState := blockchain_api.FSMStateType_RUNNING
		blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&fsmState, nil)

		mockBlocksProducer := kafka.NewKafkaAsyncProducerMock()
		publishCh := mockBlocksProducer.PublishChannel()

		mockP2PNode := new(MockServerP2PNode)
		mockP2PNode.On("GetPeerStartingHeight", mock.Anything).Return(int32(0), false)
		mockP2PNode.On("SetPeerStartingHeight", mock.Anything, mock.Anything).Return()
		mockP2PNode.On("UpdatePeerHeight", mock.Anything, mock.Anything).Return()

		// Create the sync peer ID that we'll use for testing
		syncPeerID, err := peer.Decode("12D3KooWKd2kacFFXWtbYtkDAsTP8fhEX1TbunV9Afimr7m1E8Yg")
		require.NoError(t, err)

		// Create a real SyncManager with the sync peer set
		syncManager := NewSyncManager(ulogger.New("test-syncmanager"), &chaincfg.RegressionNetParams)
		// Manually set the sync peer for testing
		syncManager.syncPeer = syncPeerID

		server := &Server{
			settings:                  tSettings,
			logger:                    ulogger.New("test-server"),
			blockchainClient:          blockchainClient,
			blocksKafkaProducerClient: mockBlocksProducer,
			gCtx:                      context.Background(),
			P2PNode:                   mockP2PNode,
			peerBlockHashes:           sync.Map{},
			syncManager:               syncManager,
		}

		// Use a valid peer ID for testing
		peerIDStr := "12D3KooWKd2kacFFXWtbYtkDAsTP8fhEX1TbunV9Afimr7m1E8Yg"

		server.checkAndTriggerSync(p2p.HandshakeMessage{
			PeerID:     peerIDStr,
			BestHeight: 1111,
			BestHash:   "00000000000000000007d3c1e3b3e3b3e3b3e3b3e3b3e3b3e3b3e3b3e3b3e3b3",
		}, 123)

		// Wait for message with timeout
		select {
		case msg := <-publishCh:
			assert.NotNil(t, msg)
		case <-time.After(100 * time.Millisecond):
			t.Fatal("Expected message to be published but timeout occurred")
		}

		// do not send message of BestHeight is less than or equal to the current height
		server.checkAndTriggerSync(p2p.HandshakeMessage{
			BestHeight: 111,
		}, 123)

		select {
		case msg2 := <-publishCh:
			t.Errorf("Expected no message to be sent, but got: %v", msg2)
		default:
			// No message received, which is expected
		}
	})

	t.Run("do not sync in legacy sync mode", func(t *testing.T) {
		blockchainClient := new(blockchain.Mock)
		fsmState := blockchain_api.FSMStateType_LEGACYSYNCING
		blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&fsmState, nil)

		mockBlocksProducer := kafka.NewKafkaAsyncProducerMock()
		publishCh := mockBlocksProducer.PublishChannel()

		mockP2PNode := new(MockServerP2PNode)
		// P2PNode methods should not be called when in LEGACYSYNCING mode

		server := &Server{
			settings:                  tSettings,
			logger:                    ulogger.New("test-server"),
			blockchainClient:          blockchainClient,
			blocksKafkaProducerClient: mockBlocksProducer,
			gCtx:                      context.Background(),
			P2PNode:                   mockP2PNode,
			peerBlockHashes:           sync.Map{},
		}

		// Use a valid peer ID for testing
		peerIDStr := "12D3KooWKd2kacFFXWtbYtkDAsTP8fhEX1TbunV9Afimr7m1E8Yg"

		server.checkAndTriggerSync(p2p.HandshakeMessage{
			PeerID:     peerIDStr,
			BestHeight: 1111,
			BestHash:   "00000000000000000007d3c1e3b3e3b3e3b3e3b3e3b3e3b3e3b3e3b3e3b3e3b3",
		}, 123)

		// make sure not message was sent to the producer
		select {
		case msg := <-publishCh:
			t.Errorf("Expected no message to be sent, but got: %v", msg)
		default:
			// No message received, which is expected
		}
	})
}

func TestBootstrapPersistentMerging(t *testing.T) {
	t.Run("bootstrap addresses merged when persistent enabled", func(t *testing.T) {
		// Create test settings with bootstrap persistent enabled
		tSettings := createBaseTestSettings()
		tSettings.P2P.BootstrapAddresses = []string{
			"/dns4/bootstrap1.example.com/tcp/9901/p2p/12D3KooWTest1",
			"/dns4/bootstrap2.example.com/tcp/9901/p2p/12D3KooWTest2",
		}
		tSettings.P2P.StaticPeers = []string{
			"/dns4/static1.example.com/tcp/9901/p2p/12D3KooWStatic1",
		}
		tSettings.P2P.BootstrapPersistent = true
		tSettings.P2P.SharedKey = "test-shared-key"
		tSettings.P2P.ListenAddresses = []string{"127.0.0.1"}

		// Call the bootstrap merging logic (extract just the merging part)
		staticPeers := tSettings.P2P.StaticPeers
		if tSettings.P2P.BootstrapPersistent {
			staticPeers = append(staticPeers, tSettings.P2P.BootstrapAddresses...)
		}

		// Verify that bootstrap addresses were merged
		require.Len(t, staticPeers, 3) // 1 static + 2 bootstrap
		assert.Contains(t, staticPeers, "/dns4/static1.example.com/tcp/9901/p2p/12D3KooWStatic1")
		assert.Contains(t, staticPeers, "/dns4/bootstrap1.example.com/tcp/9901/p2p/12D3KooWTest1")
		assert.Contains(t, staticPeers, "/dns4/bootstrap2.example.com/tcp/9901/p2p/12D3KooWTest2")
	})

	t.Run("bootstrap addresses not merged when persistent disabled", func(t *testing.T) {
		// Create test settings with bootstrap persistent disabled
		tSettings := createBaseTestSettings()
		tSettings.P2P.BootstrapAddresses = []string{
			"/dns4/bootstrap1.example.com/tcp/9901/p2p/12D3KooWTest1",
		}
		tSettings.P2P.StaticPeers = []string{
			"/dns4/static1.example.com/tcp/9901/p2p/12D3KooWStatic1",
		}
		tSettings.P2P.BootstrapPersistent = false

		// Call the bootstrap merging logic
		staticPeers := tSettings.P2P.StaticPeers
		if tSettings.P2P.BootstrapPersistent {
			staticPeers = append(staticPeers, tSettings.P2P.BootstrapAddresses...)
		}

		// Verify that bootstrap addresses were NOT merged
		require.Len(t, staticPeers, 1) // Only static peer
		assert.Contains(t, staticPeers, "/dns4/static1.example.com/tcp/9901/p2p/12D3KooWStatic1")
		assert.NotContains(t, staticPeers, "/dns4/bootstrap1.example.com/tcp/9901/p2p/12D3KooWTest1")
	})
}

// createBaseTestSettings is a local replacement for test.CreateBaseTestSettings
func createBaseTestSettings() *settings.Settings {
	s := settings.NewSettings()
	s.SubtreeValidation.BlacklistedBaseURLs = make(map[string]struct{})

	// Disable AutoNAT service in tests to avoid conflicts
	// libp2p doesn't allow multiple NAT managers to be configured
	// Keep NAT port mapping disabled as well
	s.P2P.EnableNATService = false
	s.P2P.EnableNATPortMap = false

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
				HandshakeTopic:  "handshake",
				MiningOnTopic:   "mining",
				RejectedTxTopic: "rejected",
				SharedKey:       "sharedkey",
				ListenMode:      settings.ListenModeFull,
				PrivateKey:      "privkey",
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
			name: "missing HandshakeTopic",
			modify: func(s *settings.Settings) {
				s.P2P.HandshakeTopic = ""
			},
			wantErrMsg: "p2p_handshake_topic not set in config",
		},
		{
			name: "missing MiningOnTopic",
			modify: func(s *settings.Settings) {
				s.P2P.MiningOnTopic = ""
			},
			wantErrMsg: "p2p_mining_on_topic not set in config",
		},
		{
			name: "missing RejectedTxTopic",
			modify: func(s *settings.Settings) {
				s.P2P.RejectedTxTopic = ""
			},
			wantErrMsg: "p2p_rejected_tx_topic not set in config",
		},
		{
			name: "missing SharedKey",
			modify: func(s *settings.Settings) {
				s.P2P.SharedKey = ""
			},
			wantErrMsg: "error getting p2p_shared_key",
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
				nil, nil, nil, nil, nil, nil,
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
		server, err := NewServer(ctx, logger, settings, mockClient, nil, nil, nil, nil, nil)

		// Verify
		require.NoError(t, err)
		require.NotNil(t, server)
		mockClient.AssertExpectations(t)
	})

	t.Run("no key in settings - should generate new key", func(t *testing.T) {
		// Setup
		mockClient := &blockchain.Mock{}

		settings := createBaseTestSettings()
		settings.P2P.PrivateKey = "" // No key in settings
		settings.P2P.StaticPeers = []string{}
		settings.P2P.ListenAddresses = []string{"127.0.0.1"}

		// No blockchain client expectations - we don't use it for key storage anymore

		// Execute
		server, err := NewServer(ctx, logger, settings, mockClient, nil, nil, nil, nil, nil)

		// Verify
		require.NoError(t, err)
		require.NotNil(t, server)
		require.NotNil(t, server.P2PNode, "P2P node should be created")
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
		_, err := NewServer(ctx, logger, settings, mockClient, nil, nil, nil, nil, nil)

		// Verify - should fail with invalid key
		require.Error(t, err)
		require.Contains(t, err.Error(), "decoding")
		mockClient.AssertExpectations(t)
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

	server, err := NewServer(ctx, logger, settings, mockClient, nil, nil, nil, nil, nil)
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

	server, err := NewServer(ctx, logger, settings, mockClient, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	err = server.Init(ctx)
	require.NoError(t, err)
	require.Equal(t, "http://fallback.example.com", server.AssetHTTPAddressURL)
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

	server, err := NewServer(ctx, logger, settings, mockClient, nil, nil, nil, nil, nil)
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
	mockP2PNode := new(MockServerP2PNode)
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
			Bits:           model.NBit{0x1d, 0x00, 0xff, 0xff},
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
	mockP2PNode.On("HostID").Return(peer.ID("mock-peer-id"))
	mockValidation := new(blockvalidation.MockBlockValidation)
	logger := ulogger.New("test")
	settings := createBaseTestSettings()
	settings.P2P.PrivateKey = "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdefabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
	settings.BlockChain.StoreURL = &url.URL{Scheme: "sqlitememory"}

	// Ensure only one NAT manager is configured
	settings.P2P.EnableNATPortMap = false

	server, err := NewServer(ctx, logger, settings, mockBlockchain, nil, nil, mockRejectedKafka, mockBlocksProducer, mockSubtreeProducer)
	require.NoError(t, err)

	server.rejectedTxKafkaConsumerClient = mockRejectedKafka
	server.invalidBlocksKafkaConsumerClient = mockInvalidBlocksKafka
	server.invalidSubtreeKafkaConsumerClient = mockInvalidSubtreeKafka
	server.subtreeKafkaProducerClient = mockSubtreeProducer
	server.blocksKafkaProducerClient = mockBlocksProducer

	server.P2PNode = mockP2PNode
	mockP2PNode.On("SetTopicHandler", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockP2PNode.On("GetTopic", mock.Anything).Return(topic)
	mockP2PNode.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	server.blockValidationClient = mockValidation

	// Run server
	go func() {
		err := server.Start(ctx, readyCh)
		require.NoError(t, err)
	}()

	select {
	case <-readyCh:
		// ok
	case <-time.After(2 * time.Second):
		t.Fatal("readyCh was not closed")
	}
}

func TestInvalidSubtreeHandlerHappyPath(t *testing.T) {
	banHandler := &testBanHandler{}
	banManager := &PeerBanManager{
		peerBanScores: make(map[string]*BanScore),
		reasonPoints: map[BanReason]int{
			ReasonInvalidSubtree: 10,
		},
		banThreshold:  100,
		banDuration:   time.Hour,
		decayInterval: time.Minute,
		decayAmount:   1,
		handler:       banHandler,
	}

	mockBC := new(blockchain.Mock)
	st := blockchain_api.FSMStateType_RUNNING
	mockBC.On("GetFSMCurrentState", mock.Anything).Return(&st, nil)

	s := &Server{
		logger:           ulogger.New("test"),
		banManager:       banManager,
		blockchainClient: mockBC,
	}

	hash := "subtree-hash-456"
	peerID := "peer-xyz"
	entry := peerMapEntry{
		peerID:    peerID,
		timestamp: time.Now(),
	}
	s.subtreePeerMap.Store(hash, entry)

	m := &kafkamessage.KafkaInvalidSubtreeTopicMessage{
		SubtreeHash: hash,
		Reason:      "invalid_subtree",
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

	_, ok := s.subtreePeerMap.Load(hash)
	require.False(t, ok, "entry should be deleted")

	// TODO: Fix this test to use the interface properly
	// s.banManager.mu.RLock()
	// score := s.banManager.peerBanScores[peerID]
	// s.banManager.mu.RUnlock()
	// if assert.NotNil(t, score, "peer should have a score entry") {
	// 	assert.Equal(t, int(10), score.Score)
	// }

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

		mockP2P := new(MockServerP2PNode)
		s := &Server{
			logger:              logger,
			blockchainClient:    mockBC,
			P2PNode:             mockP2P,
			rejectedTxTopicName: topicDefaultString, // optional, to avoid empty string
		}

		h := s.rejectedHandler(ctx)
		err := h(mkKafkaMsg(mkPayload("a3f1c4e2a3f1c4e2a3f1c4e2a3f1c4e2a3f1c4e2a3f1c4e2a3f1c4e2a3f1c4e2", "invalid_script")))
		require.NoError(t, err)

		mockP2P.AssertNotCalled(t, "HostID")
		mockP2P.AssertNotCalled(t, "Publish", mock.Anything, mock.Anything, mock.Anything)

		mockBC.AssertExpectations(t)
		mockP2P.AssertExpectations(t)
	})

	t.Run("publish error is logged but handler returns nil", func(t *testing.T) {
		mockBC := new(blockchain.Mock)
		state := blockchain_api.FSMStateType_RUNNING
		mockBC.On("GetFSMCurrentState", mock.Anything).Return(&state, nil)

		mockP2P := new(MockServerP2PNode)
		mockP2P.On("HostID").Return(peer.ID("peer-1"))
		mockP2P.
			On("Publish", mock.Anything, "rejected-topic", mock.Anything).
			Return(errors.NewConfigurationError("Can't publish P2P topic"))

		s := &Server{
			logger:              logger,
			blockchainClient:    mockBC,
			P2PNode:             mockP2P,
			rejectedTxTopicName: "rejected-topic",
		}

		h := s.rejectedHandler(ctx)
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

		mockP2P := new(MockServerP2PNode)

		s := &Server{
			logger:           logger,
			blockchainClient: mockBC,
			P2PNode:          mockP2P,
		}

		h := s.rejectedHandler(ctx)
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
		mockP2P := new(MockServerP2PNode)
		mockP2P.On("HostID").Return(peer.ID("peer-123"))

		s := &Server{
			logger:              logger,
			blockchainClient:    mockBC,
			P2PNode:             mockP2P,
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
			var m p2p.RejectedTxMessage
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

		// Return value for HostID
		mockP2P.On("HostID").Return(peer.ID("KoPCcJr6w9A"))

		// Handler execution
		h := s.rejectedHandler(ctx)
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

func TestServerSendDirectHandshakeSendToPeerSuccess(t *testing.T) {
	ctx := context.Background()

	mockBC := new(blockchain.Mock)
	mockP2P := new(MockServerP2PNode)
	logger := ulogger.New("test")

	// Create a valid BlockHeader with initialized fields
	prevHash, _ := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
	merkleRoot, _ := chainhash.NewHashFromStr("4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b")

	validHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  prevHash,
		HashMerkleRoot: merkleRoot,
		Timestamp:      1231006505,
		Bits:           model.NBit{0x1d, 0x00, 0xff, 0xff},
		Nonce:          2083236893,
	}

	meta := &model.BlockHeaderMeta{Height: 123}

	mockBC.On("GetBestBlockHeader", mock.Anything).Return(validHeader, meta, nil)

	expectedPeerID := peer.ID("KoPCcJr6w9A")
	var capturedMsg []byte
	mockP2P.On("HostID").Return(expectedPeerID)
	mockP2P.On("SendToPeer", mock.Anything, expectedPeerID, mock.MatchedBy(func(b []byte) bool {
		capturedMsg = b
		return true
	})).Return(nil)

	s := &Server{
		logger:              logger,
		blockchainClient:    mockBC,
		P2PNode:             mockP2P,
		AssetHTTPAddressURL: "https://datahub.test",
		bitcoinProtocolID:   "/bitcoin-sv",
		topicPrefix:         "testnet",
	}

	s.sendDirectHandshake(ctx, expectedPeerID)

	mockBC.AssertExpectations(t)
	mockP2P.AssertExpectations(t)

	var msg p2p.HandshakeMessage
	require.NoError(t, json.Unmarshal(capturedMsg, &msg))
	require.Equal(t, uint32(123), msg.BestHeight)
	require.Equal(t, "testnet", msg.TopicPrefix)
	require.Equal(t, "https://datahub.test", msg.DataHubURL)
}

func TestServerSendDirectHandshakeSendToPeerFailsFallbackToPublish(t *testing.T) {
	ctx := context.Background()

	mockBC := new(blockchain.Mock)
	mockP2P := new(MockServerP2PNode)
	logger := ulogger.New("test")

	prevHash, _ := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
	merkleRoot, _ := chainhash.NewHashFromStr("4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b")

	validHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  prevHash,
		HashMerkleRoot: merkleRoot,
		Timestamp:      1231006505,
		Bits:           model.NBit{0x1d, 0x00, 0xff, 0xff},
		Nonce:          2083236893,
	}
	meta := &model.BlockHeaderMeta{Height: 123}
	mockBC.On("GetBestBlockHeader", mock.Anything).Return(validHeader, meta, nil)

	mockP2P.On("HostID").Return(peer.ID("mockPeer"))
	mockP2P.On("SendToPeer", mock.Anything, mock.Anything, mock.Anything).Return(errors.NewConfigurationError("stream failed"))

	var published atomic.Pointer[[]byte]
	mockP2P.On("Publish", mock.Anything, "test-prefix-handshake", mock.MatchedBy(func(b []byte) bool {
		copied := make([]byte, len(b))
		copy(copied, b)
		published.Store(&copied)
		return true
	})).Return(nil)

	s := &Server{
		logger:              logger,
		blockchainClient:    mockBC,
		P2PNode:             mockP2P,
		handshakeTopicName:  "test-prefix-handshake",
		topicPrefix:         "test-prefix",
		AssetHTTPAddressURL: "https://hub",
		bitcoinProtocolID:   "/bitcoin-sv",
	}

	s.sendDirectHandshake(ctx, peer.ID("mockPeer"))

	// wait fallback async (and poll)
	var result []byte
	require.Eventually(t, func() bool {
		ptr := published.Load()
		if ptr != nil {
			result = *ptr
			return true
		}
		return false
	}, 3*time.Second, 100*time.Millisecond)

	require.NotNil(t, result)
	var m p2p.HandshakeMessage
	require.NoError(t, json.Unmarshal(result, &m))
	require.Equal(t, uint32(123), m.BestHeight)
	require.Equal(t, "test-prefix", m.TopicPrefix)
}
func TestServerSendHandshakeToPeerCallsSendToPeer(t *testing.T) {
	ctx := context.Background()
	mockP2P := new(MockServerP2PNode)

	expectedPeerID := peer.ID("abc")
	expectedMsg := []byte("hello")

	mockP2P.On("SendToPeer", ctx, expectedPeerID, expectedMsg).Return(nil)

	s := &Server{P2PNode: mockP2P}

	err := s.sendHandshakeToPeer(ctx, expectedPeerID, expectedMsg)
	require.NoError(t, err)

	mockP2P.AssertExpectations(t)
}

func TestServerHandleNodeStatusTopic(t *testing.T) {
	ctx := context.Background()

	t.Run("message_from_self_forwards_to_websocket_but_does_not_update_peer_height", func(t *testing.T) {
		logger := ulogger.New("test")

		mockP2P := new(MockServerP2PNode)
		mockP2P.On("HostID").Return(peer.ID("my-peer-id"))

		notifCh := make(chan *notificationMsg, 1)

		s := &Server{
			logger:         logger,
			P2PNode:        mockP2P,
			notificationCh: notifCh,
			nodeStatusMap:  expiringmap.New[string, *NodeStatusMessage](1 * time.Minute),
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

		mockP2P := new(MockServerP2PNode)
		mockP2P.On("HostID").Return(selfPeerID)

		notifCh := make(chan *notificationMsg, 1)

		s := &Server{
			logger:         logger,
			P2PNode:        mockP2P,
			notificationCh: notifCh,
			nodeStatusMap:  expiringmap.New[string, *NodeStatusMessage](1 * time.Minute),
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

func TestReceiveHandshakeStreamHandler(t *testing.T) {
	ctx := context.Background()

	// Setup peer IDs
	remotePeerIDStr := "12D3KooWQyYzzZpAMnsLgDK5bWZ5CPJSvxGgRA9SSGpS3zXw5tHr"
	remotePeerID, err := peer.Decode(remotePeerIDStr)
	require.NoError(t, err)

	handshakeMsg := fmt.Sprintf(`{"Type":"verack","PeerID":"%s"}`, remotePeerIDStr)
	msgBytes := []byte(handshakeMsg)

	// Mock P2PNode
	mockP2P := new(MockServerP2PNode)
	mockP2P.On("GetProcessName").Return("test-node")
	mockP2P.On("UpdateBytesReceived", uint64(len(msgBytes))).Return()
	mockP2P.On("UpdateLastReceived").Return()
	mockP2P.On("HostID").Return(remotePeerID)
	mockP2P.On("UpdatePeerHeight", mock.Anything, mock.Anything).Return()
	mockP2P.On("GetPeerStartingHeight", mock.Anything).Return(int32(0), false) // First time seeing peer
	mockP2P.On("SetPeerStartingHeight", mock.Anything, mock.Anything).Return()

	// Mock conn
	mockConn := new(MockNetworkConn)
	mockConn.On("RemotePeer").Return(remotePeerID)

	mockBlockchainClient := new(blockchain.Mock)
	fsmState := blockchain.FSMStateRUNNING

	// Create a valid BlockHeader with initialized fields
	prevHash, _ := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
	merkleRoot, _ := chainhash.NewHashFromStr("4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b")

	validHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  prevHash,
		HashMerkleRoot: merkleRoot,
		Timestamp:      1231006505,
		Bits:           model.NBit{0x1d, 0x00, 0xff, 0xff},
		Nonce:          2083236893,
	}

	meta := &model.BlockHeaderMeta{Height: 100}
	mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).Return(validHeader, meta, nil).Maybe()
	mockBlockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&fsmState, nil)

	// Mock stream
	mockStream := new(MockNetworkStream)
	mockStream.On("Read", mock.Anything).Run(func(args mock.Arguments) {
		buf := args.Get(0).([]byte)
		copy(buf, handshakeMsg)
	}).Return(len(handshakeMsg), nil).Once()
	mockStream.On("Read", mock.Anything).Return(0, io.EOF).Maybe()
	mockStream.On("Close").Return(nil)
	mockStream.On("Conn").Return(mockConn)

	// Server
	server := &Server{
		P2PNode:          mockP2P,
		gCtx:             ctx,
		logger:           ulogger.New("test"),
		topicPrefix:      "testnet",
		blockchainClient: mockBlockchainClient,
	}

	mockP2P.On("SendToPeer", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockBlockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&fsmState, nil)

	// Act
	server.receiveHandshakeStreamHandler(mockStream)

	// Assert
	mockBlockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&fsmState, nil)
	mockStream.AssertExpectations(t)
	mockConn.AssertExpectations(t)

}

func TestServerP2PNodeConnected(t *testing.T) {
	ctx := context.Background()

	testPeerID := peer.ID("12D3KooWQyYzzZpAMnsLgDK5bWZ5CPJSvxGgRA9SSGpS3zXw5tHr")

	mockBlockchainClient := new(blockchain.Mock)
	fsmState := blockchain.FSMStateRUNNING

	// Create a valid BlockHeader with initialized fields
	prevHash, _ := chainhash.NewHashFromStr("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
	merkleRoot, _ := chainhash.NewHashFromStr("4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b")

	validHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  prevHash,
		HashMerkleRoot: merkleRoot,
		Timestamp:      1231006505,
		Bits:           model.NBit{0x1d, 0x00, 0xff, 0xff},
		Nonce:          2083236893,
	}

	meta := &model.BlockHeaderMeta{Height: 100}
	mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).Return(validHeader, meta, nil).Maybe()
	mockBlockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&fsmState, nil)

	// Real SyncManager (non mocked)
	syncMgr := NewSyncManager(ulogger.New("syncmgr-test"), &chaincfg.TestNetParams)

	validIP := "192.168.1.100"
	addr, err := ma.NewMultiaddr(fmt.Sprintf("/ip4/%s/tcp/8333", validIP))
	require.NoError(t, err)

	peerInfo := p2p.PeerInfo{
		ID:    testPeerID,
		Addrs: []ma.Multiaddr{addr},
	}

	// Mocks
	handshakeCalled := make(chan struct{}, 1)
	mockP2P := new(MockServerP2PNode)
	mockP2P.On("ConnectedPeers").Return([]p2p.PeerInfo{peerInfo})
	mockP2P.On("GetPeerStartingHeight", testPeerID).Return(int32(0), false).Maybe()
	mockP2P.On("SetPeerStartingHeight", testPeerID, int32(100)).Return().Maybe()
	mockP2P.On("HostID").Return(testPeerID)
	mockP2P.
		On("Publish", mock.Anything, "test-prefix-handshake", mock.AnythingOfType("[]uint8")).
		Run(func(args mock.Arguments) {
			handshakeCalled <- struct{}{}
		}).
		Return(nil).
		Maybe()
	mockP2P.On("SendToPeer", mock.Anything, mock.Anything, mock.Anything).Return(errors.NewConfigurationError("stream failed"))

	server := &Server{
		P2PNode:            mockP2P,
		logger:             ulogger.New("server-test"),
		syncManager:        syncMgr,
		blockchainClient:   mockBlockchainClient,
		topicPrefix:        "test-prefix",
		handshakeTopicName: "test-prefix-handshake",
	}

	// Act
	server.P2PNodeConnected(ctx, testPeerID)

	// Assert: wait for SetPeerStartingHeight to be called
	require.Eventually(t, func() bool {
		mockP2P.AssertExpectations(t)
		return true
	}, 2*time.Second, 100*time.Millisecond)

	// Assert: wait for sendDirectHandshake to be called
	select {
	case <-handshakeCalled:
		// OK
	case <-time.After(2 * time.Second):
		t.Fatal("sendDirectHandshake was not called")
	}
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
		Bits:           model.NBit{0x1d, 0x00, 0xff, 0xff},
		Nonce:          1234,
	}
	meta := &model.BlockHeaderMeta{Height: 150}

	// --- Mocks ---
	mockP2P := new(MockServerP2PNode)
	mockP2P.On("HostID").Return(peer.ID("12D3KooWTestPeer")).Maybe()
	mockP2P.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	mockBlockchain := new(blockchain.Mock)
	mockBlockchain.On("GetBlockHeader", ctx, testHash).Return(header, meta, nil).Once()
	mockBlockchain.On("GetBestBlockHeader", mock.Anything).Return(header, &model.BlockHeaderMeta{Height: 100}, nil).Maybe()
	mockBlockchain.On("GetFSMCurrentState", mock.Anything).Return(&fsmState, nil)

	testSettings := settings.NewSettings()
	testSettings.Coinbase.ArbitraryText = "MockMiner"

	mockServer := &Server{
		P2PNode:             mockP2P,
		blockchainClient:    mockBlockchain,
		logger:              ulogger.New("test"),
		blockTopicName:      "block-topic",
		AssetHTTPAddressURL: "https://datahub.node",
		settings:            testSettings,
		startTime:           time.Now(),
		syncConnectionTimes: sync.Map{},
		peerBlockHashes:     sync.Map{},
		notificationCh:      make(chan *notificationMsg, 1),
	}

	err := mockServer.handleBlockNotification(ctx, testHash)
	require.NoError(t, err)

	mockP2P.AssertExpectations(t)
	mockBlockchain.AssertExpectations(t)
}

func TestHandleMiningOnNotificationSuccess(t *testing.T) {
	ctx := context.Background()

	mockBlockchainClient := new(blockchain.Mock)
	mockP2PNode := new(MockServerP2PNode)
	logger := ulogger.New("test")
	fsmState := blockchain.FSMStateRUNNING

	prevBlockHash, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000000")

	header := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  prevBlockHash,
		HashMerkleRoot: model.GenesisBlockHeader.HashMerkleRoot,
		Timestamp:      1234567890,
		Bits:           model.NBit{0x1d, 0x00, 0xff, 0xff},
		Nonce:          1234,
	}
	meta := &model.BlockHeaderMeta{
		Height:      100,
		Miner:       "MockMiner",
		SizeInBytes: 2048,
		TxCount:     12,
	}

	// Set expectations
	mockBlockchainClient.On("GetBestBlockHeader", mock.Anything).Return(header, meta, nil)
	mockBlockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&fsmState, nil)
	mockP2PNode.On("HostID").Return(peer.ID("75MDc5Ffg7m7gfZLY1Fszq"))
	mockP2PNode.On("Publish", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

	testSettings := settings.NewSettings()
	testSettings.Coinbase.ArbitraryText = "MockMiner"

	s := &Server{
		blockchainClient:    mockBlockchainClient,
		P2PNode:             mockP2PNode,
		logger:              logger,
		miningOnTopicName:   "mining-topic",
		AssetHTTPAddressURL: "https://datahub.node",
		syncConnectionTimes: sync.Map{},
		peerBlockHashes:     sync.Map{},
		settings:            testSettings,
		notificationCh:      make(chan *notificationMsg, 2),
	}

	err := s.handleMiningOnNotification(ctx)
	require.NoError(t, err)

	select {
	case msg := <-s.notificationCh:
		assert.Equal(t, "mining_on", msg.Type)
		assert.Equal(t, prevBlockHash.String(), msg.PreviousHash)
		assert.Equal(t, uint32(100), msg.Height)
		assert.Equal(t, "MockMiner", msg.Miner)
		assert.Equal(t, uint64(12), msg.TxCount)
		assert.Equal(t, uint64(2048), msg.SizeInBytes)
	default:
		t.Fatal("expected message on notificationCh")
	}

	mockBlockchainClient.AssertExpectations(t)
	mockP2PNode.AssertExpectations(t)
}

func TestHandleSubtreeNotificationSuccess(t *testing.T) {
	ctx := context.Background()
	hash := &chainhash.Hash{0x1}
	subtreeTopicName := "subtree-topic"

	mockP2P := new(MockServerP2PNode)
	mockP2P.On("HostID").Return(peer.ID("peer-123"))
	mockP2P.On("Publish", ctx, subtreeTopicName, mock.Anything).Return(nil)

	server := &Server{
		P2PNode:             mockP2P,
		subtreeTopicName:    subtreeTopicName,
		AssetHTTPAddressURL: "https://datahub.node",
	}

	err := server.handleSubtreeNotification(ctx, hash)
	assert.NoError(t, err)

	// Verify that Publish was called
	mockP2P.AssertCalled(t, "Publish", ctx, subtreeTopicName, mock.Anything)
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

	mockP2P := new(MockServerP2PNode)
	mockP2P.On("HostID").Return(peer.ID("peer-123"))
	mockP2P.On("Publish", ctx, subtreeTopicName, mock.Anything).Return(nil)

	server := &Server{
		P2PNode:             mockP2P,
		subtreeTopicName:    subtreeTopicName,
		AssetHTTPAddressURL: "https://datahub.node",
		logger:              logger,
	}

	err := server.processBlockchainNotification(ctx, notification)
	assert.NoError(t, err)
	mockP2P.AssertCalled(t, "Publish", ctx, subtreeTopicName, mock.Anything)
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

	mockP2P := new(MockServerP2PNode)
	mockP2P.On("Stop", ctx).Return(nil)

	mockKafkaConsumer := new(MockKafkaConsumerGroup)
	mockKafkaConsumer.On("Close").Return(nil)

	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()

	// Pre-load elements into the maps
	server := &Server{
		logger:                           logger,
		P2PNode:                          mockP2P,
		rejectedTxKafkaConsumerClient:    mockKafkaConsumer,
		invalidBlocksKafkaConsumerClient: mockKafkaConsumer,
		peerMapCleanupTicker:             ticker,
	}

	server.blockPeerMap.Store("key1", "value1")
	server.subtreePeerMap.Store("key2", "value2")

	err := server.Stop(ctx)
	assert.NoError(t, err)

	mockP2P.AssertCalled(t, "Stop", ctx)
	mockKafkaConsumer.AssertNumberOfCalls(t, "Close", 2)

	// Check that maps are empty
	_, ok := server.blockPeerMap.Load("test")
	assert.False(t, ok)
	_, ok = server.subtreePeerMap.Load("test")
	assert.False(t, ok)
}

func TestDisconnectPeerSuccess(t *testing.T) {
	ctx := context.Background()
	peerID := "12D3KooWQ89fFeXZtbj4Lmq2Z3zAqz1QzAAzC7D2yxjZK7XWuK6h"
	logger := ulogger.New("test")
	testSettings := settings.NewSettings()

	mockP2P := new(MockServerP2PNode)
	decodedPeerID, _ := peer.Decode(peerID)
	mockP2P.On("DisconnectPeer", ctx, decodedPeerID).Return(nil)

	server := &Server{
		P2PNode:         mockP2P,
		peerBlockHashes: sync.Map{},
		logger:          logger,
		syncManager:     NewSyncManager(logger, testSettings.ChainCfgParams),
	}

	req := &p2p_api.DisconnectPeerRequest{PeerId: peerID}

	resp, err := server.DisconnectPeer(ctx, req)
	assert.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Empty(t, resp.Error)

	mockP2P.AssertCalled(t, "DisconnectPeer", ctx, decodedPeerID)
}

func TestDisconnectPeerInvalidID(t *testing.T) {
	ctx := context.Background()
	invalidPeerID := "invalid-peer-id"
	logger := ulogger.New("test")

	server := &Server{
		P2PNode: new(MockServerP2PNode),
		logger:  logger,
	}

	req := &p2p_api.DisconnectPeerRequest{PeerId: invalidPeerID}

	resp, err := server.DisconnectPeer(ctx, req)
	assert.NoError(t, err)
	assert.False(t, resp.Success)
	assert.Contains(t, resp.Error, "invalid peer ID")
}

func TestDisconnectPeerNoP2PNode(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.New("test")

	server := &Server{
		P2PNode: nil,
		logger:  logger,
	}

	req := &p2p_api.DisconnectPeerRequest{PeerId: "12D3KooWQ89fFeXZtbj4Lmq2Z3zAqz1QzAAzC7D2yxjZK7XWuK6h"}

	resp, err := server.DisconnectPeer(ctx, req)
	assert.NoError(t, err)
	assert.False(t, resp.Success)
	assert.Equal(t, "P2P node not available", resp.Error)
}

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
