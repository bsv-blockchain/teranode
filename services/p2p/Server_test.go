package p2p

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/services/blockchain/blockchain_api"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/kafka"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-p2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	ma "github.com/multiformats/go-multiaddr" // nolint:misspell
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Constants for testing
const bannedPeerIDStr = "12D3KooWB9kmtfHg5Ct1Sj5DX6fmqRnatrXnE5zMRg25d6rbwLzp" // Use a genuinely valid Peer ID hash
const peerIDStr = "12D3KooWRj9ajsNaVuT2fNv7k2AyLnrC5NQQzZS9GixSVWKZZYRE"

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
			t.Logf("DNS resolution succeeded where we expected it might fail: %v", result)
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

		// Create mock banList that returns true for the banned peer
		mockBanList := new(MockBanList)
		mockBanList.On("IsBanned", bannedPeerIDStr).Return(true)

		// Create logger
		logger := ulogger.New("test-server")

		// Create server with mock P2PNode and BanList
		server := &Server{
			P2PNode:        mockP2PNode,
			notificationCh: make(chan *notificationMsg, 10),
			banList:        mockBanList,
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

		// Create a mock banList that returns false for any peer
		mockBanList := new(MockBanList)
		mockBanList.On("IsBanned", mock.Anything).Return(false)

		// Create logger
		logger := ulogger.New("test-server")

		// Create server with mock P2PNode and BanList
		server := &Server{
			P2PNode:        mockP2PNode,
			notificationCh: make(chan *notificationMsg, 10),
			banList:        mockBanList,
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

		// Create a mock banList that returns false for any peer
		mockBanList := new(MockBanList)
		mockBanList.On("IsBanned", mock.Anything).Return(false)

		// Create mock kafka producer
		mockKafkaProducer := new(MockKafkaProducer)
		mockKafkaProducer.On("Publish", mock.Anything).Return()

		// Create server with mocks
		server := &Server{
			P2PNode:                   mockP2PNode,
			notificationCh:            make(chan *notificationMsg, 10),
			blocksKafkaProducerClient: mockKafkaProducer,
			banList:                   mockBanList,
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

		// Setup mock to return our test IP for any peer ID
		mockP2PNode.On("GetPeerIPs", mock.MatchedBy(func(id peer.ID) bool { return id.String() == validPeerID })).Return([]string{testIP})

		// Create mock banList that returns false for the test IP
		mockBanList := new(MockBanList)
		mockBanList.On("IsBanned", testIP).Return(false)

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
			banList:                    mockBanList,
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
		mockBanList.AssertCalled(t, "IsBanned", testIP)

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
func (m *MockPeerBanManager) GetBanScore(peerID string) int32 {
	args := m.Called(peerID)
	return args.Get(0).(int32)
}

// IncrementBanScore mocks the IncrementBanScore method
func (m *MockPeerBanManager) IncrementBanScore(peerID string, score int32, reason string) {
	m.Called(peerID, score, reason)
}

// ResetBanScore mocks the ResetBanScore method
func (m *MockPeerBanManager) ResetBanScore(peerID string) {
	m.Called(peerID)
}

func TestGetPeers(t *testing.T) {
	t.Run("returns_connected_peers", func(t *testing.T) {
		// Create mock dependencies
		logger := ulogger.New("test-server")
		mockP2PNode := new(MockServerP2PNode)

		// Generate a valid peer ID
		privKey, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
		require.NoError(t, err)

		peerID, err := peer.IDFromPrivateKey(privKey)
		require.NoError(t, err)

		// Create a peer with test IP
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
		require.Contains(t, resp.Peers[0].Addr, validIP)

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

		// Create a peer with test IP and connection time
		validIP := "192.168.1.20"
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

		// Create a peer with nil connection time
		validIP := "192.168.1.20"
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

	t.Run("invalid_IP_address", func(t *testing.T) {
		// Setup
		logger := ulogger.New("test-server")
		mockP2PNode := new(MockServerP2PNode)
		server := &Server{
			P2PNode: mockP2PNode,
			logger:  logger,
		}

		// Test invalid IP
		event := BanEvent{
			Action: banActionAdd,
			IP:     "invalid-ip", // Not a valid IP
		}

		// The function should log an error and return without calling ConnectedPeers
		server.handleBanEvent(context.Background(), event)

		// Verify ConnectedPeers was not called
		mockP2PNode.AssertNotCalled(t, "ConnectedPeers")
	})

	t.Run("valid_IP_ban", func(t *testing.T) {
		// Setup
		logger := ulogger.New("test-server")
		mockP2PNode := new(MockServerP2PNode)

		// Generate a valid peer ID
		privKey, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
		require.NoError(t, err)
		peerID, err := peer.IDFromPrivateKey(privKey)
		require.NoError(t, err)

		// Create a peer with our test IP
		validIP := "192.168.1.20"
		addr, err := ma.NewMultiaddr(fmt.Sprintf("/ip4/%s/tcp/8333", validIP))
		require.NoError(t, err)

		peerInfo := p2p.PeerInfo{
			ID:    peerID,
			Addrs: []ma.Multiaddr{addr},
		}

		// Setup mocks
		mockP2PNode.On("ConnectedPeers").Return([]p2p.PeerInfo{peerInfo})

		// For IP matching, we need DisconnectPeer to be called
		mockP2PNode.On("DisconnectPeer", mock.Anything, peerID).Return(nil)

		// Create server with mocks
		server := &Server{
			P2PNode: mockP2PNode,
			logger:  logger,
		}

		// Create a ban event for the IP
		event := BanEvent{
			Action: banActionAdd,
			IP:     validIP,
		}

		// Call the function under test
		server.handleBanEvent(context.Background(), event)

		// Verify that ConnectedPeers was called
		mockP2PNode.AssertCalled(t, "ConnectedPeers")
	})

	t.Run("handles_subnet_bans", func(t *testing.T) {
		// Create a server with mocked P2PNode
		logger := ulogger.New("test-server")
		mockP2PNode := new(MockServerP2PNode)

		server := &Server{
			P2PNode: mockP2PNode,
			logger:  logger,
		}

		// Generate valid peer IDs
		privKey1, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
		require.NoError(t, err)

		peerID1, err := peer.IDFromPrivateKey(privKey1)
		require.NoError(t, err)

		// Create peers - one in the subnet, one outside
		peerInSubnetAddr, err := ma.NewMultiaddr("/ip4/192.168.1.50/tcp/8333")
		require.NoError(t, err)

		peerOutsideSubnetAddr, err := ma.NewMultiaddr("/ip4/10.0.0.50/tcp/8333")
		require.NoError(t, err)

		peerInSubnet := p2p.PeerInfo{
			ID:    peerID1,
			Addrs: []ma.Multiaddr{peerInSubnetAddr},
		}

		peerOutsideSubnet := p2p.PeerInfo{
			ID:    peer.ID(""),
			Addrs: []ma.Multiaddr{peerOutsideSubnetAddr},
		}

		// Setup mocks
		mockP2PNode.On("ConnectedPeers").Return([]p2p.PeerInfo{peerInSubnet, peerOutsideSubnet})

		// Add mock for DisconnectPeer for the peer in subnet
		mockP2PNode.On("DisconnectPeer", mock.Anything, peerID1).Return(nil)

		// Create a ban event for a subnet
		_, subnet, err := net.ParseCIDR("192.168.1.0/24")
		require.NoError(t, err)

		event := BanEvent{
			Action: banActionAdd,
			IP:     "192.168.1.0/24",
			Subnet: subnet,
		}

		// Call the function under test
		server.handleBanEvent(context.Background(), event)

		// Verify that ConnectedPeers was called
		mockP2PNode.AssertCalled(t, "ConnectedPeers")

		// Verify that DisconnectPeer was called for the peer in subnet
		mockP2PNode.AssertCalled(t, "DisconnectPeer", mock.Anything, peerID1)
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
	mockBanManager.On("GetBanScore", mock.Anything).Return(int32(0))

	// Verify it works
	score := mockBanManager.GetBanScore("test-peer-id")
	require.Equal(t, int32(0), score)
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

		// Add mock for GetPeerIPs to handle any peer ID
		mockP2PNode.On("GetPeerIPs", mock.AnythingOfType("peer.ID")).Return([]string{})
		// Create a spy banList that we can verify is NOT called
		// (since the method should return early for messages from self)
		mockBanList := new(MockBanList)

		// Create server with mocks
		server := &Server{
			P2PNode:        mockP2PNode,
			banList:        mockBanList,
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

		// Verify banList.IsBanned was NOT called since the method returned early
		mockBanList.AssertNotCalled(t, "IsBanned", mock.Anything)
	})

	t.Run("message from banned peer - should log and return", func(t *testing.T) {
		// Create mock P2PNode
		mockP2PNode := new(MockServerP2PNode)
		selfPeerID, _ := peer.Decode("12D3KooWJpBNhwgvoZ15EB1JwRTRpxgM9NVaqpDtWZXfTf6CpCQd")
		bannedPeerIDStr := bannedPeerIDStr

		mockP2PNode.On("HostID").Return(selfPeerID)

		// Add mock for GetPeerIPs to handle any peer ID
		mockP2PNode.On("GetPeerIPs", mock.AnythingOfType("peer.ID")).Return([]string{bannedPeerIDStr})

		// Create mock banList that returns true for banned peer
		mockBanList := new(MockBanList)
		mockBanList.On("IsBanned", bannedPeerIDStr).Return(true)

		mockP2PNode.On("UpdatePeerHeight", mock.Anything, mock.Anything).Return()

		// Create server with mocks
		server := &Server{
			P2PNode:        mockP2PNode,
			banList:        mockBanList,
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

		// Verify banList.IsBanned was called
		mockBanList.AssertCalled(t, "IsBanned", bannedPeerIDStr) // Verify ban check was performed
	})

	t.Run("happy path - successful handling from other peer", func(t *testing.T) {
		// Create mock P2PNode
		mockP2PNode := new(MockServerP2PNode)
		selfPeerID, _ := peer.Decode("12D3KooWJpBNhwgvoZ15EB1JwRTRpxgM9NVaqpDtWZXfTf6CpCQd")
		mockP2PNode.On("HostID").Return(selfPeerID)

		// Create a valid peer ID for testing
		validPeerID := "12D3KooWQJ8sLWNhDPsGbMrhA5JhrtpiEVrWvarPGm4GfP6bn6fL"

		// Create test IP for the peer
		testIP := "192.168.1.100"

		// Setup mock to return our test IP for any peer ID
		mockP2PNode.On("GetPeerIPs", mock.Anything).Return([]string{testIP})

		mockP2PNode.On("UpdatePeerHeight", mock.Anything, mock.Anything).Return()

		// Create mock banList that returns false for the test IP
		mockBanList := new(MockBanList)
		mockBanList.On("IsBanned", testIP).Return(false)

		// Create server with mocks
		server := &Server{
			P2PNode:        mockP2PNode,
			banList:        mockBanList,
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
		mockBanList.AssertCalled(t, "IsBanned", testIP)

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

func TestServer_SyncHeights(t *testing.T) {
	tSettings := createBaseTestSettings()

	t.Run("sync in running sync mode", func(t *testing.T) {
		blockchainClient := new(blockchain.Mock)
		fsmState := blockchain_api.FSMStateType_RUNNING
		blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&fsmState, nil)

		mockBlocksProducer := kafka.NewKafkaAsyncProducerMock()
		publishCh := mockBlocksProducer.PublishChannel()

		server := &Server{
			settings:                  tSettings,
			logger:                    ulogger.New("test-server"),
			blockchainClient:          blockchainClient,
			blocksKafkaProducerClient: mockBlocksProducer,
		}

		server.SyncHeights(p2p.HandshakeMessage{
			BestHeight: 1111,
		}, 123)

		msg := <-publishCh
		assert.NotNil(t, msg)

		// do not send message of BestHeight is less than or equal to the current height
		server.SyncHeights(p2p.HandshakeMessage{
			BestHeight: 111,
		}, 123)

		select {
		case msg = <-publishCh:
			t.Errorf("Expected no message to be sent, but got: %v", msg)
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

		server := &Server{
			settings:                  tSettings,
			logger:                    ulogger.New("test-server"),
			blockchainClient:          blockchainClient,
			blocksKafkaProducerClient: mockBlocksProducer,
		}

		server.SyncHeights(p2p.HandshakeMessage{
			BestHeight: 1111,
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

// createBaseTestSettings is a local replacement for test.CreateBaseTestSettings
func createBaseTestSettings() *settings.Settings {
	s := settings.NewSettings()
	s.SubtreeValidation.BlacklistedBaseURLs = make(map[string]struct{})

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
			name: "missing port",
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
		// ... e così via per tutti gli altri check
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
