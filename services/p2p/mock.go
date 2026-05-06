package p2p

import (
	"context"
	"time"

	p2pMessageBus "github.com/bsv-blockchain/go-p2p-message-bus"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/mock"
)

// MockServerP2PClient is a mock implementation of P2PNodeI interface for testing purposes.
// This mock provides a testable substitute for the real P2P node implementation, allowing
// unit tests to verify P2P service behavior without requiring actual network connections
// or peer discovery. The mock uses testify/mock framework to record method calls and
// return predefined responses.
//
// Key features:
//   - Records all method calls with arguments for verification
//   - Allows setting expected return values and errors
//   - Supports configurable peer ID for testing different scenarios
//   - Provides full interface compatibility with P2PNodeI
//
// Usage in tests:
//
//	mockNode := &MockServerP2PClient{}
//	mockNode.On("Start", mock.Anything, mock.Anything, mock.Anything).Return(nil)
//	// Use mockNode in place of real P2PNodeI implementation
type MockServerP2PClient struct {
	mock.Mock                          // Embedded mock for method call recording and expectations
	peerID    peer.ID                  // Configurable peer ID for testing scenarios
	peers     []p2pMessageBus.PeerInfo // If non-nil, returned by GetPeers() instead of the default
}

func (m *MockServerP2PClient) Start(ctx context.Context, streamHandler func(network.Stream), topicNames ...string) error {
	args := m.Called(ctx, streamHandler, topicNames)
	return args.Error(0)
}

func (m *MockServerP2PClient) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockServerP2PClient) Publish(ctx context.Context, topicName string, msgBytes []byte) error {
	args := m.Called(ctx, topicName, msgBytes)
	return args.Error(0)
}

func (m *MockServerP2PClient) Subscribe(topicName string) <-chan p2pMessageBus.Message {
	args := m.Called(topicName)
	return args.Get(0).(<-chan p2pMessageBus.Message)
}

func (m *MockServerP2PClient) GetID() string {
	if m.peerID != "" {
		return m.peerID.String()
	}

	args := m.Called()
	return args.Get(0).(peer.ID).String()
}

func (m *MockServerP2PClient) Connect(ctx context.Context, peerMultiaddr string) error {
	args := m.Called(ctx, peerMultiaddr)
	return args.Error(0)
}

func (m *MockServerP2PClient) GetPeers() []p2pMessageBus.PeerInfo {
	if m.peers != nil {
		return m.peers
	}

	peers := []p2pMessageBus.PeerInfo{}
	peers = append(peers, p2pMessageBus.PeerInfo{})

	return peers
}

// MockBanList is a mock implementation of BanListI interface for testing purposes.
// This mock provides a testable substitute for the real ban list implementation,
// allowing unit tests to verify ban management behavior without requiring actual
// database operations or persistent storage. The mock uses testify/mock framework
// to record method calls and return predefined responses.
//
// Key features:
//   - Records all method calls with arguments for verification
//   - Allows setting expected return values and errors for ban operations
//   - Supports testing of ban/unban workflows without database dependencies
//   - Provides full interface compatibility with BanListI
//
// Usage in tests:
//
//	mockBanList := &MockBanList{}
//	mockBanList.On("IsBanned", "192.168.1.1").Return(true)
//	// Use mockBanList in place of real BanListI implementation
type MockBanList struct {
	mock.Mock // Embedded mock for method call recording and expectations
}

func (m *MockBanList) IsBanned(ipStr string) bool {
	args := m.Called(ipStr)
	return args.Bool(0)
}

func (m *MockBanList) Add(ctx context.Context, ipOrSubnet string, expirationTime time.Time) error {
	args := m.Called(ctx, ipOrSubnet, expirationTime)
	return args.Error(0)
}

func (m *MockBanList) Remove(ctx context.Context, ipOrSubnet string) error {
	args := m.Called(ctx, ipOrSubnet)
	return args.Error(0)
}

func (m *MockBanList) ListBanned() []string {
	args := m.Called()
	return args.Get(0).([]string)
}

func (m *MockBanList) Subscribe() chan BanEvent {
	args := m.Called()
	return args.Get(0).(chan BanEvent)
}

func (m *MockBanList) Unsubscribe(ch chan BanEvent) {
	m.Called(ch)
}

func (m *MockBanList) Init(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockBanList) Clear() {
	m.Called()
}

// MockKafkaProducer is a mock implementation of Kafka producer interface for testing purposes.
// This mock provides a testable substitute for the real Kafka producer implementation,
// allowing unit tests to verify message publishing behavior without requiring actual
// Kafka broker connections. The mock uses testify/mock framework to record method
// calls and return predefined responses.
//
// Key features:
//   - Records all method calls with arguments for verification
//   - Allows setting expected return values and errors for publishing operations
//   - Supports testing of message publishing workflows without Kafka dependencies
//   - Provides full interface compatibility with Kafka producer interface
//
// Usage in tests:
//
//	mockProducer := &MockKafkaProducer{}
//	mockProducer.On("Publish", mock.Anything).Return()
//	// Use mockProducer in place of real Kafka producer implementation
type MockKafkaProducer struct {
	mock.Mock // Embedded mock for method call recording and expectations
}

func (m *MockKafkaProducer) Publish(msg *kafka.Message) {
	m.Called(msg)
}

func (m *MockKafkaProducer) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockKafkaProducer) BrokersURL() []string {
	args := m.Called()
	return args.Get(0).([]string)
}

func (m *MockKafkaProducer) Start(ctx context.Context, msgCh chan *kafka.Message) {
	m.Called(ctx, msgCh)
}

func (m *MockKafkaProducer) Stop() error {
	args := m.Called()
	return args.Error(0)
}
