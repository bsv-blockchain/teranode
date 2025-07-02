package p2p

import (
	"context"
	"time"

	"github.com/bitcoin-sv/teranode/util/kafka"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/mock"
)

// MockServerP2PNode is a mock implementation of P2PNodeI interface for testing purposes.
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
//	mockNode := &MockServerP2PNode{}
//	mockNode.On("Start", mock.Anything, mock.Anything, mock.Anything).Return(nil)
//	// Use mockNode in place of real P2PNodeI implementation
type MockServerP2PNode struct {
	mock.Mock         // Embedded mock for method call recording and expectations
	peerID    peer.ID // Configurable peer ID for testing scenarios
}

func (m *MockServerP2PNode) Start(ctx context.Context, streamHandler func(network.Stream), topicNames ...string) error {
	args := m.Called(ctx, streamHandler, topicNames)
	return args.Error(0)
}

func (m *MockServerP2PNode) Stop(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockServerP2PNode) SetTopicHandler(ctx context.Context, topicName string, handler Handler) error {
	args := m.Called(ctx, topicName, handler)
	return args.Error(0)
}

func (m *MockServerP2PNode) GetTopic(topicName string) *pubsub.Topic {
	args := m.Called(topicName)
	if result := args.Get(0); result != nil {
		return result.(*pubsub.Topic)
	}

	return nil
}

func (m *MockServerP2PNode) Publish(ctx context.Context, topicName string, msgBytes []byte) error {
	args := m.Called(ctx, topicName, msgBytes)
	return args.Error(0)
}

func (m *MockServerP2PNode) HostID() peer.ID {
	if m.peerID != "" {
		return m.peerID
	}

	args := m.Called()

	return args.Get(0).(peer.ID)
}

func (m *MockServerP2PNode) ConnectedPeers() []PeerInfo {
	args := m.Called()
	return args.Get(0).([]PeerInfo)
}

func (m *MockServerP2PNode) CurrentlyConnectedPeers() []PeerInfo {
	// args := m.Called()
	// return args.Get(0).([]PeerInfo)
	peers := []PeerInfo{}
	peers = append(peers, PeerInfo{})

	return peers
}

func (m *MockServerP2PNode) DisconnectPeer(ctx context.Context, peerID peer.ID) error {
	args := m.Called(ctx, peerID)
	return args.Error(0)
}

func (m *MockServerP2PNode) SendToPeer(ctx context.Context, pid peer.ID, msg []byte) error {
	args := m.Called(ctx, pid, msg)
	return args.Error(0)
}

func (m *MockServerP2PNode) LastSend() time.Time {
	args := m.Called()
	return args.Get(0).(time.Time)
}

func (m *MockServerP2PNode) LastRecv() time.Time {
	args := m.Called()
	return args.Get(0).(time.Time)
}

func (m *MockServerP2PNode) BytesSent() uint64 {
	args := m.Called()
	return args.Get(0).(uint64)
}

func (m *MockServerP2PNode) BytesReceived() uint64 {
	args := m.Called()
	return args.Get(0).(uint64)
}

func (m *MockServerP2PNode) GetProcessName() string {
	args := m.Called()
	return args.String(0)
}

func (m *MockServerP2PNode) UpdateBytesReceived(bytesCount uint64) {
	m.Called(bytesCount)
}

func (m *MockServerP2PNode) UpdateLastReceived() {
	m.Called()
}

func (m *MockServerP2PNode) GetPeerIPs(pid peer.ID) []string {
	args := m.Called(pid)
	return args.Get(0).([]string)
}

func (m *MockServerP2PNode) UpdatePeerHeight(pid peer.ID, height int32) {
	m.Called(pid, height)
}

func (m *MockServerP2PNode) SetPeerConnectedCallback(callback func(context.Context, peer.ID)) {
	m.Called(callback)
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
