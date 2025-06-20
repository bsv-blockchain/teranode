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

// MockServerP2PNode is a mock implementation of P2PNodeI
type MockServerP2PNode struct {
	mock.Mock
	peerID peer.ID
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

type MockBanList struct {
	mock.Mock
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

type MockKafkaProducer struct {
	mock.Mock
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
