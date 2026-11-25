package p2p

import (
	"context"
	"net"
	"sync"
	"time"

	p2pMessageBus "github.com/bsv-blockchain/go-p2p-message-bus"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
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

	// Admission calls (Block*/Unblock*) are recorded here rather than through
	// mock.Mock, so tests that reach the ban paths need no expectations. A
	// Block* call that succeeds also closes the connections it covers, as the
	// real bus does: it drops the matching Addrs from peers. admissionMu
	// guards peers once a test has handed the client to the server.
	admissionMu    sync.Mutex
	admissionCalls []string
	blockErr       error // returned by every Block* call when set
}

// recordAdmission appends one "Method arg" entry and returns err.
func (m *MockServerP2PClient) recordAdmission(call string, err error) error {
	m.admissionMu.Lock()
	defer m.admissionMu.Unlock()

	m.admissionCalls = append(m.admissionCalls, call)

	return err
}

// AdmissionCalls returns the Block*/Unblock* calls made so far, in order.
func (m *MockServerP2PClient) AdmissionCalls() []string {
	m.admissionMu.Lock()
	defer m.admissionMu.Unlock()

	return append([]string(nil), m.admissionCalls...)
}

func (m *MockServerP2PClient) BlockAddr(ip net.IP) error {
	if m.blockErr == nil {
		m.closeConns(func(_ string, remote net.IP) bool { return remote != nil && remote.Equal(ip) })
	}

	return m.recordAdmission("BlockAddr "+ip.String(), m.blockErr)
}

func (m *MockServerP2PClient) UnblockAddr(ip net.IP) error {
	return m.recordAdmission("UnblockAddr "+ip.String(), nil)
}

func (m *MockServerP2PClient) BlockSubnet(ipnet *net.IPNet) error {
	if m.blockErr == nil {
		m.closeConns(func(_ string, remote net.IP) bool { return remote != nil && ipnet.Contains(remote) })
	}

	return m.recordAdmission("BlockSubnet "+ipnet.String(), m.blockErr)
}

func (m *MockServerP2PClient) UnblockSubnet(ipnet *net.IPNet) error {
	return m.recordAdmission("UnblockSubnet "+ipnet.String(), nil)
}

func (m *MockServerP2PClient) BlockPeer(peerID string) error {
	if m.blockErr == nil {
		m.closeConns(func(id string, _ net.IP) bool { return id == peerID })
	}

	return m.recordAdmission("BlockPeer "+peerID, m.blockErr)
}

// closeConns does what the real bus's closeRefusedConns does to the peers
// fixture: it drops every address refused reports covered, given the peer's
// ID and the address's leading IP (nil when it has none; for a relay circuit,
// the relay's). A peer keeps its entry with no addresses, as GetPeers lists a
// topic peer with no open connection. The slice is replaced, not edited, so a
// slice GetPeers returned earlier is left as it was.
func (m *MockServerP2PClient) closeConns(refused func(id string, remote net.IP) bool) {
	m.admissionMu.Lock()
	defer m.admissionMu.Unlock()

	if m.peers == nil {
		return
	}

	next := make([]p2pMessageBus.PeerInfo, 0, len(m.peers))
	for _, p := range m.peers {
		kept := make([]string, 0, len(p.Addrs))
		for _, addr := range p.Addrs {
			var remote net.IP
			if maddr, err := ma.NewMultiaddr(addr); err == nil {
				remote, _ = manet.ToIP(maddr)
			}
			if !refused(p.ID, remote) {
				kept = append(kept, addr)
			}
		}
		p.Addrs = kept
		next = append(next, p)
	}

	m.peers = next
}

func (m *MockServerP2PClient) UnblockPeer(peerID string) error {
	return m.recordAdmission("UnblockPeer "+peerID, nil)
}

// PeerAddrs answers from the peers fixture, as the real client answers from
// its connection table: the Addrs of the matching entry, or nil when there is
// none or it has no addresses (not connected).
func (m *MockServerP2PClient) PeerAddrs(peerID string) []string {
	m.admissionMu.Lock()
	defer m.admissionMu.Unlock()

	for _, p := range m.peers {
		if p.ID == peerID && len(p.Addrs) > 0 {
			return p.Addrs
		}
	}

	return nil
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
	m.admissionMu.Lock()
	defer m.admissionMu.Unlock()

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

func (m *MockBanList) ActiveNetworks() []*net.IPNet {
	args := m.Called()
	return args.Get(0).([]*net.IPNet)
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

func (m *MockKafkaProducer) TryPublish(msg *kafka.Message) bool {
	args := m.Called(msg)
	return args.Bool(0)
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
