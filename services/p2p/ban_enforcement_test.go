package p2p

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	p2pMessageBus "github.com/bsv-blockchain/go-p2p-message-bus"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/p2p/p2p_api"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// recordingBanList records Add calls and answers IsBanned from a fixed set.
type recordingBanList struct {
	noopBanList
	mu     sync.Mutex
	added  []string
	banned map[string]bool
}

func (r *recordingBanList) Add(_ context.Context, ipOrSubnet string, _ time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.added = append(r.added, ipOrSubnet)
	return nil
}

func (r *recordingBanList) IsBanned(ipStr string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.banned[ipStr]
}

func (r *recordingBanList) addedEntries() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.added...)
}

// disconnectCapableClient simulates a future go-p2p-message-bus client that
// implements the networkDisconnector capability.
type disconnectCapableClient struct {
	MockServerP2PClient
	mu           sync.Mutex
	disconnected []peer.ID
}

func (d *disconnectCapableClient) DisconnectPeer(_ context.Context, pid peer.ID) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.disconnected = append(d.disconnected, pid)
	return nil
}

func TestExtractIPFromMultiaddr(t *testing.T) {
	tests := []struct {
		addr string
		want string
	}{
		{"/ip4/203.0.113.7/tcp/9905", "203.0.113.7"},
		{"/ip4/203.0.113.7/tcp/9905/p2p/12D3KooWAfBVdmphtMFPVq3GEpcg3QMiRbrwD9mpd6D6fc4CswRw", "203.0.113.7"},
		{"/ip6/2001:db8::1/tcp/9905", "2001:db8::1"},
		{"/dns4/peer.example.com/tcp/9905", ""},
		// Relay circuits: the ip4/ip6 component is the RELAY's transport
		// address, not the peer's, and must never be extracted.
		{"/ip4/203.0.113.7/tcp/9905/p2p/12D3KooWAfBVdmphtMFPVq3GEpcg3QMiRbrwD9mpd6D6fc4CswRw/p2p-circuit", ""},
		{fmt.Sprintf("/ip4/203.0.113.7/tcp/9905/p2p/12D3KooWAfBVdmphtMFPVq3GEpcg3QMiRbrwD9mpd6D6fc4CswRw/p2p-circuit/p2p/%s", mustNewPeerID(t)), ""},
		{"/ip6/2001:db8::1/tcp/9905/p2p/12D3KooWAfBVdmphtMFPVq3GEpcg3QMiRbrwD9mpd6D6fc4CswRw/p2p-circuit", ""},
		{"not-a-multiaddr", ""},
		{"", ""},
	}
	for _, tt := range tests {
		require.Equal(t, tt.want, extractIPFromMultiaddr(tt.addr), "addr %q", tt.addr)
		// Circuit rows must return "" because of the circuit rejection: assert
		// they parse AND carry a literal IP component that was deliberately
		// not extracted.
		if strings.Contains(tt.addr, "/p2p-circuit") {
			maddr, err := ma.NewMultiaddr(tt.addr)
			require.NoError(t, err, "addr %q must be a valid multiaddr", tt.addr)
			_, err4 := maddr.ValueForProtocol(ma.P_IP4)
			_, err6 := maddr.ValueForProtocol(ma.P_IP6)
			require.True(t, err4 == nil || err6 == nil,
				"circuit addr %q must carry a literal IP so \"\" proves the circuit rejection", tt.addr)
		}
	}
}

func TestOnPeerBanned_BansIPNotMultiaddr(t *testing.T) {
	s, _ := newServerWithLocalRegistry(t)
	pid := mustNewPeerID(t)

	banList := &recordingBanList{}
	s.banList = banList
	s.P2PClient = &MockServerP2PClient{peers: []p2pMessageBus.PeerInfo{{
		ID: pid.String(),
		Addrs: []string{
			fmt.Sprintf("/ip4/203.0.113.7/tcp/9905/p2p/%s", pid),
			fmt.Sprintf("/ip4/203.0.113.7/udp/9906/p2p/%s", pid), // same IP, must be deduped
			"/dns4/peer.example.com/tcp/9905",                    // no literal IP, must be skipped
		},
	}}}

	s.onPeerBanned(pid.String(), "spam")

	added := banList.addedEntries()
	require.Equal(t, []string{"203.0.113.7"}, added, "ban list must receive the bare IP, deduped, never multiaddrs")
	for _, entry := range added {
		require.NotNil(t, net.ParseIP(entry), "ban list entry %q must be a parseable IP", entry)
	}
}

// TestOnPeerBanned_RelayedAddrDoesNotBanRelayIP guards against collateral IP
// bans: a peer connected over a libp2p circuit reports the RELAY's transport
// address, so banning that IP would ban the relay (e.g. the bootstrap node)
// and every peer behind it. Only directly-observed addresses may produce an
// IP ban; the peer-ID ban still applies regardless.
func TestOnPeerBanned_RelayedAddrDoesNotBanRelayIP(t *testing.T) {
	s, _ := newServerWithLocalRegistry(t)
	pid := mustNewPeerID(t)
	relayID := mustNewPeerID(t)

	banList := &recordingBanList{}
	s.banList = banList
	s.P2PClient = &MockServerP2PClient{peers: []p2pMessageBus.PeerInfo{{
		ID: pid.String(),
		Addrs: []string{
			// Relayed connection: the ip4 component is the relay's address.
			fmt.Sprintf("/ip4/203.0.113.7/tcp/9905/p2p/%s/p2p-circuit/p2p/%s", relayID, pid),
			// Direct connection: the only address allowed to produce an IP ban.
			fmt.Sprintf("/ip4/198.51.100.9/tcp/9905/p2p/%s", pid),
		},
	}}}

	s.onPeerBanned(pid.String(), "spam")

	require.Equal(t, []string{"198.51.100.9"}, banList.addedEntries(),
		"only the directly-observed IP may be banned, never the relay's")
}

func TestOnPeerBanned_CircuitOnlyAddrsBanNoIP(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	pid := mustNewPeerID(t)
	relayID := mustNewPeerID(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String()})

	banList := &recordingBanList{}
	s.banList = banList
	s.P2PClient = &MockServerP2PClient{peers: []p2pMessageBus.PeerInfo{{
		ID:    pid.String(),
		Addrs: []string{fmt.Sprintf("/ip4/203.0.113.7/tcp/9905/p2p/%s/p2p-circuit/p2p/%s", relayID, pid)},
	}}}

	s.onPeerBanned(pid.String(), "spam")

	require.Empty(t, banList.addedEntries(),
		"a circuit-only peer must produce no IP ban; the peer-ID ban covers it")

	// The peer-ID ban must still take full effect: gossip filter cache marked
	// banned and the peer removed from the registry.
	v, ok := s.banStatusCache.Load(pid.String())
	require.True(t, ok, "banStatusCache must be primed on ban")
	require.True(t, v.(banStatusCacheEntry).banned, "banStatusCache must mark the peer banned")
	_, found := reg.Get(pid.String())
	require.False(t, found, "the banned circuit-only peer must be removed from the registry")
}

// TestDisconnectPeersOnBanList_IgnoresRelayedPeersOfBannedRelayIP: once a
// relay's IP is on the ban list (e.g. an operator ban), peers connected
// THROUGH that relay must not be swept - only peers directly at that IP.
func TestDisconnectPeersOnBanList_IgnoresRelayedPeersOfBannedRelayIP(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	relayedPID := mustNewPeerID(t)
	relayID := mustNewPeerID(t)
	reg.Register(&blockchain.PeerInfo{ID: relayedPID.String()})

	s.banList = &recordingBanList{banned: map[string]bool{"198.51.100.9": true}}
	s.P2PClient = &MockServerP2PClient{peers: []p2pMessageBus.PeerInfo{{
		ID:    relayedPID.String(),
		Addrs: []string{fmt.Sprintf("/ip4/198.51.100.9/tcp/9905/p2p/%s/p2p-circuit/p2p/%s", relayID, relayedPID)},
	}}}

	s.disconnectPeersOnBanList(context.Background(), "sweep")

	_, found := reg.Get(relayedPID.String())
	require.True(t, found, "a peer reached over a circuit must not be evicted when the relay's IP is banned")
}

func TestServer_ConnectPeer_AllowsCircuitThroughBannedRelayIP(t *testing.T) {
	s, _ := newServerWithLocalRegistry(t)
	s.banList = &recordingBanList{banned: map[string]bool{"198.51.100.9": true}}

	addr := fmt.Sprintf("/ip4/198.51.100.9/tcp/9905/p2p/12D3KooWAfBVdmphtMFPVq3GEpcg3QMiRbrwD9mpd6D6fc4CswRw/p2p-circuit/p2p/%s", mustNewPeerID(t))
	client := &MockServerP2PClient{}
	client.On("Connect", mock.Anything, addr).Return(nil)
	s.P2PClient = client

	resp, err := s.ConnectPeer(context.Background(), &p2p_api.ConnectPeerRequest{PeerAddress: addr})
	require.NoError(t, err)
	require.True(t, resp.Success, "a circuit dial target is not the relay; the relay IP ban must not block it")
	client.AssertExpectations(t)
}

func TestServer_ConnectPeer_AllowsCircuitViaDNSAddrOfBannedRelay(t *testing.T) {
	s, _ := newServerWithLocalRegistry(t)
	s.banList = &recordingBanList{banned: map[string]bool{"127.0.0.1": true, "::1": true}}

	addr := fmt.Sprintf("/dns4/localhost/tcp/9905/p2p/12D3KooWAfBVdmphtMFPVq3GEpcg3QMiRbrwD9mpd6D6fc4CswRw/p2p-circuit/p2p/%s", mustNewPeerID(t))
	client := &MockServerP2PClient{}
	client.On("Connect", mock.Anything, addr).Return(nil)
	s.P2PClient = client

	resp, err := s.ConnectPeer(context.Background(), &p2p_api.ConnectPeerRequest{PeerAddress: addr})
	require.NoError(t, err)
	require.True(t, resp.Success, "the DNS name in a circuit addr is the relay's, not the dial target's")
	client.AssertExpectations(t)
}

func TestHandleBanEvent_IPOnlyEventDisconnectsMatchingPeer(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	pid := mustNewPeerID(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String()})
	require.Equal(t, 1, reg.Count())

	s.banList = &recordingBanList{banned: map[string]bool{"198.51.100.9": true}}
	s.P2PClient = &MockServerP2PClient{peers: []p2pMessageBus.PeerInfo{{
		ID:    pid.String(),
		Addrs: []string{fmt.Sprintf("/ip4/198.51.100.9/tcp/9905/p2p/%s", pid)},
	}}}

	// IP-only event, as emitted by BanList.Add (operator BanPeer RPC).
	s.handleBanEvent(context.Background(), BanEvent{Action: banActionAdd, IP: "198.51.100.9"})

	require.Equal(t, 0, reg.Count(), "peer with banned IP must be removed from the registry")
}

func TestDisconnectPreExistingBannedPeers_DisconnectsOnStartup(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	bannedPID := mustNewPeerID(t)
	okPID := mustNewPeerID(t)
	reg.Register(&blockchain.PeerInfo{ID: bannedPID.String()})
	reg.Register(&blockchain.PeerInfo{ID: okPID.String()})

	s.banList = &recordingBanList{banned: map[string]bool{"198.51.100.9": true}}
	s.P2PClient = &MockServerP2PClient{peers: []p2pMessageBus.PeerInfo{
		{ID: bannedPID.String(), Addrs: []string{fmt.Sprintf("/ip4/198.51.100.9/tcp/9905/p2p/%s", bannedPID)}},
		{ID: okPID.String(), Addrs: []string{fmt.Sprintf("/ip4/192.0.2.10/tcp/9905/p2p/%s", okPID)}},
	}}

	s.disconnectPreExistingBannedPeers(context.Background())

	require.Equal(t, 1, reg.Count(), "only the banned peer must be removed")
	_, found := reg.Get(okPID.String())
	require.True(t, found, "non-banned peer must survive the startup sweep")
}

func TestServer_DisconnectPeer_RemovesPeer(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	pid := mustNewPeerID(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String()})

	resp, err := s.DisconnectPeer(context.Background(), &p2p_api.DisconnectPeerRequest{PeerId: pid.String()})
	require.NoError(t, err)
	require.True(t, resp.Success)
	require.Equal(t, 0, reg.Count())
}

func TestServer_DisconnectPeer_InvalidPeerID(t *testing.T) {
	s, _ := newServerWithLocalRegistry(t)

	resp, err := s.DisconnectPeer(context.Background(), &p2p_api.DisconnectPeerRequest{PeerId: "not-a-peer-id"})
	require.NoError(t, err)
	require.False(t, resp.Success)
	require.NotEmpty(t, resp.Error)
}

func TestDisconnectBannedPeerByID_UsesNetworkDisconnectWhenAvailable(t *testing.T) {
	s, _ := newServerWithLocalRegistry(t)
	pid := mustNewPeerID(t)

	client := &disconnectCapableClient{}
	s.P2PClient = client

	s.disconnectBannedPeerByID(context.Background(), pid, "test")

	require.Equal(t, []peer.ID{pid}, client.disconnected,
		"ban path must sever the libp2p connection when the client supports it")
}

func TestDisconnectPeersOnBanList_SeversConnectionEvenWhenNotInRegistry(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	pid := mustNewPeerID(t)
	require.Equal(t, 0, reg.Count(), "peer intentionally absent from the registry")

	s.banList = &recordingBanList{banned: map[string]bool{"198.51.100.9": true}}
	client := &disconnectCapableClient{}
	client.peers = []p2pMessageBus.PeerInfo{{
		ID:    pid.String(),
		Addrs: []string{fmt.Sprintf("/ip4/198.51.100.9/tcp/9905/p2p/%s", pid)},
	}}
	s.P2PClient = client

	s.disconnectPeersOnBanList(context.Background(), "sweep")

	require.Equal(t, []peer.ID{pid}, client.disconnected,
		"sweep must still sever the libp2p connection when the client supports it")
}

func TestServer_ConnectPeer_RefusesBannedAddress(t *testing.T) {
	s, _ := newServerWithLocalRegistry(t)
	s.banList = &recordingBanList{banned: map[string]bool{"198.51.100.9": true}}

	client := &MockServerP2PClient{}
	s.P2PClient = client

	resp, err := s.ConnectPeer(context.Background(), &p2p_api.ConnectPeerRequest{
		PeerAddress: "/ip4/198.51.100.9/tcp/9905/p2p/12D3KooWAfBVdmphtMFPVq3GEpcg3QMiRbrwD9mpd6D6fc4CswRw",
	})
	require.NoError(t, err)
	require.False(t, resp.Success)
	client.AssertNotCalled(t, "Connect", mock.Anything, mock.Anything)
}

func TestServer_ConnectPeer_RefusesBannedDNSAddress(t *testing.T) {
	s, _ := newServerWithLocalRegistry(t)
	s.banList = &recordingBanList{banned: map[string]bool{"127.0.0.1": true, "::1": true}}

	client := &MockServerP2PClient{}
	s.P2PClient = client

	resp, err := s.ConnectPeer(context.Background(), &p2p_api.ConnectPeerRequest{
		PeerAddress: "/dns4/localhost/tcp/9905/p2p/12D3KooWAfBVdmphtMFPVq3GEpcg3QMiRbrwD9mpd6D6fc4CswRw",
	})
	require.NoError(t, err)
	require.False(t, resp.Success, "a DNS multiaddr resolving to a banned IP must be refused")
	client.AssertNotCalled(t, "Connect", mock.Anything, mock.Anything)
}

func TestServer_ConnectPeer_FailsClosedOnUnresolvableDNS(t *testing.T) {
	s, _ := newServerWithLocalRegistry(t)
	s.banList = &recordingBanList{banned: map[string]bool{"198.51.100.9": true}}

	client := &MockServerP2PClient{}
	s.P2PClient = client

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := s.ConnectPeer(ctx, &p2p_api.ConnectPeerRequest{
		PeerAddress: "/dns4/does-not-exist.invalid/tcp/9905/p2p/12D3KooWAfBVdmphtMFPVq3GEpcg3QMiRbrwD9mpd6D6fc4CswRw",
	})
	require.NoError(t, err)
	require.False(t, resp.Success, "an unresolvable DNS multiaddr must be refused (fail closed)")
	require.NotEmpty(t, resp.Error)
	client.AssertNotCalled(t, "Connect", mock.Anything, mock.Anything)
}

// TestServer_ConnectPeer_FailsClosedOnInvalidMultiaddr pins the fail-closed
// ordering now that the parse is the first gate in checkMultiaddrBanned.
func TestServer_ConnectPeer_FailsClosedOnInvalidMultiaddr(t *testing.T) {
	s, _ := newServerWithLocalRegistry(t)
	s.banList = &recordingBanList{}

	client := &MockServerP2PClient{}
	s.P2PClient = client

	resp, err := s.ConnectPeer(context.Background(), &p2p_api.ConnectPeerRequest{PeerAddress: "not-a-multiaddr"})
	require.NoError(t, err)
	require.False(t, resp.Success, "an unparseable multiaddr must be refused (fail closed)")
	require.NotEmpty(t, resp.Error)
	client.AssertNotCalled(t, "Connect", mock.Anything, mock.Anything)
}

func TestServer_ConnectPeer_ConnectsViaClient(t *testing.T) {
	s, _ := newServerWithLocalRegistry(t)
	s.banList = &recordingBanList{}

	addr := "/ip4/192.0.2.10/tcp/9905/p2p/12D3KooWAfBVdmphtMFPVq3GEpcg3QMiRbrwD9mpd6D6fc4CswRw"
	client := &MockServerP2PClient{}
	client.On("Connect", mock.Anything, addr).Return(nil)
	s.P2PClient = client

	resp, err := s.ConnectPeer(context.Background(), &p2p_api.ConnectPeerRequest{PeerAddress: addr})
	require.NoError(t, err)
	require.True(t, resp.Success)
	client.AssertExpectations(t)
}

func TestShouldSkipBannedPeer_IPBanWithoutRegistryBan(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	pid := mustNewPeerID(t)

	s.banList = &recordingBanList{banned: map[string]bool{"198.51.100.9": true}}
	s.P2PClient = &MockServerP2PClient{peers: []p2pMessageBus.PeerInfo{{
		ID:    pid.String(),
		Addrs: []string{fmt.Sprintf("/ip4/198.51.100.9/tcp/9905/p2p/%s", pid)},
	}}}

	require.False(t, reg.IsBannedPeer(pid.String()), "peer intentionally not banned in the registry")
	require.True(t, s.shouldSkipBannedPeer(pid.String(), "test"),
		"an operator IP ban must drop gossip even without a registry score ban")
}

// TestShouldSkipBannedPeer_RelayedPeerOfBannedRelayIPNotSkipped: gossip from a
// peer reached over a circuit must not be dropped just because the RELAY's IP
// is on the ban list.
func TestShouldSkipBannedPeer_RelayedPeerOfBannedRelayIPNotSkipped(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	pid := mustNewPeerID(t)
	relayID := mustNewPeerID(t)

	s.banList = &recordingBanList{banned: map[string]bool{"198.51.100.9": true}}
	s.P2PClient = &MockServerP2PClient{peers: []p2pMessageBus.PeerInfo{{
		ID:    pid.String(),
		Addrs: []string{fmt.Sprintf("/ip4/198.51.100.9/tcp/9905/p2p/%s/p2p-circuit/p2p/%s", relayID, pid)},
	}}}

	require.False(t, reg.IsBannedPeer(pid.String()), "peer intentionally not banned in the registry")
	require.False(t, s.shouldSkipBannedPeer(pid.String(), "test"),
		"a relay IP ban must not drop gossip from peers behind the relay")
}

// TestCleanupPeerMaps_EvictsExpiredIPBanEntries mirrors the reputationCache
// eviction test: isPeerIPBanned only ever inserts, so cleanupPeerMaps must
// sweep expired ipBanCache entries or the map grows once per unique peer ID.
func TestCleanupPeerMaps_EvictsExpiredIPBanEntries(t *testing.T) {
	s := &Server{
		logger:     ulogger.TestLogger{},
		peerMapTTL: time.Minute,
	}

	now := time.Now()
	s.ipBanCache.Store("expired-peer", ipBanCacheEntry{
		banned:    true,
		expiresAt: now.Add(-time.Second),
	})
	s.ipBanCache.Store("fresh-peer", ipBanCacheEntry{
		banned:    false,
		expiresAt: now.Add(reputationCacheTTL),
	})

	s.cleanupPeerMaps()

	_, expiredStillThere := s.ipBanCache.Load("expired-peer")
	require.False(t, expiredStillThere, "expired ipBanCache entry must be evicted")
	_, freshStillThere := s.ipBanCache.Load("fresh-peer")
	require.True(t, freshStillThere, "fresh ipBanCache entry must survive cleanup")
}

func TestHandleBlockTopic_IPBannedPeerDropped(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	banned := mustNewPeerID(t)
	self := mustNewPeerID(t)

	s.notificationCh = make(chan *notificationMsg, 10)
	s.banList = &recordingBanList{banned: map[string]bool{"198.51.100.9": true}}
	client := &MockServerP2PClient{peerID: self}
	client.peers = []p2pMessageBus.PeerInfo{{
		ID:    banned.String(),
		Addrs: []string{fmt.Sprintf("/ip4/198.51.100.9/tcp/9905/p2p/%s", banned)},
	}}
	s.P2PClient = client

	msg, err := json.Marshal(BlockMessage{
		PeerID:     banned.String(),
		Hash:       "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f",
		Height:     1,
		DataHubURL: "http://203.0.113.5:8090",
	})
	require.NoError(t, err)

	s.handleBlockTopic(context.Background(), msg, banned.String())

	require.Empty(t, s.notificationCh, "IP-banned peer's block must not reach WebSocket clients")
	require.Equal(t, 0, reg.Count(), "IP-banned peer must not be re-registered by its gossip")
}

// banPeerInRegistry pushes a peer over the score-based ban threshold.
func banPeerInRegistry(t *testing.T, reg *blockchain.CentralizedPeerRegistry, pid peer.ID) {
	t.Helper()
	reg.AddBanScore(pid.String(), "spam", 0)
	_, banned := reg.AddBanScore(pid.String(), "spam", 0)
	require.True(t, banned, "two spam scores must cross the default ban threshold")
}

func TestHandleBlockTopic_BannedPeerDroppedBeforeRegistrationAndForwarding(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	banned := mustNewPeerID(t)
	self := mustNewPeerID(t)
	banPeerInRegistry(t, reg, banned)

	s.notificationCh = make(chan *notificationMsg, 10)
	s.P2PClient = &MockServerP2PClient{peerID: self}

	msg, err := json.Marshal(BlockMessage{
		PeerID:     banned.String(),
		Hash:       "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f",
		Height:     1,
		DataHubURL: "http://203.0.113.5:8090",
	})
	require.NoError(t, err)

	s.handleBlockTopic(context.Background(), msg, banned.String())

	require.Empty(t, s.notificationCh, "banned peer's block must not reach WebSocket clients")
	_, ok := s.blockPeerMap.Load("000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f")
	require.False(t, ok, "banned peer must not be recorded in the block peer map")
	// The handler must not have (re-)registered the banned peer; if a ban-score
	// entry exists it must show no message activity.
	if got, found := reg.Get(banned.String()); found {
		require.True(t, got.LastMessageTime.IsZero(), "banned peer must not be re-registered as active")
		require.Zero(t, got.BytesReceived, "banned peer's traffic must not be accounted")
	}
}

func TestHandleNodeStatusTopic_BannedPeerDropped(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	banned := mustNewPeerID(t)
	self := mustNewPeerID(t)
	banPeerInRegistry(t, reg, banned)

	s.notificationCh = make(chan *notificationMsg, 10)
	s.P2PClient = &MockServerP2PClient{peerID: self}

	msg, err := json.Marshal(NodeStatusMessage{
		PeerID:        banned.String(),
		BestHeight:    123,
		BestBlockHash: "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f",
	})
	require.NoError(t, err)

	s.handleNodeStatusTopic(context.Background(), msg, banned.String())

	require.Empty(t, s.notificationCh, "banned peer's node_status must not reach WebSocket clients")
	if got, found := reg.Get(banned.String()); found {
		require.Zero(t, got.Height, "banned peer's height must not be updated")
	}
}
