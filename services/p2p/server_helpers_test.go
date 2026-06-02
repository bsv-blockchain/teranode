package p2p

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newServerWithLocalRegistry(t *testing.T) (*Server, *blockchain.CentralizedPeerRegistry) {
	t.Helper()

	reg := blockchain.NewCentralizedPeerRegistry(blockchain.DefaultBanConfig())
	return &Server{
		peerRegistry: blockchain.NewLocalPeerRegistryClient(reg),
		logger:       ulogger.TestLogger{},
		gCtx:         context.Background(),
		settings: &settings.Settings{
			P2P: settings.P2PSettings{
				AllowPrunedNodeFallback: true,
			},
		},
	}, reg
}

func mustNewPeerID(t *testing.T) peer.ID {
	t.Helper()
	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 0)
	require.NoError(t, err)
	pid, err := peer.IDFromPrivateKey(priv)
	require.NoError(t, err)
	return pid
}

func TestServerHelpers_AddPeer_Registers(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	pid := mustNewPeerID(t)

	s.addPeer(pid, "client/1.0", 100, nil, "http://peer.example")

	got, ok := reg.Get(pid.String())
	require.True(t, ok)
	require.Equal(t, "client/1.0", got.ClientName)
	require.Equal(t, uint32(100), got.Height)
	require.False(t, got.IsConnected, "addPeer leaves IsConnected=false")
}

func TestServerHelpers_AddConnectedPeer_FlipsConnected(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	pid := mustNewPeerID(t)

	s.addConnectedPeer(pid, "", 50, nil, "")

	got, ok := reg.Get(pid.String())
	require.True(t, ok)
	require.True(t, got.IsConnected, "addConnectedPeer must flip the flag")
}

func TestServerHelpers_RemovePeer_DropsEntry(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	pid := mustNewPeerID(t)
	s.addConnectedPeer(pid, "", 1, nil, "")
	require.Equal(t, 1, reg.Count())

	s.removePeer(pid)

	require.Equal(t, 0, reg.Count())
}

func TestServerHelpers_GetPeer_FoundAndNotFound(t *testing.T) {
	s, _ := newServerWithLocalRegistry(t)
	pid := mustNewPeerID(t)

	_, found := s.getPeer(pid)
	require.False(t, found)

	s.addPeer(pid, "", 1, nil, "")
	got, found := s.getPeer(pid)
	require.True(t, found)
	require.Equal(t, pid.String(), got.ID)
}

func TestServerHelpers_UpdateStorage_PersistsMode(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	pid := mustNewPeerID(t)
	s.addPeer(pid, "", 1, nil, "")

	s.updateStorage(pid, "pruned")
	got, _ := reg.Get(pid.String())
	require.Equal(t, "pruned", got.Storage)

	// Empty mode must be a no-op (existing setting preserved).
	s.updateStorage(pid, "")
	got, _ = reg.Get(pid.String())
	require.Equal(t, "pruned", got.Storage)
}

func TestServerHelpers_InjectPeerForTesting_MarksFull(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	pid := mustNewPeerID(t)

	s.InjectPeerForTesting(pid, "test-client", "http://peer.example", 99, nil)

	got, ok := reg.Get(pid.String())
	require.True(t, ok)
	require.Equal(t, "full", got.Storage, "InjectPeerForTesting always sets storage=full")
	require.Equal(t, uint32(99), got.Height)
}

func TestServerHelpers_GetSyncPeer_NoCoordinator(t *testing.T) {
	s, _ := newServerWithLocalRegistry(t)
	require.Empty(t, s.getSyncPeer())
}

func TestServerHelpers_GetPeerIDFromDataHubURL(t *testing.T) {
	s, _ := newServerWithLocalRegistry(t)
	pid := mustNewPeerID(t)

	require.Empty(t, s.getPeerIDFromDataHubURL("http://anywhere"))

	s.addPeer(pid, "", 1, nil, "http://datahub.example/api/v1")

	require.Equal(t, pid.String(), s.getPeerIDFromDataHubURL("http://datahub.example/api/v1"))
	require.Empty(t, s.getPeerIDFromDataHubURL("http://other.example"))
}

func TestServerHelpers_ShouldSkipBannedPeer_FlagsBanned(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	pid := mustNewPeerID(t)

	require.False(t, s.shouldSkipBannedPeer(pid.String(), "test"), "no ban → don't skip")

	reg.AddBanScore(pid.String(), "spam", 0)
	reg.AddBanScore(pid.String(), "spam", 0)
	require.True(t, s.shouldSkipBannedPeer(pid.String(), "test"), "score-banned peer is skipped")
}

func TestServerHelpers_ShouldSkipUnhealthyPeer(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	pid := mustNewPeerID(t)

	// Unknown peer ID (not in registry) — must not skip; that's a relay path.
	require.False(t, s.shouldSkipUnhealthyPeer(pid.String(), "test"))

	// Register and drive reputation below threshold.
	reg.Register(&blockchain.PeerInfo{ID: pid.String()})
	reg.UpdateMetrics(pid.String(), 0, 0, 0, false, false, true, 0)
	require.True(t, s.shouldSkipUnhealthyPeer(pid.String(), "test"))

	// Non-decodable IDs (hostname-like) are not skipped.
	require.False(t, s.shouldSkipUnhealthyPeer("not-an-id", "test"))
}

func TestServerHelpers_AddProtocolViolation_AccumulatesScore(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	s.banList = noopBanList{}
	pid := mustNewPeerID(t)

	for i := 0; i < 6; i++ {
		s.addProtocolViolation(pid.String())
	}

	// onPeerBanned removes the peer entry once the threshold is crossed, so
	// check the ban via the registry's IsBannedPeer rather than reading IsBanned
	// off PeerInfo (which is gone).
	require.True(t, reg.IsBannedPeer(pid.String()),
		"protocol_violation = 20; 6 hits = 120 should ban")
}

func TestServerHelpers_ApplyBanScore_NilRegistryNoPanic(t *testing.T) {
	s := &Server{logger: ulogger.TestLogger{}, gCtx: context.Background()}
	require.NotPanics(t, func() { s.applyBanScore("anything", "spam") })
}

func TestServerHelpers_OnPeerBanned_InvalidIDReturnsCleanly(t *testing.T) {
	s, _ := newServerWithLocalRegistry(t)
	require.NotPanics(t, func() { s.onPeerBanned("not-a-peer-id", "spam") })
}

func TestServerHelpers_OnPeerBanned_NoP2PClientStillRemovesPeer(t *testing.T) {
	// onPeerBanned now reads s.settings.P2P.BanDuration; with a custom value
	// set, the helper must still finish the libp2p-side cleanup without
	// panicking when P2PClient is nil (matches the ban-list-only deployment).
	s, reg := newServerWithLocalRegistry(t)
	s.settings.P2P.BanDuration = 7 * time.Minute
	pid := mustNewPeerID(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String()})

	require.NotPanics(t, func() { s.onPeerBanned(pid.String(), "spam") })

	_, ok := reg.Get(pid.String())
	require.False(t, ok, "removePeer must run even with no P2PClient")
}

func TestServer_UpdateBytesReceived_SenderDelta(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	pid := mustNewPeerID(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String()})

	s.updateBytesReceived(pid.String(), "", 1024)
	s.updateBytesReceived(pid.String(), "", 256)

	got, _ := reg.Get(pid.String())
	require.Equal(t, uint64(1280), got.BytesReceived, "delta path must accumulate without read-modify-write")
}

func TestServer_UpdateBytesReceived_GossipUpdatesBoth(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	sender := mustNewPeerID(t)
	originator := mustNewPeerID(t)
	reg.Register(&blockchain.PeerInfo{ID: sender.String()})
	reg.Register(&blockchain.PeerInfo{ID: originator.String()})

	s.updateBytesReceived(sender.String(), originator.String(), 500)

	gotSender, _ := reg.Get(sender.String())
	gotOriginator, _ := reg.Get(originator.String())
	require.Equal(t, uint64(500), gotSender.BytesReceived)
	require.Equal(t, uint64(500), gotOriginator.BytesReceived)
}

func TestServer_UpdateBytesReceived_SameSenderAndOriginatorOnlyOnce(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	pid := mustNewPeerID(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String()})

	// When the originator equals the sender we should NOT double-count.
	s.updateBytesReceived(pid.String(), pid.String(), 100)

	got, _ := reg.Get(pid.String())
	require.Equal(t, uint64(100), got.BytesReceived)
}

func TestServer_UpdateBytesReceived_BadIDIsLoggedNotPanicked(t *testing.T) {
	s, _ := newServerWithLocalRegistry(t)
	require.NotPanics(t, func() { s.updateBytesReceived("not-a-peer-id", "", 10) })
}

func TestServer_UpdateBytesReceived_NilRegistryNoOp(t *testing.T) {
	s := &Server{logger: ulogger.TestLogger{}, gCtx: context.Background()}
	require.NotPanics(t, func() { s.updateBytesReceived("any", "", 10) })
}

func TestIsUnsafeIP(t *testing.T) {
	tests := []struct {
		name     string
		ip       string
		expected string
	}{
		// Safe public IPs
		{"public_ipv4", "8.8.8.8", ""},
		{"public_ipv4_2", "1.1.1.1", ""},
		{"public_ipv6", "2607:f8b0:4004:800::200e", ""},

		// Loopback addresses
		{"loopback_ipv4", "127.0.0.1", "loopback address"},
		{"loopback_ipv4_other", "127.0.0.2", "loopback address"},
		{"loopback_ipv6", "::1", "loopback address"},

		// Private addresses
		{"private_10", "10.0.0.1", "private address"},
		{"private_172", "172.16.0.1", "private address"},
		{"private_192", "192.168.1.1", "private address"},
		{"private_ipv6", "fd00::1", "private address"},

		// Link-local addresses
		{"linklocal_ipv4", "169.254.1.1", "link-local address"},
		{"linklocal_ipv6", "fe80::1", "link-local address"},

		// Unspecified addresses
		{"unspecified_ipv4", "0.0.0.0", "unspecified address"},
		{"unspecified_ipv6", "::", "unspecified address"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ip := net.ParseIP(tt.ip)
			require.NotNil(t, ip, "Failed to parse IP: %s", tt.ip)
			result := isUnsafeIP(ip)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsLocalhostHostname(t *testing.T) {
	tests := []struct {
		hostname string
		expected bool
	}{
		{"localhost", true},
		{"sub.localhost", true},
		{"deep.sub.localhost", true},
		{"example.com", false},
		{"localhosted.com", false},
		{"notlocalhost", false},
		{"localhost.example.com", false},
	}

	for _, tt := range tests {
		t.Run(tt.hostname, func(t *testing.T) {
			result := isLocalhostHostname(tt.hostname)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestValidateDataHubURL(t *testing.T) {
	server := &Server{
		logger: ulogger.New("test"),
	}

	tests := []struct {
		name        string
		url         string
		expectError bool
		errorMsg    string
	}{
		// Valid URLs
		{"valid_http", "http://example.com/api/v1", false, ""},
		{"valid_https", "https://example.com/api/v1", false, ""},
		{"valid_with_port", "http://example.com:8080/api", false, ""},
		{"valid_public_ip", "http://8.8.8.8/api", false, ""},
		{"valid_public_ipv6", "http://[2607:f8b0:4004:800::200e]/api", false, ""},

		// Empty URL
		{"empty_url", "", true, "empty"},

		// Invalid scheme
		{"ftp_scheme", "ftp://example.com/file", true, "invalid scheme"},
		{"file_scheme", "file:///etc/passwd", true, "invalid scheme"},
		{"no_scheme", "example.com/api", true, "invalid scheme"},

		// No hostname
		{"no_hostname", "http:///path", true, "no hostname"},

		// Loopback addresses
		{"loopback_127", "http://127.0.0.1/api", true, "loopback"},
		{"loopback_127_other", "http://127.0.0.2:8080/api", true, "loopback"},
		{"loopback_ipv6", "http://[::1]/api", true, "loopback"},

		// Private addresses
		{"private_10", "http://10.0.0.1/api", true, "private"},
		{"private_172", "http://172.16.0.1/api", true, "private"},
		{"private_192", "http://192.168.1.1/api", true, "private"},

		// Link-local addresses
		{"linklocal_169", "http://169.254.1.1/api", true, "link-local"},
		{"linklocal_ipv6", "http://[fe80::1]/api", true, "link-local"},

		// Unspecified addresses
		{"unspecified_0000", "http://0.0.0.0/api", true, "unspecified"},

		// Localhost hostname
		{"localhost", "http://localhost/api", true, "localhost"},
		{"localhost_port", "http://localhost:8080/api", true, "localhost"},
		{"sub_localhost", "http://sub.localhost/api", true, "localhost"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := server.validateDataHubURL(tt.url)
			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestCleanupPeerMaps_EvictsExpiredReputationEntries confirms that the
// reputationCache populated by shouldSkipUnhealthyPeer does not grow without
// bound — cleanupPeerMaps must sweep entries whose expiresAt has passed.
func TestCleanupPeerMaps_EvictsExpiredReputationEntries(t *testing.T) {
	s := &Server{
		logger:         ulogger.TestLogger{},
		peerMapTTL:     time.Minute,
		peerMapMaxSize: 100,
	}

	now := time.Now()
	s.reputationCache.Store("expired-peer", reputationCacheEntry{
		score:     75.0,
		expiresAt: now.Add(-time.Second),
	})
	s.reputationCache.Store("fresh-peer", reputationCacheEntry{
		score:     75.0,
		expiresAt: now.Add(reputationCacheTTL),
	})

	s.cleanupPeerMaps()

	_, expiredStillThere := s.reputationCache.Load("expired-peer")
	assert.False(t, expiredStillThere, "expired reputationCache entry must be evicted")
	_, freshStillThere := s.reputationCache.Load("fresh-peer")
	assert.True(t, freshStillThere, "fresh reputationCache entry must survive cleanup")
}
