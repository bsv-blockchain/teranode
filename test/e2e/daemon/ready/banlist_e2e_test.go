package smoke

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/services/p2p"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

const (
	sqliteURLStr              = "sqlite:///blockchain"
	msgPeerNotInitiallyBanned = "Peer should not be initially banned"
)

// TestBanListGRPCE2E tests the IP/subnet ban list functionality via gRPC.
// Note: The Server.IsBanned method checks PeerID-based bans (via banManager),
// not IP-based bans. For IP bans, we use BanPeer/UnbanPeer/ListBanned which
// operate on the banList. This test verifies IP ban operations using ListBanned.
func TestBanListGRPCE2E(t *testing.T) {
	RunSequentialTest(t, func(t *testing.T) {
		const testAPIKey = "test-ban-list-api-key" //nolint:gosec // test API key, not a real credential

		// Use file-based SQLite to avoid requiring PostgreSQL and support persistence testing
		sqliteURL, err := url.Parse(sqliteURLStr)
		require.NoError(t, err)

		daemonNode := daemon.NewTestDaemon(t, daemon.TestOptions{
			EnableRPC: true,
			EnableP2P: true,
			SettingsOverrideFunc: func(s *settings.Settings) {
				// Set a known API key so client and server use the same key
				s.GRPCAdminAPIKey = testAPIKey
				// Use SQLite file-based storage instead of PostgreSQL
				s.BlockChain.StoreURL = sqliteURL
			},
		})
		defer daemonNode.Stop(t)

		// Create client using the daemon's settings (which already has the correct ports and API key)
		clientI, err := p2p.NewClient(context.Background(), ulogger.NewVerboseTestLogger(t), daemonNode.Settings)
		require.NoError(t, err)

		client := clientI.(*p2p.Client)

		ctx := context.Background()
		ip := "192.168.100.1"
		until := time.Now().Add(1 * time.Hour).Unix()

		// Ban an IP
		err = client.BanPeer(ctx, ip, until)
		require.NoError(t, err)

		// Verify IP is in banned list
		bannedList, err := client.ListBanned(ctx)
		require.NoError(t, err)
		require.Contains(t, bannedList, ip)

		// Restart node to check persistence
		daemonNode.Stop(t)
		daemonNode.ResetServiceManagerContext(t)
		// Start again with same settings and data dir
		daemonNode = daemon.NewTestDaemon(t, daemon.TestOptions{
			EnableRPC:         true,
			EnableP2P:         true,
			SkipRemoveDataDir: true, // keep data dir for persistence
			SettingsOverrideFunc: func(s *settings.Settings) {
				s.GRPCAdminAPIKey = testAPIKey
				s.BlockChain.StoreURL = sqliteURL
			},
		})
		defer daemonNode.Stop(t)

		clientI, err = p2p.NewClient(context.Background(), ulogger.NewVerboseTestLogger(t), daemonNode.Settings)
		require.NoError(t, err)

		client = clientI.(*p2p.Client)

		// Verify IP is still banned after restart (persistence test)
		bannedList, err = client.ListBanned(ctx)
		require.NoError(t, err)
		require.Contains(t, bannedList, ip, "IP should still be banned after restart")

		// Unban the IP
		err = client.UnbanPeer(ctx, ip)
		require.NoError(t, err)

		// Verify IP is no longer in banned list
		bannedList, err = client.ListBanned(ctx)
		require.NoError(t, err)
		require.NotContains(t, bannedList, ip, "IP should be unbanned")

		// Ban a subnet
		subnet := "10.0.0.0/24"
		err = client.BanPeer(ctx, subnet, until)
		require.NoError(t, err)

		// Verify subnet is in banned list
		bannedList, err = client.ListBanned(ctx)
		require.NoError(t, err)
		require.Contains(t, bannedList, subnet, "Subnet should be in banned list")

		// --- IPv6 Ban Test ---
		ipv6 := "2406:da18:1f7:353a:b079:da22:c7d5:e166"
		until = time.Now().Add(1 * time.Hour).Unix()

		// Ban the IPv6 address
		err = client.BanPeer(ctx, ipv6, until)
		require.NoError(t, err)

		// Verify IPv6 is in banned list
		bannedList, err = client.ListBanned(ctx)
		require.NoError(t, err)
		require.Contains(t, bannedList, ipv6, "IPv6 should be in banned list")

		// Unban the IPv6 address
		err = client.UnbanPeer(ctx, ipv6)
		require.NoError(t, err)

		// Verify IPv6 is no longer in banned list
		bannedList, err = client.ListBanned(ctx)
		require.NoError(t, err)
		require.NotContains(t, bannedList, ipv6, "IPv6 should be unbanned")

		// --- IPv6 Subnet Ban Test ---
		ipv6Subnet := "2406:da18:1f7:353a::/64"
		err = client.BanPeer(ctx, ipv6Subnet, until)
		require.NoError(t, err)

		// Verify IPv6 subnet is in banned list
		bannedList, err = client.ListBanned(ctx)
		require.NoError(t, err)
		require.Contains(t, bannedList, ipv6Subnet, "IPv6 subnet should be in banned list")

		// Clear all bans
		err = client.ClearBanned(ctx)
		require.NoError(t, err)

		// Verify all bans are cleared
		bannedList, err = client.ListBanned(ctx)
		require.NoError(t, err)
		require.Empty(t, bannedList, "All bans should be cleared")
	})
}

// TestPeerIDBanE2E tests the PeerID-based ban functionality via gRPC.
// This tests the banManager which tracks ban scores per PeerID, as opposed to
// the banList which tracks IP/subnet bans. A peer becomes banned when their
// ban score exceeds the threshold (default 100).
func TestPeerIDBanE2E(t *testing.T) {
	RunSequentialTest(t, func(t *testing.T) {
		const testAPIKey = "test-peerid-ban-api-key" //nolint:gosec // test API key, not a real credential

		// Use file-based SQLite to avoid requiring PostgreSQL
		sqliteURL, err := url.Parse(sqliteURLStr)
		require.NoError(t, err)

		daemonNode := daemon.NewTestDaemon(t, daemon.TestOptions{
			EnableRPC: true,
			EnableP2P: true,
			SettingsOverrideFunc: func(s *settings.Settings) {
				s.GRPCAdminAPIKey = testAPIKey
				s.BlockChain.StoreURL = sqliteURL
			},
		})
		defer daemonNode.Stop(t)

		// Create client using the daemon's settings
		clientI, err := p2p.NewClient(context.Background(), ulogger.NewVerboseTestLogger(t), daemonNode.Settings)
		require.NoError(t, err)

		client := clientI.(*p2p.Client)
		ctx := context.Background()

		// Generate a valid PeerID
		privKey, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 0)
		require.NoError(t, err)

		peerID, err := peer.IDFromPrivateKey(privKey)
		require.NoError(t, err)

		peerIDStr := peerID.String()

		// Initially, the peer should not be banned
		isBanned, err := client.IsBanned(ctx, peerIDStr)
		require.NoError(t, err)
		require.False(t, isBanned, msgPeerNotInitiallyBanned)

		// Add ban score with "spam" reason (50 points) - first time
		// Note: Server.AddBanScore expects lowercase reason strings
		// Default threshold is 100, so one "spam" should not ban yet
		err = client.AddBanScore(ctx, peerIDStr, "spam")
		require.NoError(t, err)

		// Verify peer is still not banned (score = 50, threshold = 100)
		isBanned, err = client.IsBanned(ctx, peerIDStr)
		require.NoError(t, err)
		require.False(t, isBanned, "Peer should not be banned after first spam (50 points)")

		// Add ban score with "spam" reason again (another 50 points)
		// Total score should now be 100, which meets the threshold
		err = client.AddBanScore(ctx, peerIDStr, "spam")
		require.NoError(t, err)

		// Verify peer is now banned (score = 100, threshold = 100)
		isBanned, err = client.IsBanned(ctx, peerIDStr)
		require.NoError(t, err)
		require.True(t, isBanned, "Peer should be banned after reaching threshold (100 points)")

		// Test with a different peer using different ban reasons
		privKey2, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 0)
		require.NoError(t, err)

		peerID2, err := peer.IDFromPrivateKey(privKey2)
		require.NoError(t, err)

		peerIDStr2 := peerID2.String()

		// Add various ban reasons to accumulate score
		// protocol_violation = 20, invalid_block = 10, invalid_subtree = 10
		// Total = 40, still below threshold
		err = client.AddBanScore(ctx, peerIDStr2, "protocol_violation")
		require.NoError(t, err)
		err = client.AddBanScore(ctx, peerIDStr2, "invalid_block")
		require.NoError(t, err)
		err = client.AddBanScore(ctx, peerIDStr2, "invalid_subtree")
		require.NoError(t, err)

		// Verify peer2 is not banned yet (score = 40)
		isBanned, err = client.IsBanned(ctx, peerIDStr2)
		require.NoError(t, err)
		require.False(t, isBanned, "Peer2 should not be banned yet (40 points)")

		// Add spam (50) and another invalid_block (10)
		// Total = 100, should now be banned
		err = client.AddBanScore(ctx, peerIDStr2, "spam")
		require.NoError(t, err)
		err = client.AddBanScore(ctx, peerIDStr2, "invalid_block")
		require.NoError(t, err)

		// Verify peer2 is now banned (score = 100)
		isBanned, err = client.IsBanned(ctx, peerIDStr2)
		require.NoError(t, err)
		require.True(t, isBanned, "Peer2 should be banned after reaching threshold (100 points)")
	})
}

// TestPeerIDBanExpirationE2E tests that PeerID-based bans expire automatically
// after the configured BanDuration.
func TestPeerIDBanExpirationE2E(t *testing.T) {
	RunSequentialTest(t, func(t *testing.T) {
		const testAPIKey = "test-ban-expiration-api-key" //nolint:gosec // test API key, not a real credential
		const banDuration = 5 * time.Second

		// Use file-based SQLite to avoid requiring PostgreSQL
		sqliteURL, err := url.Parse(sqliteURLStr)
		require.NoError(t, err)

		daemonNode := daemon.NewTestDaemon(t, daemon.TestOptions{
			EnableRPC: true,
			EnableP2P: true,
			SettingsOverrideFunc: func(s *settings.Settings) {
				s.GRPCAdminAPIKey = testAPIKey
				s.BlockChain.StoreURL = sqliteURL
				// Set a short ban duration for testing expiration
				s.P2P.BanDuration = banDuration
			},
		})
		defer daemonNode.Stop(t)

		// Create client using the daemon's settings
		clientI, err := p2p.NewClient(context.Background(), ulogger.NewVerboseTestLogger(t), daemonNode.Settings)
		require.NoError(t, err)

		client := clientI.(*p2p.Client)
		ctx := context.Background()

		// Generate a valid PeerID
		privKey, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 0)
		require.NoError(t, err)

		peerID, err := peer.IDFromPrivateKey(privKey)
		require.NoError(t, err)

		peerIDStr := peerID.String()

		// Initially, the peer should not be banned
		isBanned, err := client.IsBanned(ctx, peerIDStr)
		require.NoError(t, err)
		require.False(t, isBanned, msgPeerNotInitiallyBanned)

		// Add ban score to trigger a ban (spam = 50 points x 2 = 100 = threshold)
		err = client.AddBanScore(ctx, peerIDStr, "spam")
		require.NoError(t, err)
		err = client.AddBanScore(ctx, peerIDStr, "spam")
		require.NoError(t, err)

		// Verify peer is now banned
		isBanned, err = client.IsBanned(ctx, peerIDStr)
		require.NoError(t, err)
		require.True(t, isBanned, "Peer should be banned after reaching threshold")

		// intentional: ban TTL must physically elapse (banDuration + 1s buffer); polling cannot shorten a real timeout
		t.Logf("Waiting %v for ban to expire...", banDuration+time.Second)
		time.Sleep(banDuration + time.Second)

		// Verify peer is no longer banned (ban expired)
		isBanned, err = client.IsBanned(ctx, peerIDStr)
		require.NoError(t, err)
		require.False(t, isBanned, "Peer should no longer be banned after ban duration expired")
	})
}

// TestBanListRPCE2E tests the IP/subnet ban list functionality via JSON-RPC.
// This test verifies the same functionality as TestBanListGRPCE2E but uses
// the RPC interface (setban, listbanned, clearbanned commands).
//
// Note: The 'isbanned' RPC command checks PeerID-based bans (via banManager),
// while 'setban' operates on IP-based bans (via banList). This test uses
// 'listbanned' to verify IP bans correctly.
func TestBanListRPCE2E(t *testing.T) {
	RunSequentialTest(t, func(t *testing.T) {
		const testAPIKey = "test-ban-list-rpc-api-key" //nolint:gosec // test API key, not a real credential

		// Use file-based SQLite to avoid requiring PostgreSQL
		sqliteURL, err := url.Parse(sqliteURLStr)
		require.NoError(t, err)

		daemonNode := daemon.NewTestDaemon(t, daemon.TestOptions{
			EnableRPC: true,
			EnableP2P: true,
			SettingsOverrideFunc: func(s *settings.Settings) {
				s.GRPCAdminAPIKey = testAPIKey
				s.BlockChain.StoreURL = sqliteURL
			},
		})
		defer daemonNode.Stop(t)

		ctx := context.Background()
		ip := "192.168.200.1"
		banTimeSeconds := int64(3600) // 1 hour in seconds

		// Ban an IP using RPC setban command
		result, err := daemonNode.CallRPC(ctx, "setban", []interface{}{ip, "add", banTimeSeconds, false})
		require.NoError(t, err)
		t.Logf("setban add result: %s", result)

		// Verify IP appears in listbanned (setban uses banList, so we verify via listbanned)
		result, err = daemonNode.CallRPC(ctx, "listbanned", []interface{}{})
		require.NoError(t, err)
		require.Contains(t, result, ip, "IP should appear in banned list after setban add")

		// Unban the IP using RPC setban remove command
		result, err = daemonNode.CallRPC(ctx, "setban", []interface{}{ip, "remove"})
		require.NoError(t, err)
		t.Logf("setban remove result: %s", result)

		// Verify IP is no longer in listbanned
		result, err = daemonNode.CallRPC(ctx, "listbanned", []interface{}{})
		require.NoError(t, err)
		require.NotContains(t, result, ip, "IP should not appear in banned list after setban remove")

		// Ban a subnet using RPC
		subnet := "10.0.1.0/24"
		result, err = daemonNode.CallRPC(ctx, "setban", []interface{}{subnet, "add", banTimeSeconds, false})
		require.NoError(t, err)
		t.Logf("setban subnet result: %s", result)

		// Verify subnet is in banned list
		result, err = daemonNode.CallRPC(ctx, "listbanned", []interface{}{})
		require.NoError(t, err)
		require.Contains(t, result, subnet, "Subnet should appear in banned list")

		// --- IPv6 Ban Test via RPC ---
		ipv6 := "2406:da18:1f7:353b:b079:da22:c7d5:e166"

		// Ban the IPv6 address
		result, err = daemonNode.CallRPC(ctx, "setban", []interface{}{ipv6, "add", banTimeSeconds, false})
		require.NoError(t, err)
		t.Logf("setban IPv6 result: %s", result)

		// Verify IPv6 appears in listbanned
		result, err = daemonNode.CallRPC(ctx, "listbanned", []interface{}{})
		require.NoError(t, err)
		require.Contains(t, result, ipv6, "IPv6 should appear in banned list")

		// Unban the IPv6 address
		_, err = daemonNode.CallRPC(ctx, "setban", []interface{}{ipv6, "remove"})
		require.NoError(t, err)

		// Verify IPv6 is no longer in listbanned
		result, err = daemonNode.CallRPC(ctx, "listbanned", []interface{}{})
		require.NoError(t, err)
		require.NotContains(t, result, ipv6, "IPv6 should not appear in banned list after removal")

		// Note: IPv6 subnet bans via RPC have a known bug - see TestBanListRPCIPv6SubnetBug

		// Clear all bans using RPC clearbanned command
		result, err = daemonNode.CallRPC(ctx, "clearbanned", []interface{}{})
		require.NoError(t, err)
		t.Logf("clearbanned result: %s", result)

		// Verify all bans are cleared
		result, err = daemonNode.CallRPC(ctx, "listbanned", []interface{}{})
		require.NoError(t, err)
		// After clearing, the list should be empty (result should not contain our test IPs)
		require.NotContains(t, result, subnet, "Subnet should not appear after clearbanned")
	})
}

// TestBanListRPCAbsoluteTimeE2E tests the setban command with absolute time parameter.
// When the 'absolute' flag is true, the ban time is interpreted as a Unix timestamp
// rather than a duration in seconds.
func TestBanListRPCAbsoluteTimeE2E(t *testing.T) {
	RunSequentialTest(t, func(t *testing.T) {
		const testAPIKey = "test-ban-absolute-api-key"

		// Use file-based SQLite to avoid requiring PostgreSQL
		sqliteURL, err := url.Parse(sqliteURLStr)
		require.NoError(t, err)

		daemonNode := daemon.NewTestDaemon(t, daemon.TestOptions{
			EnableRPC: true,
			EnableP2P: true,
			SettingsOverrideFunc: func(s *settings.Settings) {
				s.GRPCAdminAPIKey = testAPIKey
				s.BlockChain.StoreURL = sqliteURL
			},
		})
		defer daemonNode.Stop(t)

		ctx := context.Background()
		ip := "192.168.201.1"

		// Use absolute time - ban until 1 hour from now using absolute Unix timestamp
		absoluteExpireTime := time.Now().Add(1 * time.Hour).Unix()

		// Ban an IP using RPC setban command with absolute=true
		result, err := daemonNode.CallRPC(ctx, "setban", []interface{}{ip, "add", absoluteExpireTime, true})
		require.NoError(t, err)
		t.Logf("setban with absolute time result: %s", result)

		// Verify IP appears in listbanned (setban uses banList)
		result, err = daemonNode.CallRPC(ctx, "listbanned", []interface{}{})
		require.NoError(t, err)
		require.Contains(t, result, ip, "IP should appear in banned list with absolute time")

		// Clean up - remove the ban
		_, err = daemonNode.CallRPC(ctx, "setban", []interface{}{ip, "remove"})
		require.NoError(t, err)

		// Verify ban was removed
		result, err = daemonNode.CallRPC(ctx, "listbanned", []interface{}{})
		require.NoError(t, err)
		require.NotContains(t, result, ip, "IP should not appear in banned list after removal")
	})
}

// TestBanListRPCIPv6Subnet tests IPv6 CIDR subnet banning via RPC.
// This test verifies that the isIPOrSubnet function correctly handles IPv6 CIDR
// notation (e.g., "2406:da18:1f7:353b::/64") without incorrectly treating colons
// as port separators.
//
// Previously tracked bug: isIPOrSubnet was splitting on ":" before ParseCIDR,
// which broke IPv6 subnet notation. This has been fixed by passing CIDR notation
// directly to net.ParseCIDR without port-stripping logic.
func TestBanListRPCIPv6Subnet(t *testing.T) {
	RunSequentialTest(t, func(t *testing.T) {
		const testAPIKey = "test-ipv6-subnet-api-key" //nolint:gosec // test API key, not a real credential

		// Use file-based SQLite to avoid requiring PostgreSQL
		sqliteURL, err := url.Parse(sqliteURLStr)
		require.NoError(t, err)

		daemonNode := daemon.NewTestDaemon(t, daemon.TestOptions{
			EnableRPC: true,
			EnableP2P: true,
			SettingsOverrideFunc: func(s *settings.Settings) {
				s.GRPCAdminAPIKey = testAPIKey
				s.BlockChain.StoreURL = sqliteURL
			},
		})
		defer daemonNode.Stop(t)

		ctx := context.Background()
		banTimeSeconds := int64(3600)

		// Ban IPv6 subnet via RPC setban command
		ipv6Subnet := "2406:da18:1f7:353b::/64"
		_, err = daemonNode.CallRPC(ctx, "setban", []interface{}{ipv6Subnet, "add", banTimeSeconds, false})
		require.NoError(t, err, "Should be able to ban IPv6 subnet via RPC")

		// Verify IPv6 subnet is in banned list
		result, err := daemonNode.CallRPC(ctx, "listbanned", []interface{}{})
		require.NoError(t, err)
		require.Contains(t, result, ipv6Subnet, "IPv6 subnet should appear in banned list")
	})
}

// TestIsBannedRPCWithPeerID tests the 'isbanned' RPC command with PeerID format.
//
// This test verifies that the 'isbanned' RPC correctly handles both IP addresses
// and PeerID strings. The implementation now:
// - Accepts both IP/subnet format and PeerID format (removed strict IP validation)
// - Checks both banList (IP-based) and banManager (PeerID-based) via P2P client
// - Only calls legacy client for valid IP formats
//
// Previously tracked bug: There was a mismatch where 'isbanned' validated for IP
// format but only checked PeerID-based bans (banManager). This has been fixed to
// support both ban systems by making IP validation optional and checking both
// banList and banManager.
func TestIsBannedRPCWithPeerID(t *testing.T) {
	RunSequentialTest(t, func(t *testing.T) {
		const testAPIKey = "test-isbanned-rpc-api-key"

		// Use file-based SQLite to avoid requiring PostgreSQL
		sqliteURL, err := url.Parse(sqliteURLStr)
		require.NoError(t, err)

		daemonNode := daemon.NewTestDaemon(t, daemon.TestOptions{
			EnableRPC: true,
			EnableP2P: true,
			SettingsOverrideFunc: func(s *settings.Settings) {
				s.GRPCAdminAPIKey = testAPIKey
				s.BlockChain.StoreURL = sqliteURL
			},
		})
		defer daemonNode.Stop(t)

		// Create gRPC P2P client for AddBanScore
		clientI, err := p2p.NewClient(context.Background(), ulogger.NewVerboseTestLogger(t), daemonNode.Settings)
		require.NoError(t, err)
		client := clientI.(*p2p.Client)

		ctx := context.Background()

		// Generate a valid PeerID
		privKey, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 0)
		require.NoError(t, err)
		peerID, err := peer.IDFromPrivateKey(privKey)
		require.NoError(t, err)
		peerIDStr := peerID.String()

		// Check if PeerID is not initially banned (now accepts PeerID format)
		result, err := daemonNode.CallRPC(ctx, "isbanned", []interface{}{peerIDStr})
		require.NoError(t, err, "Should accept PeerID format since it checks both ban systems")
		require.Contains(t, result, "false", msgPeerNotInitiallyBanned)

		// Add ban score via gRPC to trigger a ban (spam=50 x 2 = 100 = threshold)
		err = client.AddBanScore(ctx, peerIDStr, "spam")
		require.NoError(t, err)
		err = client.AddBanScore(ctx, peerIDStr, "spam")
		require.NoError(t, err)

		// Verify peer is now banned via RPC isbanned command
		result, err = daemonNode.CallRPC(ctx, "isbanned", []interface{}{peerIDStr})
		require.NoError(t, err)
		require.Contains(t, result, "true", "Peer should be banned after AddBanScore")
	})
}
