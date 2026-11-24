package p2p

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	p2pMessageBus "github.com/bsv-blockchain/go-p2p-message-bus"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/p2p/p2p_api"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
)

// newAdmissionTestServer wires a server with a real ban list and a recording
// bus client.
func newAdmissionTestServer(t *testing.T) (*Server, *blockchain.CentralizedPeerRegistry, *MockServerP2PClient) {
	t.Helper()

	s, reg := newServerWithLocalRegistry(t)
	bl, _ := newOwnedUnbanBanList(t)
	s.banList = bl

	client := &MockServerP2PClient{peerID: mustNewPeerID(t)}
	s.P2PClient = client

	return s, reg, client
}

// callsSince returns the admission calls made after the first n.
func callsSince(client *MockServerP2PClient, n int) []string {
	return client.AdmissionCalls()[n:]
}

func banAddr(t *testing.T, s *Server, addr string) {
	t.Helper()

	resp, err := s.BanPeer(context.Background(), &p2p_api.BanPeerRequest{Addr: addr, Until: time.Now().Add(time.Hour).Unix()})
	require.NoError(t, err)
	require.True(t, resp.Ok)
}

func unbanAddr(t *testing.T, s *Server, addr string) {
	t.Helper()

	resp, err := s.UnbanPeer(context.Background(), &p2p_api.UnbanPeerRequest{Addr: addr})
	require.NoError(t, err)
	require.True(t, resp.Ok)
}

// An operator ban is enforced at admission before the RPC returns. Single
// addresses go to BlockAddr, in whatever spelling the ban used, and wider
// networks to BlockSubnet.
func TestBanPeer_BlocksAtAdmission(t *testing.T) {
	s, _, client := newAdmissionTestServer(t)

	banAddr(t, s, "198.51.100.9")
	banAddr(t, s, "198.51.100.9:8333") // the same address: no second rule
	banAddr(t, s, "192.0.2.7/32")
	banAddr(t, s, "::ffff:192.0.2.8")
	banAddr(t, s, "2001:db8::7")
	banAddr(t, s, "203.0.113.0/24")

	require.ElementsMatch(t, []string{
		"BlockAddr 198.51.100.9",
		"BlockAddr 192.0.2.7",
		"BlockAddr 192.0.2.8",
		"BlockAddr 2001:db8::7",
		"BlockSubnet 203.0.113.0/24",
	}, client.AdmissionCalls())
}

// An unban lifts the admission rule once no remaining ban asks for it: a
// host-sized CIDR left in force by a host unban keeps the address blocked.
func TestUnbanPeer_LiftsAdmissionRuleNoBanStillWants(t *testing.T) {
	s, _, client := newAdmissionTestServer(t)

	banAddr(t, s, "198.51.100.9")
	banAddr(t, s, "198.51.100.9/32")
	banAddr(t, s, "203.0.113.0/24")
	n := len(client.AdmissionCalls())

	// A host unban removes the bare spelling only; the /32 still bans it.
	unbanAddr(t, s, "198.51.100.9")
	require.Empty(t, callsSince(client, n), "the address is still banned by its /32")

	unbanAddr(t, s, "198.51.100.9/32")
	require.Equal(t, []string{"UnblockAddr 198.51.100.9"}, callsSince(client, n))

	unbanAddr(t, s, "203.0.113.0/24")
	require.Equal(t, []string{"UnblockAddr 198.51.100.9", "UnblockSubnet 203.0.113.0/24"}, callsSince(client, n))
}

// A ban removed by something other than this service's RPC (the legacy
// service shares the list) reaches admission through its ban-list event.
func TestHandleBanEvent_RemoveLiftsAdmissionRule(t *testing.T) {
	s, _, client := newAdmissionTestServer(t)

	banAddr(t, s, "198.51.100.9")
	n := len(client.AdmissionCalls())

	require.NoError(t, s.banList.Remove(context.Background(), "198.51.100.9"))
	require.Empty(t, callsSince(client, n), "nothing syncs until the event is handled")

	s.handleBanEvent(context.Background(), BanEvent{Action: "remove", IP: "198.51.100.9"})
	require.Equal(t, []string{"UnblockAddr 198.51.100.9"}, callsSince(client, n))
}

// An expired ban raises no event; the sweep's sync lifts its rule.
func TestSyncAdmissionBans_ExpiryLiftsRule(t *testing.T) {
	s, _, client := newAdmissionTestServer(t)

	require.NoError(t, s.banList.Add(context.Background(), "198.51.100.9", time.Now().Add(1500*time.Millisecond)))
	s.syncAdmissionBans()
	require.Equal(t, []string{"BlockAddr 198.51.100.9"}, client.AdmissionCalls())

	require.Eventually(t, func() bool {
		s.syncAdmissionBans()
		return len(client.AdmissionCalls()) == 2
	}, 5*time.Second, 50*time.Millisecond)
	require.Equal(t, "UnblockAddr 198.51.100.9", client.AdmissionCalls()[1])
}

// Clear raises no event, so ClearBanned lifts every IP and subnet rule itself.
func TestClearBanned_LiftsEveryAdmissionRule(t *testing.T) {
	s, _, client := newAdmissionTestServer(t)

	banAddr(t, s, "198.51.100.9")
	banAddr(t, s, "203.0.113.0/24")
	n := len(client.AdmissionCalls())

	resp, err := s.ClearBanned(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.True(t, resp.Ok)
	require.ElementsMatch(t, []string{"UnblockAddr 198.51.100.9", "UnblockSubnet 203.0.113.0/24"}, callsSince(client, n))
}

// A reputation reset lifts no ban, in the registry or on the ban list, so it
// lifts no admission rule either, for one peer or all.
func TestResetReputation_LiftsNoAdmissionRule(t *testing.T) {
	for _, all := range []bool{false, true} {
		t.Run(fmt.Sprintf("allPeers=%v", all), func(t *testing.T) {
			s, reg, client := newAdmissionTestServer(t)
			banned := mustNewPeerID(t)
			banPeerInRegistry(t, reg, banned)
			banAddr(t, s, "198.51.100.9")
			s.primeBannedPeers()
			s.syncAdmissionBans()
			require.Contains(t, client.AdmissionCalls(), "BlockPeer "+banned.String())
			n := len(client.AdmissionCalls())

			req := &p2p_api.ResetReputationRequest{PeerId: banned.String()}
			if all {
				req.PeerId = ""
			}

			_, err := s.ResetReputation(context.Background(), req)
			require.NoError(t, err)

			// A sync before the re-list lands (a ban event, the sweep) and
			// one after it both find the same bans in force.
			s.syncAdmissionBans()
			require.Empty(t, callsSince(client, n), "before the re-list")

			s.refreshBannedPeersIfStale()
			s.syncAdmissionBans()
			require.Empty(t, callsSince(client, n), "after the re-list")
		})
	}
}

// A registry ban that lapses lifts the peer's admission block on the next
// sync once the mirror has re-listed without it.
func TestSyncAdmissionBans_RegistryBanLapseLiftsPeerBlock(t *testing.T) {
	s, reg, client := newAdmissionTestServer(t)
	banned := mustNewPeerID(t)
	banPeerInRegistry(t, reg, banned)

	s.primeBannedPeers()
	s.syncAdmissionBans()
	require.Equal(t, []string{"BlockPeer " + banned.String()}, client.AdmissionCalls())

	// The registry stops listing it.
	_, gen := s.bannedPeers.state(time.Now())
	require.True(t, s.bannedPeers.replace(nil, time.Now(), gen))

	s.syncAdmissionBans()
	require.Equal(t, []string{"BlockPeer " + banned.String(), "UnblockPeer " + banned.String()}, client.AdmissionCalls())
}

// The registry also lists wire-protocol peers, whose IDs the gater cannot
// hold; they are skipped rather than logged as failures on every sync.
func TestSyncAdmissionBans_SkipsNonLibp2pIDs(t *testing.T) {
	s, _, client := newAdmissionTestServer(t)
	s.bannedPeers.add("legacy-198.51.100.9:8333", time.Now())

	s.syncAdmissionBans()
	require.Empty(t, client.AdmissionCalls())
}

// A score-threshold ban blocks the peer ID and each directly observed IP at
// admission at once. A relay's IP is not the peer's and is not blocked.
func TestOnPeerBanned_BlocksPeerAndAddressesAtAdmission(t *testing.T) {
	s, _, client := newAdmissionTestServer(t)
	pid := mustNewPeerID(t)
	relayID := mustNewPeerID(t)
	client.peers = []p2pMessageBus.PeerInfo{{
		ID: pid.String(),
		Addrs: []string{
			fmt.Sprintf("/ip4/198.51.100.9/tcp/9905/p2p/%s", pid),
			fmt.Sprintf("/ip4/203.0.113.7/tcp/9905/p2p/%s/p2p-circuit/p2p/%s", relayID, pid),
		},
	}}

	s.onPeerBanned(pid.String(), ReasonSpam)

	require.ElementsMatch(t, []string{"BlockAddr 198.51.100.9", "BlockPeer " + pid.String()}, client.AdmissionCalls())
	require.True(t, s.banList.IsBanned("198.51.100.9"))

	// A later sync wants the same rules and applies nothing new.
	s.syncAdmissionBans()
	require.Len(t, client.AdmissionCalls(), 2)
}

// A Block* failure is logged and does not stop the application-layer ban:
// the IP is on the ban list, the peer is in the banned set and dropped from
// the registry, and the gossip gate refuses it. The next sync retries, and a
// rule that keeps failing is logged as an error only once.
func TestOnPeerBanned_BlockFailureLoggedAndBanStillApplied(t *testing.T) {
	s, reg, client := newAdmissionTestServer(t)
	logger := &warnRecorder{}
	s.logger = logger

	pid := mustNewPeerID(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String()})
	client.peers = []p2pMessageBus.PeerInfo{{ID: pid.String(), Addrs: []string{fmt.Sprintf("/ip4/198.51.100.9/tcp/9905/p2p/%s", pid)}}}
	client.blockErr = errors.NewServiceError("gater unavailable")

	s.onPeerBanned(pid.String(), ReasonSpam)

	require.ElementsMatch(t, []string{"BlockAddr 198.51.100.9", "BlockPeer " + pid.String()}, client.AdmissionCalls())
	require.Len(t, admissionErrors(logger), 2, "each failed rule is logged")

	require.True(t, s.banList.IsBanned("198.51.100.9"), "the IP ban is recorded")
	require.True(t, s.bannedPeers.contains(pid.String()), "the peer ban is in force for gossip")
	require.Equal(t, 0, reg.Count(), "the peer is dropped at the application layer")
	require.True(t, s.shouldSkipBannedPeer(pid.String(), "test"))

	// The sweep retries both rules without logging them as errors again.
	s.syncAdmissionBans()
	require.Len(t, client.AdmissionCalls(), 4)
	require.Len(t, admissionErrors(logger), 2)

	// Once the bus accepts them they are applied and not retried.
	client.blockErr = nil
	s.syncAdmissionBans()
	s.syncAdmissionBans()
	require.Len(t, client.AdmissionCalls(), 6)
}

func admissionErrors(l *warnRecorder) []string {
	var out []string

	for _, e := range l.errors {
		if strings.Contains(e, "at connection admission failed") {
			out = append(out, e)
		}
	}

	return out
}

// An operator disconnect closes the peer's connections through the bus. The
// bus has no plain disconnect, so it blocks and at once unblocks the peer.
func TestServer_DisconnectPeer_SeversThroughTheBus(t *testing.T) {
	s, reg, client := newAdmissionTestServer(t)
	pid := mustNewPeerID(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String()})

	resp, err := s.DisconnectPeer(context.Background(), &p2p_api.DisconnectPeerRequest{PeerId: pid.String()})
	require.NoError(t, err)
	require.True(t, resp.Success)

	require.Equal(t, []string{"BlockPeer " + pid.String(), "UnblockPeer " + pid.String()}, client.AdmissionCalls())
	require.Equal(t, 0, reg.Count())
}

// Disconnecting a banned peer must not lift its ban's block.
func TestServer_DisconnectPeer_LeavesABannedPeerBlocked(t *testing.T) {
	s, _, client := newAdmissionTestServer(t)
	pid := mustNewPeerID(t)

	s.onPeerBanned(pid.String(), ReasonSpam)
	n := len(client.AdmissionCalls())

	_, err := s.DisconnectPeer(context.Background(), &p2p_api.DisconnectPeerRequest{PeerId: pid.String()})
	require.NoError(t, err)
	require.Empty(t, callsSince(client, n))
}

// intakeOrderClient records, at the first topic subscription, whether the
// registry's banned set was loaded and which admission rules were applied, so
// the test can pin that both happen before any gossip can arrive.
type intakeOrderClient struct {
	MockServerP2PClient
	s      *Server
	banned string

	subscriptions   int
	primedAtFirst   bool
	callsAtFirstSub []string
}

func (c *intakeOrderClient) Subscribe(string) <-chan p2pMessageBus.Message {
	if c.subscriptions == 0 {
		c.primedAtFirst = c.s.bannedPeers.contains(c.banned)
		c.callsAtFirstSub = c.AdmissionCalls()
	}

	c.subscriptions++

	ch := make(chan p2pMessageBus.Message)
	close(ch)

	return ch
}

// Start's gossip intake loads the registry's banned set and applies every ban
// at admission before it subscribes to a topic: a peer banned before this
// process started is refused a connection and dropped from its first message.
func TestStartGossipIntake_EnforcesBansBeforeSubscribing(t *testing.T) {
	s, reg, _ := newAdmissionTestServer(t)
	banned := mustNewPeerID(t)
	banPeerInRegistry(t, reg, banned)
	require.NoError(t, s.banList.Add(context.Background(), "198.51.100.9", time.Now().Add(time.Hour)))

	client := &intakeOrderClient{s: s, banned: banned.String()}
	s.P2PClient = client

	s.startGossipIntake(context.Background())

	require.Equal(t, 4, client.subscriptions)
	require.True(t, client.primedAtFirst, "the banned set must be loaded before the first subscription")
	require.ElementsMatch(t, []string{"BlockAddr 198.51.100.9", "BlockPeer " + banned.String()}, client.callsAtFirstSub,
		"every ban must be applied at admission before the first subscription")
}
