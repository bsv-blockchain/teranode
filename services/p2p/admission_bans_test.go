package p2p

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	p2pMessageBus "github.com/bsv-blockchain/go-p2p-message-bus"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/p2p/p2p_api"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
)

// newAdmissionTestServer wires a server with a real ban list and a recording
// bus client.
func newAdmissionTestServer(t *testing.T) (*Server, *blockchain.CentralizedPeerRegistry, *MockServerP2PClient) {
	t.Helper()

	s, reg := newServerWithLocalRegistry(t)
	s.settings.P2P.EnforceBansAtAdmission = true
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

// An expired ban raises no event; the sweep tick lifts its rule.
func TestBanSweepTick_ExpiryLiftsRule(t *testing.T) {
	s, _, client := newAdmissionTestServer(t)

	require.NoError(t, s.banList.Add(context.Background(), "198.51.100.9", time.Now().Add(1500*time.Millisecond)))
	s.banSweepTick(context.Background())
	require.Equal(t, []string{"BlockAddr 198.51.100.9"}, client.AdmissionCalls())

	require.Eventually(t, func() bool {
		s.banSweepTick(context.Background())
		return len(client.AdmissionCalls()) == 2
	}, 5*time.Second, 50*time.Millisecond)
	require.Equal(t, "UnblockAddr 198.51.100.9", client.AdmissionCalls()[1])
}

// The ticker runs the sweep with no ban event at all: a ban added straight to
// the list, as another holder of the shared list would, reaches admission on
// the next tick.
func TestListenForBanEvents_TickerRunsTheSweep(t *testing.T) {
	prev := banSweepInterval
	banSweepInterval = 20 * time.Millisecond
	t.Cleanup(func() { banSweepInterval = prev })

	s, _, client := newAdmissionTestServer(t)
	require.NoError(t, s.banList.Add(context.Background(), "198.51.100.9", time.Now().Add(time.Hour)))
	require.Empty(t, client.AdmissionCalls())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		defer close(done)
		s.listenForBanEvents(ctx)
	}()

	t.Cleanup(func() {
		cancel()
		<-done
	})

	require.Eventually(t, func() bool {
		return slices.Contains(client.AdmissionCalls(), "BlockAddr 198.51.100.9")
	}, 5*time.Second, 10*time.Millisecond)
}

// withSyncCoordinator gives s a sync coordinator over its registry and makes
// peerID its current sync peer.
func withSyncCoordinator(t *testing.T, s *Server, peerID string) *SyncCoordinator {
	t.Helper()

	sc := NewSyncCoordinator(context.Background(), ulogger.TestLogger{}, s.settings, s.peerRegistry,
		NewPeerSelector(ulogger.TestLogger{}, s.settings), nil, nil)
	sc.SetGetLocalHeightCallback(func(context.Context) uint32 { return 0 })
	t.Cleanup(func() { sc.Stop(context.Background()) })

	sc.currentSyncPeer = peerID
	s.syncCoordinator = sc

	return sc
}

// A registered peer connected from an address that gets banned leaves the
// registry and stops being the sync peer, whichever path applies the ban.
// The fake bus closes the covered connections inside Block*, as the real one
// does, so with admission enforcement on the peer is listed with no
// addresses by the time any walk of GetPeers runs. With it off the
// connection stays open and the walk finds it.
func TestBanOnAddress_DropsConnectedPeerFromRegistryAndSync(t *testing.T) {
	relayID := mustNewPeerID(t)

	tests := []struct {
		name      string
		enforce   bool
		circuit   bool // the peer is reached through a relay on the banned address
		ban       func(t *testing.T, s *Server)
		keepsConn bool
	}{
		{
			name:    "BanPeer RPC",
			enforce: true,
			ban:     func(t *testing.T, s *Server) { banAddr(t, s, "198.51.100.9") },
		},
		{
			name:    "BanPeer RPC, registry lookup fails",
			enforce: true,
			ban: func(t *testing.T, s *Server) {
				counting := newCountingRegistryClient(s.peerRegistry)
				counting.failGetPeer = errors.NewServiceError("registry unavailable")
				s.peerRegistry = counting
				banAddr(t, s, "198.51.100.9")
			},
		},
		{
			name:    "BanPeer RPC, subnet",
			enforce: true,
			ban:     func(t *testing.T, s *Server) { banAddr(t, s, "198.51.100.0/24") },
		},
		{
			name:    "BanPeer RPC, relay address",
			enforce: true,
			circuit: true,
			ban:     func(t *testing.T, s *Server) { banAddr(t, s, "198.51.100.9") },
		},
		{
			name:    "ban-list event",
			enforce: true,
			ban: func(t *testing.T, s *Server) {
				require.NoError(t, s.banList.Add(context.Background(), "198.51.100.9", time.Now().Add(time.Hour)))
				s.handleBanEvent(context.Background(), BanEvent{Action: banActionAdd, IP: "198.51.100.9"})
			},
		},
		{
			name:    "sweep tick, ban added without an event",
			enforce: true,
			ban: func(t *testing.T, s *Server) {
				require.NoError(t, s.banList.Add(context.Background(), "198.51.100.9", time.Now().Add(time.Hour)))
				s.banSweepTick(context.Background())
			},
		},
		{
			name:      "admission off, BanPeer RPC then ban-list event",
			enforce:   false,
			keepsConn: true,
			ban: func(t *testing.T, s *Server) {
				banAddr(t, s, "198.51.100.9")
				s.handleBanEvent(context.Background(), BanEvent{Action: banActionAdd, IP: "198.51.100.9"})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, reg, client := newAdmissionTestServer(t)
			s.settings.P2P.EnforceBansAtAdmission = tt.enforce

			pid := mustNewPeerID(t)
			addr := fmt.Sprintf("/ip4/198.51.100.9/tcp/9905/p2p/%s", pid)
			if tt.circuit {
				addr = fmt.Sprintf("/ip4/198.51.100.9/tcp/9905/p2p/%s/p2p-circuit/p2p/%s", relayID, pid)
			}

			reg.Register(&blockchain.PeerInfo{ID: pid.String()})
			client.peers = []p2pMessageBus.PeerInfo{{ID: pid.String(), Addrs: []string{addr}}}
			sc := withSyncCoordinator(t, s, pid.String())

			tt.ban(t, s)

			_, inRegistry := reg.Get(pid.String())
			require.False(t, inRegistry, "the peer leaves the registry")
			require.Empty(t, sc.GetCurrentSyncPeer(), "the peer stops being the sync peer")
			require.Equal(t, tt.keepsConn, len(client.PeerAddrs(pid.String())) > 0)
		})
	}
}

// A peer whose registration is still queued in the batcher when a ban closes
// its connection is not in the registry yet. Its queued registration is
// dropped, so the next flush does not register a peer with no connection.
func TestBanOnAddress_DropsQueuedRegistration(t *testing.T) {
	s, reg, client := newAdmissionTestServer(t)
	s.registryBatcher = newPeerRegistryBatcher(context.Background(), ulogger.TestLogger{}, s.peerRegistry, time.Hour)

	pid := mustNewPeerID(t)
	client.peers = []p2pMessageBus.PeerInfo{{ID: pid.String(), Addrs: []string{fmt.Sprintf("/ip4/198.51.100.9/tcp/9905/p2p/%s", pid)}}}
	s.addConnectedPeer(pid, "client", peerHeightClaim{Height: 10, AdvertisedHeight: 10}, nil, "https://datahub.example")

	banAddr(t, s, "198.51.100.9")
	s.registryBatcher.flushOnce(context.Background())

	_, inRegistry := reg.Get(pid.String())
	require.False(t, inRegistry, "the flush does not register the disconnected peer")
}

// relayNodeStatus delivers pid's own node_status as another peer relays it,
// and reports whether it registered pid.
func relayNodeStatus(t *testing.T, s *Server, reg *blockchain.CentralizedPeerRegistry, pid peer.ID) bool {
	t.Helper()

	msg, err := json.Marshal(NodeStatusMessage{
		PeerID:        pid.String(),
		BaseURL:       "https://datahub.example.com",
		BestHeight:    200,
		BestBlockHash: "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f",
	})
	require.NoError(t, err)

	s.handleNodeStatusTopic(context.Background(), msg, pid.String())

	_, registered := reg.Get(pid.String())

	return registered
}

// After an IP or subnet ban closes a host's connection, its relayed gossip
// neither registers it nor makes it the sync peer until no rule covers its IP;
// a peer behind a banned relay is not gossip-banned, and admission off is as before.
func TestBanOnAddress_DropsTheClosedHostsRelayedGossipUntilLifted(t *testing.T) {
	relayID := mustNewPeerID(t)
	unban := func(addr string) func(t *testing.T, s *Server) {
		return func(t *testing.T, s *Server) { unbanAddr(t, s, addr) }
	}

	tests := []struct {
		name       string
		enforce    bool
		circuit    bool
		otherAddr  bool                            // the host also holds a connection from an unbanned address
		bans       []string                        // applied in order, each synced
		lifts      []func(t *testing.T, s *Server) // the last one readmits the host
		dropsWhile bool                            // relayed gossip is dropped while a ban stands
	}{
		{name: "address, unban", enforce: true, dropsWhile: true,
			bans: []string{"198.51.100.9"}, lifts: []func(*testing.T, *Server){unban("198.51.100.9")}},
		{name: "subnet, unban", enforce: true, dropsWhile: true,
			bans: []string{"198.51.100.0/24"}, lifts: []func(*testing.T, *Server){unban("198.51.100.0/24")}},
		{name: "address, clearbanned", enforce: true, dropsWhile: true,
			bans: []string{"198.51.100.9"}, lifts: []func(*testing.T, *Server){func(t *testing.T, s *Server) {
				_, err := s.ClearBanned(context.Background(), &emptypb.Empty{})
				require.NoError(t, err)
			}}},
		{name: "address, removed by another holder of the list, then the sweep", enforce: true, dropsWhile: true,
			bans: []string{"198.51.100.9"}, lifts: []func(*testing.T, *Server){func(t *testing.T, s *Server) {
				require.NoError(t, s.banList.Remove(context.Background(), "198.51.100.9"))
				s.banSweepTick(context.Background())
			}}},
		{name: "another key still wants the rule", enforce: true, dropsWhile: true,
			bans:  []string{"198.51.100.9", "198.51.100.9/32"},
			lifts: []func(*testing.T, *Server){unban("198.51.100.9"), unban("198.51.100.9/32")}},
		{name: "a later subnet still covers the address", enforce: true, dropsWhile: true,
			bans:  []string{"198.51.100.9", "198.51.100.0/24"},
			lifts: []func(*testing.T, *Server){unban("198.51.100.9"), unban("198.51.100.0/24")}},
		{name: "a connection from an unbanned address does not readmit it", enforce: true, otherAddr: true, dropsWhile: true,
			bans: []string{"198.51.100.9"}, lifts: []func(*testing.T, *Server){unban("198.51.100.9")}},
		{name: "relay address: the peer behind it is not banned on gossip", enforce: true, circuit: true,
			bans: []string{"198.51.100.9"}, lifts: []func(*testing.T, *Server){unban("198.51.100.9")}},
		{name: "admission off: the open connection is IP-checked as before", enforce: false, dropsWhile: true,
			bans: []string{"198.51.100.9"}, lifts: []func(*testing.T, *Server){unban("198.51.100.9")}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, reg, client := newAdmissionTestServer(t)
			s.settings.P2P.EnforceBansAtAdmission = tt.enforce
			s.notificationCh = make(chan *notificationMsg, 10)

			pid := mustNewPeerID(t)
			addr := fmt.Sprintf("/ip4/198.51.100.9/tcp/9905/p2p/%s", pid)
			if tt.circuit {
				addr = fmt.Sprintf("/ip4/198.51.100.9/tcp/9905/p2p/%s/p2p-circuit/p2p/%s", relayID, pid)
			}

			addrs := []string{addr}
			if tt.otherAddr {
				addrs = append(addrs, fmt.Sprintf("/ip4/192.0.2.4/tcp/9905/p2p/%s", pid))
			}

			client.peers = []p2pMessageBus.PeerInfo{{ID: pid.String(), Addrs: addrs}}
			reg.Register(&blockchain.PeerInfo{ID: pid.String()})
			// Forced, the peer is chosen whenever it is registered, so the
			// sync assertions turn on registration alone.
			s.settings.P2P.ForceSyncPeer = pid.String()
			sc := withSyncCoordinator(t, s, pid.String())

			for _, ban := range tt.bans {
				require.NoError(t, s.banList.Add(context.Background(), ban, time.Now().Add(time.Hour)))
				s.syncAdmissionBans()
				s.disconnectPeersOnBanList(context.Background(), "test")
			}
			_, inRegistry := reg.Get(pid.String())
			require.False(t, inRegistry, "the ban drops the peer")

			for i, lift := range tt.lifts {
				require.Equal(t, !tt.dropsWhile, relayNodeStatus(t, s, reg, pid), "relayed gossip before lift %d", i)
				require.NoError(t, sc.TriggerSync())
				require.Equal(t, !tt.dropsWhile, sc.GetCurrentSyncPeer() == pid.String(), "sync peer before lift %d", i)

				lift(t, s)
			}

			require.True(t, relayNodeStatus(t, s, reg, pid), "relayed gossip registers the peer once no rule covers it")
			require.NoError(t, sc.TriggerSync())
			require.Equal(t, pid.String(), sc.GetCurrentSyncPeer(), "and it can be the sync peer again")
		})
	}
}

// The remembered peers are capped: past the cap a peer is not remembered,
// and lifting the rule releases the rest.
func TestBanOnAddress_RememberedPeersAreCapped(t *testing.T) {
	s, _, client := newAdmissionTestServer(t)

	peers := make([]p2pMessageBus.PeerInfo, 0, maxIPRuleClosedPeers+1)
	for i := 0; i <= maxIPRuleClosedPeers; i++ {
		id := fmt.Sprintf("peer-%d", i)
		peers = append(peers, p2pMessageBus.PeerInfo{ID: id, Addrs: []string{"/ip4/198.51.100.9/tcp/9905"}})
	}
	client.peers = peers

	banAddr(t, s, "198.51.100.9")
	require.Len(t, s.admission.closed, maxIPRuleClosedPeers)
	require.True(t, s.admission.closedByIPRule("peer-0"))
	require.False(t, s.admission.closedByIPRule(fmt.Sprintf("peer-%d", maxIPRuleClosedPeers)))

	unbanAddr(t, s, "198.51.100.9")
	require.Empty(t, s.admission.closed)
	require.False(t, s.admission.closedByIPRule("peer-0"))
}

// A registry ban recorded without this process's onPeerBanned (another
// service's AddBanScore) reaches admission through a sync, which blocks the
// peer ID and closes its connection. The sync drops the peer too.
func TestSyncAdmissionBans_PeerBlockDropsTheConnectedPeer(t *testing.T) {
	s, reg, client := newAdmissionTestServer(t)
	pid := mustNewPeerID(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String()})
	client.peers = []p2pMessageBus.PeerInfo{{ID: pid.String(), Addrs: []string{fmt.Sprintf("/ip4/192.0.2.4/tcp/9905/p2p/%s", pid)}}}
	sc := withSyncCoordinator(t, s, pid.String())

	banPeerInRegistry(t, reg, pid)
	s.primeBannedPeers()
	s.banSweepTick(context.Background())

	require.Equal(t, []string{"BlockPeer " + pid.String()}, client.AdmissionCalls())
	_, inRegistry := reg.Get(pid.String())
	require.False(t, inRegistry, "the peer leaves the registry")
	require.True(t, reg.IsBannedPeer(pid.String()), "its ban outlives the record")
	require.Empty(t, sc.GetCurrentSyncPeer(), "the peer stops being the sync peer")
}

// A score-threshold ban blocks the banned peer's IP, which closes the
// connection of every other peer on that address too. Those peers leave the
// registry and the sync coordinator, and their relayed gossip is dropped.
func TestOnPeerBanned_DropsOtherPeersOnTheBannedAddress(t *testing.T) {
	s, reg, client := newAdmissionTestServer(t)
	s.notificationCh = make(chan *notificationMsg, 10)

	banned := mustNewPeerID(t)
	sameHost := mustNewPeerID(t)
	elsewhere := mustNewPeerID(t)

	for _, pid := range []string{banned.String(), sameHost.String(), elsewhere.String()} {
		reg.Register(&blockchain.PeerInfo{ID: pid})
	}

	client.peers = []p2pMessageBus.PeerInfo{
		{ID: banned.String(), Addrs: []string{fmt.Sprintf("/ip4/198.51.100.9/tcp/9905/p2p/%s", banned)}},
		{ID: sameHost.String(), Addrs: []string{fmt.Sprintf("/ip4/198.51.100.9/tcp/9906/p2p/%s", sameHost)}},
		{ID: elsewhere.String(), Addrs: []string{fmt.Sprintf("/ip4/192.0.2.4/tcp/9905/p2p/%s", elsewhere)}},
	}
	sc := withSyncCoordinator(t, s, sameHost.String())

	s.onPeerBanned(banned.String(), ReasonSpam)

	_, inRegistry := reg.Get(banned.String())
	require.False(t, inRegistry, "the banned peer leaves the registry")
	_, inRegistry = reg.Get(sameHost.String())
	require.False(t, inRegistry, "the other peer on the banned address leaves the registry")
	require.Empty(t, sc.GetCurrentSyncPeer(), "and stops being the sync peer")
	_, inRegistry = reg.Get(elsewhere.String())
	require.True(t, inRegistry, "a peer on another address stays")
	require.False(t, relayNodeStatus(t, s, reg, sameHost), "the other peer's relayed gossip is dropped")
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

// A registry ban that lapses raises no event. Once the mirror is due a
// refresh, the sweep tick re-lists the registry and lifts the peer's
// admission block.
func TestBanSweepTick_RegistryBanLapseLiftsPeerBlock(t *testing.T) {
	s, reg, client := newAdmissionTestServer(t)
	banned := mustNewPeerID(t)
	banPeerInRegistry(t, reg, banned)

	s.primeBannedPeers()
	s.banSweepTick(context.Background())
	require.Equal(t, []string{"BlockPeer " + banned.String()}, client.AdmissionCalls())

	// The registry stops listing it, and the mirror comes due for a re-list.
	reg.ClearBannedPeers()
	s.bannedPeers.invalidate()

	s.banSweepTick(context.Background())
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

	// The next sweep tick retries both rules without logging them as errors
	// again.
	s.banSweepTick(context.Background())
	require.Len(t, client.AdmissionCalls(), 4)
	require.Len(t, admissionErrors(logger), 2)

	// Once the bus accepts them the next tick applies them, and later ticks
	// do not retry them.
	client.blockErr = nil
	s.banSweepTick(context.Background())
	require.ElementsMatch(t, []string{"BlockAddr 198.51.100.9", "BlockPeer " + pid.String()}, client.AdmissionCalls()[4:])
	s.banSweepTick(context.Background())
	require.Len(t, client.AdmissionCalls(), 6)
}

// With p2p_enforce_bans_at_admission off, no path reaches the gater: operator
// bans, a score-threshold ban, the sweep, a clear and an operator disconnect
// make no Block* or Unblock* call. The bans are still enforced on messages
// and the peers still leave the registry.
func TestAdmission_OffSwitchMakesNoBusCalls(t *testing.T) {
	s, reg, client := newAdmissionTestServer(t)
	s.settings.P2P.EnforceBansAtAdmission = false

	banAddr(t, s, "198.51.100.9")
	banAddr(t, s, "203.0.113.0/24")
	require.True(t, s.banList.IsBanned("198.51.100.9"))

	scored := mustNewPeerID(t)
	reg.Register(&blockchain.PeerInfo{ID: scored.String()})
	client.peers = []p2pMessageBus.PeerInfo{{ID: scored.String(), Addrs: []string{fmt.Sprintf("/ip4/192.0.2.4/tcp/9905/p2p/%s", scored)}}}
	s.onPeerBanned(scored.String(), ReasonSpam)
	require.True(t, s.shouldSkipBannedPeer(scored.String(), "test"), "the ban is enforced on messages")
	require.True(t, s.banList.IsBanned("192.0.2.4"))

	listed := mustNewPeerID(t)
	banPeerInRegistry(t, reg, listed)
	s.primeBannedPeers()
	s.banSweepTick(context.Background())

	other := mustNewPeerID(t)
	reg.Register(&blockchain.PeerInfo{ID: other.String()})
	resp, err := s.DisconnectPeer(context.Background(), &p2p_api.DisconnectPeerRequest{PeerId: other.String()})
	require.NoError(t, err)
	require.True(t, resp.Success)
	_, inRegistry := s.getPeer(other)
	require.False(t, inRegistry, "an operator disconnect still drops the peer at the application layer")

	unbanAddr(t, s, "198.51.100.9")
	_, err = s.ClearBanned(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)

	require.Empty(t, client.AdmissionCalls())
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
