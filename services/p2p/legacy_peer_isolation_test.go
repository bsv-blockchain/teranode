package p2p

import (
	"context"
	"testing"

	p2pMessageBus "github.com/bsv-blockchain/go-p2p-message-bus"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/stretchr/testify/require"
)

// registerWirePeer adds a connected wire-protocol peer straight to the
// registry, the way the legacy service does.
func registerWirePeer(t *testing.T, reg *blockchain.CentralizedPeerRegistry, id string, height uint32, dataHubURL string) {
	t.Helper()

	reg.Register(&blockchain.PeerInfo{
		ID:               id,
		TransportType:    blockchain_api.TransportType_TRANSPORT_WIRE_PROTOCOL,
		TransportTypeSet: true,
		NetworkAddress:   "203.0.113.7:8333",
		Height:           height,
		DataHubURL:       dataHubURL,
		Legacy:           &blockchain.LegacyPeerInfo{ProtocolVersion: 70016},
	})
	require.NoError(t, blockchain.NewLocalPeerRegistryClient(reg).UpdateConnectionState(
		context.Background(), id, true))
}

// TestGetNodeStatusMessage_SeparatesLegacyPeerCount checks that a wire-protocol
// peer does not change the meaning of ConnectedPeersCount, which other
// Teranode nodes already consume, and lands in its own figure instead.
func TestGetNodeStatusMessage_SeparatesLegacyPeerCount(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)

	s.addConnectedPeer(mustNewPeerID(t), "client/1.0", 10, nil, "")
	s.addConnectedPeer(mustNewPeerID(t), "client/1.0", 20, nil, "")
	registerWirePeer(t, reg, "legacy:203.0.113.7:8333", 30, "")
	registerWirePeer(t, reg, "legacy:203.0.113.8:8333", 31, "")
	registerWirePeer(t, reg, "legacy:203.0.113.9:8333", 32, "")

	msg := s.getNodeStatusMessage(context.Background())
	require.NotNil(t, msg)
	require.Equal(t, 2, msg.ConnectedPeersCount, "wire peers must not inflate the libp2p count")
	require.Equal(t, 3, msg.LegacyConnectedPeersCount)
}

// TestSyncCoordinator_NeverSeesWirePeer is a regression test. The catchup engine
// fetches blocks over HTTP from a peer's DataHub, so a wire-protocol peer is
// never a candidate. The exclusion must hold on transport, not on an empty
// DataHubURL — so this test gives the wire peer a DataHub URL and a far higher
// height, which would otherwise make it the obvious pick.
func TestSyncCoordinator_NeverSeesWirePeer(t *testing.T) {
	sc, reg := newTestSyncCoordinator(t)

	registerWirePeer(t, reg, "legacy:203.0.113.7:8333", 999_999, "http://203.0.113.7:8090")

	peers := sc.listAllPeers()
	for _, p := range peers {
		require.NotEqual(t, blockchain_api.TransportType_TRANSPORT_WIRE_PROTOCOL, p.TransportType,
			"the coordinator must never see a wire peer")
	}
	require.Empty(t, peers, "the only registered peer is a wire peer, so nothing is visible")

	require.True(t, sc.isCaughtUp(),
		"an ahead wire peer with a DataHub URL must not make the node look behind")
	require.Empty(t, sc.selectNewSyncPeer(), "a wire peer must never be selected for sync")
}

// TestGetPeerRegistry_ExcludesWirePeers checks the p2p read path stays
// libp2p-only, so its client never logs a decode warning for a legacy ID.
func TestGetPeerRegistry_ExcludesWirePeers(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)

	s.addConnectedPeer(mustNewPeerID(t), "client/1.0", 10, nil, "")
	registerWirePeer(t, reg, "legacy:203.0.113.7:8333", 30, "")

	resp, err := s.GetPeerRegistry(context.Background(), nil)
	require.NoError(t, err)
	require.Len(t, resp.Peers, 1)
	require.NotEqual(t, "legacy:203.0.113.7:8333", resp.Peers[0].Id)
}

// TestGetPeers_ExcludesWirePeers covers the second p2p read RPC.
func TestGetPeers_ExcludesWirePeers(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)

	s.addConnectedPeer(mustNewPeerID(t), "client/1.0", 10, nil, "")
	registerWirePeer(t, reg, "legacy:203.0.113.7:8333", 30, "")

	resp, err := s.GetPeers(context.Background(), nil)
	require.NoError(t, err)
	require.Len(t, resp.Peers, 1)
	require.NotEqual(t, "legacy:203.0.113.7:8333", resp.Peers[0].Id)
}

// TestReconcileConnectionStates_LeavesWirePeersConnected is a regression test.
// reconcileConnectionStates builds its liveness set from P2PClient.GetPeers(),
// which reports libp2p IDs only, so a "legacy:host:port" key can never be a
// member. Listing the registry without a transport filter therefore drove every
// connected legacy peer down the "flagged but not live" arm and cleared it,
// which zeroed legacy_connected_peers_count and greyed every dashboard row.
//
// Nothing repairs that from the legacy side: the reconcile loop only asserts
// the connected state on a peer's first sighting, and Register never writes
// IsConnected on an existing entry.
//
// This asserts the end state rather than the filter argument, so it still fails
// if the filter is dropped again.
func TestReconcileConnectionStates_LeavesWirePeersConnected(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)

	liveID := mustNewPeerID(t)
	goneID := mustNewPeerID(t)

	s.addConnectedPeer(liveID, "", 0, nil, "")
	s.addConnectedPeer(goneID, "", 0, nil, "")
	registerWirePeer(t, reg, "legacy:203.0.113.7:8333", 912344, "")
	registerWirePeer(t, reg, "legacy:198.51.100.9:8333", 912100, "")

	s.P2PClient = &MockServerP2PClient{peers: []p2pMessageBus.PeerInfo{
		{ID: liveID.String(), Addrs: []string{"/ip4/10.0.0.1/tcp/9905"}},
		{ID: goneID.String()},
	}}

	s.reconcileConnectionStates(context.Background())

	for _, id := range []string{"legacy:203.0.113.7:8333", "legacy:198.51.100.9:8333"} {
		got, ok := reg.Get(id)
		require.True(t, ok, "%s must still be registered", id)
		require.True(t, got.IsConnected,
			"%s is a wire peer: a libp2p connectedness sweep must not clear it", id)
	}

	// The libp2p half of the sweep must keep working.
	got, ok := reg.Get(liveID.String())
	require.True(t, ok)
	require.True(t, got.IsConnected, "peer with a live connection keeps its flag")

	got, ok = reg.Get(goneID.String())
	require.True(t, ok)
	require.False(t, got.IsConnected, "disconnected libp2p peer is still cleared")
}
