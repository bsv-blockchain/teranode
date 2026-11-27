package p2p

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	p2pMessageBus "github.com/bsv-blockchain/go-p2p-message-bus"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/p2p/p2p_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

// countingP2PClient counts GetPeers walks, the O(all tracked authors) call the
// gossip gate used to run once per unknown author.
type countingP2PClient struct {
	MockServerP2PClient
	walks atomic.Int32
}

func (c *countingP2PClient) GetPeers() []p2pMessageBus.PeerInfo {
	c.walks.Add(1)
	return c.MockServerP2PClient.GetPeers()
}

// newGateBoundsTestServer wires a server whose registry calls and GetPeers
// walks are counted, with one live neighbour connected.
func newGateBoundsTestServer(t *testing.T, flood int) (*Server, *countingRegistryClient, *countingP2PClient, peer.ID, *kafka.KafkaAsyncProducerMock) {
	t.Helper()

	s, _ := newServerWithLocalRegistry(t)
	setServerLocalHeight(t, s, 100)

	counting := newCountingRegistryClient(s.peerRegistry)
	s.peerRegistry = counting
	// Manual-flush batcher: registry writes queue up instead of running inline.
	s.registryBatcher = newPeerRegistryBatcher(context.Background(), ulogger.TestLogger{}, counting, time.Hour)

	neighbour := mustNewPeerID(t)
	client := &countingP2PClient{}
	client.peerID = mustNewPeerID(t)
	client.peers = []p2pMessageBus.PeerInfo{{ID: neighbour.String(), Addrs: []string{"/ip4/10.0.0.1/tcp/9905"}}}
	s.P2PClient = client
	s.notificationCh = make(chan *notificationMsg, flood+8)

	producer := kafka.NewKafkaAsyncProducerMockWithBuffer(flood + 8)
	s.blocksKafkaProducerClient = producer

	return s, counting, client, neighbour, producer
}

func blockAnnouncement(t *testing.T, from peer.ID, i int) []byte {
	t.Helper()

	msg, err := json.Marshal(BlockMessage{
		PeerID:     from.String(),
		ClientName: "client/1.0",
		DataHubURL: "http://peer.example",
		Hash:       fmt.Sprintf("%064x", i+1),
		Height:     101,
	})
	require.NoError(t, err)

	return msg
}

// A stream of well-formed announcements from distinct, never-before-seen
// authors (relayed, no connection) must not cost a registry round-trip or a
// cache entry per author: the ban gate is answered from the mirrored banned
// set, liveness from the bus's connection table, and the reputation gate does
// not apply to relayed authors. Before this, each such message cost an
// IsPeerBanned round-trip, a GetPeers walk, and inserts into the gate's
// per-author caches, keyed by an identity the sender mints for free.
func TestHandleBlockTopic_FreshAuthorsCostNoPerAuthorLookupsOrCacheEntries(t *testing.T) {
	const flood = 200

	s, counting, client, _, producer := newGateBoundsTestServer(t, flood)

	start := time.Now()
	for i := 0; i < flood; i++ {
		author := mustNewPeerID(t)
		s.handleBlockTopic(context.Background(), blockAnnouncement(t, author, i), author.String())
	}
	elapsed := time.Since(start)

	require.Len(t, producer.PublishChannel(), flood, "well-formed announcements from unknown authors are still forwarded")

	// The registry budget is one listing per interval plus the initial load,
	// however long the loop took: it must not track flood.
	require.Zero(t, counting.callCount("IsPeerBanned"), "the ban gate must not look authors up one by one")
	require.LessOrEqual(t, counting.callCount("ListBannedPeers"), 1+int(elapsed/bannedPeerRefreshInterval), "the banned set is listed once per refresh interval, not per author")
	require.Zero(t, counting.callCount("GetPeer"), "relayed authors must not cost a reputation lookup")
	require.Zero(t, s.reputationCache.Len(), "relayed authors must not occupy reputation cache entries")
	require.Zero(t, client.walks.Load(), "unknown authors must not cost GetPeers walks")
}

// A message that fails field validation from an unknown author must leave no
// gate state behind and cost no ban or reputation lookup. The protocol
// violation is still scored (that write is the one registry call the message
// costs, unchanged from before).
func TestHandleBlockTopic_InvalidFieldsFromUnknownAuthorLeaveNoGateState(t *testing.T) {
	s, counting, client, _, _ := newGateBoundsTestServer(t, 1)

	author := mustNewPeerID(t)
	msg, err := json.Marshal(BlockMessage{
		PeerID:     author.String(),
		DataHubURL: "http://peer.example",
		Hash:       "not-a-hash",
		Height:     101,
	})
	require.NoError(t, err)

	s.handleBlockTopic(context.Background(), msg, author.String())

	require.Empty(t, s.notificationCh, "invalid message must not be forwarded")
	require.Equal(t, 1, counting.callCount("AddBanScore"), "the violation is scored")
	require.Zero(t, counting.callCount("IsPeerBanned"))
	require.Zero(t, counting.callCount("GetPeer"))
	require.Zero(t, s.reputationCache.Len(), "no reputation entry may be left behind for the author")
	require.Zero(t, client.walks.Load())
}

// A directly connected low-reputation peer is still gated, from a cache entry
// that is filled once per TTL.
func TestHandleSubtreeTopic_ConnectedLowReputationPeerStillGated(t *testing.T) {
	s, _, _, neighbour, _ := newGateBoundsTestServer(t, 1)
	reg := blockchain.NewCentralizedPeerRegistry(blockchain.DefaultBanConfig())
	counting := newCountingRegistryClient(blockchain.NewLocalPeerRegistryClient(reg))
	s.peerRegistry = counting

	reg.Register(&blockchain.PeerInfo{ID: neighbour.String()})
	reg.UpdateMetrics(neighbour.String(), 0, 0, 0, false, false, true, 0)

	for i := 0; i < 20; i++ {
		require.True(t, s.shouldSkipUnhealthyPeer(neighbour.String(), "test"), "connected low-reputation peer is skipped")
	}

	require.Equal(t, 1, counting.callCount("GetPeer"), "one lookup per TTL for a connected peer")
	require.Equal(t, 1, s.reputationCache.Len())
}

// A relayed low-reputation author (known to the registry, no open connection)
// is deliberately not reputation-gated: looking it up would cost a GetPeer
// round-trip per novel author, the amplification the gate exists to avoid.
// Its messages are processed, nothing is looked up and nothing is cached. The
// ban gate still applies to relayed authors.
func TestShouldSkipUnhealthyPeer_RelayedLowReputationAuthorNotGated(t *testing.T) {
	s, _, _, _, _ := newGateBoundsTestServer(t, 1)
	reg := blockchain.NewCentralizedPeerRegistry(blockchain.DefaultBanConfig())
	counting := newCountingRegistryClient(blockchain.NewLocalPeerRegistryClient(reg))
	s.peerRegistry = counting

	relayed := mustNewPeerID(t)
	reg.Register(&blockchain.PeerInfo{ID: relayed.String()})
	reg.UpdateMetrics(relayed.String(), 0, 0, 0, false, false, true, 0)

	info, ok := reg.Get(relayed.String())
	require.True(t, ok)
	require.Less(t, info.ReputationScore, 20.0, "fixture: the author is below the reputation threshold")

	for i := 0; i < 20; i++ {
		require.False(t, s.shouldSkipUnhealthyPeer(relayed.String(), "test"), "relayed author is not reputation-gated")
	}

	require.Zero(t, counting.callCount("GetPeer"), "no registry lookup for an unconnected author")
	require.Zero(t, s.reputationCache.Len(), "no reputation entry for an unconnected author")
}

// p2p_peer_map_max_size reaches the reputation cache through
// applyPeerMapLimits, not only the attribution maps.
func TestApplyPeerMapLimits_BoundsReputationCache(t *testing.T) {
	tSettings := &settings.Settings{}
	tSettings.P2P.PeerMapMaxSize = 7

	s := &Server{logger: ulogger.TestLogger{}}
	s.applyPeerMapLimits(tSettings)

	expiresAt := time.Now().Add(time.Minute)
	for i := 0; i < 20; i++ {
		s.reputationCache.Set(fmt.Sprintf("peer-%d", i), reputationCacheEntry{}, expiresAt)
	}

	require.Equal(t, 7, s.reputationCache.Len(), "the configured p2p_peer_map_max_size must bound the reputation cache")
}

// The reputation cache is bounded at insert: more connected peers than the cap
// rotate the cache instead of growing it.
func TestReputationCache_BoundedAtInsert(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	s.reputationCache.setMaxSize(8)

	var peers []p2pMessageBus.PeerInfo
	var ids []string
	for i := 0; i < 20; i++ {
		pid := mustNewPeerID(t)
		ids = append(ids, pid.String())
		peers = append(peers, p2pMessageBus.PeerInfo{ID: pid.String(), Addrs: []string{fmt.Sprintf("/ip4/10.0.0.%d/tcp/9905", i+1)}})
		reg.Register(&blockchain.PeerInfo{ID: pid.String()})
	}
	s.P2PClient = &MockServerP2PClient{peers: peers}

	for _, id := range ids {
		require.False(t, s.shouldSkipUnhealthyPeer(id, "test"))
	}

	require.Equal(t, 8, s.reputationCache.Len(), "cache must hold at most its cap")
	_, newest := s.reputationCache.Get(ids[len(ids)-1], time.Now())
	require.True(t, newest, "the most recent peer must be the one kept")
	_, oldest := s.reputationCache.Get(ids[0], time.Now())
	require.False(t, oldest, "the oldest peer must have been evicted")
}

// A neighbour at a banned IP is caught by the message-level IP check on its
// first message, however recently it connected.
func TestIsPeerIPBanned_NewNeighbourCaughtOnFirstMessage(t *testing.T) {
	s, _ := newServerWithLocalRegistry(t)
	s.banList = &recordingBanList{banned: map[string]bool{"198.51.100.9": true}}

	client := &countingP2PClient{}
	client.peers = []p2pMessageBus.PeerInfo{{ID: mustNewPeerID(t).String(), Addrs: []string{"/ip4/10.0.0.1/tcp/9905"}}}
	s.P2PClient = client

	require.False(t, s.hasLiveConnection(mustNewPeerID(t).String()))

	banned := mustNewPeerID(t)
	client.peers = append(client.peers, p2pMessageBus.PeerInfo{ID: banned.String(), Addrs: []string{"/ip4/198.51.100.9/tcp/9905"}})

	require.True(t, s.isPeerIPBanned(banned.String()))
	require.Zero(t, client.walks.Load())
}

// A low-reputation neighbour is reputation-gated from its first message after
// it connects.
func TestShouldSkipUnhealthyPeer_NewLowReputationNeighbourGatedOnFirstMessage(t *testing.T) {
	s, _, client, neighbour, _ := newGateBoundsTestServer(t, 1)
	reg := blockchain.NewCentralizedPeerRegistry(blockchain.DefaultBanConfig())
	counting := newCountingRegistryClient(blockchain.NewLocalPeerRegistryClient(reg))
	s.peerRegistry = counting

	require.True(t, s.hasLiveConnection(neighbour.String()))

	fresh := mustNewPeerID(t)
	reg.Register(&blockchain.PeerInfo{ID: fresh.String()})
	reg.UpdateMetrics(fresh.String(), 0, 0, 0, false, false, true, 0)
	client.peers = append(client.peers, p2pMessageBus.PeerInfo{ID: fresh.String(), Addrs: []string{"/ip4/10.0.0.3/tcp/9905"}})

	require.True(t, s.shouldSkipUnhealthyPeer(fresh.String(), "test"), "the connected low-reputation peer is gated on its first message")
	require.Equal(t, 1, counting.callCount("GetPeer"))
	require.Zero(t, client.walks.Load(), "liveness must not walk GetPeers")
}

// An operator reset re-reads the registry's ban state at once instead of
// serving the mirror's previous view for the rest of its refresh interval,
// for one peer and for all peers.
func TestResetReputation_RereadsBanStateImmediately(t *testing.T) {
	for _, allPeers := range []bool{false, true} {
		t.Run(fmt.Sprintf("allPeers=%v", allPeers), func(t *testing.T) {
			s, reg := newServerWithLocalRegistry(t)
			pid := mustNewPeerID(t)

			banPeerInRegistry(t, reg, pid)
			s.bannedPeers.invalidate()
			require.True(t, s.shouldSkipBannedPeer(pid.String(), "test"), "precondition: registry ban is enforced")

			// The registry lifts the ban; the mirror still holds the old view.
			reg.ClearBannedPeers()
			require.True(t, s.shouldSkipBannedPeer(pid.String(), "test"), "precondition: mirror serves its last view within the interval")

			req := &p2p_api.ResetReputationRequest{PeerId: pid.String()}
			if allPeers {
				req.PeerId = ""
			}

			_, err := s.ResetReputation(context.Background(), req)
			require.NoError(t, err)

			require.False(t, s.shouldSkipBannedPeer(pid.String(), "test"), "a reset must re-read the registry rather than serve the stale ban")
		})
	}
}

// A reputation reset does not lift registry bans, so it must not stop the
// mirror enforcing them: while the forced re-list is in flight, or after it
// fails, the mirror's previous view stays in force.
func TestResetReputation_KeepsEnforcingBansUntilReListLands(t *testing.T) {
	for _, allPeers := range []bool{false, true} {
		t.Run(fmt.Sprintf("allPeers=%v", allPeers), func(t *testing.T) {
			s, reg := newServerWithLocalRegistry(t)
			pid := mustNewPeerID(t)
			banPeerInRegistry(t, reg, pid)

			counting := newCountingRegistryClient(s.peerRegistry)
			s.peerRegistry = counting
			s.primeBannedPeers()

			req := &p2p_api.ResetReputationRequest{PeerId: pid.String()}
			if allPeers {
				req.PeerId = ""
			}

			// Hold the forced re-list in flight: a concurrent lookup answers
			// from the mirror's view.
			s.bannedPeers.refreshMu.Lock()

			_, err := s.ResetReputation(context.Background(), req)
			require.NoError(t, err)
			require.True(t, s.isRegistryBanned(pid.String()), "a ban must stay enforced while the re-list after a reset is in flight")

			s.bannedPeers.refreshMu.Unlock()

			// Let the re-list fail: the previous view stays in force.
			counting.failListBannedPeers = errors.NewServiceError("registry unavailable")
			require.True(t, s.isRegistryBanned(pid.String()), "a ban must stay enforced when the re-list after a reset fails")
			require.Equal(t, 2, counting.callCount("ListBannedPeers"), "the reset forced one re-list")
		})
	}
}

// A failed reputation lookup is cached for a full reputationCacheTTL from
// when the lookup returned, so a registry slower than the TTL is still asked
// once per TTL rather than once per message.
func TestShouldSkipUnhealthyPeer_FailureCachedFromWhenLookupReturned(t *testing.T) {
	s, _, _, neighbour, _ := newGateBoundsTestServer(t, 1)
	reg := blockchain.NewCentralizedPeerRegistry(blockchain.DefaultBanConfig())
	counting := newCountingRegistryClient(blockchain.NewLocalPeerRegistryClient(reg))
	counting.failGetPeer = errors.NewServiceError("registry unavailable")

	var returned time.Time

	counting.onGetPeer = func() {
		time.Sleep(50 * time.Millisecond)
		returned = time.Now()
	}
	s.peerRegistry = counting

	require.False(t, s.shouldSkipUnhealthyPeer(neighbour.String(), "test"), "a failed lookup treats the peer as healthy")
	require.Equal(t, 1, counting.callCount("GetPeer"))

	_, cached := s.reputationCache.Get(neighbour.String(), returned.Add(reputationCacheTTL-time.Millisecond))
	require.True(t, cached, "the failure must stay cached for a full TTL from when the lookup returned")
}
