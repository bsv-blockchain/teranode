package p2p

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	p2pMessageBus "github.com/bsv-blockchain/go-p2p-message-bus"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/p2p/p2p_api"
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
// set, liveness from the connection snapshot, and the reputation gate does
// not apply to relayed authors. Before this, each such message cost an
// IsPeerBanned and a GetPeer round-trip, a GetPeers walk, and four map inserts
// keyed by an identity the sender mints for free.
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

	// The registry and walk budgets are one per interval plus the initial
	// load, however long the loop took: they must not track flood.
	require.Zero(t, counting.callCount("IsPeerBanned"), "the ban gate must not look authors up one by one")
	require.LessOrEqual(t, counting.callCount("ListBannedPeers"), 1+int(elapsed/bannedPeerRefreshInterval), "the banned set is listed once per refresh interval, not per author")
	require.Zero(t, counting.callCount("GetPeer"), "relayed authors must not cost a reputation lookup")
	require.Zero(t, s.reputationCache.Len(), "relayed authors must not occupy reputation cache entries")
	require.LessOrEqual(t, int(client.walks.Load()), 1+int(elapsed/liveConnMissRefreshInterval), "GetPeers walks must not scale with the number of unknown authors")
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
	require.LessOrEqual(t, int(client.walks.Load()), 1)
}

// A directly connected low-reputation peer is still gated, from a cache entry
// that is filled once per TTL.
func TestHandleSubtreeTopic_ConnectedLowReputationPeerStillGated(t *testing.T) {
	s, counting, _, neighbour, _ := newGateBoundsTestServer(t, 1)
	reg := blockchain.NewCentralizedPeerRegistry(blockchain.DefaultBanConfig())
	s.peerRegistry = newCountingRegistryClient(blockchain.NewLocalPeerRegistryClient(reg))
	counting = s.peerRegistry.(*countingRegistryClient)

	reg.Register(&blockchain.PeerInfo{ID: neighbour.String()})
	reg.UpdateMetrics(neighbour.String(), 0, 0, 0, false, false, true, 0)

	for i := 0; i < 20; i++ {
		require.True(t, s.shouldSkipUnhealthyPeer(neighbour.String(), "test"), "connected low-reputation peer is skipped")
	}

	require.Equal(t, 1, counting.callCount("GetPeer"), "one lookup per TTL for a connected peer")
	require.Equal(t, 1, s.reputationCache.Len())
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

func TestBoundedTTLCache_EvictsOldestAtCap(t *testing.T) {
	var c boundedTTLCache[int]
	c.setMaxSize(3)
	exp := time.Now().Add(time.Minute)

	c.Set("a", 1, exp)
	c.Set("b", 2, exp)
	c.Set("c", 3, exp)
	c.Set("d", 4, exp)

	require.Equal(t, 3, c.Len())
	_, ok := c.Get("a", time.Now())
	require.False(t, ok, "oldest insert is evicted first")
	for _, k := range []string{"b", "c", "d"} {
		_, ok := c.Get(k, time.Now())
		require.True(t, ok, "%s must survive", k)
	}
}

func TestBoundedTTLCache_RefreshMovesKeyToNewest(t *testing.T) {
	var c boundedTTLCache[int]
	c.setMaxSize(3)
	exp := time.Now().Add(time.Minute)

	c.Set("a", 1, exp)
	c.Set("b", 2, exp)
	c.Set("c", 3, exp)
	c.Set("a", 10, exp)
	c.Set("d", 4, exp)

	v, ok := c.Get("a", time.Now())
	require.True(t, ok, "a refreshed key is the newest, not the eviction victim")
	require.Equal(t, 10, v)
	_, ok = c.Get("b", time.Now())
	require.False(t, ok, "the oldest un-refreshed key is evicted")
}

func TestBoundedTTLCache_ExpiryOnGetAndSweep(t *testing.T) {
	var c boundedTTLCache[int]
	now := time.Now()

	c.Set("expired", 1, now.Add(-time.Second))
	c.Set("fresh", 2, now.Add(time.Minute))
	c.Set("swept", 3, now.Add(-time.Second))

	_, ok := c.Get("expired", now)
	require.False(t, ok, "an expired entry is a miss")
	require.Equal(t, 2, c.Len(), "the expired entry is dropped on lookup")

	require.Equal(t, 1, c.DeleteExpired(now))
	require.Equal(t, 1, c.Len())
	_, ok = c.Get("fresh", now)
	require.True(t, ok)
}

func TestBoundedTTLCache_ZeroValueIsBounded(t *testing.T) {
	var c boundedTTLCache[struct{}]
	exp := time.Now().Add(time.Minute)

	for i := 0; i < defaultPeerMapMaxSize+5; i++ {
		c.Set(fmt.Sprintf("k%d", i), struct{}{}, exp)
	}

	require.Equal(t, defaultPeerMapMaxSize, c.Len(), "an unconfigured cache falls back to the default cap")
}

func TestBoundedTTLCache_LoweredCapDrainsOnInsert(t *testing.T) {
	var c boundedTTLCache[int]
	exp := time.Now().Add(time.Minute)
	for i := 0; i < 10; i++ {
		c.Set(fmt.Sprintf("k%d", i), i, exp)
	}

	c.setMaxSize(4)
	c.Set("new", 1, exp)

	require.Equal(t, 4, c.Len(), "a new key drains an over-cap cache down to the cap")
}

// The live-connection snapshot is rebuilt at most once per
// liveConnMissRefreshInterval on misses, so unknown authors cannot each force
// a GetPeers walk; hits never walk.
func TestLiveConnSnapshot_WalksAreThrottled(t *testing.T) {
	s, _ := newServerWithLocalRegistry(t)
	neighbour := mustNewPeerID(t)
	client := &countingP2PClient{}
	client.peers = []p2pMessageBus.PeerInfo{{ID: neighbour.String(), Addrs: []string{"/ip4/10.0.0.1/tcp/9905"}}}
	s.P2PClient = client

	start := time.Now()
	for i := 0; i < 100; i++ {
		require.False(t, s.hasLiveConnection(mustNewPeerID(t).String()))
	}
	require.LessOrEqual(t, int(client.walks.Load()), 1+int(time.Since(start)/liveConnMissRefreshInterval), "misses must not each walk GetPeers")

	before := client.walks.Load()
	for i := 0; i < 100; i++ {
		require.True(t, s.hasLiveConnection(neighbour.String()))
	}
	require.Equal(t, before, client.walks.Load(), "hits are served from the snapshot")

	// A neighbour that connects after the snapshot is found once the snapshot
	// is rebuilt.
	late := mustNewPeerID(t)
	client.peers = append(client.peers, p2pMessageBus.PeerInfo{ID: late.String(), Addrs: []string{"/ip4/10.0.0.2/tcp/9905"}})
	s.liveConns.invalidate()
	require.True(t, s.hasLiveConnection(late.String()))
	addrs, live := s.liveConnAddrs(late.String())
	require.True(t, live)
	require.Equal(t, []string{"/ip4/10.0.0.2/tcp/9905"}, addrs)
}

// A ban applied locally while a registry listing was in flight must survive
// the listing being installed without it.
func TestBannedPeerMirror_LocalBanSurvivesStraddlingRefresh(t *testing.T) {
	var m bannedPeerMirror
	now := time.Now()
	pid := mustNewPeerID(t).String()

	m.add(pid, now)
	_, gen := m.state(now)
	require.True(t, m.replace(nil, now.Add(time.Millisecond), gen))
	require.True(t, m.contains(pid), "a fresh local ban must survive a refresh that predates it")

	require.True(t, m.replace(nil, now.Add(bannedPeerRefreshInterval), gen))
	require.False(t, m.contains(pid), "once the registry has had a full interval to report it, the registry's view wins")

	m.add(pid, now)
	m.remove(pid)
	require.False(t, m.contains(pid), "an unban is immediate")
}

// An unban applied locally while a registry listing was in flight must not be
// undone by that listing landing afterwards: the pre-reset listing still
// names the peer, and installing it would re-ban the peer for a full refresh
// interval right after the operator lifted the ban.
func TestBannedPeerMirror_UnbanSurvivesStraddlingRefresh(t *testing.T) {
	var m bannedPeerMirror
	now := time.Now()
	pid := mustNewPeerID(t).String()

	_, gen := m.state(now)
	require.True(t, m.replace([]string{pid}, now, gen))
	require.True(t, m.contains(pid))

	// A refresh starts (captures gen), then the operator unbans, then the
	// refresh's listing arrives, still naming the peer.
	_, inFlight := m.state(now)
	m.remove(pid)
	require.False(t, m.replace([]string{pid}, now.Add(time.Millisecond), inFlight), "a listing that straddled an unban must be discarded")
	require.False(t, m.contains(pid), "the unban must hold")

	stale, next := m.state(now.Add(time.Millisecond))
	require.True(t, stale, "the discarded listing must leave the mirror due a re-list")

	// The re-list, started after the unban, is installed normally.
	require.True(t, m.replace(nil, now.Add(2*time.Millisecond), next))
	require.False(t, m.contains(pid))

	// clear is an unban of everything and invalidates in-flight listings too.
	_, inFlight = m.state(now)
	m.clear()
	require.False(t, m.replace([]string{pid}, now, inFlight))
	require.False(t, m.contains(pid))
}

// The gossip path never waits on a refresh another worker is running: while
// one lookup holds the round-trip, concurrent lookups answer from the current
// view at once, even before the first load has landed.
func TestIsRegistryBanned_NeverWaitsOnInFlightRefresh(t *testing.T) {
	s, _ := newServerWithLocalRegistry(t)

	// Simulate a refresh stuck in its registry round-trip.
	s.bannedPeers.refreshMu.Lock()
	defer s.bannedPeers.refreshMu.Unlock()

	done := make(chan bool, 1)
	go func() { done <- s.isRegistryBanned(mustNewPeerID(t).String()) }()

	select {
	case banned := <-done:
		require.False(t, banned, "an unknown author fails open while the first load is in flight")
	case <-time.After(2 * time.Second):
		t.Fatal("isRegistryBanned blocked behind another worker's refresh")
	}
}

// Start primes the mirror so a peer banned before the process started is
// dropped from its first message, without the hot path having to wait.
func TestPrimeBannedPeers_LoadsPreexistingBans(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	pid := mustNewPeerID(t)
	banPeerInRegistry(t, reg, pid)

	counting := newCountingRegistryClient(s.peerRegistry)
	s.peerRegistry = counting

	s.primeBannedPeers()

	require.Equal(t, 1, counting.callCount("ListBannedPeers"))
	require.True(t, s.shouldSkipBannedPeer(pid.String(), "test"), "a pre-existing ban is enforced from the first message")
	require.Equal(t, 1, counting.callCount("ListBannedPeers"), "the primed mirror serves the first lookup without another round-trip")
}

// Cap evictions are counted for the periodic sweep's capacity diagnostic and
// the counter resets on read.
func TestBoundedTTLCache_CountsEvictions(t *testing.T) {
	var c boundedTTLCache[int]
	c.setMaxSize(2)
	expires := time.Now().Add(time.Minute)

	c.Set("a", 1, expires)
	c.Set("b", 2, expires)
	require.Zero(t, c.EvictionsSinceLastRead())

	c.Set("c", 3, expires)
	c.Set("d", 4, expires)
	require.Equal(t, 2, c.EvictionsSinceLastRead())
	require.Zero(t, c.EvictionsSinceLastRead(), "the counter resets on read")
}

// An operator reset re-reads the registry's ban state at once instead of
// serving the mirror's previous view for the rest of its refresh interval.
func TestResetReputation_RereadsBanStateImmediately(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	pid := mustNewPeerID(t)

	banPeerInRegistry(t, reg, pid)
	s.bannedPeers.invalidate()
	require.True(t, s.shouldSkipBannedPeer(pid.String(), "test"), "precondition: registry ban is enforced")

	// The registry lifts the ban; the mirror still holds the old view.
	reg.ClearBannedPeers()
	require.True(t, s.shouldSkipBannedPeer(pid.String(), "test"), "precondition: mirror serves its last view within the interval")

	_, err := s.ResetReputation(context.Background(), &p2p_api.ResetReputationRequest{PeerId: pid.String()})
	require.NoError(t, err)

	require.False(t, s.shouldSkipBannedPeer(pid.String(), "test"), "a reset must re-read the registry rather than serve the stale ban")
}
