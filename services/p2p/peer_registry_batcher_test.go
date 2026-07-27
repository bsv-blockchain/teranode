package p2p

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	p2pMessageBus "github.com/bsv-blockchain/go-p2p-message-bus"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// countingRegistryClient wraps a PeerRegistryClientI and counts the write RPCs
// the batcher is supposed to coalesce, so tests can assert amplification
// bounds directly.
type countingRegistryClient struct {
	blockchain.PeerRegistryClientI
	mu    sync.Mutex
	calls map[string]int
}

func newCountingRegistryClient(inner blockchain.PeerRegistryClientI) *countingRegistryClient {
	return &countingRegistryClient{PeerRegistryClientI: inner, calls: make(map[string]int)}
}

func (c *countingRegistryClient) count(method string) {
	c.mu.Lock()
	c.calls[method]++
	c.mu.Unlock()
}

func (c *countingRegistryClient) callCount(method string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls[method]
}

func (c *countingRegistryClient) RegisterPeer(ctx context.Context, info *blockchain.PeerInfo) error {
	c.count("RegisterPeer")
	return c.PeerRegistryClientI.RegisterPeer(ctx, info)
}

func (c *countingRegistryClient) UpdateConnectionState(ctx context.Context, peerID string, connected bool) error {
	c.count("UpdateConnectionState")
	return c.PeerRegistryClientI.UpdateConnectionState(ctx, peerID, connected)
}

func (c *countingRegistryClient) UpdateLastMessageTime(ctx context.Context, peerID string) error {
	c.count("UpdateLastMessageTime")
	return c.PeerRegistryClientI.UpdateLastMessageTime(ctx, peerID)
}

func (c *countingRegistryClient) UpdatePeerMetrics(ctx context.Context, peerID string, height uint32, bytesSentDelta, bytesRecvDelta uint64, recordSuccess, recordFailure, recordMalicious bool, responseTimeMs int64) error {
	c.count("UpdatePeerMetrics")
	return c.PeerRegistryClientI.UpdatePeerMetrics(ctx, peerID, height, bytesSentDelta, bytesRecvDelta, recordSuccess, recordFailure, recordMalicious, responseTimeMs)
}

func (c *countingRegistryClient) UpdateStorage(ctx context.Context, peerID, storage string) error {
	c.count("UpdateStorage")
	return c.PeerRegistryClientI.UpdateStorage(ctx, peerID, storage)
}

func (c *countingRegistryClient) IsPeerBanned(ctx context.Context, peerID string) (bool, error) {
	c.count("IsPeerBanned")
	return c.PeerRegistryClientI.IsPeerBanned(ctx, peerID)
}

// newBatcherWithCountingRegistry returns a manual-flush batcher (interval far
// in the future, not started) over a counting client backed by a real local
// registry.
func newBatcherWithCountingRegistry() (*peerRegistryBatcher, *countingRegistryClient, *blockchain.CentralizedPeerRegistry) {
	reg := blockchain.NewCentralizedPeerRegistry(blockchain.DefaultBanConfig())
	counting := newCountingRegistryClient(blockchain.NewLocalPeerRegistryClient(reg))
	b := newPeerRegistryBatcher(context.Background(), ulogger.TestLogger{}, counting, time.Hour)
	return b, counting, reg
}

func TestPeerRegistryBatcher_CoalescesFloodIntoOneBatch(t *testing.T) {
	b, counting, reg := newBatcherWithCountingRegistry()
	pid := mustNewPeerID(t).String()

	// Simulate 1000 gossip messages from the same connected peer.
	for i := 0; i < 1000; i++ {
		b.enqueueRegister(pid, "", 0, nil, "", true)
		b.enqueueLastMessage(pid)
		b.enqueueBytesReceived(pid, 100)
	}

	b.flushOnce()

	require.Equal(t, 1, counting.callCount("RegisterPeer"), "1000 messages must coalesce into one RegisterPeer")
	require.Equal(t, 1, counting.callCount("UpdateConnectionState"))
	require.Equal(t, 1, counting.callCount("UpdateLastMessageTime"))
	require.Equal(t, 1, counting.callCount("UpdatePeerMetrics"))

	got, ok := reg.Get(pid)
	require.True(t, ok)
	require.True(t, got.IsConnected)
	require.Equal(t, uint64(100_000), got.BytesReceived, "byte deltas must accumulate, not overwrite")
}

func TestPeerRegistryBatcher_MergesLatestRegistrationData(t *testing.T) {
	b, _, reg := newBatcherWithCountingRegistry()
	pid := mustNewPeerID(t).String()

	hash1 := chainhash.HashH([]byte("block one"))
	hash2 := chainhash.HashH([]byte("block two"))

	b.enqueueRegister(pid, "client/1.0", 10, &hash1, "http://hub.example", false)
	b.enqueueRegister(pid, "", 11, &hash2, "", false)

	b.flushOnce()

	got, ok := reg.Get(pid)
	require.True(t, ok)
	require.Equal(t, "client/1.0", got.ClientName, "empty fields must not clobber earlier values")
	require.Equal(t, uint32(11), got.Height, "later non-zero height wins")
	require.Equal(t, hash2.String(), got.BlockHash.String())
	require.Equal(t, "http://hub.example", got.DataHubURL)
}

func TestPeerRegistryBatcher_SkipsReassertWithinTTL(t *testing.T) {
	b, counting, _ := newBatcherWithCountingRegistry()
	pid := mustNewPeerID(t).String()

	b.enqueueRegister(pid, "", 0, nil, "", true)
	b.enqueueLastMessage(pid)
	b.flushOnce()

	// Second round with no new registration data: only the last-message touch
	// should go out, not another RegisterPeer/UpdateConnectionState.
	b.enqueueRegister(pid, "", 0, nil, "", true)
	b.enqueueLastMessage(pid)
	b.flushOnce()

	require.Equal(t, 1, counting.callCount("RegisterPeer"), "recently asserted peer must not be re-registered")
	require.Equal(t, 1, counting.callCount("UpdateConnectionState"))
	require.Equal(t, 2, counting.callCount("UpdateLastMessageTime"))
}

func TestPeerRegistryBatcher_NewInfoForcesRegister(t *testing.T) {
	b, counting, reg := newBatcherWithCountingRegistry()
	pid := mustNewPeerID(t).String()

	b.enqueueRegister(pid, "", 0, nil, "", true)
	b.flushOnce()
	require.Equal(t, 1, counting.callCount("RegisterPeer"))

	// A height update (new block announced) must reach the registry on the
	// next flush even though the peer was registered recently.
	b.enqueueRegister(pid, "", 42, nil, "", false)
	b.flushOnce()

	require.Equal(t, 2, counting.callCount("RegisterPeer"))
	got, _ := reg.Get(pid)
	require.Equal(t, uint32(42), got.Height)
}

func TestPeerRegistryBatcher_ForgetForcesReRegister(t *testing.T) {
	b, counting, reg := newBatcherWithCountingRegistry()
	pid := mustNewPeerID(t).String()

	b.enqueueRegister(pid, "", 0, nil, "", true)
	b.flushOnce()
	require.Equal(t, 1, counting.callCount("RegisterPeer"))

	// Peer removed from the registry (disconnect/ban) — batcher must forget it
	// so the next message re-registers instead of being skipped as fresh.
	require.NoError(t, counting.RemovePeer(context.Background(), pid))
	b.forget(pid)

	b.enqueueLastMessage(pid)
	b.flushOnce()

	require.Equal(t, 2, counting.callCount("RegisterPeer"))
	_, ok := reg.Get(pid)
	require.True(t, ok, "peer must be back in the registry after forget + new message")
}

func TestPeerRegistryBatcher_BackpressureDropsBeyondCap(t *testing.T) {
	b, _, _ := newBatcherWithCountingRegistry()

	b.mu.Lock()
	for i := 0; i < registryBatcherMaxPending; i++ {
		b.pending[fmt.Sprintf("peer-%d", i)] = &pendingPeerUpdate{touchLastMessage: true}
	}
	b.mu.Unlock()

	ok := b.enqueue("one-peer-too-many", func(u *pendingPeerUpdate) { u.touchLastMessage = true })
	require.False(t, ok, "enqueue beyond the cap must be dropped")

	// Updates for peers already pending must still merge.
	ok = b.enqueue("peer-0", func(u *pendingPeerUpdate) { u.bytesReceived += 7 })
	require.True(t, ok, "existing pending peers must still accept merges at the cap")

	b.mu.Lock()
	dropped := b.dropped
	pendingLen := len(b.pending)
	b.mu.Unlock()
	require.Equal(t, uint64(1), dropped)
	require.Equal(t, registryBatcherMaxPending, pendingLen)
}

func TestPeerRegistryBatcher_StopFlushesPending(t *testing.T) {
	reg := blockchain.NewCentralizedPeerRegistry(blockchain.DefaultBanConfig())
	counting := newCountingRegistryClient(blockchain.NewLocalPeerRegistryClient(reg))
	// Long interval: the ticker never fires during the test; only stop() flushes.
	b := newPeerRegistryBatcher(context.Background(), ulogger.TestLogger{}, counting, time.Hour)
	b.start()

	pid := mustNewPeerID(t).String()
	b.enqueueRegister(pid, "client/1.0", 5, nil, "", true)
	b.enqueueBytesReceived(pid, 123)

	b.stop()

	got, ok := reg.Get(pid)
	require.True(t, ok, "stop must flush pending updates")
	require.Equal(t, uint32(5), got.Height)
	require.Equal(t, uint64(123), got.BytesReceived)
}

func TestPeerRegistryBatcher_SynchronousModeFlushesInline(t *testing.T) {
	reg := blockchain.NewCentralizedPeerRegistry(blockchain.DefaultBanConfig())
	counting := newCountingRegistryClient(blockchain.NewLocalPeerRegistryClient(reg))
	b := newPeerRegistryBatcher(context.Background(), ulogger.TestLogger{}, counting, 0)

	pid := mustNewPeerID(t).String()
	b.enqueueRegister(pid, "", 9, nil, "", false)

	got, ok := reg.Get(pid)
	require.True(t, ok, "synchronous mode must flush on enqueue")
	require.Equal(t, uint32(9), got.Height)
}

// TestServer_GossipFlood_BoundedRegistryRPCs is the end-to-end amplification
// guard for the issue this change fixes: a flood of block gossip from one peer
// must not translate into per-message registry RPCs. With the batcher in
// place, the whole flood costs one IsPeerBanned lookup (cached afterwards) on
// the hot path and one small batch of writes at flush time.
func TestServer_GossipFlood_BoundedRegistryRPCs(t *testing.T) {
	reg := blockchain.NewCentralizedPeerRegistry(blockchain.DefaultBanConfig())
	counting := newCountingRegistryClient(blockchain.NewLocalPeerRegistryClient(reg))

	s := &Server{
		peerRegistry: counting,
		logger:       ulogger.TestLogger{},
		gCtx:         context.Background(),
		settings: &settings.Settings{
			P2P: settings.P2PSettings{
				AllowPrunedNodeFallback:            true,
				MaxUnvalidatedAdvertisedHeightLead: 10_000,
			},
		},
	}
	s.registryBatcher = newPeerRegistryBatcher(context.Background(), ulogger.TestLogger{}, counting, time.Hour)
	setServerLocalHeight(t, s, 100)

	self := mustNewPeerID(t)
	mockP2P := new(MockServerP2PClient)
	mockP2P.peerID = self
	s.P2PClient = mockP2P
	s.notificationCh = make(chan *notificationMsg, 200)

	remote := mustNewPeerID(t)
	const flood = 100
	for i := 0; i < flood; i++ {
		blockHash := chainhash.HashH([]byte(fmt.Sprintf("block %d", i))).String()
		msgBytes, err := json.Marshal(BlockMessage{
			PeerID:     remote.String(),
			ClientName: "client/1.0",
			DataHubURL: "http://peer.example",
			Hash:       blockHash,
			Height:     uint32(i + 1),
		})
		require.NoError(t, err)
		s.handleBlockTopic(context.Background(), msgBytes, remote.String())
	}

	require.Equal(t, 1, counting.callCount("IsPeerBanned"), "ban status must be cached, not checked per message")
	require.Equal(t, 0, counting.callCount("RegisterPeer"), "no registry writes on the gossip hot path")
	require.Equal(t, 0, counting.callCount("UpdateLastMessageTime"))
	require.Equal(t, 0, counting.callCount("UpdatePeerMetrics"))

	s.registryBatcher.flushOnce()

	require.Equal(t, 1, counting.callCount("RegisterPeer"), "one flush = one RegisterPeer for the flooding peer")
	require.Equal(t, 1, counting.callCount("UpdateConnectionState"))
	require.Equal(t, 1, counting.callCount("UpdateLastMessageTime"))
	require.Equal(t, 1, counting.callCount("UpdatePeerMetrics"))

	got, ok := reg.Get(remote.String())
	require.True(t, ok)
	require.Equal(t, uint32(flood), got.Height, "latest advertised height must survive coalescing")
	require.True(t, got.IsConnected)
}

// TestSubscribeToTopic_WorkerPoolBoundsAndDrains verifies the per-topic worker
// pool: exactly `workers` messages are processed concurrently (a slow message
// no longer blocks the rest of the topic), the concurrency never exceeds the
// bound, and the channel is fully drained.
func TestSubscribeToTopic_WorkerPoolBoundsAndDrains(t *testing.T) {
	const workers = 4
	const messages = 32

	s := &Server{
		logger: ulogger.TestLogger{},
		gCtx:   context.Background(),
		settings: &settings.Settings{
			P2P: settings.P2PSettings{GossipHandlerConcurrency: workers},
		},
	}

	ch := make(chan p2pMessageBus.Message, messages)
	mockP2P := new(MockServerP2PClient)
	mockP2P.On("Subscribe", "test-topic").Return((<-chan p2pMessageBus.Message)(ch))
	s.P2PClient = mockP2P

	var (
		mu            sync.Mutex
		inFlight      int
		maxInFlight   int
		processed     int
		allDone       = make(chan struct{})
		firstFourBusy = make(chan struct{})
	)

	handler := func(_ context.Context, _ []byte, _ string) {
		mu.Lock()
		inFlight++
		if inFlight > maxInFlight {
			maxInFlight = inFlight
		}
		if maxInFlight == workers {
			select {
			case <-firstFourBusy:
			default:
				close(firstFourBusy)
			}
		}
		mu.Unlock()

		// Hold the first wave until all workers are provably busy at once;
		// with a single-goroutine subscriber this would deadlock (and the
		// test would fail via timeout below).
		select {
		case <-firstFourBusy:
		case <-time.After(5 * time.Second):
		}

		mu.Lock()
		inFlight--
		processed++
		if processed == messages {
			close(allDone)
		}
		mu.Unlock()
	}

	s.subscribeToTopic(context.Background(), "test-topic", handler)

	for i := 0; i < messages; i++ {
		ch <- p2pMessageBus.Message{Data: []byte("m"), FromID: "peer"}
	}
	close(ch)

	select {
	case <-allDone:
	case <-time.After(10 * time.Second):
		t.Fatal("worker pool did not drain the topic channel")
	}

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, workers, maxInFlight, "concurrency must reach and never exceed the configured bound")
	require.Equal(t, messages, processed)
}

// TestUpdatePeerLastMessageTime_BatchedPathSkipsSelfOriginator mirrors the
// legacy behavior test: the originator entry must not be created when the
// originator is ourselves (self-gossip in single-node environments).
func TestUpdatePeerLastMessageTime_BatchedPathSkipsSelfOriginator(t *testing.T) {
	reg := blockchain.NewCentralizedPeerRegistry(blockchain.DefaultBanConfig())
	counting := newCountingRegistryClient(blockchain.NewLocalPeerRegistryClient(reg))

	self := mustNewPeerID(t)
	mockP2P := new(MockServerP2PClient)
	mockP2P.peerID = self

	s := &Server{
		peerRegistry: counting,
		logger:       ulogger.TestLogger{},
		gCtx:         context.Background(),
		P2PClient:    mockP2P,
	}
	// Synchronous batcher: every enqueue flushes inline.
	s.registryBatcher = newPeerRegistryBatcher(context.Background(), ulogger.TestLogger{}, counting, 0)

	sender := mustNewPeerID(t)
	s.updatePeerLastMessageTime(sender.String(), self.String())

	_, ok := reg.Get(sender.String())
	require.True(t, ok, "sender must be registered")
	_, ok = reg.Get(self.String())
	require.False(t, ok, "own peer ID must not be registered from self-gossip")
}
