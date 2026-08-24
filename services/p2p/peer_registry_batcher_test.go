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
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
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
	// onRegister / onUpdateConnectionState / onUpdateLastMessageTime, when
	// set, run during the RPC — lets tests interleave out-of-band batcher
	// calls with an in-flight flush cycle. The fail* errors, when set, are
	// returned by the RPC.
	onRegister                func()
	onUpdateConnectionState   func()
	onUpdateLastMessageTime   func()
	failListPeers             error
	failUpdateConnectionState error
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
	if c.onRegister != nil {
		c.onRegister()
	}
	return c.PeerRegistryClientI.RegisterPeer(ctx, info)
}

func (c *countingRegistryClient) UpdateConnectionState(ctx context.Context, peerID string, connected bool) error {
	c.count("UpdateConnectionState")
	if c.onUpdateConnectionState != nil {
		c.onUpdateConnectionState()
	}
	if c.failUpdateConnectionState != nil {
		return c.failUpdateConnectionState
	}
	return c.PeerRegistryClientI.UpdateConnectionState(ctx, peerID, connected)
}

func (c *countingRegistryClient) ListPeers(ctx context.Context, transportFilter *blockchain_api.TransportType, minReputation float64, minHeight uint32, excludeBanned, sortByStorage bool) ([]*blockchain.PeerInfo, error) {
	c.count("ListPeers")
	if c.failListPeers != nil {
		return nil, c.failListPeers
	}
	return c.PeerRegistryClientI.ListPeers(ctx, transportFilter, minReputation, minHeight, excludeBanned, sortByStorage)
}

func (c *countingRegistryClient) UpdateLastMessageTime(ctx context.Context, peerID string) error {
	c.count("UpdateLastMessageTime")
	if c.onUpdateLastMessageTime != nil {
		c.onUpdateLastMessageTime()
	}
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

	b.flushOnce(context.Background())

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

	b.flushOnce(context.Background())

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
	b.flushOnce(context.Background())

	// Second round with no new registration data: only the last-message touch
	// should go out, not another RegisterPeer/UpdateConnectionState.
	b.enqueueRegister(pid, "", 0, nil, "", true)
	b.enqueueLastMessage(pid)
	b.flushOnce(context.Background())

	require.Equal(t, 1, counting.callCount("RegisterPeer"), "recently asserted peer must not be re-registered")
	require.Equal(t, 1, counting.callCount("UpdateConnectionState"))
	require.Equal(t, 2, counting.callCount("UpdateLastMessageTime"))
}

func TestPeerRegistryBatcher_ForgetAssertStateForcesReassert(t *testing.T) {
	b, counting, reg := newBatcherWithCountingRegistry()
	pid := mustNewPeerID(t).String()

	b.enqueueRegister(pid, "", 0, nil, "", true)
	b.flushOnce(context.Background())
	require.Equal(t, 1, counting.callCount("UpdateConnectionState"))

	// The reconciler clears the registry flag out-of-band and drops the
	// batcher's reassert memory; the peer's next message must re-mark it
	// connected instead of being skipped as recently asserted.
	require.NoError(t, counting.UpdateConnectionState(context.Background(), pid, false))
	b.forgetAssertState(pid)

	b.enqueueRegister(pid, "", 0, nil, "", true)
	b.flushOnce(context.Background())

	got, ok := reg.Get(pid)
	require.True(t, ok)
	require.True(t, got.IsConnected, "reconnecting peer must be re-marked connected after forgetAssertState")
}

func TestPeerRegistryBatcher_ForgetAssertStateDuringFlushNotResurrected(t *testing.T) {
	b, counting, _ := newBatcherWithCountingRegistry()
	pid := mustNewPeerID(t).String()

	// Assert connected once so lastAsserted holds a fresh connectedAt.
	b.enqueueRegister(pid, "", 0, nil, "", true)
	b.flushOnce(context.Background())
	require.Equal(t, 1, counting.callCount("UpdateConnectionState"))

	// Next cycle sends a register (new client name) but no connection assert
	// (still within registryReassertTTL). A reconciler forgetAssertState lands
	// mid-flush, after the loop snapshotted the peer's assert state.
	counting.onRegister = func() { b.forgetAssertState(pid) }
	b.enqueueRegister(pid, "client/2.0", 0, nil, "", true)
	b.flushOnce(context.Background())
	counting.onRegister = nil
	require.Equal(t, 1, counting.callCount("UpdateConnectionState"), "still within reassert TTL")

	// The stale connectedAt must not have been resurrected by the in-flight
	// flush: the peer's next message re-asserts connected immediately.
	b.enqueueRegister(pid, "", 0, nil, "", true)
	b.flushOnce(context.Background())
	require.Equal(t, 2, counting.callCount("UpdateConnectionState"), "forgotten assert state must force a re-assert on the next flush")
}

func TestPeerRegistryBatcher_ForgetDuringConnectOnlyFlushZerosSnapshot(t *testing.T) {
	b, counting, _ := newBatcherWithCountingRegistry()
	pid := mustNewPeerID(t).String()

	b.enqueueRegister(pid, "", 0, nil, "", true)
	b.flushOnce(context.Background())

	// Age only the connection assert so the next flush re-asserts connected
	// without re-registering.
	b.mu.Lock()
	st := b.lastAsserted[pid]
	st.connectedAt = time.Now().Add(-2 * registryReassertTTL)
	b.lastAsserted[pid] = st
	b.mu.Unlock()

	counting.onUpdateConnectionState = func() { b.forgetAssertState(pid) }
	b.enqueueRegister(pid, "", 0, nil, "", true)
	b.flushOnce(context.Background())
	counting.onUpdateConnectionState = nil

	// A forget that raced the cycle zeroes the WHOLE snapshot, including the
	// connect assert this cycle sent: the reconciler's clear may have landed
	// after that RPC, and keeping connectedAt would mask it (see the
	// reconnect-after-masked-clear test below for the observable failure).
	b.mu.Lock()
	st = b.lastAsserted[pid]
	b.mu.Unlock()
	require.True(t, st.connectedAt.IsZero(), "connect assert must be forgotten even though this cycle sent it")
	require.True(t, st.registeredAt.IsZero(), "stale registeredAt must not be resurrected")
}

func TestPeerRegistryBatcher_ReconnectAfterMaskedClearIsReflagged(t *testing.T) {
	b, counting, reg := newBatcherWithCountingRegistry()
	pid := mustNewPeerID(t).String()

	// Cycle 1 asserts connected=true. Mid-flush — after the connect RPC has
	// already landed — the reconciler clears the flag and forgets the assert
	// state. The reconciler's write is the later one, so the registry ends at
	// false while the batcher just asserted true.
	counting.onUpdateLastMessageTime = func() {
		reg.UpdateConnectionState(pid, false)
		b.forgetAssertState(pid)
	}
	b.enqueueRegister(pid, "", 0, nil, "", true)
	b.enqueueLastMessage(pid)
	b.flushOnce(context.Background())
	counting.onUpdateLastMessageTime = nil

	got, ok := reg.Get(pid)
	require.True(t, ok)
	require.False(t, got.IsConnected, "the reconciler's clear landed last")

	// The peer reconnects and gossips: its next message must re-flag it
	// immediately, not up to registryReassertTTL later.
	b.enqueueRegister(pid, "", 0, nil, "", true)
	b.flushOnce(context.Background())

	got, _ = reg.Get(pid)
	require.True(t, got.IsConnected, "a reconnected, gossiping peer must be re-flagged connected on its next message")
}

func TestPeerRegistryBatcher_NewInfoForcesRegister(t *testing.T) {
	b, counting, reg := newBatcherWithCountingRegistry()
	pid := mustNewPeerID(t).String()

	b.enqueueRegister(pid, "", 0, nil, "", true)
	b.flushOnce(context.Background())
	require.Equal(t, 1, counting.callCount("RegisterPeer"))

	// A height update (new block announced) must reach the registry on the
	// next flush even though the peer was registered recently.
	b.enqueueRegister(pid, "", 42, nil, "", false)
	b.flushOnce(context.Background())

	require.Equal(t, 2, counting.callCount("RegisterPeer"))
	got, _ := reg.Get(pid)
	require.Equal(t, uint32(42), got.Height)
}

func TestPeerRegistryBatcher_ForgetForcesReRegister(t *testing.T) {
	b, counting, reg := newBatcherWithCountingRegistry()
	pid := mustNewPeerID(t).String()

	b.enqueueRegister(pid, "", 0, nil, "", true)
	b.flushOnce(context.Background())
	require.Equal(t, 1, counting.callCount("RegisterPeer"))

	// Peer removed from the registry (disconnect/ban) — batcher must forget it
	// so the next message re-registers instead of being skipped as fresh.
	require.NoError(t, counting.RemovePeer(context.Background(), pid))
	b.forget(pid)

	b.enqueueLastMessage(pid)
	b.flushOnce(context.Background())

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

	ok := b.enqueue("one-peer-too-many", &pendingPeerUpdate{touchLastMessage: true})
	require.False(t, ok, "enqueue beyond the cap must be dropped")

	// Updates for peers already pending must still merge.
	ok = b.enqueue("peer-0", &pendingPeerUpdate{bytesReceived: 7})
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

	b.stop(context.Background())

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
	remote := mustNewPeerID(t)
	mockP2P := new(MockServerP2PClient)
	mockP2P.peerID = self
	// The flooding peer is directly connected (live Addrs), so the hot path's
	// liveness check may flag it IsConnected.
	mockP2P.peers = []p2pMessageBus.PeerInfo{{ID: remote.String(), Addrs: []string{"/ip4/10.0.0.1/tcp/9905"}}}
	s.P2PClient = mockP2P
	s.notificationCh = make(chan *notificationMsg, 200)

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

	s.registryBatcher.flushOnce(context.Background())

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
		barrierMissed bool
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
		// with a single-goroutine subscriber this would deadlock. The timeout
		// exists only to avoid a hang — it is recorded and fails the test.
		select {
		case <-firstFourBusy:
		case <-time.After(30 * time.Second):
			mu.Lock()
			barrierMissed = true
			mu.Unlock()
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
	require.False(t, barrierMissed, "all workers must become busy simultaneously; the barrier timed out")
	require.Equal(t, workers, maxInFlight, "concurrency must reach and never exceed the configured bound")
	require.Equal(t, messages, processed)
}

// blockingRegistryClient lets a test pause inside RegisterPeer to interleave a
// concurrent forget() with an in-flight flush.
type blockingRegistryClient struct {
	blockchain.PeerRegistryClientI
	enteredRegister chan string
	releaseRegister chan struct{}
}

func (c *blockingRegistryClient) RegisterPeer(ctx context.Context, info *blockchain.PeerInfo) error {
	c.enteredRegister <- info.ID
	<-c.releaseRegister
	return c.PeerRegistryClientI.RegisterPeer(ctx, info)
}

// TestPeerRegistryBatcher_ForgetDuringFlushDoesNotStickAssertState reproduces
// the removal race: a peer removed while its coalesced update is mid-flush
// must not end up recorded as freshly asserted, otherwise its next message
// would be skipped for RegisterPeer for up to registryReassertTTL while the
// registry no longer has the entry.
func TestPeerRegistryBatcher_ForgetDuringFlushDoesNotStickAssertState(t *testing.T) {
	reg := blockchain.NewCentralizedPeerRegistry(blockchain.DefaultBanConfig())
	local := blockchain.NewLocalPeerRegistryClient(reg)
	blocking := &blockingRegistryClient{
		PeerRegistryClientI: local,
		enteredRegister:     make(chan string),
		releaseRegister:     make(chan struct{}),
	}
	counting := newCountingRegistryClient(blocking)
	b := newPeerRegistryBatcher(context.Background(), ulogger.TestLogger{}, counting, time.Hour)

	pid := mustNewPeerID(t).String()
	other := mustNewPeerID(t).String()

	// Two peers pending so the flush is provably mid-cycle when we interleave.
	b.enqueueRegister(pid, "", 0, nil, "", true)
	b.enqueueRegister(other, "", 0, nil, "", true)

	flushDone := make(chan struct{})
	go func() {
		defer close(flushDone)
		b.flushOnce(context.Background())
	}()

	// Wait until the flush is inside RegisterPeer for the first peer, then
	// remove whichever peer that is (map order is not deterministic).
	first := recvRegisterEntered(t, blocking.enteredRegister)
	b.forget(first)
	require.NoError(t, counting.RemovePeer(context.Background(), first))
	blocking.releaseRegister <- struct{}{}

	// Release the second peer's RegisterPeer as well.
	recvRegisterEntered(t, blocking.enteredRegister)
	blocking.releaseRegister <- struct{}{}

	select {
	case <-flushDone:
	case <-time.After(10 * time.Second):
		t.Fatal("flush did not complete")
	}

	// The forgotten peer's next message must trigger a fresh RegisterPeer on
	// the following flush (assert state must not have been recorded).
	registerCallsBefore := counting.callCount("RegisterPeer")
	b.enqueueLastMessage(first)

	// Drain the extra blocking-hook round for this flush.
	go func() {
		<-blocking.enteredRegister
		blocking.releaseRegister <- struct{}{}
	}()
	b.flushOnce(context.Background())

	require.Equal(t, registerCallsBefore+1, counting.callCount("RegisterPeer"),
		"peer removed mid-flush must be re-registered on its next message")
	_, ok := reg.Get(first)
	require.True(t, ok, "peer must be back in the registry")
}

// recvRegisterEntered receives from the blocking hook with a timeout so a
// broken interleaving fails fast instead of hanging the suite.
func recvRegisterEntered(t *testing.T, ch chan string) string {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for RegisterPeer to be entered")
		return ""
	}
}

func TestPeerRegistryBatcher_StopWithoutStartDoesNotBlock(t *testing.T) {
	b, _, _ := newBatcherWithCountingRegistry()

	done := make(chan struct{})
	go func() {
		b.stop(context.Background()) // start() was never called — must not block on doneCh
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("stop() deadlocked when start() was never called")
	}
}

// TestPeerRegistryBatcher_StopFlushesAfterParentCtxCancelled guards the
// shutdown ordering in production: the service manager cancels the parent
// context before calling Stop, and the final flush must still reach the
// registry.
func TestPeerRegistryBatcher_StopFlushesAfterParentCtxCancelled(t *testing.T) {
	reg := blockchain.NewCentralizedPeerRegistry(blockchain.DefaultBanConfig())
	counting := newCountingRegistryClient(blockchain.NewLocalPeerRegistryClient(reg))

	ctx, cancel := context.WithCancel(context.Background())
	b := newPeerRegistryBatcher(ctx, ulogger.TestLogger{}, counting, time.Hour)
	b.start()

	pid := mustNewPeerID(t).String()
	b.enqueueRegister(pid, "client/1.0", 7, nil, "", true)

	cancel()
	b.stop(context.Background())

	got, ok := reg.Get(pid)
	require.True(t, ok, "final flush must survive parent context cancellation")
	require.Equal(t, uint32(7), got.Height)
}

// TestPeerRegistryBatcher_HeightMergeIsMonotonic guards against advertised
// heights regressing when concurrent topic workers enqueue out of message
// order: the highest height (with its paired hash) must win within a flush
// window.
func TestPeerRegistryBatcher_HeightMergeIsMonotonic(t *testing.T) {
	b, _, reg := newBatcherWithCountingRegistry()
	pid := mustNewPeerID(t).String()

	hashNew := chainhash.HashH([]byte("newer block"))
	hashOld := chainhash.HashH([]byte("older block"))

	// Out-of-order enqueue: the higher height arrives first.
	b.enqueueRegister(pid, "", 42, &hashNew, "", false)
	b.enqueueRegister(pid, "", 41, &hashOld, "", false)

	b.flushOnce(context.Background())

	got, ok := reg.Get(pid)
	require.True(t, ok)
	require.Equal(t, uint32(42), got.Height, "lower height enqueued later must not win")
	require.Equal(t, hashNew.String(), got.BlockHash.String(), "hash must stay paired with the winning height")
}

// TestPeerRegistryBatcher_RequeueOnRegisterFailure verifies that a transient
// RegisterPeer failure does not silently drop the peer's accumulated metrics:
// they are retried on the next flush.
func TestPeerRegistryBatcher_RequeueOnRegisterFailure(t *testing.T) {
	reg := blockchain.NewCentralizedPeerRegistry(blockchain.DefaultBanConfig())
	failing := &failFirstRegisterClient{PeerRegistryClientI: blockchain.NewLocalPeerRegistryClient(reg)}
	b := newPeerRegistryBatcher(context.Background(), ulogger.TestLogger{}, failing, time.Hour)

	pid := mustNewPeerID(t).String()
	b.enqueueRegister(pid, "client/1.0", 5, nil, "", true)
	b.enqueueBytesReceived(pid, 500)

	b.flushOnce(context.Background()) // RegisterPeer fails; batch must be requeued
	_, ok := reg.Get(pid)
	require.False(t, ok, "failed register must not partially apply")

	b.flushOnce(context.Background()) // retry succeeds
	got, ok := reg.Get(pid)
	require.True(t, ok)
	require.Equal(t, uint32(5), got.Height)
	require.Equal(t, uint64(500), got.BytesReceived, "accumulated bytes must survive the failed flush")
	require.True(t, got.IsConnected)
}

type failFirstRegisterClient struct {
	blockchain.PeerRegistryClientI
	attempts int
}

func (c *failFirstRegisterClient) RegisterPeer(ctx context.Context, info *blockchain.PeerInfo) error {
	c.attempts++
	if c.attempts == 1 {
		return errors.NewServiceError("transient registry error")
	}
	return c.PeerRegistryClientI.RegisterPeer(ctx, info)
}

type failFirstMetricsClient struct {
	blockchain.PeerRegistryClientI
	attempts int
}

func (c *failFirstMetricsClient) UpdatePeerMetrics(ctx context.Context, peerID string, height uint32, bytesSentDelta, bytesRecvDelta uint64, recordSuccess, recordFailure, recordMalicious bool, responseTimeMs int64) error {
	c.attempts++
	if c.attempts == 1 {
		return errors.NewServiceError("transient registry error")
	}
	return c.PeerRegistryClientI.UpdatePeerMetrics(ctx, peerID, height, bytesSentDelta, bytesRecvDelta, recordSuccess, recordFailure, recordMalicious, responseTimeMs)
}

// TestPeerRegistryBatcher_RequeueOnMetricsFailure verifies that a failure in a
// non-RegisterPeer RPC (here UpdatePeerMetrics, after RegisterPeer succeeded)
// also requeues the failed intent instead of silently dropping the peer's
// accumulated byte deltas with the flushed update.
func TestPeerRegistryBatcher_RequeueOnMetricsFailure(t *testing.T) {
	reg := blockchain.NewCentralizedPeerRegistry(blockchain.DefaultBanConfig())
	failing := &failFirstMetricsClient{PeerRegistryClientI: blockchain.NewLocalPeerRegistryClient(reg)}
	b := newPeerRegistryBatcher(context.Background(), ulogger.TestLogger{}, failing, time.Hour)

	pid := mustNewPeerID(t).String()
	b.enqueueRegister(pid, "client/1.0", 5, nil, "", true)
	b.enqueueBytesReceived(pid, 500)

	b.flushOnce(context.Background()) // RegisterPeer succeeds, UpdatePeerMetrics fails
	got, ok := reg.Get(pid)
	require.True(t, ok, "registration must have applied")
	require.Zero(t, got.BytesReceived, "metrics RPC failed")

	b.flushOnce(context.Background()) // failed byte delta retried
	got, _ = reg.Get(pid)
	require.Equal(t, uint64(500), got.BytesReceived, "byte delta must survive a failed UpdatePeerMetrics")
}

// TestPeerRegistryBatcher_TombstoneMapCountBounded verifies the removal
// tombstone map is count-bounded like pending and lastAsserted: fresh
// tombstones at the cap are skipped (re-registration is still guaranteed by
// the lastAsserted deletion), expired ones are swept to make room.
func TestPeerRegistryBatcher_TombstoneMapCountBounded(t *testing.T) {
	b, _, _ := newBatcherWithCountingRegistry()

	now := time.Now()

	b.mu.Lock()
	for i := 0; i < registryBatcherMaxPending; i++ {
		b.removed[fmt.Sprintf("peer-%d", i)] = now
	}
	b.mu.Unlock()

	b.forget("one-too-many")

	b.mu.Lock()
	require.Len(t, b.removed, registryBatcherMaxPending, "fresh tombstones at the cap must not be exceeded")
	_, inserted := b.removed["one-too-many"]
	require.False(t, inserted)

	// With expired tombstones, the sweep makes room.
	stale := now.Add(-2 * registryTombstoneAge)
	for i := 0; i < registryBatcherMaxPending; i++ {
		b.removed[fmt.Sprintf("peer-%d", i)] = stale
	}
	b.mu.Unlock()

	b.forget("one-too-many")

	b.mu.Lock()
	_, inserted = b.removed["one-too-many"]
	require.True(t, inserted, "expired tombstones must be swept to make room")
	b.mu.Unlock()
}

// TestPeerRegistryBatcher_ReenqueueDuringFlushDoesNotFlushStaleSnapshot closes
// the forget → re-enqueue interleaving inside one flush cycle: the re-enqueue
// clears the persistent tombstone, but the loop must still skip the peer's
// stale pre-removal snapshot (via the flush-scoped removal set) and flush the
// fresh data next cycle instead.
func TestPeerRegistryBatcher_ReenqueueDuringFlushDoesNotFlushStaleSnapshot(t *testing.T) {
	reg := blockchain.NewCentralizedPeerRegistry(blockchain.DefaultBanConfig())
	blocking := &blockingRegistryClient{
		PeerRegistryClientI: blockchain.NewLocalPeerRegistryClient(reg),
		enteredRegister:     make(chan string),
		releaseRegister:     make(chan struct{}),
	}
	counting := newCountingRegistryClient(blocking)
	b := newPeerRegistryBatcher(context.Background(), ulogger.TestLogger{}, counting, time.Hour)

	first := mustNewPeerID(t).String()
	second := mustNewPeerID(t).String()
	b.enqueueRegister(first, "stale/1.0", 10, nil, "", false)
	b.enqueueRegister(second, "stale/1.0", 10, nil, "", false)

	flushDone := make(chan struct{})
	go func() {
		defer close(flushDone)
		b.flushOnce(context.Background())
	}()

	// The flush is wedged inside RegisterPeer for one peer; the OTHER peer's
	// snapshot has not been processed yet. Forget it, then re-enqueue it —
	// which clears its persistent tombstone.
	wedged := recvRegisterEntered(t, blocking.enteredRegister)
	other := first
	if wedged == first {
		other = second
	}
	b.forget(other)
	b.enqueueRegister(other, "fresh/2.0", 42, nil, "", false)
	blocking.releaseRegister <- struct{}{}

	select {
	case <-flushDone:
	case <-time.After(10 * time.Second):
		t.Fatal("flush did not complete")
	}

	// The stale snapshot must NOT have been flushed: only the wedged peer's
	// RegisterPeer went out this cycle.
	require.Equal(t, 1, counting.callCount("RegisterPeer"), "stale pre-removal snapshot must be skipped")
	_, ok := reg.Get(other)
	require.False(t, ok, "forgotten peer must not be resurrected by its stale snapshot")

	// The fresh post-removal data flushes on the next cycle.
	go func() {
		<-blocking.enteredRegister
		blocking.releaseRegister <- struct{}{}
	}()
	b.flushOnce(context.Background())

	got, ok := reg.Get(other)
	require.True(t, ok, "fresh data must flush on the next cycle")
	require.Equal(t, "fresh/2.0", got.ClientName)
	require.Equal(t, uint32(42), got.Height)
}

// TestPeerRegistryBatcher_StopHonorsBudget verifies ChiR1: stop is bounded by
// the caller's context (the service manager's per-service stop budget). With
// a flush wedged inside a registry RPC and an already-expired budget, stop
// must return promptly instead of waiting out registryFlushTimeout.
func TestPeerRegistryBatcher_StopHonorsBudget(t *testing.T) {
	reg := blockchain.NewCentralizedPeerRegistry(blockchain.DefaultBanConfig())
	blocking := &blockingRegistryClient{
		PeerRegistryClientI: blockchain.NewLocalPeerRegistryClient(reg),
		enteredRegister:     make(chan string),
		releaseRegister:     make(chan struct{}),
	}
	b := newPeerRegistryBatcher(context.Background(), ulogger.TestLogger{}, blocking, 10*time.Millisecond)
	b.start()

	pid := mustNewPeerID(t).String()
	b.enqueueRegister(pid, "", 0, nil, "", true)

	// Wait until the ticker flush is wedged inside RegisterPeer.
	recvRegisterEntered(t, blocking.enteredRegister)

	expired, cancel := context.WithCancel(context.Background())
	cancel()

	stopReturned := make(chan struct{})
	go func() {
		b.stop(expired)
		close(stopReturned)
	}()

	select {
	case <-stopReturned:
	case <-time.After(5 * time.Second):
		t.Fatal("stop did not honor the exhausted budget while a flush was in flight")
	}

	// Unwedge the in-flight flush so the goroutine can exit cleanly.
	blocking.releaseRegister <- struct{}{}
}

// TestPeerRegistryBatcher_AssertStateCountBounded verifies the assert-state
// map cannot grow beyond registryBatcherMaxPending under a flood of distinct
// (spoofable) peer IDs: fresh entries at the cap reject new recordings, stale
// entries are swept to make room.
func TestPeerRegistryBatcher_AssertStateCountBounded(t *testing.T) {
	b, _, _ := newBatcherWithCountingRegistry()

	now := time.Now()

	b.mu.Lock()
	for i := 0; i < registryBatcherMaxPending; i++ {
		b.lastAsserted[fmt.Sprintf("peer-%d", i)] = registryAssertState{registeredAt: now}
	}
	b.recordAssertStateLocked("one-too-many", registryAssertState{registeredAt: now})
	require.Len(t, b.lastAsserted, registryBatcherMaxPending, "fresh entries at the cap must not be exceeded")
	_, recorded := b.lastAsserted["one-too-many"]
	require.False(t, recorded)

	// With stale entries, the sweep makes room and the new entry is recorded.
	stale := now.Add(-2 * registryReassertTTL)
	for i := 0; i < registryBatcherMaxPending; i++ {
		b.lastAsserted[fmt.Sprintf("peer-%d", i)] = registryAssertState{registeredAt: stale}
	}
	b.recordAssertStateLocked("one-too-many", registryAssertState{registeredAt: now})
	_, recorded = b.lastAsserted["one-too-many"]
	require.True(t, recorded, "stale entries must be swept to make room")
	b.mu.Unlock()
}

// TestSubscribeToTopic_PanicInHandlerDoesNotCrash verifies ChiR3: a gossip
// handler panicking on a crafted message is recovered per message, and the
// workers keep processing subsequent messages.
func TestSubscribeToTopic_PanicInHandlerDoesNotCrash(t *testing.T) {
	s := &Server{
		logger: ulogger.TestLogger{},
		gCtx:   context.Background(),
		settings: &settings.Settings{
			P2P: settings.P2PSettings{GossipHandlerConcurrency: 2},
		},
	}

	ch := make(chan p2pMessageBus.Message, 4)
	mockP2P := new(MockServerP2PClient)
	mockP2P.On("Subscribe", "panic-topic").Return((<-chan p2pMessageBus.Message)(ch))
	s.P2PClient = mockP2P

	var mu sync.Mutex
	var processed []string
	done := make(chan struct{})

	handler := func(_ context.Context, data []byte, _ string) {
		if string(data) == "boom" {
			panic("crafted message")
		}
		mu.Lock()
		processed = append(processed, string(data))
		if len(processed) == 2 {
			close(done)
		}
		mu.Unlock()
	}

	s.subscribeToTopic(context.Background(), "panic-topic", handler)

	ch <- p2pMessageBus.Message{Data: []byte("boom"), FromID: "attacker"}
	ch <- p2pMessageBus.Message{Data: []byte("ok-1"), FromID: "peer"}
	ch <- p2pMessageBus.Message{Data: []byte("ok-2"), FromID: "peer"}
	close(ch)

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("messages after a handler panic were not processed")
	}
}

// erroringBanClient fails every IsPeerBanned lookup, simulating a degraded
// registry.
type erroringBanClient struct {
	blockchain.PeerRegistryClientI
}

func (c *erroringBanClient) IsPeerBanned(_ context.Context, _ string) (bool, error) {
	return false, errors.NewServiceError("registry unavailable")
}

// TestShouldSkipBannedPeer_ErrorBreaker verifies ChiR2: when IsPeerBanned
// errors, the failure is cached (fail open) so a gossip flood against a
// degraded registry costs one RPC per peer per TTL instead of one per message.
func TestShouldSkipBannedPeer_ErrorBreaker(t *testing.T) {
	reg := blockchain.NewCentralizedPeerRegistry(blockchain.DefaultBanConfig())
	counting := newCountingRegistryClient(&erroringBanClient{blockchain.NewLocalPeerRegistryClient(reg)})

	s := &Server{
		peerRegistry: counting,
		logger:       ulogger.TestLogger{},
		gCtx:         context.Background(),
	}

	pid := mustNewPeerID(t).String()
	for i := 0; i < 100; i++ {
		require.False(t, s.shouldSkipBannedPeer(pid, "test"), "lookup errors must fail open")
	}

	require.Equal(t, 1, counting.callCount("IsPeerBanned"), "failed lookups must be cached, not retried per message")
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
