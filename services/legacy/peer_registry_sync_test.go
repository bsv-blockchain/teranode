package legacy

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// countingRegistry wraps a real local registry client and counts the calls the
// reconcile loop makes, so a test can assert that an unchanged peer costs
// nothing. Only call counting is added; every write lands in a real
// CentralizedPeerRegistry.
type countingRegistry struct {
	blockchain.PeerRegistryClientI
	registerCalls   int
	connStateCalls  map[string][]bool
	metricsCalls    int
	lastMessageCals int
	failRegister    error
	failLastMessage bool
}

func newCountingRegistry(reg *blockchain.CentralizedPeerRegistry) *countingRegistry {
	return &countingRegistry{
		PeerRegistryClientI: blockchain.NewLocalPeerRegistryClient(reg),
		connStateCalls:      make(map[string][]bool),
	}
}

func (c *countingRegistry) RegisterPeer(ctx context.Context, info *blockchain.PeerInfo) error {
	if c.failRegister != nil {
		return c.failRegister
	}

	c.registerCalls++

	return c.PeerRegistryClientI.RegisterPeer(ctx, info)
}

func (c *countingRegistry) UpdateConnectionState(ctx context.Context, peerID string, connected bool) error {
	c.connStateCalls[peerID] = append(c.connStateCalls[peerID], connected)

	return c.PeerRegistryClientI.UpdateConnectionState(ctx, peerID, connected)
}

func (c *countingRegistry) UpdatePeerMetrics(ctx context.Context, peerID string, height uint32,
	bytesSentDelta, bytesRecvDelta uint64, recordSuccess, recordFailure, recordMalicious bool,
	responseTimeMs int64) error {
	c.metricsCalls++

	return c.PeerRegistryClientI.UpdatePeerMetrics(ctx, peerID, height, bytesSentDelta,
		bytesRecvDelta, recordSuccess, recordFailure, recordMalicious, responseTimeMs)
}

func (c *countingRegistry) UpdateLastMessageTime(ctx context.Context, peerID string) error {
	c.lastMessageCals++

	if c.failLastMessage {
		return context.DeadlineExceeded
	}

	return c.PeerRegistryClientI.UpdateLastMessageTime(ctx, peerID)
}

func testSyncSettings() *settings.Settings {
	tSettings := settings.NewSettings()
	tSettings.Legacy.PeerRegistryEnabled = true
	tSettings.Legacy.PeerRegistrySyncInterval = time.Second

	return tSettings
}

func testSnapshot(addr string, bytesRecv uint64, lastRecv time.Time) peerSnapshot {
	return peerSnapshot{
		id:            legacyRegistryID(addr),
		addr:          addr,
		userAgent:     "/Bitcoin SV:1.0.16/",
		height:        912345,
		bytesSent:     100,
		bytesReceived: bytesRecv,
		lastRecv:      lastRecv,
		legacy: blockchain.LegacyPeerInfo{
			Inbound:         false,
			ProtocolVersion: 70016,
			ServiceFlags:    0x25,
			PingMicros:      42000,
			StartingHeight:  912000,
			IsSyncPeer:      true,
			TimeConnected:   time.Unix(1750000000, 0).UTC(),
		},
	}
}

// TestReconcile_RegistersNewPeer checks a first sighting registers with the
// wire-protocol transport, the legacy: ID and the legacy block.
func TestReconcile_RegistersNewPeer(t *testing.T) {
	reg := blockchain.NewCentralizedPeerRegistry(blockchain.DefaultBanConfig())
	counting := newCountingRegistry(reg)

	snap := testSnapshot("203.0.113.7:8333", 500, time.Unix(1750000100, 0))
	sync := newPeerRegistrySync(ulogger.TestLogger{}, testSyncSettings(), counting,
		func() []peerSnapshot { return []peerSnapshot{snap} })

	sync.reconcile(context.Background())

	got, ok := reg.Get("legacy:203.0.113.7:8333")
	require.True(t, ok)
	require.Equal(t, blockchain_api.TransportType_TRANSPORT_WIRE_PROTOCOL, got.TransportType)
	require.Equal(t, "203.0.113.7:8333", got.NetworkAddress)
	require.Equal(t, "/Bitcoin SV:1.0.16/", got.ClientName)
	require.Equal(t, uint32(912345), got.Height)
	require.True(t, got.IsConnected)
	require.NotNil(t, got.Legacy)
	require.Equal(t, uint32(70016), got.Legacy.ProtocolVersion)
	require.True(t, got.Legacy.IsSyncPeer)
	require.Equal(t, uint64(500), got.BytesReceived, "first sighting pushes the whole total")
	require.Equal(t, 1, counting.registerCalls)
}

// TestReconcile_UnchangedPeerCostsNoRegisterCall checks the skip rule.
func TestReconcile_UnchangedPeerCostsNoRegisterCall(t *testing.T) {
	reg := blockchain.NewCentralizedPeerRegistry(blockchain.DefaultBanConfig())
	counting := newCountingRegistry(reg)

	snap := testSnapshot("203.0.113.7:8333", 500, time.Unix(1750000100, 0))
	sync := newPeerRegistrySync(ulogger.TestLogger{}, testSyncSettings(), counting,
		func() []peerSnapshot { return []peerSnapshot{snap} })

	sync.reconcile(context.Background())
	sync.reconcile(context.Background())

	require.Equal(t, 1, counting.registerCalls, "an unchanged peer must not re-register")
	require.Equal(t, []bool{true}, counting.connStateCalls["legacy:203.0.113.7:8333"],
		"connection state must be asserted once, not every tick")
}

// TestReconcile_VanishedPeerMarkedDisconnectedOnce checks step 4 of the loop,
// and the TTL trap: the peer must also leave the tracking map, so no later tick
// re-registers it and refreshes its LastSeen clock.
func TestReconcile_VanishedPeerMarkedDisconnectedOnce(t *testing.T) {
	reg := blockchain.NewCentralizedPeerRegistry(blockchain.DefaultBanConfig())
	counting := newCountingRegistry(reg)

	snap := testSnapshot("203.0.113.7:8333", 500, time.Unix(1750000100, 0))
	present := true
	sync := newPeerRegistrySync(ulogger.TestLogger{}, testSyncSettings(), counting,
		func() []peerSnapshot {
			if present {
				return []peerSnapshot{snap}
			}

			return []peerSnapshot{}
		})

	sync.reconcile(context.Background())
	present = false
	sync.reconcile(context.Background())
	sync.reconcile(context.Background())

	got, ok := reg.Get("legacy:203.0.113.7:8333")
	require.True(t, ok, "the entry stays for history; the TTL reaps it")
	require.False(t, got.IsConnected)
	require.Equal(t, []bool{true, false}, counting.connStateCalls["legacy:203.0.113.7:8333"],
		"disconnect must be asserted exactly once")
	require.Equal(t, 1, counting.registerCalls,
		"a vanished peer must never be re-registered: Register refreshes the TTL clock")
}

// TestReconcile_NilSnapshotIsNotADisconnect is a regression test: getPeers
// returns nil when its query channel is full or its reply times out. That must
// not read as "every peer went away".
func TestReconcile_NilSnapshotIsNotADisconnect(t *testing.T) {
	reg := blockchain.NewCentralizedPeerRegistry(blockchain.DefaultBanConfig())
	counting := newCountingRegistry(reg)

	snap := testSnapshot("203.0.113.7:8333", 500, time.Unix(1750000100, 0))
	healthy := true
	sync := newPeerRegistrySync(ulogger.TestLogger{}, testSyncSettings(), counting,
		func() []peerSnapshot {
			if healthy {
				return []peerSnapshot{snap}
			}

			return nil
		})

	sync.reconcile(context.Background())
	healthy = false
	sync.reconcile(context.Background())

	got, ok := reg.Get("legacy:203.0.113.7:8333")
	require.True(t, ok)
	require.True(t, got.IsConnected, "a nil snapshot must not mark peers disconnected")
	require.Equal(t, []bool{true}, counting.connStateCalls["legacy:203.0.113.7:8333"])
}

// TestReconcile_ByteCountersAreDeltas checks the delta conversion, including a
// counter reset after a reconnect.
func TestReconcile_ByteCountersAreDeltas(t *testing.T) {
	reg := blockchain.NewCentralizedPeerRegistry(blockchain.DefaultBanConfig())
	counting := newCountingRegistry(reg)

	current := testSnapshot("203.0.113.7:8333", 500, time.Unix(1750000100, 0))
	sync := newPeerRegistrySync(ulogger.TestLogger{}, testSyncSettings(), counting,
		func() []peerSnapshot { return []peerSnapshot{current} })

	sync.reconcile(context.Background())

	current.bytesReceived = 1200
	current.lastRecv = time.Unix(1750000200, 0)
	sync.reconcile(context.Background())

	got, _ := reg.Get("legacy:203.0.113.7:8333")
	require.Equal(t, uint64(1200), got.BytesReceived, "500 then a delta of 700")

	// A reconnect resets the peer's counters. The delta must treat the reset
	// total as new bytes rather than wrapping around uint64.
	current.bytesReceived = 10
	current.lastRecv = time.Unix(1750000300, 0)
	sync.reconcile(context.Background())

	got, _ = reg.Get("legacy:203.0.113.7:8333")
	require.Equal(t, uint64(1210), got.BytesReceived,
		"a backwards counter means a replaced connection, so its 10 bytes are new")
}

// TestReconcile_RegistryErrorDoesNotStopTheLoop checks that a failing registry
// leaves the loop able to recover on the next tick.
func TestReconcile_RegistryErrorDoesNotStopTheLoop(t *testing.T) {
	reg := blockchain.NewCentralizedPeerRegistry(blockchain.DefaultBanConfig())
	counting := newCountingRegistry(reg)
	counting.failRegister = context.DeadlineExceeded

	snap := testSnapshot("203.0.113.7:8333", 500, time.Unix(1750000100, 0))
	sync := newPeerRegistrySync(ulogger.TestLogger{}, testSyncSettings(), counting,
		func() []peerSnapshot { return []peerSnapshot{snap} })

	sync.reconcile(context.Background())

	_, ok := reg.Get("legacy:203.0.113.7:8333")
	require.False(t, ok)

	counting.failRegister = nil
	sync.reconcile(context.Background())

	got, ok := reg.Get("legacy:203.0.113.7:8333")
	require.True(t, ok, "the next tick must retry a failed registration")
	require.True(t, got.IsConnected)
}

// TestLegacyRegistryID checks the ID never collides with a libp2p peer ID.
func TestLegacyRegistryID(t *testing.T) {
	require.Equal(t, "legacy:203.0.113.7:8333", legacyRegistryID("203.0.113.7:8333"))
	require.Equal(t, "legacy:[2001:db8::1]:8333", legacyRegistryID("[2001:db8::1]:8333"))
}

// TestReconcile_ReconnectDoesNotDoubleCountBytes is a regression test. A
// vanished peer is dropped from lastSeen, but its registry entry survives until
// TTL cleanup. Byte counters travel as deltas that UpdateMetrics ADDS, so a peer
// seen "for the first time" against a surviving entry must not re-add its whole
// running total. getPeers() only returns peers whose Connected() is true, so a
// peer can leave one snapshot and return with its counters still climbing.
func TestReconcile_ReconnectDoesNotDoubleCountBytes(t *testing.T) {
	reg := blockchain.NewCentralizedPeerRegistry(blockchain.DefaultBanConfig())
	counting := newCountingRegistry(reg)

	snap := testSnapshot("203.0.113.7:8333", 1000, time.Unix(1750000100, 0))
	present := true
	sync := newPeerRegistrySync(ulogger.TestLogger{}, testSyncSettings(), counting,
		func() []peerSnapshot {
			if present {
				return []peerSnapshot{snap}
			}

			return []peerSnapshot{}
		})

	sync.reconcile(context.Background())

	got, _ := reg.Get("legacy:203.0.113.7:8333")
	require.Equal(t, uint64(1000), got.BytesReceived)

	// The peer drops out of one snapshot, then returns with its counters still
	// running. The registry entry survived the gap.
	present = false
	sync.reconcile(context.Background())

	present = true
	snap.bytesReceived = 1200
	snap.lastRecv = time.Unix(1750000200, 0)
	sync.reconcile(context.Background())

	got, _ = reg.Get("legacy:203.0.113.7:8333")
	require.Equal(t, uint64(1200), got.BytesReceived,
		"the running total must not be re-added on top of the surviving entry")
	require.True(t, got.IsConnected, "the peer must be marked connected again")
}

// TestReconcile_ReconnectWithResetCountersDoesNotRegress covers the other
// reappearance shape: a genuinely new TCP connection whose counters restart at
// zero must not drag the stored total backwards.
func TestReconcile_ReconnectWithResetCountersDoesNotRegress(t *testing.T) {
	reg := blockchain.NewCentralizedPeerRegistry(blockchain.DefaultBanConfig())
	counting := newCountingRegistry(reg)

	snap := testSnapshot("203.0.113.7:8333", 1000, time.Unix(1750000100, 0))
	present := true
	sync := newPeerRegistrySync(ulogger.TestLogger{}, testSyncSettings(), counting,
		func() []peerSnapshot {
			if present {
				return []peerSnapshot{snap}
			}

			return []peerSnapshot{}
		})

	sync.reconcile(context.Background())
	present = false
	sync.reconcile(context.Background())

	// Fresh connection: the peer's own counters start again from near zero.
	present = true
	snap.bytesReceived = 50
	snap.lastRecv = time.Unix(1750000300, 0)
	sync.reconcile(context.Background())

	got, _ := reg.Get("legacy:203.0.113.7:8333")
	require.Equal(t, uint64(1050), got.BytesReceived,
		"a reset counter contributes its new bytes on top of the stored lifetime total")

	// Subsequent growth on the new connection is tracked normally.
	snap.bytesReceived = 90
	snap.lastRecv = time.Unix(1750000400, 0)
	sync.reconcile(context.Background())

	got, _ = reg.Get("legacy:203.0.113.7:8333")
	require.Equal(t, uint64(1090), got.BytesReceived)
}

// failingGetPeer wraps a registry client and fails only GetPeer, so the byte
// baseline fallback can be exercised.
type failingGetPeer struct {
	blockchain.PeerRegistryClientI
}

func (f failingGetPeer) GetPeer(_ context.Context, _ string) (*blockchain.PeerInfo, bool, error) {
	return nil, false, context.DeadlineExceeded
}

// TestReconcile_BaselineLookupFailureStillReports checks that a failed byte
// baseline lookup degrades to reporting the whole total rather than dropping the
// peer or skipping its metrics.
func TestReconcile_BaselineLookupFailureStillReports(t *testing.T) {
	reg := blockchain.NewCentralizedPeerRegistry(blockchain.DefaultBanConfig())
	client := failingGetPeer{blockchain.NewLocalPeerRegistryClient(reg)}

	snap := testSnapshot("203.0.113.7:8333", 500, time.Unix(1750000100, 0))
	sync := newPeerRegistrySync(ulogger.TestLogger{}, testSyncSettings(), client,
		func() []peerSnapshot { return []peerSnapshot{snap} })

	sync.reconcile(context.Background())

	got, ok := reg.Get("legacy:203.0.113.7:8333")
	require.True(t, ok, "the peer must still be registered")
	require.True(t, got.IsConnected)
	require.Equal(t, uint64(500), got.BytesReceived,
		"without a baseline the whole total is reported")
}

// hangingRegistry blocks every RegisterPeer until its context is done, standing
// in for an unresponsive blockchain service.
type hangingRegistry struct {
	blockchain.PeerRegistryClientI
	waited time.Duration
}

func (h *hangingRegistry) RegisterPeer(ctx context.Context, _ *blockchain.PeerInfo) error {
	started := time.Now()
	<-ctx.Done()
	h.waited = time.Since(started)

	return ctx.Err()
}

// TestReconcile_BoundsEachRegistryCall checks a stalled registry costs one
// bounded wait rather than hanging the tick for as long as gRPC takes to give
// up. The reconcile loop is a goroutine, so an unbounded call would also delay
// the shutdown path that only runs between ticks.
func TestReconcile_BoundsEachRegistryCall(t *testing.T) {
	reg := blockchain.NewCentralizedPeerRegistry(blockchain.DefaultBanConfig())
	hanging := &hangingRegistry{PeerRegistryClientI: blockchain.NewLocalPeerRegistryClient(reg)}

	snap := testSnapshot("203.0.113.7:8333", 500, time.Unix(1750000100, 0))
	sync := newPeerRegistrySync(ulogger.TestLogger{}, testSyncSettings(), hanging,
		func() []peerSnapshot { return []peerSnapshot{snap} })
	sync.rpcTimeout = 50 * time.Millisecond

	done := make(chan struct{})
	go func() {
		sync.reconcile(context.Background())
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("reconcile did not return: a registry call was not bounded")
	}

	require.Less(t, hanging.waited, time.Second,
		"the call must be cut off at the RPC timeout, not left to the transport")

	// A peer whose registration failed must not be recorded as pushed, so the
	// next tick retries it.
	_, ok := reg.Get("legacy:203.0.113.7:8333")
	require.False(t, ok)
}

// TestReconcile_DefaultRPCTimeoutIsSet guards the wiring: an unset timeout would
// make context.WithTimeout fire immediately and every call fail.
func TestReconcile_DefaultRPCTimeoutIsSet(t *testing.T) {
	reg := blockchain.NewCentralizedPeerRegistry(blockchain.DefaultBanConfig())
	sync := newPeerRegistrySync(ulogger.TestLogger{}, testSyncSettings(),
		blockchain.NewLocalPeerRegistryClient(reg), func() []peerSnapshot { return nil })

	require.Equal(t, defaultRegistryRPCTimeout, sync.rpcTimeout)
	require.Positive(t, sync.rpcTimeout)
}

// failingMetrics wraps a registry client and fails only UpdatePeerMetrics.
type failingMetrics struct {
	blockchain.PeerRegistryClientI
	fail bool
}

func (f *failingMetrics) UpdatePeerMetrics(ctx context.Context, peerID string, height uint32,
	bytesSentDelta, bytesRecvDelta uint64, recordSuccess, recordFailure, recordMalicious bool,
	responseTimeMs int64) error {
	if f.fail {
		return context.DeadlineExceeded
	}

	return f.PeerRegistryClientI.UpdatePeerMetrics(ctx, peerID, height, bytesSentDelta,
		bytesRecvDelta, recordSuccess, recordFailure, recordMalicious, responseTimeMs)
}

// TestReconcile_FailedMetricsPushIsRetriedNextTick is a regression test. A
// failed metrics call used only to log, then fall through to the unconditional
// lastSeen rebaseline, so the tick's byte delta was dropped for good rather than
// retried. The RegisterPeer path already avoided that by continuing.
func TestReconcile_FailedMetricsPushIsRetriedNextTick(t *testing.T) {
	reg := blockchain.NewCentralizedPeerRegistry(blockchain.DefaultBanConfig())
	client := &failingMetrics{PeerRegistryClientI: blockchain.NewLocalPeerRegistryClient(reg)}

	snap := testSnapshot("203.0.113.7:8333", 500, time.Unix(1750000100, 0))
	sync := newPeerRegistrySync(ulogger.TestLogger{}, testSyncSettings(), client,
		func() []peerSnapshot { return []peerSnapshot{snap} })

	// First tick registers the peer but its byte push fails.
	client.fail = true
	sync.reconcile(context.Background())

	got, ok := reg.Get("legacy:203.0.113.7:8333")
	require.True(t, ok, "registration itself succeeded")
	require.Zero(t, got.BytesReceived, "the failed push contributed nothing")

	// The registry recovers. The peer has since received more bytes. The next
	// tick must report everything not yet accepted, not just the new increment.
	client.fail = false
	snap.bytesReceived = 700
	snap.lastRecv = time.Unix(1750000200, 0)
	sync.reconcile(context.Background())

	got, _ = reg.Get("legacy:203.0.113.7:8333")
	require.Equal(t, uint64(700), got.BytesReceived,
		"the dropped 500 must be retried, not lost to a rebaseline")
}

// TestReconcile_FailedLastMessagePushIsRetriedNextTick covers the same rule for
// the last-message timestamp.
func TestReconcile_FailedLastMessagePushIsRetriedNextTick(t *testing.T) {
	reg := blockchain.NewCentralizedPeerRegistry(blockchain.DefaultBanConfig())
	counting := newCountingRegistry(reg)
	counting.failLastMessage = true

	snap := testSnapshot("203.0.113.7:8333", 500, time.Unix(1750000100, 0))
	sync := newPeerRegistrySync(ulogger.TestLogger{}, testSyncSettings(), counting,
		func() []peerSnapshot { return []peerSnapshot{snap} })

	sync.reconcile(context.Background())
	require.Equal(t, 1, counting.lastMessageCals)

	// Same lastRecv, recovered registry: the tick must retry rather than decide
	// nothing advanced.
	counting.failLastMessage = false
	sync.reconcile(context.Background())
	require.Equal(t, 2, counting.lastMessageCals,
		"a failed last-message push must be retried on the next tick")
}

// TestReconcile_ClearsPhantomConnectedOnFirstTick is a regression test for a
// restart hazard that only became reachable once the p2p connection sweep
// stopped clearing wire peers.
//
// The loop's disconnect sweep only knows peers it has seen itself. After a
// legacy-service restart lastSeen is empty, so an entry left IsConnected=true
// by the previous process is in neither current nor lastSeen and nothing
// touches it. It would report as a connected legacy peer until the registry TTL
// expired it, which is measured in hours.
func TestReconcile_ClearsPhantomConnectedOnFirstTick(t *testing.T) {
	reg := blockchain.NewCentralizedPeerRegistry(blockchain.DefaultBanConfig())
	counting := newCountingRegistry(reg)
	client := blockchain.NewLocalPeerRegistryClient(reg)

	// State left behind by a previous process: two connected wire peers.
	for _, id := range []string{"legacy:203.0.113.7:8333", "legacy:198.51.100.9:8333"} {
		reg.Register(&blockchain.PeerInfo{
			ID:               id,
			TransportType:    blockchain_api.TransportType_TRANSPORT_WIRE_PROTOCOL,
			TransportTypeSet: true,
			Legacy:           &blockchain.LegacyPeerInfo{ProtocolVersion: 70016},
		})
		require.NoError(t, client.UpdateConnectionState(context.Background(), id, true))
	}

	// A libp2p peer must be left alone: it is not this service's to manage.
	reg.Register(&blockchain.PeerInfo{
		ID:               "12D3KooWGRUEbFsXTBnpVRHtE3ZBSbSMd4x8hs9NfCVCNhqTFPHb",
		TransportType:    blockchain_api.TransportType_TRANSPORT_HTTP,
		TransportTypeSet: true,
	})
	require.NoError(t, client.UpdateConnectionState(context.Background(),
		"12D3KooWGRUEbFsXTBnpVRHtE3ZBSbSMd4x8hs9NfCVCNhqTFPHb", true))

	// Fresh loop, as after a restart. Only one of the two peers reconnects.
	snap := testSnapshot("203.0.113.7:8333", 500, time.Unix(1750000100, 0))
	sync := newPeerRegistrySync(ulogger.TestLogger{}, testSyncSettings(), counting,
		func() []peerSnapshot { return []peerSnapshot{snap} })

	sync.reconcile(context.Background())

	got, ok := reg.Get("legacy:203.0.113.7:8333")
	require.True(t, ok)
	require.True(t, got.IsConnected, "the peer that reconnected stays connected")

	got, ok = reg.Get("legacy:198.51.100.9:8333")
	require.True(t, ok)
	require.False(t, got.IsConnected,
		"a peer left connected by the previous process must be cleared on the first tick")

	got, ok = reg.Get("12D3KooWGRUEbFsXTBnpVRHtE3ZBSbSMd4x8hs9NfCVCNhqTFPHb")
	require.True(t, ok)
	require.True(t, got.IsConnected, "a libp2p peer is not this loop's to clear")

	// The pass runs once, not on every tick.
	sync.reconcile(context.Background())
	require.Equal(t, []bool{false}, counting.connStateCalls["legacy:198.51.100.9:8333"],
		"the phantom must be cleared exactly once")
}

// TestReconcile_NilFirstSnapshotDoesNotClearPhantoms checks the startup pass is
// gated on having real data. A nil snapshot at startup must not be read as
// "nothing is connected".
func TestReconcile_NilFirstSnapshotDoesNotClearPhantoms(t *testing.T) {
	reg := blockchain.NewCentralizedPeerRegistry(blockchain.DefaultBanConfig())
	client := blockchain.NewLocalPeerRegistryClient(reg)

	reg.Register(&blockchain.PeerInfo{
		ID:               "legacy:203.0.113.7:8333",
		TransportType:    blockchain_api.TransportType_TRANSPORT_WIRE_PROTOCOL,
		TransportTypeSet: true,
	})
	require.NoError(t, client.UpdateConnectionState(context.Background(),
		"legacy:203.0.113.7:8333", true))

	healthy := false
	sync := newPeerRegistrySync(ulogger.TestLogger{}, testSyncSettings(), client,
		func() []peerSnapshot {
			if healthy {
				return []peerSnapshot{}
			}

			return nil
		})

	sync.reconcile(context.Background())

	got, _ := reg.Get("legacy:203.0.113.7:8333")
	require.True(t, got.IsConnected, "a nil snapshot must not clear anything")

	// Once the server answers, an empty peer list is real data and the phantom goes.
	healthy = true
	sync.reconcile(context.Background())

	got, _ = reg.Get("legacy:203.0.113.7:8333")
	require.False(t, got.IsConnected)
}
