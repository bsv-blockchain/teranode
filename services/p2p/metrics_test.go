package p2p

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// failingListPeersRegistry wraps a real registry client but forces ListPeers
// to fail, so updateConnectedPeersGauge's error path can be exercised without
// a fake implementation of the whole PeerRegistryClientI interface.
type failingListPeersRegistry struct {
	blockchain.PeerRegistryClientI
}

func (f *failingListPeersRegistry) ListPeers(_ context.Context, _ *blockchain_api.TransportType, _ float64, _ uint32, _, _ bool) ([]*blockchain.PeerInfo, error) {
	return nil, errors.New(errors.ERR_ERROR, "registry unavailable")
}

// TestInitPrometheusMetricsIdempotent verifies that calling initPrometheusMetrics
// multiple times (which happens naturally: once via the package init() and again
// from NewServer / other tests in this package) never panics or double-registers
// with the default Prometheus registerer.
func TestInitPrometheusMetricsIdempotent(t *testing.T) {
	require.NotPanics(t, func() {
		initPrometheusMetrics()
		initPrometheusMetrics()
		initPrometheusMetrics()
	})

	require.NotNil(t, prometheusP2PConnectedPeers)
	require.NotNil(t, prometheusP2PBanEvents)
	require.NotNil(t, prometheusP2PCatchupAttempts)
	require.NotNil(t, prometheusP2PCatchupSuccesses)
	require.NotNil(t, prometheusP2PCatchupFailures)
	require.NotNil(t, prometheusP2PWebsocketConnections)
}

// TestPrometheusMetricsRegisteredWithTeranodeNamespace confirms all p2p metrics
// are registered with the default gatherer under the teranode_p2p_ prefix.
func TestPrometheusMetricsRegisteredWithTeranodeNamespace(t *testing.T) {
	initPrometheusMetrics()

	// CounterVec collectors only surface in Gather() once a label combination
	// has been materialized; touch it (Add(0) is a no-op on the value) so its
	// family is present to check below.
	prometheusP2PBanEvents.WithLabelValues("namespace_check").Add(0)
	prometheusP2PCatchupFailures.WithLabelValues("namespace_check").Add(0)

	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	checked := map[string]bool{
		"teranode_p2p_connected_peers":         false,
		"teranode_p2p_ban_events_total":        false,
		"teranode_p2p_catchup_attempts_total":  false,
		"teranode_p2p_catchup_successes_total": false,
		"teranode_p2p_catchup_failures_total":  false,
		"teranode_p2p_websocket_connections":   false,
	}

	for _, family := range families {
		if _, ok := checked[family.GetName()]; ok {
			checked[family.GetName()] = true
		}
	}

	for name, found := range checked {
		require.True(t, found, "metric %s should be registered", name)
	}
}

// TestPrometheusMetricsIncrementAndSet pokes each collector directly to
// confirm it behaves as a plain counter/gauge (Inc/Set/labels). It does not
// exercise production call sites — see TestRecordCatchupAttempt_IncrementsCounter,
// TestRecordCatchupSuccess_IncrementsCounter, TestRecordCatchupFailure_IncrementsFailureCounter,
// TestOnPeerBanned_IncrementsBanEventsCounter, TestBanPeer_IncrementsBanEventsCounter,
// TestClientChannelMapUpdatesWebsocketConnectionsGauge and
// TestUpdateConnectedPeersGauge_ReflectsLivePeerCount for those.
func TestPrometheusMetricsIncrementAndSet(t *testing.T) {
	initPrometheusMetrics()

	attemptsBefore := testutil.ToFloat64(prometheusP2PCatchupAttempts)
	successesBefore := testutil.ToFloat64(prometheusP2PCatchupSuccesses)
	banBefore := testutil.ToFloat64(prometheusP2PBanEvents.WithLabelValues("test_reason"))

	prometheusP2PCatchupAttempts.Inc()
	prometheusP2PCatchupSuccesses.Inc()
	prometheusP2PBanEvents.WithLabelValues("test_reason").Inc()
	prometheusP2PConnectedPeers.Set(3)
	prometheusP2PWebsocketConnections.Set(2)

	require.Equal(t, attemptsBefore+1, testutil.ToFloat64(prometheusP2PCatchupAttempts))
	require.Equal(t, successesBefore+1, testutil.ToFloat64(prometheusP2PCatchupSuccesses))
	require.Equal(t, banBefore+1, testutil.ToFloat64(prometheusP2PBanEvents.WithLabelValues("test_reason")))
	require.InDelta(t, 3, testutil.ToFloat64(prometheusP2PConnectedPeers), 0.0001)
	require.InDelta(t, 2, testutil.ToFloat64(prometheusP2PWebsocketConnections), 0.0001)
}

// TestClientChannelMapUpdatesWebsocketConnectionsGauge confirms add/remove on
// the websocket client registry keeps the connection-count gauge in sync,
// covering the actual increment/decrement call sites wired into HandleWebSocket.
func TestClientChannelMapUpdatesWebsocketConnectionsGauge(t *testing.T) {
	initPrometheusMetrics()

	// The gauge is process-wide (fed by every clientChannelMap instance), so
	// pin a known baseline rather than assuming the previous test left it at
	// zero, and assert deltas from that baseline rather than absolute values.
	prometheusP2PWebsocketConnections.Set(0)
	before := testutil.ToFloat64(prometheusP2PWebsocketConnections)

	cm := newClientChannelMap()

	ch1 := make(chan []byte, 1)
	ch2 := make(chan []byte, 1)

	cm.add(ch1, nil)
	require.InDelta(t, before+1, testutil.ToFloat64(prometheusP2PWebsocketConnections), 0.0001)

	cm.add(ch2, nil)
	require.InDelta(t, before+2, testutil.ToFloat64(prometheusP2PWebsocketConnections), 0.0001)

	cm.remove(ch1)
	require.InDelta(t, before+1, testutil.ToFloat64(prometheusP2PWebsocketConnections), 0.0001)

	cm.remove(ch2)
	require.InDelta(t, before, testutil.ToFloat64(prometheusP2PWebsocketConnections), 0.0001)
}

// TestUpdateConnectedPeersGauge_ReflectsLivePeerCount confirms the gauge
// tracks the registry's directly-connected peer count, independent of any
// ticker, and specifically that gossiped/disconnected registry entries are
// excluded — the same filter getNodeStatusMessage applies to
// connected_peers_count (Server.go).
func TestUpdateConnectedPeersGauge_ReflectsLivePeerCount(t *testing.T) {
	initPrometheusMetrics()

	s, reg, pid := freshTestServer(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String(), IsConnected: true})
	reg.Register(&blockchain.PeerInfo{ID: "connected-2", IsConnected: true})
	reg.Register(&blockchain.PeerInfo{ID: "connected-3", IsConnected: true})
	reg.Register(&blockchain.PeerInfo{ID: "gossiped-only", IsConnected: false})

	s.updateConnectedPeersGauge(context.Background())
	require.InDelta(t, 3, testutil.ToFloat64(prometheusP2PConnectedPeers), 0.0001,
		"gossiped/disconnected registry entries must not be counted")

	reg.UpdateConnectionState("connected-2", false)
	s.updateConnectedPeersGauge(context.Background())
	require.InDelta(t, 2, testutil.ToFloat64(prometheusP2PConnectedPeers), 0.0001)
}

// TestUpdateConnectedPeersGauge_NilRegistryIsNoOp confirms a nil peerRegistry
// (e.g. before NewServer has fully wired the service) leaves the gauge
// untouched rather than panicking.
func TestUpdateConnectedPeersGauge_NilRegistryIsNoOp(t *testing.T) {
	initPrometheusMetrics()
	prometheusP2PConnectedPeers.Set(7)

	s := &Server{}
	require.NotPanics(t, func() { s.updateConnectedPeersGauge(context.Background()) })

	require.InDelta(t, 7, testutil.ToFloat64(prometheusP2PConnectedPeers), 0.0001, "nil peerRegistry must leave the gauge untouched")
}

// TestUpdateConnectedPeersGauge_ListPeersErrorLeavesGaugePreviousValue confirms
// a ListPeers failure leaves the gauge at its last-known value instead of
// publishing a wrong reading (e.g. resetting to zero).
func TestUpdateConnectedPeersGauge_ListPeersErrorLeavesGaugePreviousValue(t *testing.T) {
	initPrometheusMetrics()
	prometheusP2PConnectedPeers.Set(5)

	s := &Server{
		peerRegistry: &failingListPeersRegistry{},
		logger:       ulogger.TestLogger{},
	}

	s.updateConnectedPeersGauge(context.Background())
	require.InDelta(t, 5, testutil.ToFloat64(prometheusP2PConnectedPeers), 0.0001, "a ListPeers error must leave the gauge untouched")
}

// TestStartConnectedPeersMonitor_UpdatesGaugeOnItsOwnTicker guards against the
// connected-peers gauge going stale between (or entirely if it stops)
// unrelated diagnostic-logging ticks: this test starts ONLY the dedicated
// monitor (no NAT-logging goroutine at all) and confirms the gauge still
// tracks the live directly-connected peer count on its own ticker.
func TestStartConnectedPeersMonitor_UpdatesGaugeOnItsOwnTicker(t *testing.T) {
	initPrometheusMetrics()

	s, reg, pid := freshTestServer(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String(), IsConnected: true})
	reg.Register(&blockchain.PeerInfo{ID: "connected-2", IsConnected: true})

	ctx, cancel := context.WithCancel(context.Background())
	done := s.startConnectedPeersMonitor(ctx, 5*time.Millisecond)

	require.Eventually(t, func() bool {
		return testutil.ToFloat64(prometheusP2PConnectedPeers) == 2
	}, time.Second, 5*time.Millisecond, "gauge should reflect the live directly-connected peer count from the dedicated monitor")

	cancel()
	<-done // wait for the monitor goroutine to exit before mutating the registry

	reg.Register(&blockchain.PeerInfo{ID: "connected-3", IsConnected: true})
	reg.Register(&blockchain.PeerInfo{ID: "connected-4", IsConnected: true})
	ctx2, cancel2 := context.WithCancel(context.Background())
	done2 := s.startConnectedPeersMonitor(ctx2, 5*time.Millisecond)

	require.Eventually(t, func() bool {
		return testutil.ToFloat64(prometheusP2PConnectedPeers) == 4
	}, time.Second, 5*time.Millisecond, "gauge should update to the new peer count from a fresh monitor")

	cancel2()
	<-done2
}
