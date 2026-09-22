// Package p2p provides peer-to-peer networking functionality for the Teranode system.
// This file contains the Prometheus metrics definitions and initialization for the
// p2p service, following the same registration pattern used by the other pipeline
// services (see services/propagation/metrics.go and services/blockassembly/metrics.go).
package p2p

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Prometheus metrics for monitoring the p2p service's peer registry, ban list,
// catchup coordination, and websocket subscriber activity.
var (
	prometheusP2PPublishBlocked                *prometheus.CounterVec
	prometheusP2PWebsocketNotificationsDropped *prometheus.CounterVec
	prometheusP2PWebsocketClientsEvicted       prometheus.Counter
	prometheusP2PGossipKafkaPublishDropped     *prometheus.CounterVec

	// prometheusP2PConnectedPeers tracks the number of peers currently connected
	// to this node over the libp2p network.
	prometheusP2PConnectedPeers prometheus.Gauge

	// prometheusP2PBanEvents counts peer ban events, labelled by ban reason.
	prometheusP2PBanEvents *prometheus.CounterVec

	// prometheusP2PCatchupAttempts counts catchup attempts made against peers.
	prometheusP2PCatchupAttempts prometheus.Counter

	// prometheusP2PCatchupSuccesses counts successful catchups completed from peers.
	prometheusP2PCatchupSuccesses prometheus.Counter

	// prometheusP2PCatchupFailures counts failed catchup attempts against peers,
	// labelled by normalized failure kind (e.g. "generic", "block_incomplete").
	prometheusP2PCatchupFailures *prometheus.CounterVec

	// prometheusP2PWebsocketConnections tracks the number of currently connected
	// websocket notification subscribers.
	prometheusP2PWebsocketConnections prometheus.Gauge
)

var prometheusMetricsInitOnce sync.Once

// initPrometheusMetrics initializes all Prometheus metrics for the p2p service.
// This function uses sync.Once to ensure metrics are initialized exactly once,
// preventing duplicate metric registration errors.
func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

// _initPrometheusMetrics is the internal implementation that registers all
// Prometheus metrics used by the p2p service. Each metric is namespaced under
// 'teranode' and the 'p2p' subsystem.
func _initPrometheusMetrics() {
	prometheusP2PPublishBlocked = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "p2p",
			Name:      "publish_blocked_total",
			Help:      "Number of outbound P2P messages suppressed by the per-FSM-state allow-list, by topic, FSM state, and stage (precheck = expected skip before any work, chokepoint = a publish that leaked past the pre-checks)",
		},
		[]string{"topic", "fsm_state", "stage"},
	)

	prometheusP2PWebsocketNotificationsDropped = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "p2p",
			Name:      "websocket_notifications_dropped_total",
			Help:      "Number of websocket notifications dropped because the notification channel was full, by notification type",
		},
		[]string{"type"},
	)

	prometheusP2PGossipKafkaPublishDropped = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "p2p",
			Name:      "gossip_kafka_publish_dropped_total",
			Help:      "Number of gossip announcements dropped because the Kafka producer's publish channel was full, by topic",
		},
		[]string{"topic"},
	)

	prometheusP2PWebsocketClientsEvicted = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "p2p",
			Name:      "websocket_clients_evicted_total",
			Help:      "Number of websocket clients evicted from the broadcast fan-out because their send buffer was full when a broadcast reached them",
		},
	)

	prometheusP2PConnectedPeers = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "p2p",
			Name:      "connected_peers",
			Help:      "Number of peers currently connected to this node over the p2p network",
		},
	)

	prometheusP2PBanEvents = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "p2p",
			Name:      "ban_events_total",
			Help:      "Total number of peer ban events, labelled by ban reason",
		},
		[]string{"reason"},
	)

	prometheusP2PCatchupAttempts = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "p2p",
			Name:      "catchup_attempts_total",
			Help:      "Total number of catchup attempts made against peers",
		},
	)

	prometheusP2PCatchupSuccesses = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "p2p",
			Name:      "catchup_successes_total",
			Help:      "Total number of successful catchups completed from peers",
		},
	)

	prometheusP2PCatchupFailures = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "p2p",
			Name:      "catchup_failures_total",
			Help:      "Total number of failed catchup attempts against peers, labelled by failure kind",
		},
		[]string{"kind"},
	)

	prometheusP2PWebsocketConnections = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "p2p",
			Name:      "websocket_connections",
			Help:      "Number of currently connected websocket notification subscribers",
		},
	)
}

// notificationDropped records a websocket notification that was dropped
// because the shared notification channel was full.
func notificationDropped(notificationType string) {
	initPrometheusMetrics()
	prometheusP2PWebsocketNotificationsDropped.WithLabelValues(notificationType).Inc()
}

// gossipPublishDropped records a gossip announcement dropped by TryPublish
// under producer backpressure. A counter rather than a warn log: a backlogged
// broker is exactly the state a replay flood produces, and one log line per
// dropped announcement would bury the log when it most needs to stay readable.
func gossipPublishDropped(topic string) {
	initPrometheusMetrics()
	prometheusP2PGossipKafkaPublishDropped.WithLabelValues(topic).Inc()
}
