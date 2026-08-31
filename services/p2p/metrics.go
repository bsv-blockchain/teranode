package p2p

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prometheusP2PPublishBlocked                *prometheus.CounterVec
	prometheusP2PWebsocketNotificationsDropped *prometheus.CounterVec
	prometheusP2PWebsocketClientsEvicted       prometheus.Counter

	prometheusMetricsInitOnce sync.Once
)

// initPrometheusMetrics initializes the p2p Prometheus metrics.
// It uses sync.Once so it is safe to call from multiple entry points.
func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

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

	prometheusP2PWebsocketClientsEvicted = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "p2p",
			Name:      "websocket_clients_evicted_total",
			Help:      "Number of websocket clients evicted from the broadcast fan-out because their send buffer was full when a broadcast reached them",
		},
	)
}

// notificationDropped records a websocket notification that was dropped
// because the shared notification channel was full.
func notificationDropped(notificationType string) {
	initPrometheusMetrics()
	prometheusP2PWebsocketNotificationsDropped.WithLabelValues(notificationType).Inc()
}
