// Package p2p provides peer-to-peer networking functionality for the Teranode system.
package p2p

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Prometheus metrics for the P2P service's HTTP/WebSocket surface.
var (
	prometheusP2PPublishBlocked *prometheus.CounterVec

	// prometheusP2PWebSocketConnections tracks the current number of active
	// /p2p-ws websocket connections.
	prometheusP2PWebSocketConnections prometheus.Gauge

	// prometheusP2PHTTPRateLimited counts P2P HTTP requests rejected by the
	// per-IP rate limiter.
	prometheusP2PHTTPRateLimited prometheus.Counter

	// prometheusMetricsInitOnce ensures metrics are initialized exactly once.
	prometheusMetricsInitOnce sync.Once
)

// initPrometheusMetrics safely initializes P2P Prometheus metrics using
// sync.Once to ensure thread-safe single initialization.
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

	prometheusP2PWebSocketConnections = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "p2p",
			Name:      "websocket_connections",
			Help:      "Current number of active /p2p-ws websocket connections",
		},
	)

	prometheusP2PHTTPRateLimited = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "p2p",
			Name:      "http_rate_limited",
			Help:      "Number of P2P HTTP requests rejected by the rate limiter",
		},
	)
}
