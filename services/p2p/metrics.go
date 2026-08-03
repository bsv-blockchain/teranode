package p2p

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prometheusP2PPublishBlocked *prometheus.CounterVec

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
			Name:      "publish_blocked",
			Help:      "Number of outbound P2P publishes dropped by the per-FSM-state allow-list, by topic and FSM state",
		},
		[]string{"topic", "fsm_state"},
	)
}
