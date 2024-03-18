package bootstrap

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"sync"
)

var (
	prometheusHealth                prometheus.Counter
	prometheusConnect               prometheus.Counter
	prometheusGetNodes              prometheus.Counter
	prometheusBroadcastNotification prometheus.Counter
)

var (
	prometheusMetricsInitOnce sync.Once
)

func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

func _initPrometheusMetrics() {
	prometheusHealth = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "bootstrap",
			Name:      "health",
			Help:      "Number of calls to the Health endpoint",
		},
	)

	prometheusConnect = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "bootstrap",
			Name:      "connect",
			Help:      "Number of calls to the Connect endpoint",
		},
	)

	prometheusGetNodes = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "bootstrap",
			Name:      "get_nodes",
			Help:      "Number of calls to the GetNodes endpoint",
		},
	)

	prometheusBroadcastNotification = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "bootstrap",
			Name:      "broadcast_notification",
			Help:      "Number of calls to the BroadcastNotification endpoint",
		},
	)
}
