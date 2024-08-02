package bootstrap

import (
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"sync"
)

var (
	prometheusHealth                prometheus.Counter
	prometheusConnect               prometheus.Counter
	prometheusGetNodes              prometheus.Histogram
	prometheusBroadcastNotification prometheus.Histogram
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

	prometheusGetNodes = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "bootstrap",
			Name:      "get_nodes",
			Help:      "Histogram of calls to the GetNodes endpoint",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBroadcastNotification = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "bootstrap",
			Name:      "broadcast_notification",
			Help:      "Histogram of calls to the BroadcastNotification endpoint",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
}
