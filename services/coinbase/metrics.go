package coinbase

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"sync"
)

var (
	prometheusHealth                prometheus.Counter
	prometheusRequestFunds          prometheus.Counter
	prometheusDistributeTransaction prometheus.Counter
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
			Namespace: "coinbase",
			Name:      "health",
			Help:      "Number of calls to the Health endpoint",
		},
	)

	prometheusRequestFunds = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "coinbase",
			Name:      "request_funds",
			Help:      "Number of calls to the RequestFunds endpoint",
		},
	)

	prometheusDistributeTransaction = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "coinbase",
			Name:      "distribute_transaction",
			Help:      "Number of calls to the DistributeTransaction endpoint",
		},
	)
}
