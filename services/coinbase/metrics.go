package coinbase

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prometheusHealth                prometheus.Counter
	prometheusRequestFunds          prometheus.Counter
	prometheusDistributeTransaction prometheus.Counter
)

var prometheusMetricsInitialized = false

func initPrometheusMetrics() {
	if prometheusMetricsInitialized {
		return
	}

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

	prometheusMetricsInitialized = true
}
