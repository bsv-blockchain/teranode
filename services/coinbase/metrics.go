package coinbase

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prometheusHealth        prometheus.Counter
	prometheusGetUtxo       prometheus.Counter
	prometheusMarkUtxoSpent prometheus.Counter
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

	prometheusGetUtxo = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "coinbase",
			Name:      "get_utxo",
			Help:      "Number of calls to the GetUtxo endpoint",
		},
	)

	prometheusMarkUtxoSpent = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "coinbase",
			Name:      "mark_utxo_spent",
			Help:      "Number of calls to the MarkUtxoSpent endpoint",
		},
	)

	prometheusMetricsInitialized = true
}
