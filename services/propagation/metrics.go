package propagation

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prometheusHealth                prometheus.Counter
	prometheusProcessedTransactions prometheus.Counter
	prometheusInvalidTransactions   prometheus.Counter
	prometheusTransactionDuration   prometheus.Histogram
	prometheusTransactionSize       prometheus.Histogram
)

var prometheusMetricsInitialized = false

func initPrometheusMetrics() {
	if prometheusMetricsInitialized {
		return
	}

	prometheusHealth = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "propagation",
			Name:      "health",
			Help:      "Number of calls to the health endpoint of the propagation service",
		},
	)
	prometheusProcessedTransactions = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "propagation",
			Name:      "processed_transactions",
			Help:      "Number of transactions processed by the propagation service",
		},
	)
	prometheusInvalidTransactions = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "propagation",
			Name:      "invalid_transactions",
			Help:      "Number of transactions found invalid by the propagation service",
		},
	)
	prometheusTransactionDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "propagation",
			Name:      "transactions_duration",
			Help:      "Duration of transaction processing by the propagation service",
		},
	)
	prometheusTransactionSize = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "propagation",
			Name:      "transactions_size",
			Help:      "Size of transactions processed by the propagation service",
		},
	)

	prometheusMetricsInitialized = true
}
