package validator

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

var prometheusMetricsInitialised = false

func initPrometheusMetrics() {
	if prometheusMetricsInitialised {
		return
	}

	prometheusHealth = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "validator",
			Name:      "health",
			Help:      "Number of calls to the health endpoint",
		},
	)
	prometheusProcessedTransactions = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "validator",
			Name:      "processed_transactions",
			Help:      "Number of transactions processed by the validator service",
		},
	)
	prometheusInvalidTransactions = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "validator",
			Name:      "invalid_transactions",
			Help:      "Number of transactions found invalid by the validator service",
		},
	)
	prometheusTransactionDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "validator",
			Name:      "transactions_duration",
			Help:      "Duration of transaction processing by the validator service",
		},
	)
	prometheusTransactionSize = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "validator",
			Name:      "transactions_size",
			Help:      "Size of transactions processed by the validator service",
		},
	)

	prometheusMetricsInitialised = true
}
