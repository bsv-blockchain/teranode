package propagation

import (
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"sync"
)

var (
	prometheusHealth                prometheus.Counter
	prometheusProcessedTransactions prometheus.Counter
	prometheusInvalidTransactions   prometheus.Counter
	prometheusTransactionDuration   prometheus.Histogram
	prometheusTransactionSize       prometheus.Histogram
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
			Name:      "transactions_duration_millis",
			Help:      "Duration of transaction processing by the propagation service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusTransactionSize = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "propagation",
			Name:      "transactions_size_v2",
			Help:      "Size of transactions processed by the propagation service",
			Buckets:   util.MetricsBucketsSize,
		},
	)
}
