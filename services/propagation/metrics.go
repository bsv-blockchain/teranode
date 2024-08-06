package propagation

import (
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"sync"
)

var (
	prometheusHealth                    prometheus.Histogram
	prometheusProcessedTransactions     prometheus.Histogram
	prometheusProcessedTransactionBatch prometheus.Histogram
	prometheusTransactionSize           prometheus.Histogram
	prometheusInvalidTransactions       prometheus.Counter
)

var (
	prometheusMetricsInitOnce sync.Once
)

func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

func _initPrometheusMetrics() {
	prometheusHealth = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "propagation",
			Name:      "health",
			Help:      "Histogram of calls to the health endpoint of the propagation service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusProcessedTransactions = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "propagation",
			Name:      "transactions",
			Help:      "Histogram of transaction processing by the propagation service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusProcessedTransactionBatch = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "propagation",
			Name:      "transactions_batch",
			Help:      "Histogram of transaction processing by the propagation service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusTransactionSize = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "propagation",
			Name:      "transactions_size",
			Help:      "Size of transactions processed by the propagation service",
			Buckets:   util.MetricsBucketsSize,
		},
	)
	prometheusInvalidTransactions = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "propagation",
			Name:      "invalid_transactions",
			Help:      "Number of transactions found invalid by the propagation service",
		},
	)
}
