package propagation

import (
	"sync"

	"github.com/bitcoin-sv/teranode/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prometheusHealth                    prometheus.Histogram
	prometheusProcessedTransactions     prometheus.Histogram
	prometheusProcessedTransactionBatch prometheus.Histogram
	prometheusProcessedHandleSingleTx   prometheus.Histogram
	prometheusProcessedHandleMultipleTx prometheus.Histogram
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
			Namespace: "teranode",
			Subsystem: "propagation",
			Name:      "health",
			Help:      "Histogram of calls to the health endpoint of the propagation service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusProcessedTransactions = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "propagation",
			Name:      "transactions",
			Help:      "Histogram of transaction processing by the propagation service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusProcessedTransactionBatch = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "propagation",
			Name:      "transactions_batch",
			Help:      "Histogram of transaction processing by the propagation service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusProcessedHandleSingleTx = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "propagation",
			Name:      "handle_single_tx",
			Help:      "Histogram of transaction processing by the propagation service using HTTP",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusProcessedHandleMultipleTx = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "propagation",
			Name:      "handle_multiple_tx",
			Help:      "Histogram of multiple transaction processing by the propagation service using HTTP",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusTransactionSize = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "propagation",
			Name:      "transactions_size",
			Help:      "Size of transactions processed by the propagation service",
			Buckets:   util.MetricsBucketsSize,
		},
	)
	prometheusInvalidTransactions = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "propagation",
			Name:      "invalid_transactions",
			Help:      "Number of transactions found invalid by the propagation service",
		},
	)
}
