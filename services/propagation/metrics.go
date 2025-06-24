// Package propagation implements Bitcoin SV transaction propagation and validation services.
// This file contains the Prometheus metrics definitions and initialization for comprehensive
// monitoring of the propagation service's performance, throughput, and operational health.
//
// The metrics system provides detailed observability into:
//   - Transaction processing rates and latencies
//   - Batch processing performance and efficiency
//   - Error rates and failure patterns across different operations
//   - Health check response times and availability
//   - Resource utilization and system performance indicators
//
// Metrics Collection:
// All metrics are automatically registered with Prometheus and collected at regular intervals.
// The metrics follow standard Prometheus naming conventions and include appropriate labels
// for dimensional analysis and filtering.
//
// Key Metric Categories:
//   - Histograms: For measuring latencies and processing times
//   - Counters: For tracking event occurrences and error rates
//   - Gauges: For measuring current resource levels and states
//
// Integration:
// These metrics integrate with the broader Teranode monitoring infrastructure and can be
// scraped by Prometheus servers for alerting, dashboards, and long-term trend analysis.
package propagation

import (
	"sync"

	"github.com/bitcoin-sv/teranode/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Prometheus metrics for monitoring propagation service performance and health.
// These metrics provide comprehensive observability into transaction processing,
// system performance, and error conditions across all service operations.
var (
	prometheusHealth                    prometheus.Histogram
	prometheusProcessedTransactions     prometheus.Histogram
	prometheusProcessedTransactionBatch prometheus.Histogram
	prometheusProcessedHandleSingleTx   prometheus.Histogram
	prometheusProcessedHandleMultipleTx prometheus.Histogram
	prometheusTransactionSize           prometheus.Histogram
	prometheusInvalidTransactions       prometheus.Counter
)

// Synchronization primitive for ensuring metrics are initialized exactly once.
var (
	prometheusMetricsInitOnce sync.Once
)

// initPrometheusMetrics initializes all Prometheus metrics for the propagation service.
// This function uses sync.Once to ensure metrics are initialized exactly once,
// preventing duplicate metric registration errors. It serves as the public
// entry point for metrics initialization, delegating to the internal implementation.
func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

// _initPrometheusMetrics is the internal implementation that registers all Prometheus metrics.
// This function defines and registers the following metrics:
// - Health endpoint latency histogram
// - Transaction processing latency histograms (single, batch, HTTP single, HTTP multiple)
// - Transaction size histogram for monitoring data volume
// - Invalid transaction counter for monitoring error rates
//
// Each metric is properly namespaced under 'teranode' and the 'propagation' subsystem
// with appropriate bucket definitions based on the expected value distributions.
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
