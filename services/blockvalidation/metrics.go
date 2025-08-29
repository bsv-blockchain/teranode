// Package blockvalidation implements block validation for Bitcoin SV nodes in Teranode.
//
// This package provides the core functionality for validating Bitcoin blocks, managing block subtrees,
// and processing transaction metadata. It is designed for high-performance operation at scale,
// supporting features like:
//
// - Concurrent block validation with optimistic mining support
// - Subtree-based block organization and validation
// - Transaction metadata caching and management
// - Automatic chain catchup when falling behind
// - Integration with Kafka for distributed operation
//
// The package exposes gRPC interfaces for block validation operations,
// making it suitable for use in distributed Teranode deployments.
package blockvalidation

import (
	"sync"

	"github.com/bitcoin-sv/teranode/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prometheusBlockValidationHealth            prometheus.Counter
	prometheusBlockValidationBlockFoundCh      prometheus.Gauge
	prometheusBlockValidationBlockFound        prometheus.Histogram
	prometheusBlockValidationCatchupCh         prometheus.Gauge
	prometheusBlockValidationCatchup           prometheus.Histogram
	prometheusBlockValidationProcessBlockFound prometheus.Histogram

	// block validation
	prometheusBlockValidationValidateBlock      prometheus.Histogram
	prometheusBlockValidationReValidateBlock    prometheus.Histogram
	prometheusBlockValidationReValidateBlockErr prometheus.Histogram

	// expiring cache metrics
	prometheusBlockValidationLastValidatedBlocksCache prometheus.Gauge
	prometheusBlockValidationBlockExistsCache         prometheus.Gauge
	prometheusBlockValidationSubtreeExistsCache       prometheus.Gauge

	// catchup operation metrics
	prometheusCatchupDuration       *prometheus.HistogramVec
	prometheusCatchupBlocksFetched  *prometheus.CounterVec
	prometheusCatchupHeadersFetched *prometheus.CounterVec
	prometheusCatchupErrors         *prometheus.CounterVec
	prometheusCatchupActive         prometheus.Gauge
)

var (
	prometheusMetricsInitOnce sync.Once
)

// initPrometheusMetrics initializes all the Prometheus metrics for the blockvalidation service.
// This function uses sync.Once to ensure metrics are only initialized once,
// regardless of how many times it's called, preventing duplicate metric registration errors.
func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

// _initPrometheusMetrics is the internal implementation that registers all Prometheus metrics
// used by the blockvalidation service. The metrics are organized into several categories:
//
// - Service health and availability metrics
// - Channel buffer monitoring metrics
// - Performance timing metrics for key operations
// - Cache statistics metrics
// - Transaction processing metrics
//
// These metrics provide comprehensive visibility into the performance, throughput,
// and resource utilization of the block validation service during operation.
func _initPrometheusMetrics() {
	prometheusBlockValidationHealth = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "blockvalidation",
			Name:      "health",
			Help:      "Number of health checks",
		},
	)

	prometheusBlockValidationBlockFoundCh = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "blockvalidation",
			Name:      "block_found_ch",
			Help:      "Number of blocks found buffered in the block found channel",
		},
	)

	prometheusBlockValidationBlockFound = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockvalidation",
			Name:      "block_found",
			Help:      "Histogram of calls to BlockFound method",
			Buckets:   util.MetricsBucketsSeconds,
		},
	)

	prometheusBlockValidationCatchupCh = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "blockvalidation",
			Name:      "catchup_ch",
			Help:      "Number of catchups buffered in the catchup channel",
		},
	)

	prometheusBlockValidationCatchup = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockvalidation",
			Name:      "catchup",
			Help:      "Histogram of catchup events",
			Buckets:   util.MetricsBucketsSeconds,
		},
	)

	prometheusBlockValidationProcessBlockFound = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockvalidation",
			Name:      "process_block_found",
			Help:      "Histogram of process block found",
			Buckets:   util.MetricsBucketsSeconds,
		},
	)

	prometheusBlockValidationValidateBlock = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockvalidation",
			Name:      "validate_block",
			Help:      "Histogram of calls to ValidateBlock method",
			Buckets:   util.MetricsBucketsSeconds,
		},
	)

	prometheusBlockValidationReValidateBlock = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockvalidation",
			Name:      "revalidate_block",
			Help:      "Histogram of re-validate block",
			Buckets:   util.MetricsBucketsSeconds,
		},
	)

	prometheusBlockValidationReValidateBlockErr = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockvalidation",
			Name:      "revalidate_block_err",
			Help:      "Number of blocks revalidated with error",
			Buckets:   util.MetricsBucketsSeconds,
		},
	)

	prometheusBlockValidationLastValidatedBlocksCache = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "blockvalidation",
			Name:      "last_validated_blocks_cache",
			Help:      "Number of blocks in the last validated blocks cache",
		},
	)

	prometheusBlockValidationBlockExistsCache = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "blockvalidation",
			Name:      "block_exists_cache",
			Help:      "Number of blocks in the block exists cache",
		},
	)

	prometheusBlockValidationSubtreeExistsCache = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "blockvalidation",
			Name:      "subtree_exists_cache",
			Help:      "Number of subtrees in the subtree exists cache",
		},
	)

	// Initialize catchup operation metrics
	prometheusCatchupDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockvalidation",
			Name:      "catchup_duration_seconds",
			Help:      "Duration of catchup operations in seconds",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
		[]string{"peer_id", "success"},
	)

	prometheusCatchupBlocksFetched = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "blockvalidation",
			Name:      "catchup_blocks_fetched_total",
			Help:      "Total number of blocks fetched during catchup",
		},
		[]string{"peer_id"},
	)

	prometheusCatchupHeadersFetched = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "blockvalidation",
			Name:      "catchup_headers_fetched_total",
			Help:      "Total number of headers fetched during catchup",
		},
		[]string{"peer_id"},
	)

	prometheusCatchupErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "blockvalidation",
			Name:      "catchup_errors_total",
			Help:      "Total number of errors during catchup operations",
		},
		[]string{"peer_id", "error_type"},
	)

	prometheusCatchupActive = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "blockvalidation",
			Name:      "catchup_active",
			Help:      "Number of active catchup operations (0 or 1)",
		},
	)
}
