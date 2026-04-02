// Package blockvalidation implements block validation for BSV Blockchain nodes in Teranode.
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

	"github.com/bsv-blockchain/teranode/util"
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

	// priority queue metrics
	prometheusBlockPriorityQueueSize      *prometheus.GaugeVec
	prometheusBlockPriorityQueueAdded     *prometheus.CounterVec
	prometheusBlockPriorityQueueProcessed *prometheus.CounterVec

	// fork processing metrics
	prometheusForkCount             prometheus.Gauge
	prometheusForkProcessingWorkers prometheus.Gauge
	prometheusForkBlocksProcessed   *prometheus.CounterVec

	// enhanced fork lifecycle metrics
	prometheusForkCreated           *prometheus.CounterVec
	prometheusForkLifetime          prometheus.Histogram
	prometheusForkDepth             prometheus.Histogram
	prometheusForkResolved          *prometheus.CounterVec
	prometheusForkResolutionDepth   prometheus.Histogram
	prometheusOrphanedForks         prometheus.Counter
	prometheusProcessingBlocksStuck prometheus.Gauge
	prometheusForkAverageDepth      prometheus.Gauge
	prometheusForkLongestDepth      prometheus.Gauge
	prometheusAverageForkLifetime   prometheus.Gauge

	blockQueueSkipCount prometheus.Histogram
	blockQueueWaitTime  prometheus.Histogram
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

	// Initialize priority queue metrics
	prometheusBlockPriorityQueueSize = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "blockvalidation",
			Name:      "priority_queue_size",
			Help:      "Current size of the block priority queue by priority level",
		},
		[]string{"priority"},
	)

	prometheusBlockPriorityQueueAdded = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "blockvalidation",
			Name:      "priority_queue_added_total",
			Help:      "Total number of blocks added to priority queue by priority level",
		},
		[]string{"priority"},
	)

	prometheusBlockPriorityQueueProcessed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "blockvalidation",
			Name:      "priority_queue_processed_total",
			Help:      "Total number of blocks processed from priority queue by priority level",
		},
		[]string{"priority", "result"},
	)

	// Initialize fork processing metrics
	prometheusForkCount = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "blockvalidation",
			Name:      "fork_count",
			Help:      "Current number of active forks being tracked",
		},
	)

	prometheusForkProcessingWorkers = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "blockvalidation",
			Name:      "fork_processing_workers",
			Help:      "Current number of active fork processing workers",
		},
	)

	prometheusForkBlocksProcessed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "blockvalidation",
			Name:      "fork_blocks_processed_total",
			Help:      "Total number of blocks processed on forks by fork ID",
		},
		[]string{"fork_id", "result"},
	)

	prometheusForkCreated = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "blockvalidation",
			Name:      "fork_created_total",
			Help:      "Total number of forks created",
		},
		[]string{"reason"},
	)

	prometheusForkLifetime = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockvalidation",
			Name:      "fork_lifetime_seconds",
			Help:      "Lifetime of forks in seconds",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 15),
		},
	)

	prometheusForkDepth = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockvalidation",
			Name:      "fork_depth_blocks",
			Help:      "Depth of forks in blocks",
			Buckets:   prometheus.LinearBuckets(1, 1, 20),
		},
	)

	prometheusForkResolved = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "blockvalidation",
			Name:      "fork_resolved_total",
			Help:      "Total number of forks resolved",
		},
		[]string{"result"},
	)

	prometheusForkResolutionDepth = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockvalidation",
			Name:      "fork_resolution_depth_blocks",
			Help:      "Depth of forks when resolved",
			Buckets:   prometheus.LinearBuckets(1, 1, 20),
		},
	)

	prometheusOrphanedForks = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "blockvalidation",
			Name:      "fork_orphaned_total",
			Help:      "Total number of forks marked as orphaned",
		},
	)

	prometheusProcessingBlocksStuck = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "blockvalidation",
			Name:      "processing_blocks_stuck",
			Help:      "Number of blocks stuck in processing state",
		},
	)

	prometheusForkAverageDepth = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "blockvalidation",
			Name:      "fork_average_depth",
			Help:      "Average depth of active forks in blocks",
		},
	)

	prometheusForkLongestDepth = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "blockvalidation",
			Name:      "fork_longest_depth",
			Help:      "Longest active fork depth in blocks",
		},
	)

	prometheusAverageForkLifetime = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "blockvalidation",
			Name:      "fork_average_lifetime_seconds",
			Help:      "Average lifetime of active forks in seconds",
		},
	)

	blockQueueSkipCount = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockvalidation",
			Name:      "queue_skip_count",
			Help:      "Number of times blocks were skipped before processing",
			Buckets:   []float64{1, 5, 10, 20, 50},
		},
	)

	blockQueueWaitTime = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockvalidation",
			Name:      "queue_wait_seconds",
			Help:      "Time blocks spend in queue before processing",
			Buckets:   prometheus.DefBuckets,
		},
	)
}
