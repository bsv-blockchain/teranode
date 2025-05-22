// Package blockpersister provides comprehensive functionality for persisting blockchain blocks and their associated data.
// It implements metrics collection for monitoring performance, throughput, and operational health of the block persistence process.
package blockpersister

import (
	"sync"

	"github.com/bitcoin-sv/teranode/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Prometheus metrics variables for monitoring block persister operations.
// These metrics track performance, throughput, and error conditions across all aspects of the
// block persistence process, enabling operational monitoring, alerting, and performance optimization.
var (
	// prometheusBlockPersisterValidateSubtree tracks the duration of individual subtree validation operations
	// in milliseconds. This helps identify performance bottlenecks in the validation pipeline.
	prometheusBlockPersisterValidateSubtree prometheus.Histogram

	// prometheusBlockPersisterValidateSubtreeRetry counts the number of times subtree validation
	// needed to be retried, indicating transient failures or race conditions that required resolution.
	prometheusBlockPersisterValidateSubtreeRetry prometheus.Counter

	// prometheusBlockPersisterValidateSubtreeHandler tracks the total duration of the subtree
	// handler including validation and peripheral operations, measured in milliseconds.
	prometheusBlockPersisterValidateSubtreeHandler prometheus.Histogram

	// prometheusBlockPersisterPersistBlock measures the total time required to persist a complete
	// block including all its subtrees and UTXO changes, in milliseconds. This is a key performance
	// indicator for overall system throughput.
	prometheusBlockPersisterPersistBlock prometheus.Histogram

	// prometheusBlockPersisterBlessMissingTransaction tracks the duration of operations that
	// resolve or "bless" transactions that were initially missing from the primary data store.
	prometheusBlockPersisterBlessMissingTransaction prometheus.Histogram

	// prometheusBlockPersisterSetTXMetaCacheKafka measures the time taken to set transaction
	// metadata in the Kafka-based caching layer, in microseconds.
	prometheusBlockPersisterSetTXMetaCacheKafka prometheus.Histogram

	// prometheusBlockPersisterDelTXMetaCacheKafka tracks the duration of operations that
	// delete transaction metadata from the Kafka-based cache, in microseconds.
	prometheusBlockPersisterDelTXMetaCacheKafka prometheus.Histogram

	// prometheusBlockPersisterSetTXMetaCacheKafkaErrors counts errors encountered when
	// attempting to set transaction metadata in the Kafka cache, helping identify integration issues.
	prometheusBlockPersisterSetTXMetaCacheKafkaErrors prometheus.Counter

	// prometheusBlockPersisterBlocks measures the complete processing time for blocks
	// in the block persister service, from receipt to full persistence, in milliseconds.
	prometheusBlockPersisterBlocks prometheus.Histogram

	// prometheusBlockPersisterSubtrees tracks the duration of processing complete subtrees
	// in the block persister service, in milliseconds.
	prometheusBlockPersisterSubtrees prometheus.Histogram

	// prometheusBlockPersisterSubtreeBatch measures the time taken to process a batch of subtrees
	// in the block persister service, in milliseconds, helping optimize batch size configurations.
	prometheusBlockPersisterSubtreeBatch prometheus.Histogram
)

var (
	// prometheusMetricsInitOnce ensures that metrics initialization occurs exactly once,
	// regardless of how many times initPrometheusMetrics() is called, preventing duplicate
	// metric registration which would cause Prometheus to panic.
	prometheusMetricsInitOnce sync.Once
)

// initPrometheusMetrics initializes all Prometheus metrics once.
// This function is called during server initialization to ensure metrics are registered
// with Prometheus before they are used. It uses sync.Once to guarantee thread-safe
// initialization exactly one time, preventing registration conflicts.
func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

// _initPrometheusMetrics is the internal implementation of metrics initialization.
// It creates and registers all the Prometheus metrics with appropriate namespaces,
// help text, and bucket configurations optimized for the expected value distributions.
func _initPrometheusMetrics() {
	prometheusBlockPersisterValidateSubtree = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockpersister",
			Name:      "validate_subtree",
			Help:      "Histogram of subtree validation",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockPersisterValidateSubtreeRetry = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "blockpersister",
			Name:      "validate_subtree_retry",
			Help:      "Number of retries when subtrees validated",
		},
	)

	prometheusBlockPersisterValidateSubtreeHandler = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockpersister",
			Name:      "validate_subtree_handler",
			Help:      "Histogram of subtree handler",
			Buckets:   util.MetricsBucketsMilliLongSeconds,
		},
	)

	prometheusBlockPersisterPersistBlock = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockpersister",
			Name:      "persist_block",
			Help:      "Histogram of PersistBlock in the blockpersister service",
			Buckets:   util.MetricsBucketsMilliLongSeconds,
		},
	)

	prometheusBlockPersisterBlessMissingTransaction = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockpersister",
			Name:      "bless_missing_transaction",
			Help:      "Histogram of bless missing transaction",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockPersisterSetTXMetaCacheKafka = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockpersister",
			Name:      "set_tx_meta_cache_kafka",
			Help:      "Histogram of setting tx meta cache from kafka",
			Buckets:   util.MetricsBucketsMicroSeconds,
		},
	)

	prometheusBlockPersisterDelTXMetaCacheKafka = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockpersister",
			Name:      "del_tx_meta_cache_kafka",
			Help:      "Duration of deleting tx meta cache from kafka",
			Buckets:   util.MetricsBucketsMicroSeconds,
		},
	)

	prometheusBlockPersisterSetTXMetaCacheKafkaErrors = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "blockpersister",
			Name:      "set_tx_meta_cache_kafka_errors",
			Help:      "Number of errors setting tx meta cache from kafka",
		},
	)

	prometheusBlockPersisterBlocks = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockpersister",
			Name:      "blocks_duration",
			Help:      "Duration of block processing by the block persister service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockPersisterSubtrees = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockpersister",
			Name:      "subtrees_duration",
			Help:      "Duration of subtree processing by the block persister service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockPersisterSubtreeBatch = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockpersister",
			Name:      "subtree_batch_duration",
			Help:      "Duration of a subtree batch processing by the block persister service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
}
