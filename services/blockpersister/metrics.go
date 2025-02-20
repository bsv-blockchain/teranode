// Package blockpersister provides functionality for persisting blockchain blocks and their associated data.
package blockpersister

import (
	"sync"

	"github.com/bitcoin-sv/teranode/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Prometheus metrics variables for monitoring block persister operations
var (
	// prometheusBlockPersisterValidateSubtree tracks subtree validation duration
	prometheusBlockPersisterValidateSubtree prometheus.Histogram

	// prometheusBlockPersisterValidateSubtreeRetry counts subtree validation retries
	prometheusBlockPersisterValidateSubtreeRetry prometheus.Counter

	// prometheusBlockPersisterValidateSubtreeHandler tracks subtree handler duration
	prometheusBlockPersisterValidateSubtreeHandler prometheus.Histogram

	// prometheusBlockPersisterPersistBlock tracks block persistence duration
	prometheusBlockPersisterPersistBlock prometheus.Histogram

	// prometheusBlockPersisterBlessMissingTransaction tracks missing transaction blessing duration
	prometheusBlockPersisterBlessMissingTransaction prometheus.Histogram

	// prometheusBlockPersisterSetTXMetaCacheKafka tracks Kafka tx meta cache set duration
	prometheusBlockPersisterSetTXMetaCacheKafka prometheus.Histogram

	// prometheusBlockPersisterDelTXMetaCacheKafka tracks Kafka tx meta cache delete duration
	prometheusBlockPersisterDelTXMetaCacheKafka prometheus.Histogram

	// prometheusBlockPersisterSetTXMetaCacheKafkaErrors counts Kafka tx meta cache set errors
	prometheusBlockPersisterSetTXMetaCacheKafkaErrors prometheus.Counter

	// prometheusBlockPersisterBlocks tracks block processing duration
	prometheusBlockPersisterBlocks prometheus.Histogram

	// prometheusBlockPersisterSubtrees tracks subtree processing duration
	prometheusBlockPersisterSubtrees prometheus.Histogram

	// prometheusBlockPersisterSubtreeBatch tracks subtree batch processing duration
	prometheusBlockPersisterSubtreeBatch prometheus.Histogram
)

var (
	prometheusMetricsInitOnce sync.Once
)

// initPrometheusMetrics initializes all Prometheus metrics once
func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

// _initPrometheusMetrics is the internal implementation of metrics initialization
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
