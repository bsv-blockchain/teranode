package blockpersister

import (
	"sync"

	"github.com/bitcoin-sv/ubsv/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

//nolint:unused //TODO: enable these later
var (
	prometheusBlockPersisterValidateSubtree                 prometheus.Counter
	prometheusBlockPersisterValidateSubtreeRetry            prometheus.Counter
	prometheusBlockPersisterValidateSubtreeHandler          prometheus.Histogram
	prometheusBlockPersisterValidateSubtreeDuration         prometheus.Histogram
	prometheusBlockPersisterBlessMissingTransaction         prometheus.Counter
	prometheusBlockPersisterBlessMissingTransactionDuration prometheus.Histogram
	prometheusBlockPersisterSetTXMetaCacheKafka             prometheus.Histogram
	prometheusBlockPersisterDelTXMetaCacheKafka             prometheus.Histogram
	prometheusBlockPersisterSetTXMetaCacheKafkaErrors       prometheus.Counter
	prometheusBlockPersisterBlocks                          prometheus.Histogram
	prometheusBlockPersisterSubtrees                        prometheus.Histogram
	prometheusBlockPersisterSubtreeBatch                    prometheus.Histogram
)

var (
	prometheusMetricsInitOnce sync.Once
)

func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

func _initPrometheusMetrics() {
	prometheusBlockPersisterValidateSubtree = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockpersister",
			Name:      "validate_subtree",
			Help:      "Number of subtrees validated",
		},
	)

	prometheusBlockPersisterValidateSubtreeRetry = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockpersister",
			Name:      "validate_subtree_retry",
			Help:      "Number of retries when subtrees validated",
		},
	)

	prometheusBlockPersisterValidateSubtreeHandler = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockpersister",
			Name:      "validate_subtree_handler_millis",
			Help:      "Duration of subtree handler",
			Buckets:   util.MetricsBucketsMilliLongSeconds,
		},
	)

	prometheusBlockPersisterValidateSubtreeDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockpersister",
			Name:      "validate_subtree_duration_millis",
			Help:      "Duration of validate subtree",
			Buckets:   util.MetricsBucketsMilliLongSeconds,
		},
	)

	prometheusBlockPersisterBlessMissingTransaction = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockpersister",
			Name:      "bless_missing_transaction",
			Help:      "Number of missing transactions blessed",
		},
	)

	prometheusBlockPersisterBlessMissingTransactionDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockpersister",
			Name:      "bless_missing_transaction_duration_millis",
			Help:      "Duration of bless missing transaction",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockPersisterSetTXMetaCacheKafka = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockpersister",
			Name:      "set_tx_meta_cache_kafka_micros",
			Help:      "Duration of setting tx meta cache from kafka",
			Buckets:   util.MetricsBucketsMicroSeconds,
		},
	)

	prometheusBlockPersisterDelTXMetaCacheKafka = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockpersister",
			Name:      "del_tx_meta_cache_kafka_micros",
			Help:      "Duration of deleting tx meta cache from kafka",
			Buckets:   util.MetricsBucketsMicroSeconds,
		},
	)

	prometheusBlockPersisterSetTXMetaCacheKafkaErrors = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockpersister",
			Name:      "set_tx_meta_cache_kafka_errors",
			Help:      "Number of errors setting tx meta cache from kafka",
		},
	)

	prometheusBlockPersisterBlocks = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockpersister",
			Name:      "blocks_duration_millis",
			Help:      "Duration of block processing by the block persister service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockPersisterSubtrees = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockpersister",
			Name:      "subtrees_duration_millis",
			Help:      "Duration of subtree processing by the block persister service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockPersisterSubtreeBatch = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockpersister",
			Name:      "subtree_batch_duration_millis",
			Help:      "Duration of a subtree batch processing by the block persister service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
}
