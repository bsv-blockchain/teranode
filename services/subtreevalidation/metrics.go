package subtreevalidation

import (
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"sync"
)

//nolint:unused //TODO: enable these later
var (
	prometheusSubtreeValidationValidateSubtree                 prometheus.Counter
	prometheusSubtreeValidationValidateSubtreeRetry            prometheus.Counter
	prometheusSubtreeValidationValidateSubtreeHandler          prometheus.Histogram
	prometheusSubtreeValidationValidateSubtreeDuration         prometheus.Histogram
	prometheusSubtreeValidationBlessMissingTransaction         prometheus.Counter
	prometheusSubtreeValidationBlessMissingTransactionDuration prometheus.Histogram
	prometheusSubtreeValidationSetTXMetaCacheKafka             prometheus.Histogram
	prometheusSubtreeValidationDelTXMetaCacheKafka             prometheus.Histogram
	prometheusSubtreeValidationSetTXMetaCacheKafkaErrors       prometheus.Counter
)

var (
	prometheusMetricsInitOnce sync.Once
)

func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

func _initPrometheusMetrics() {
	prometheusSubtreeValidationValidateSubtree = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "subtreevalidation",
			Name:      "validate_subtree",
			Help:      "Number of subtrees validated",
		},
	)

	prometheusSubtreeValidationValidateSubtreeRetry = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "subtreevalidation",
			Name:      "validate_subtree_retry",
			Help:      "Number of retries when subtrees validated",
		},
	)

	prometheusSubtreeValidationValidateSubtreeHandler = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "subtreevalidation",
			Name:      "validate_subtree_handler_millis",
			Help:      "Duration of subtree handler",
			Buckets:   util.MetricsBucketsMilliLongSeconds,
		},
	)

	prometheusSubtreeValidationValidateSubtreeDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "subtreevalidation",
			Name:      "validate_subtree_duration_millis",
			Help:      "Duration of validate subtree",
			Buckets:   util.MetricsBucketsMilliLongSeconds,
		},
	)

	prometheusSubtreeValidationBlessMissingTransaction = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "subtreevalidation",
			Name:      "bless_missing_transaction",
			Help:      "Number of missing transactions blessed",
		},
	)

	prometheusSubtreeValidationBlessMissingTransactionDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "subtreevalidation",
			Name:      "bless_missing_transaction_duration_millis",
			Help:      "Duration of bless missing transaction",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusSubtreeValidationSetTXMetaCacheKafka = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "subtreevalidation",
			Name:      "set_tx_meta_cache_kafka_micros",
			Help:      "Duration of setting tx meta cache from kafka",
			Buckets:   util.MetricsBucketsMicroSeconds,
		},
	)

	prometheusSubtreeValidationDelTXMetaCacheKafka = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "subtreevalidation",
			Name:      "del_tx_meta_cache_kafka_micros",
			Help:      "Duration of deleting tx meta cache from kafka",
			Buckets:   util.MetricsBucketsMicroSeconds,
		},
	)

	prometheusSubtreeValidationSetTXMetaCacheKafkaErrors = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "subtreevalidation",
			Name:      "set_tx_meta_cache_kafka_errors",
			Help:      "Number of errors setting tx meta cache from kafka",
		},
	)
}
