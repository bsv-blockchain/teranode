package subtreevalidation

import (
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"sync"
)

var (
	prometheusHealth                                     prometheus.Histogram
	prometheusSubtreeValidationCheckSubtree              prometheus.Histogram
	prometheusSubtreeValidationValidateSubtree           prometheus.Histogram
	prometheusSubtreeValidationValidateSubtreeRetry      prometheus.Counter
	prometheusSubtreeValidationValidateSubtreeHandler    prometheus.Histogram
	prometheusSubtreeValidationValidateSubtreeDuration   prometheus.Histogram
	prometheusSubtreeValidationBlessMissingTransaction   prometheus.Histogram
	prometheusSubtreeValidationSetTXMetaCacheKafka       prometheus.Histogram
	prometheusSubtreeValidationDelTXMetaCacheKafka       prometheus.Histogram
	prometheusSubtreeValidationSetTXMetaCacheKafkaErrors prometheus.Counter
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
			Namespace: "subtreevalidation",
			Name:      "health",
			Help:      "Histogram of calls to health endpoint",
			Buckets:   util.MetricsBucketsMilliLongSeconds,
		},
	)

	prometheusSubtreeValidationCheckSubtree = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "subtreevalidation",
			Name:      "check_subtree",
			Help:      "Duration of calls to checkSubtree endpoint",
			Buckets:   util.MetricsBucketsMilliLongSeconds,
		},
	)

	prometheusSubtreeValidationValidateSubtree = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "subtreevalidation",
			Name:      "validate_subtree",
			Help:      "Histogram of subtrees validated",
			Buckets:   util.MetricsBucketsMilliLongSeconds,
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
			Name:      "validate_subtree_handler",
			Help:      "Duration of subtree handler",
			Buckets:   util.MetricsBucketsMilliLongSeconds,
		},
	)

	prometheusSubtreeValidationValidateSubtreeDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "subtreevalidation",
			Name:      "validate_subtree_duration",
			Help:      "Duration of validate subtree",
			Buckets:   util.MetricsBucketsMilliLongSeconds,
		},
	)

	prometheusSubtreeValidationBlessMissingTransaction = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "subtreevalidation",
			Name:      "bless_missing_transaction",
			Help:      "Duration of bless missing transaction",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusSubtreeValidationSetTXMetaCacheKafka = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "subtreevalidation",
			Name:      "set_tx_meta_cache_kafka",
			Help:      "Duration of setting tx meta cache from kafka",
			Buckets:   util.MetricsBucketsMicroSeconds,
		},
	)

	prometheusSubtreeValidationDelTXMetaCacheKafka = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "subtreevalidation",
			Name:      "del_tx_meta_cache_kafka",
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
