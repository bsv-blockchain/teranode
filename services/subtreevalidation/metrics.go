// Package subtreevalidation provides functionality for validating subtrees in a blockchain context.
// It handles the validation of transaction subtrees, manages transaction metadata caching,
// and interfaces with blockchain and validation services.
package subtreevalidation

import (
	"sync"

	"github.com/bsv-blockchain/teranode/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Prometheus metrics for monitoring subtree validation service performance and health.
//
// These metrics provide comprehensive observability into the subtree validation service,
// enabling monitoring of operation durations, success/failure rates, retry patterns,
// and overall service health. The metrics are automatically registered with Prometheus
// and can be scraped by monitoring systems.
//
// Metric Categories:
//   - Health metrics: Service availability and readiness checks
//   - Operation metrics: Duration and success rates of validation operations
//   - Retry metrics: Tracking of retry attempts and patterns
//   - Handler metrics: Performance of individual API handlers
//   - Processing metrics: Internal processing step performance
//
// All histogram metrics use standard duration buckets appropriate for blockchain
// validation operations, typically ranging from milliseconds to seconds.
var (
	// prometheusHealth tracks the duration of health check operations.
	// This histogram measures how long health checks take to complete,
	// which is important for monitoring service responsiveness.
	prometheusHealth prometheus.Histogram

	// prometheusSubtreeValidationCheckSubtree tracks the duration of subtree existence checks.
	// This histogram measures the time taken to verify if a subtree already exists
	// in storage, which is a common optimization operation.
	prometheusSubtreeValidationCheckSubtree prometheus.Histogram

	// prometheusSubtreeValidationValidateSubtree tracks the duration of complete subtree validation operations.
	// This is the primary metric for monitoring the core validation functionality,
	// measuring end-to-end validation time including all sub-operations.
	prometheusSubtreeValidationValidateSubtree prometheus.Histogram

	// prometheusSubtreeValidationValidateSubtreeRetry counts retry attempts during subtree validation.
	// This counter tracks how often validation operations need to be retried,
	// which helps identify reliability issues or transient failures.
	prometheusSubtreeValidationValidateSubtreeRetry prometheus.Counter

	// prometheusSubtreeValidationValidateSubtreeHandler tracks the duration of validation API handler operations.
	// This histogram measures the time spent in the gRPC handler layer,
	// including request processing and response generation.
	prometheusSubtreeValidationValidateSubtreeHandler prometheus.Histogram

	// prometheusSubtreeValidationValidateSubtreeDuration tracks detailed validation processing time.
	// This histogram provides granular timing information for the internal
	// validation logic, separate from handler overhead.
	prometheusSubtreeValidationValidateSubtreeDuration prometheus.Histogram

	// prometheusSubtreeValidationBlessMissingTransaction tracks the duration of bless missing transaction operations.
	// This histogram measures the time taken to handle missing transactions,
	// which is an important aspect of subtree validation.
	prometheusSubtreeValidationBlessMissingTransaction prometheus.Histogram

	// prometheusSubtreeValidationSetTXMetaCacheKafka tracks the duration of setting tx meta cache from kafka operations.
	// This histogram measures the time taken to update the transaction metadata cache
	// from Kafka messages, which is a critical step in subtree validation.
	prometheusSubtreeValidationSetTXMetaCacheKafka prometheus.Histogram

	// prometheusSubtreeValidationDelTXMetaCacheKafka tracks the duration of deleting tx meta cache from kafka operations.
	// This histogram measures the time taken to remove transaction metadata from the cache
	// based on Kafka messages, which is an important maintenance operation.
	prometheusSubtreeValidationDelTXMetaCacheKafka prometheus.Histogram

	// prometheusSubtreeValidationSetTXMetaCacheKafkaErrors counts errors setting tx meta cache from kafka operations.
	// This counter tracks how often errors occur when updating the transaction metadata cache
	// from Kafka messages, which helps identify issues with the cache or Kafka connection.
	prometheusSubtreeValidationSetTXMetaCacheKafkaErrors prometheus.Counter
)

var (
	prometheusMetricsInitOnce sync.Once
)

func InitPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

func _initPrometheusMetrics() {
	prometheusHealth = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "subtreevalidation",
			Name:      "health",
			Help:      "Histogram of calls to health endpoint",
			Buckets:   util.MetricsBucketsMilliLongSeconds,
		},
	)

	prometheusSubtreeValidationCheckSubtree = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "subtreevalidation",
			Name:      "check_subtree",
			Help:      "Duration of calls to checkSubtree endpoint",
			Buckets:   util.MetricsBucketsMilliLongSeconds,
		},
	)

	prometheusSubtreeValidationValidateSubtree = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "subtreevalidation",
			Name:      "validate_subtree",
			Help:      "Histogram of subtrees validated",
			Buckets:   util.MetricsBucketsMilliLongSeconds,
		},
	)

	prometheusSubtreeValidationValidateSubtreeRetry = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "subtreevalidation",
			Name:      "validate_subtree_retry",
			Help:      "Number of retries when subtrees validated",
		},
	)

	prometheusSubtreeValidationValidateSubtreeHandler = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "subtreevalidation",
			Name:      "validate_subtree_handler",
			Help:      "Duration of subtree handler",
			Buckets:   util.MetricsBucketsMilliLongSeconds,
		},
	)

	prometheusSubtreeValidationValidateSubtreeDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "subtreevalidation",
			Name:      "validate_subtree_duration",
			Help:      "Duration of validate subtree",
			Buckets:   util.MetricsBucketsMilliLongSeconds,
		},
	)

	prometheusSubtreeValidationBlessMissingTransaction = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "subtreevalidation",
			Name:      "bless_missing_transaction",
			Help:      "Duration of bless missing transaction",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusSubtreeValidationSetTXMetaCacheKafka = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "subtreevalidation",
			Name:      "set_tx_meta_cache_kafka",
			Help:      "Duration of setting tx meta cache from kafka",
			Buckets:   util.MetricsBucketsMicroSeconds,
		},
	)

	prometheusSubtreeValidationDelTXMetaCacheKafka = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "subtreevalidation",
			Name:      "del_tx_meta_cache_kafka",
			Help:      "Duration of deleting tx meta cache from kafka",
			Buckets:   util.MetricsBucketsMicroSeconds,
		},
	)

	prometheusSubtreeValidationSetTXMetaCacheKafkaErrors = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "subtreevalidation",
			Name:      "set_tx_meta_cache_kafka_errors",
			Help:      "Number of errors setting tx meta cache from kafka",
		},
	)
}
