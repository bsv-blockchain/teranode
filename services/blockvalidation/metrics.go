package blockvalidation

import (
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prometheusBlockValidationHealth                          prometheus.Counter
	prometheusBlockValidationBlockFound                      prometheus.Counter
	prometheusBlockValidationBlockFoundDuration              prometheus.Histogram
	prometheusBlockValidationCatchup                         prometheus.Counter
	prometheusBlockValidationCatchupDuration                 prometheus.Histogram
	prometheusBlockValidationProcessBlockFoundDuration       prometheus.Histogram
	prometheusBlockValidationSubtreeFound                    prometheus.Counter
	prometheusBlockValidationSubtreeFoundDuration            prometheus.Histogram
	prometheusBlockValidationValidateBlock                   prometheus.Counter
	prometheusBlockValidationValidateBlockDuration           prometheus.Histogram
	prometheusBlockValidationValidateSubtree                 prometheus.Counter
	prometheusBlockValidationValidateSubtreeDuration         prometheus.Histogram
	prometheusBlockValidationBlessMissingTransaction         prometheus.Counter
	prometheusBlockValidationBlessMissingTransactionDuration prometheus.Histogram
)

var prometheusMetricsInitialised = false

func initPrometheusMetrics() {
	if prometheusMetricsInitialised {
		return
	}

	prometheusBlockValidationHealth = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockvalidation",
			Name:      "health",
			Help:      "Number of health checks",
		},
	)

	prometheusBlockValidationBlockFound = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockvalidation",
			Name:      "block_found",
			Help:      "Number of blocks found",
		},
	)

	prometheusBlockValidationBlockFoundDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockvalidation",
			Name:      "block_found_duration",
			Help:      "Duration of block found",
			Buckets:   util.MetricsBuckets,
		},
	)

	prometheusBlockValidationCatchup = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockvalidation",
			Name:      "catchup",
			Help:      "Number of catchups",
		},
	)

	prometheusBlockValidationCatchupDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockvalidation",
			Name:      "catchup_duration",
			Help:      "Duration of catchup",
			Buckets:   util.MetricsBuckets,
		},
	)

	prometheusBlockValidationProcessBlockFoundDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockvalidation",
			Name:      "process_block_found_duration",
			Help:      "Duration of process block found",
			Buckets:   util.MetricsBuckets,
		},
	)

	prometheusBlockValidationSubtreeFound = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockvalidation",
			Name:      "subtree_found",
			Help:      "Number of subtrees found",
		},
	)

	prometheusBlockValidationSubtreeFoundDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockvalidation",
			Name:      "subtree_found_duration",
			Help:      "Duration of subtree found",
			Buckets:   util.MetricsBuckets,
		},
	)

	prometheusBlockValidationValidateBlock = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockvalidation",
			Name:      "validate_block",
			Help:      "Number of blocks validated",
		},
	)

	prometheusBlockValidationValidateBlockDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockvalidation",
			Name:      "validate_block_duration",
			Help:      "Duration of validate block",
			Buckets:   util.MetricsBuckets,
		},
	)

	prometheusBlockValidationValidateSubtree = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockvalidation",
			Name:      "validate_subtree",
			Help:      "Number of subtrees validated",
		},
	)

	prometheusBlockValidationValidateSubtreeDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockvalidation",
			Name:      "validate_subtree_duration",
			Help:      "Duration of validate subtree",
			Buckets:   util.MetricsBuckets,
		},
	)

	prometheusBlockValidationBlessMissingTransaction = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockvalidation",
			Name:      "bless_missing_transaction",
			Help:      "Number of missing transactions blessed",
		},
	)

	prometheusBlockValidationBlessMissingTransactionDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockvalidation",
			Name:      "bless_missing_transaction_duration",
			Help:      "Duration of bless missing transaction",
			Buckets:   util.MetricsBuckets,
		},
	)

	prometheusMetricsInitialised = true
}
