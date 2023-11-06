package blockvalidation

import (
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prometheusBlockValidationHealth                          prometheus.Counter
	prometheusBlockValidationBlockFoundCh                    prometheus.Gauge
	prometheusBlockValidationBlockFound                      prometheus.Counter
	prometheusBlockValidationBlockFoundDuration              prometheus.Histogram
	prometheusBlockValidationCatchupCh                       prometheus.Gauge
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

	// tx meta cache stats
	prometheusBlockValidationTxMetaCacheSize       prometheus.Gauge
	prometheusBlockValidationTxMetaCacheInsertions prometheus.Gauge
	prometheusBlockValidationTxMetaCacheHits       prometheus.Gauge
	prometheusBlockValidationTxMetaCacheMisses     prometheus.Gauge
	prometheusBlockValidationTxMetaCacheEvictions  prometheus.Gauge
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

	prometheusBlockValidationBlockFoundCh = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "blockvalidation",
			Name:      "block_found_ch",
			Help:      "Number of blocks found buffered in the block found channel",
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

	prometheusBlockValidationCatchupCh = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "blockvalidation",
			Name:      "catchup_ch",
			Help:      "Number of catchups buffered in the catchup channel",
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

	prometheusBlockValidationTxMetaCacheSize = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "blockvalidation",
			Name:      "tx_meta_cache_size",
			Help:      "Number of items in the tx meta cache",
		},
	)

	prometheusBlockValidationTxMetaCacheInsertions = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "blockvalidation",
			Name:      "tx_meta_cache_insertions",
			Help:      "Number of insertions into the tx meta cache",
		},
	)

	prometheusBlockValidationTxMetaCacheHits = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "blockvalidation",
			Name:      "tx_meta_cache_hits",
			Help:      "Number of hits in the tx meta cache",
		},
	)

	prometheusBlockValidationTxMetaCacheMisses = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "blockvalidation",
			Name:      "tx_meta_cache_misses",
			Help:      "Number of misses in the tx meta cache",
		},
	)

	prometheusBlockValidationTxMetaCacheEvictions = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "blockvalidation",
			Name:      "tx_meta_cache_evictions",
			Help:      "Number of evictions in the tx meta cache",
		},
	)

	prometheusMetricsInitialised = true
}
