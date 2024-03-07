package subtreevalidation

import (
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

//nolint:unused //TODO: enable these later
var (
	prometheusSubtreeValidationHealth                     prometheus.Counter
	prometheusSubtreeValidationBlockFoundCh               prometheus.Gauge
	prometheusSubtreeValidationBlockFound                 prometheus.Counter
	prometheusSubtreeValidationBlockFoundDuration         prometheus.Histogram
	prometheusSubtreeValidationCatchupCh                  prometheus.Gauge
	prometheusSubtreeValidationCatchup                    prometheus.Counter
	prometheusSubtreeValidationCatchupDuration            prometheus.Histogram
	prometheusSubtreeValidationProcessBlockFoundDuration  prometheus.Histogram
	prometheusSubtreeValidationSubtreeFound               prometheus.Counter
	prometheusSubtreeValidationSubtreeFoundCh             prometheus.Gauge
	prometheusSubtreeValidationSubtreeFoundChWaitDuration prometheus.Histogram
	prometheusSubtreeValidationSubtreeFoundDuration       prometheus.Histogram
	prometheusSubtreeValidationSetTxMetaQueueCh           prometheus.Gauge
	//prometheusSubtreeValidationSetTxMetaQueueChWaitDuration    prometheus.Histogram
	//prometheusSubtreeValidationSetTxMetaQueueDuration          prometheus.Histogram
	prometheusSubtreeValidationValidateBlock                   prometheus.Counter
	prometheusSubtreeValidationValidateBlockDuration           prometheus.Histogram
	prometheusSubtreeValidationValidateSubtree                 prometheus.Counter
	prometheusSubtreeValidationValidateSubtreeDuration         prometheus.Histogram
	prometheusSubtreeValidationBlessMissingTransaction         prometheus.Counter
	prometheusSubtreeValidationBlessMissingTransactionDuration prometheus.Histogram

	// tx meta cache stats
	prometheusSubtreeValidationSetTXMetaCache        prometheus.Counter
	prometheusSubtreeValidationSetTXMetaCacheFrpc    prometheus.Counter
	prometheusSubtreeValidationSetTXMetaCacheKafka   prometheus.Histogram
	prometheusSubtreeValidationSetTXMetaCacheDel     prometheus.Counter
	prometheusSubtreeValidationSetTXMetaCacheDelFrpc prometheus.Counter
	prometheusSubtreeValidationSetMinedLocal         prometheus.Counter
	prometheusSubtreeValidationSetMinedMulti         prometheus.Counter
	prometheusSubtreeValidationSetMinedMultiFrpc     prometheus.Counter

	// expiring cache metrics
	prometheusSubtreeValidationLastValidatedBlocksCache prometheus.Gauge
	prometheusSubtreeValidationBlockExistsCache         prometheus.Gauge
	prometheusSubtreeValidationSubtreeExistsCache       prometheus.Gauge
)

var prometheusMetricsInitialised = false

func initPrometheusMetrics() {
	if prometheusMetricsInitialised {
		return
	}

	prometheusSubtreeValidationHealth = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockvalidation",
			Name:      "health",
			Help:      "Number of health checks",
		},
	)

	prometheusSubtreeValidationBlockFoundCh = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "blockvalidation",
			Name:      "block_found_ch",
			Help:      "Number of blocks found buffered in the block found channel",
		},
	)

	prometheusSubtreeValidationBlockFound = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockvalidation",
			Name:      "block_found",
			Help:      "Number of blocks found",
		},
	)

	prometheusSubtreeValidationBlockFoundDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockvalidation",
			Name:      "block_found_duration_seconds",
			Help:      "Duration of block found",
			Buckets:   util.MetricsBucketsSeconds,
		},
	)

	prometheusSubtreeValidationCatchupCh = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "blockvalidation",
			Name:      "catchup_ch",
			Help:      "Number of catchups buffered in the catchup channel",
		},
	)

	prometheusSubtreeValidationCatchup = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockvalidation",
			Name:      "catchup",
			Help:      "Number of catchups",
		},
	)

	prometheusSubtreeValidationCatchupDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockvalidation",
			Name:      "catchup_duration_seconds",
			Help:      "Duration of catchup",
			Buckets:   util.MetricsBucketsSeconds,
		},
	)

	prometheusSubtreeValidationProcessBlockFoundDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockvalidation",
			Name:      "process_block_found_duration_seconds",
			Help:      "Duration of process block found",
			Buckets:   util.MetricsBucketsSeconds,
		},
	)

	prometheusSubtreeValidationSubtreeFound = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockvalidation",
			Name:      "subtree_found",
			Help:      "Number of subtrees found",
		},
	)

	prometheusSubtreeValidationSubtreeFoundCh = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "blockvalidation",
			Name:      "subtree_found_ch",
			Help:      "Number of subtrees found buffered in the subtree found channel",
		},
	)

	prometheusSubtreeValidationSubtreeFoundChWaitDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockvalidation",
			Name:      "subtree_found_ch_wait_duration_millis",
			Help:      "Duration of subtree found channel wait",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusSubtreeValidationSubtreeFoundDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockvalidation",
			Name:      "subtree_found_duration_millis",
			Help:      "Duration of subtree found",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusSubtreeValidationSetTxMetaQueueCh = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "blockvalidation",
			Name:      "set_tx_meta_queue_ch",
			Help:      "Number of tx meta queue buffered in the set tx meta queue channel",
		},
	)

	//prometheusSubtreeValidationSetTxMetaQueueChWaitDuration = promauto.NewHistogram(
	//	prometheus.HistogramOpts{
	//		Namespace: "blockvalidation",
	//		Name:      "set_tx_meta_queue_ch_wait_duration_millis",
	//		Help:      "Duration of set tx meta queue channel wait",
	//		Buckets:   util.MetricsBucketsMilliSeconds,
	//	},
	//)
	//
	//prometheusSubtreeValidationSetTxMetaQueueDuration = promauto.NewHistogram(
	//	prometheus.HistogramOpts{
	//		Namespace: "blockvalidation",
	//		Name:      "set_tx_meta_queue_duration_millis",
	//		Help:      "Duration of set tx meta from queue",
	//		Buckets:   util.MetricsBucketsMilliSeconds,
	//	},
	//)

	prometheusSubtreeValidationValidateBlock = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockvalidation",
			Name:      "validate_block",
			Help:      "Number of blocks validated",
		},
	)

	prometheusSubtreeValidationValidateBlockDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockvalidation",
			Name:      "validate_block_duration_seconds",
			Help:      "Duration of validate block",
			Buckets:   util.MetricsBucketsSeconds,
		},
	)

	prometheusSubtreeValidationValidateSubtree = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockvalidation",
			Name:      "validate_subtree",
			Help:      "Number of subtrees validated",
		},
	)

	prometheusSubtreeValidationValidateSubtreeDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockvalidation",
			Name:      "validate_subtree_duration_millis",
			Help:      "Duration of validate subtree",
			Buckets:   util.MetricsBucketsMilliLongSeconds,
		},
	)

	prometheusSubtreeValidationBlessMissingTransaction = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockvalidation",
			Name:      "bless_missing_transaction",
			Help:      "Number of missing transactions blessed",
		},
	)

	prometheusSubtreeValidationBlessMissingTransactionDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockvalidation",
			Name:      "bless_missing_transaction_duration_millis",
			Help:      "Duration of bless missing transaction",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusSubtreeValidationSetTXMetaCache = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockvalidation",
			Name:      "set_tx_meta_cache",
			Help:      "Number of tx meta cache sets",
		},
	)

	prometheusSubtreeValidationSetTXMetaCacheFrpc = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockvalidation",
			Name:      "set_tx_meta_cache_frpc",
			Help:      "Number of tx meta cache sets with frpc",
		},
	)

	prometheusSubtreeValidationSetTXMetaCacheKafka = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockvalidation",
			Name:      "set_tx_meta_cache_kafka_micros",
			Help:      "Duration of setting tx meta cache from kafka",
			Buckets:   util.MetricsBucketsMicroSeconds,
		},
	)

	prometheusSubtreeValidationSetTXMetaCacheDel = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockvalidation",
			Name:      "del_tx_meta_cache",
			Help:      "Number of tx meta cache deletes",
		},
	)

	prometheusSubtreeValidationSetTXMetaCacheDelFrpc = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockvalidation",
			Name:      "del_tx_meta_cache_frpc",
			Help:      "Number of tx meta cache deletes with frpc",
		},
	)

	prometheusSubtreeValidationSetMinedLocal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockvalidation",
			Name:      "set_tx_mined_local",
			Help:      "Number of tx mined local sets",
		},
	)

	prometheusSubtreeValidationSetMinedMulti = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockvalidation",
			Name:      "set_tx_mined_multi",
			Help:      "Number of tx mined multi sets",
		},
	)

	prometheusSubtreeValidationSetMinedMultiFrpc = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockvalidation",
			Name:      "set_tx_mined_multi_frpc",
			Help:      "Number of tx mined multi sets with frpc",
		},
	)

	prometheusSubtreeValidationLastValidatedBlocksCache = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "blockvalidation",
			Name:      "last_validated_blocks_cache",
			Help:      "Number of blocks in the last validated blocks cache",
		},
	)

	prometheusSubtreeValidationBlockExistsCache = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "blockvalidation",
			Name:      "block_exists_cache",
			Help:      "Number of blocks in the block exists cache",
		},
	)

	prometheusSubtreeValidationSubtreeExistsCache = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "blockvalidation",
			Name:      "subtree_exists_cache",
			Help:      "Number of subtrees in the subtree exists cache",
		},
	)

	prometheusMetricsInitialised = true
}
