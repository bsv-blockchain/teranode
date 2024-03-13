package blockvalidation

import (
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"sync"
)

var (
	prometheusBlockValidationHealth                    prometheus.Counter
	prometheusBlockValidationBlockFoundCh              prometheus.Gauge
	prometheusBlockValidationBlockFound                prometheus.Counter
	prometheusBlockValidationBlockFoundDuration        prometheus.Histogram
	prometheusBlockValidationCatchupCh                 prometheus.Gauge
	prometheusBlockValidationCatchup                   prometheus.Counter
	prometheusBlockValidationCatchupDuration           prometheus.Histogram
	prometheusBlockValidationProcessBlockFoundDuration prometheus.Histogram
	prometheusBlockValidationSetTxMetaQueueCh          prometheus.Gauge
	//prometheusBlockValidationSetTxMetaQueueChWaitDuration    prometheus.Histogram
	//prometheusBlockValidationSetTxMetaQueueDuration          prometheus.Histogram
	prometheusBlockValidationValidateBlock                   prometheus.Counter
	prometheusBlockValidationValidateBlockDuration           prometheus.Histogram
	prometheusBlockValidationValidateSubtree                 prometheus.Counter
	prometheusBlockValidationValidateSubtreeDuration         prometheus.Histogram
	prometheusBlockValidationBlessMissingTransaction         prometheus.Counter
	prometheusBlockValidationBlessMissingTransactionDuration prometheus.Histogram

	// tx meta cache stats
	prometheusBlockValidationSetTXMetaCache        prometheus.Counter
	prometheusBlockValidationSetTXMetaCacheFrpc    prometheus.Counter
	prometheusBlockValidationSetTXMetaCacheDel     prometheus.Counter
	prometheusBlockValidationSetTXMetaCacheDelFrpc prometheus.Counter
	prometheusBlockValidationSetMinedLocal         prometheus.Counter
	prometheusBlockValidationSetMinedMulti         prometheus.Counter
	prometheusBlockValidationSetMinedMultiFrpc     prometheus.Counter

	// expiring cache metrics
	prometheusBlockValidationLastValidatedBlocksCache prometheus.Gauge
	prometheusBlockValidationBlockExistsCache         prometheus.Gauge
	prometheusBlockValidationSubtreeExistsCache       prometheus.Gauge
)

var (
	prometheusMetricsInitOnce sync.Once
)

func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

func _initPrometheusMetrics() {
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
			Name:      "block_found_duration_seconds",
			Help:      "Duration of block found",
			Buckets:   util.MetricsBucketsSeconds,
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
			Name:      "catchup_duration_seconds",
			Help:      "Duration of catchup",
			Buckets:   util.MetricsBucketsSeconds,
		},
	)

	prometheusBlockValidationProcessBlockFoundDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockvalidation",
			Name:      "process_block_found_duration_seconds",
			Help:      "Duration of process block found",
			Buckets:   util.MetricsBucketsSeconds,
		},
	)

	prometheusBlockValidationSetTxMetaQueueCh = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "blockvalidation",
			Name:      "set_tx_meta_queue_ch",
			Help:      "Number of tx meta queue buffered in the set tx meta queue channel",
		},
	)

	//prometheusBlockValidationSetTxMetaQueueChWaitDuration = promauto.NewHistogram(
	//	prometheus.HistogramOpts{
	//		Namespace: "blockvalidation",
	//		Name:      "set_tx_meta_queue_ch_wait_duration_millis",
	//		Help:      "Duration of set tx meta queue channel wait",
	//		Buckets:   util.MetricsBucketsMilliSeconds,
	//	},
	//)
	//
	//prometheusBlockValidationSetTxMetaQueueDuration = promauto.NewHistogram(
	//	prometheus.HistogramOpts{
	//		Namespace: "blockvalidation",
	//		Name:      "set_tx_meta_queue_duration_millis",
	//		Help:      "Duration of set tx meta from queue",
	//		Buckets:   util.MetricsBucketsMilliSeconds,
	//	},
	//)

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
			Name:      "validate_block_duration_seconds",
			Help:      "Duration of validate block",
			Buckets:   util.MetricsBucketsSeconds,
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
			Name:      "validate_subtree_duration_millis",
			Help:      "Duration of validate subtree",
			Buckets:   util.MetricsBucketsMilliLongSeconds,
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
			Name:      "bless_missing_transaction_duration_millis",
			Help:      "Duration of bless missing transaction",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockValidationSetTXMetaCache = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockvalidation",
			Name:      "set_tx_meta_cache",
			Help:      "Number of tx meta cache sets",
		},
	)

	prometheusBlockValidationSetTXMetaCacheFrpc = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockvalidation",
			Name:      "set_tx_meta_cache_frpc",
			Help:      "Number of tx meta cache sets with frpc",
		},
	)

	prometheusBlockValidationSetTXMetaCacheDel = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockvalidation",
			Name:      "del_tx_meta_cache",
			Help:      "Number of tx meta cache deletes",
		},
	)

	prometheusBlockValidationSetTXMetaCacheDelFrpc = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockvalidation",
			Name:      "del_tx_meta_cache_frpc",
			Help:      "Number of tx meta cache deletes with frpc",
		},
	)

	prometheusBlockValidationSetMinedLocal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockvalidation",
			Name:      "set_tx_mined_local",
			Help:      "Number of tx mined local sets",
		},
	)

	prometheusBlockValidationSetMinedMulti = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockvalidation",
			Name:      "set_tx_mined_multi",
			Help:      "Number of tx mined multi sets",
		},
	)

	prometheusBlockValidationSetMinedMultiFrpc = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockvalidation",
			Name:      "set_tx_mined_multi_frpc",
			Help:      "Number of tx mined multi sets with frpc",
		},
	)

	prometheusBlockValidationLastValidatedBlocksCache = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "blockvalidation",
			Name:      "last_validated_blocks_cache",
			Help:      "Number of blocks in the last validated blocks cache",
		},
	)

	prometheusBlockValidationBlockExistsCache = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "blockvalidation",
			Name:      "block_exists_cache",
			Help:      "Number of blocks in the block exists cache",
		},
	)

	prometheusBlockValidationSubtreeExistsCache = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "blockvalidation",
			Name:      "subtree_exists_cache",
			Help:      "Number of subtrees in the subtree exists cache",
		},
	)
}
