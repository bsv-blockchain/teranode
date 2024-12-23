package netsync

import (
	"sync"

	"github.com/bitcoin-sv/teranode/util"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	prometheusLegacyNetsyncBlockHeight                    prometheus.Gauge
	prometheusLegacyNetsyncHandleTxMsg                    prometheus.Histogram
	prometheusLegacyNetsyncHandleTxMsgValidate            prometheus.Histogram
	prometheusLegacyNetsyncProcessOrphanTransactions      prometheus.Histogram
	prometheusLegacyNetsyncHandleBlockDirect              prometheus.Histogram
	prometheusLegacyNetsyncProcessBlock                   prometheus.Histogram
	prometheusLegacyNetsyncPrepareSubtrees                prometheus.Histogram
	prometheusLegacyNetsyncValidateTransactionsLegacyMode prometheus.Histogram
	prometheusLegacyNetsyncPreValidateTransactions        prometheus.Histogram
	prometheusLegacyNetsyncValidateTransactions           prometheus.Histogram
	prometheusLegacyNetsyncExtendTransactions             prometheus.Histogram
	prometheusLegacyNetsyncCreateUtxos                    prometheus.Histogram
	prometheusLegacyNetsyncBlockTxSize                    prometheus.Histogram
	prometheusLegacyNetsyncBlockTxNrInputs                prometheus.Histogram
	prometheusLegacyNetsyncBlockTxNrOutputs               prometheus.Histogram
	prometheusLegacyNetsyncBlockTxExtend                  prometheus.Histogram
	prometheusLegacyNetsyncBlockTxValidate                prometheus.Histogram
	prometheusLegacyNetsyncOrphans                        prometheus.Gauge
	prometheusLegacyNetsyncOrphanTime                     prometheus.Histogram

	prometheusMetricsInitOnce sync.Once
)

func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

func _initPrometheusMetrics() {
	prometheusLegacyNetsyncBlockHeight = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "block_height",
		Help:      "The height of the block being processed",
	})
	prometheus.MustRegister(prometheusLegacyNetsyncBlockHeight)

	prometheusLegacyNetsyncHandleTxMsg = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "handle_tx_msg",
		Help:      "The time taken to handle a tx message",
		Buckets:   util.MetricsBucketsMilliSeconds,
	})
	prometheus.MustRegister(prometheusLegacyNetsyncHandleTxMsg)

	prometheusLegacyNetsyncHandleTxMsgValidate = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "handle_tx_msg_validate",
		Help:      "The time taken to validate a tx message",
		Buckets:   util.MetricsBucketsMilliSeconds,
	})
	prometheus.MustRegister(prometheusLegacyNetsyncHandleTxMsgValidate)

	prometheusLegacyNetsyncProcessOrphanTransactions = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "process_orphan_transactions",
		Help:      "The time taken to process orphan transactions",
		Buckets:   util.MetricsBucketsMilliSeconds,
	})
	prometheus.MustRegister(prometheusLegacyNetsyncProcessOrphanTransactions)

	prometheusLegacyNetsyncHandleBlockDirect = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "handle_block_direct",
		Help:      "The time taken to handle a block directly",
		Buckets:   util.MetricsBucketsSeconds,
	})
	prometheus.MustRegister(prometheusLegacyNetsyncHandleBlockDirect)

	prometheusLegacyNetsyncProcessBlock = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "process_block",
		Help:      "The time taken to process a block",
		Buckets:   util.MetricsBucketsSeconds,
	})
	prometheus.MustRegister(prometheusLegacyNetsyncProcessBlock)

	prometheusLegacyNetsyncPrepareSubtrees = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "prepare_subtrees",
		Help:      "The time taken to prepare the subtrees",
		Buckets:   util.MetricsBucketsMilliLongSeconds,
	})
	prometheus.MustRegister(prometheusLegacyNetsyncPrepareSubtrees)

	prometheusLegacyNetsyncValidateTransactionsLegacyMode = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "validate_transactions_legacy_mode",
		Help:      "The time taken to validate transactions in legacy mode",
		Buckets:   util.MetricsBucketsMilliLongSeconds,
	})
	prometheus.MustRegister(prometheusLegacyNetsyncValidateTransactionsLegacyMode)

	prometheusLegacyNetsyncExtendTransactions = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "extend_transactions",
		Help:      "The time taken to extend transactions",
		Buckets:   util.MetricsBucketsMilliLongSeconds,
	})
	prometheus.MustRegister(prometheusLegacyNetsyncExtendTransactions)

	prometheusLegacyNetsyncPreValidateTransactions = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "pre_validate_transactions",
		Help:      "The time taken to pre-validate transactions",
		Buckets:   util.MetricsBucketsMilliLongSeconds,
	})
	prometheus.MustRegister(prometheusLegacyNetsyncPreValidateTransactions)

	prometheusLegacyNetsyncValidateTransactions = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "validate_transactions",
		Help:      "The time taken to validate transactions",
		Buckets:   util.MetricsBucketsMilliLongSeconds,
	})
	prometheus.MustRegister(prometheusLegacyNetsyncValidateTransactions)

	prometheusLegacyNetsyncCreateUtxos = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "create_utxos",
		Help:      "The time taken to create UTXOs",
		Buckets:   util.MetricsBucketsMilliLongSeconds,
	})
	prometheus.MustRegister(prometheusLegacyNetsyncCreateUtxos)

	prometheusLegacyNetsyncBlockTxSize = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "block_tx_size",
		Help:      "The size of the transactions in the block being processed",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 20),
	})
	prometheus.MustRegister(prometheusLegacyNetsyncBlockTxSize)

	prometheusLegacyNetsyncBlockTxNrInputs = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "block_tx_nr_inputs",
		Help:      "The number of inputs in the block being processed",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 20),
	})
	prometheus.MustRegister(prometheusLegacyNetsyncBlockTxNrInputs)

	prometheusLegacyNetsyncBlockTxNrOutputs = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "block_tx_nr_outputs",
		Help:      "The number of outputs in the block being processed",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 20),
	})
	prometheus.MustRegister(prometheusLegacyNetsyncBlockTxNrOutputs)

	prometheusLegacyNetsyncBlockTxExtend = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "block_tx_extend",
		Help:      "The time taken to extend a transaction",
		Buckets:   util.MetricsBucketsMilliSeconds,
	})
	prometheus.MustRegister(prometheusLegacyNetsyncBlockTxExtend)

	prometheusLegacyNetsyncBlockTxValidate = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "block_tx_validate",
		Help:      "The time taken to validate a transaction",
		Buckets:   util.MetricsBucketsMilliSeconds,
	})
	prometheus.MustRegister(prometheusLegacyNetsyncBlockTxValidate)

	prometheusLegacyNetsyncOrphans = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "orphans",
		Help:      "The number of orphan transactions",
	})
	prometheus.MustRegister(prometheusLegacyNetsyncOrphans)

	prometheusLegacyNetsyncOrphanTime = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "orphan_time",
		Help:      "The time taken to process an orphan transaction",
		Buckets:   util.MetricsBucketsSeconds,
	})
	prometheus.MustRegister(prometheusLegacyNetsyncOrphanTime)
}
