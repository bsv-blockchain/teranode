package validator

import (
	"sync"

	"github.com/bitcoin-sv/ubsv/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prometheusHealth                              prometheus.Counter
	prometheusInvalidTransactions                 prometheus.Counter
	prometheusTransactionValidateTotal            prometheus.Histogram
	prometheusTransactionValidate                 prometheus.Histogram
	prometheusTransactionValidateBatch            prometheus.Histogram
	prometheusTransactionSpendUtxos               prometheus.Histogram
	prometheusValidateTransaction                 prometheus.Histogram
	prometheusTransactionSize                     prometheus.Histogram
	prometheusValidatorSendToBlockAssembly        prometheus.Histogram
	prometheusValidatorSendToBlockValidationKafka prometheus.Histogram
	prometheusValidatorSendToP2PKafka             prometheus.Histogram
	prometheusValidatorSetTxMeta                  prometheus.Histogram
)

var (
	prometheusMetricsInitOnce sync.Once
)

func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

func _initPrometheusMetrics() {
	prometheusHealth = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "validator",
			Name:      "health",
			Help:      "Number of calls to the health endpoint",
		},
	)
	prometheusInvalidTransactions = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "validator",
			Name:      "invalid_transactions",
			Help:      "Number of transactions found invalid by the validator service",
		},
	)
	prometheusTransactionValidateTotal = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "validator",
			Name:      "transactions_validate_total",
			Help:      "Histogram of total transaction validation",
			Buckets:   util.MetricsBucketsMicroSeconds,
		},
	)
	prometheusTransactionValidate = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "validator",
			Name:      "transactions_validate",
			Help:      "Histogram of transaction validation",
			Buckets:   util.MetricsBucketsMicroSeconds,
		},
	)
	prometheusTransactionValidateBatch = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "validator",
			Name:      "transactions_validate_batch",
			Help:      "Histogram of transaction batch validation",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusTransactionSpendUtxos = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "validator",
			Name:      "transactions_spend_utxos",
			Help:      "Histogram of transaction spending utxos",
			Buckets:   util.MetricsBucketsMicroSeconds,
		},
	)
	prometheusValidateTransaction = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "validator",
			Name:      "transactions",
			Help:      "Histogram of transaction processing by the validator service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusTransactionSize = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "validator",
			Name:      "transactions_size",
			Help:      "Size of transactions processed by the validator service",
			Buckets:   util.MetricsBucketsSize,
		},
	)
	prometheusValidatorSendToBlockAssembly = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "validator",
			Name:      "send_to_block_assembly",
			Help:      "Histogram of sending transactions to block assembly",
			Buckets:   util.MetricsBucketsMicroSeconds,
		},
	)
	prometheusValidatorSendToBlockValidationKafka = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "validator",
			Name:      "send_to_blockvalidation_kafka",
			Help:      "Histogram of sending transactions to block validation kafka",
			Buckets:   util.MetricsBucketsMicroSeconds,
		},
	)
	prometheusValidatorSendToP2PKafka = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "validator",
			Name:      "send_to_p2p_kafka",
			Help:      "Histogram of sending rejected transactions to p2p kafka",
			Buckets:   util.MetricsBucketsMicroSeconds,
		},
	)
	prometheusValidatorSetTxMeta = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "validator",
			Name:      "set_tx_meta",
			Help:      "Histogram of validator set tx meta",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
}
