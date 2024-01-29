package validator

import (
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prometheusHealth                       prometheus.Counter
	prometheusProcessedTransactions        prometheus.Counter
	prometheusInvalidTransactions          prometheus.Counter
	prometheusTransactionValidateTotal     prometheus.Histogram
	prometheusTransactionValidate          prometheus.Histogram
	prometheusTransactionValidateBatch     prometheus.Histogram
	prometheusTransactionStoreUtxos        prometheus.Histogram
	prometheusTransactionSpendUtxos        prometheus.Histogram
	prometheusTransactionDuration          prometheus.Histogram
	prometheusTransactionSize              prometheus.Histogram
	prometheusValidatorSendToBlockAssembly prometheus.Histogram
	prometheusValidatorSetTxMeta           prometheus.Histogram
	prometheusValidatorSetTxMetaCache      prometheus.Histogram
)

var prometheusMetricsInitialised = false

func initPrometheusMetrics() {
	if prometheusMetricsInitialised {
		return
	}

	prometheusHealth = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "validator",
			Name:      "health",
			Help:      "Number of calls to the health endpoint",
		},
	)
	prometheusProcessedTransactions = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "validator",
			Name:      "processed_transactions",
			Help:      "Number of transactions processed by the validator service",
		},
	)
	prometheusInvalidTransactions = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "validator",
			Name:      "invalid_transactions",
			Help:      "Number of transactions found invalid by the validator service",
		},
	)
	prometheusTransactionValidateTotal = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "validator",
			Name:      "transactions_validate_total_micros",
			Help:      "Duration of total transaction validation",
			Buckets:   util.MetricsBucketsMicroSeconds,
		},
	)
	prometheusTransactionValidate = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "validator",
			Name:      "transactions_validate_micros",
			Help:      "Duration of transaction validation",
			Buckets:   util.MetricsBucketsMicroSeconds,
		},
	)
	prometheusTransactionValidateBatch = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "validator",
			Name:      "transactions_validate_batch_millis",
			Help:      "Duration of transaction batch validation",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusTransactionStoreUtxos = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "validator",
			Name:      "transactions_store_utxos_micros",
			Help:      "Duration of transaction storing utxos",
			Buckets:   util.MetricsBucketsMicroSeconds,
		},
	)
	prometheusTransactionSpendUtxos = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "validator",
			Name:      "transactions_spend_utxos_micros",
			Help:      "Duration of transaction spending utxos",
			Buckets:   util.MetricsBucketsMicroSeconds,
		},
	)
	prometheusTransactionDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "validator",
			Name:      "transactions_duration_millis",
			Help:      "Duration of transaction processing by the validator service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusTransactionSize = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "validator",
			Name:      "transactions_size_v2",
			Help:      "Size of transactions processed by the validator service",
			Buckets:   util.MetricsBucketsSize,
		},
	)
	prometheusValidatorSendToBlockAssembly = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "validator",
			Name:      "validator_send_to_block_assembly_micros",
			Help:      "Duration of sending transactions to block assembly",
			Buckets:   util.MetricsBucketsMicroSeconds,
		},
	)
	prometheusValidatorSetTxMeta = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "validator",
			Name:      "validator_set_tx_meta_millis",
			Help:      "Duration of validator set tx meta",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusValidatorSetTxMetaCache = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "validator",
			Name:      "validator_set_tx_meta_cache_millis",
			Help:      "Duration of validator set tx meta cache",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusMetricsInitialised = true
}
