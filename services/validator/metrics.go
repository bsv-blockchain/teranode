/*
Package validator implements Bitcoin SV transaction validation functionality.

This file implements Prometheus metrics collection for the validator service,
providing detailed monitoring and observability of transaction validation operations.
*/
package validator

import (
	"sync"

	"github.com/bitcoin-sv/teranode/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Prometheus metrics collectors
var (
	// prometheusHealth tracks the number of health check calls
	prometheusHealth prometheus.Counter

	// prometheusInvalidTransactions counts invalid transactions
	prometheusInvalidTransactions prometheus.Counter

	// prometheusTransactionValidateTotal measures total validation time
	prometheusTransactionValidateTotal prometheus.Histogram

	// prometheusTransactionValidate measures individual validation steps
	prometheusTransactionValidate prometheus.Histogram

	// prometheusTransactionValidateScripts measures individual validation script steps
	prometheusTransactionValidateScripts prometheus.Histogram

	// prometheusTransactionValidateBatch measures batch validation performance
	prometheusTransactionValidateBatch prometheus.Histogram

	// prometheusTransactionSpendUtxos measures UTXO spending operations
	prometheusTransactionSpendUtxos prometheus.Histogram

	// getTransactionInputBlockHeights measures time taken to get UTXO heights
	getTransactionInputBlockHeights prometheus.Histogram

	// prometheusTransaction2PhaseCommit measures 2-phase commit operations
	prometheusTransaction2PhaseCommit prometheus.Histogram

	// prometheusValidateTransaction measures overall transaction processing
	prometheusValidateTransaction prometheus.Histogram

	// prometheusTransactionSize tracks transaction size distribution
	prometheusTransactionSize prometheus.Histogram

	// prometheusValidatorSendToBlockAssembly measures block assembly operations
	prometheusValidatorSendToBlockAssembly prometheus.Histogram

	// prometheusValidatorSendToBlockValidationKafka measures Kafka operations for block validation
	prometheusValidatorSendToBlockValidationKafka prometheus.Histogram

	// prometheusValidatorSendToP2PKafka measures Kafka operations for P2P
	prometheusValidatorSendToP2PKafka prometheus.Histogram

	// prometheusValidatorSetTxMeta measures transaction metadata operations
	prometheusValidatorSetTxMeta prometheus.Histogram
)

// Synchronization primitives
var (
	// prometheusMetricsInitOnce ensures metrics are initialized only once
	prometheusMetricsInitOnce sync.Once
)

// initPrometheusMetrics initializes all Prometheus metrics
// This function is called once during service startup
func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

// _initPrometheusMetrics is the actual initialization function for Prometheus metrics
// This function creates and registers all metrics with appropriate configuration
func _initPrometheusMetrics() {
	// Health check counter
	prometheusHealth = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "validator",
			Name:      "health",
			Help:      "Number of calls to the health endpoint",
		},
	)

	// Invalid transactions counter
	prometheusInvalidTransactions = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "validator",
			Name:      "invalid_transactions",
			Help:      "Number of transactions found invalid by the validator service",
		},
	)

	// Total validation time histogram
	prometheusTransactionValidateTotal = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "validator",
			Name:      "transactions_validate_total",
			Help:      "Histogram of total transaction validation",
			Buckets:   util.MetricsBucketsMicroSeconds,
		},
	)

	// Individual validation steps histogram
	prometheusTransactionValidate = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "validator",
			Name:      "transactions_validate",
			Help:      "Histogram of transaction validation",
			Buckets:   util.MetricsBucketsMicroSeconds,
		},
	)

	// Individual validation script steps histogram
	prometheusTransactionValidateScripts = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "validator",
			Name:      "transactions_validate_scripts",
			Help:      "Histogram of transaction script validation",
			Buckets:   util.MetricsBucketsMicroSeconds,
		},
	)

	// Batch validation histogram
	prometheusTransactionValidateBatch = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "validator",
			Name:      "transactions_validate_batch",
			Help:      "Histogram of transaction batch validation",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	// UTXO spending operations histogram
	prometheusTransactionSpendUtxos = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "validator",
			Name:      "transactions_spend_utxos",
			Help:      "Histogram of transaction spending utxos",
			Buckets:   util.MetricsBucketsMicroSeconds,
		},
	)

	// UTXO heights histogram
	getTransactionInputBlockHeights = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "validator",
			Name:      "transactions_input_block_heights",
			Help:      "Histogram of transaction input block heights",
			Buckets:   util.MetricsBucketsMicroSeconds,
		},
	)

	// 2-phase commit operations histogram
	prometheusTransaction2PhaseCommit = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "validator",
			Name:      "transactions_2phase_commit",
			Help:      "Histogram of 2-phase commit operations",
			Buckets:   util.MetricsBucketsMicroSeconds,
		},
	)

	// Overall transaction processing histogram
	prometheusValidateTransaction = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "validator",
			Name:      "transactions",
			Help:      "Histogram of transaction processing by the validator service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	// Transaction size histogram
	prometheusTransactionSize = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "validator",
			Name:      "transactions_size",
			Help:      "Size of transactions processed by the validator service",
			Buckets:   util.MetricsBucketsSize,
		},
	)

	// Block assembly operations histogram
	prometheusValidatorSendToBlockAssembly = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "validator",
			Name:      "send_to_block_assembly",
			Help:      "Histogram of sending transactions to block assembly",
			Buckets:   util.MetricsBucketsMicroSeconds,
		},
	)

	// Block validation Kafka operations histogram
	prometheusValidatorSendToBlockValidationKafka = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "validator",
			Name:      "send_to_blockvalidation_kafka",
			Help:      "Histogram of sending transactions to block validation kafka",
			Buckets:   util.MetricsBucketsMicroSeconds,
		},
	)

	// P2P Kafka operations histogram
	prometheusValidatorSendToP2PKafka = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "validator",
			Name:      "send_to_p2p_kafka",
			Help:      "Histogram of sending rejected transactions to p2p kafka",
			Buckets:   util.MetricsBucketsMicroSeconds,
		},
	)

	// Transaction metadata operations histogram
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
