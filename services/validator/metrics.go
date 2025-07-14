/*
Package validator implements comprehensive Bitcoin SV transaction validation functionality
for the Teranode blockchain node. This service provides critical validation operations
including transaction structure validation, script execution, UTXO verification, and
consensus rule enforcement to ensure blockchain integrity and compliance.

The validator service integrates with multiple Teranode components:
- UTXO store for unspent transaction output verification
- Script engine for Bitcoin script execution and validation  
- Block processor for coordinated validation workflows
- Mempool for transaction pre-validation before block inclusion

This metrics.go file implements Prometheus metrics collection for the validator service,
providing detailed monitoring and observability of transaction validation operations.
The metrics track performance, error rates, validation times, and resource utilization
to enable comprehensive monitoring of the validation pipeline's health and efficiency.

Key metrics categories include:
- Health check monitoring and service availability
- Transaction validation performance and timing
- Script validation execution metrics
- Batch processing performance tracking
- UTXO operations and database interactions
- Error rates and validation failure analysis
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

	// prometheusInvalidTransactions counts the total number of transactions that failed validation.
	// This counter increments for each transaction that violates consensus rules, has invalid scripts,
	// or fails structural validation checks. High values may indicate network attacks or client issues.
	prometheusInvalidTransactions prometheus.Counter

	// prometheusTransactionValidateTotal measures the complete end-to-end validation time for transactions.
	// This histogram tracks the total time spent validating a transaction from initial receipt through
	// final validation completion, including all validation steps and database operations. Units: seconds.
	prometheusTransactionValidateTotal prometheus.Histogram

	// prometheusTransactionValidate measures the time spent in individual transaction validation steps.
	// This histogram captures the duration of core validation operations excluding script execution,
	// such as structure validation, input/output checks, and consensus rule verification. Units: seconds.
	prometheusTransactionValidate prometheus.Histogram


	// prometheusTransactionExend measures transaction extension operations
	prometheusTransactionExtend prometheus.Histogram

	// prometheusTransactionValidateScripts measures individual validation script steps
	prometheusTransactionValidateScripts prometheus.Histogram

	// prometheusTransactionValidateBatch measures the performance of batch validation operations.
	// This histogram tracks the time required to validate multiple transactions together, enabling
	// analysis of batch processing efficiency and optimization opportunities. Units: seconds.
	prometheusTransactionValidateBatch prometheus.Histogram

	// prometheusTransactionSpendUtxos measures the time spent processing UTXO spending operations.
	// This histogram tracks database operations for retrieving, validating, and marking UTXOs as spent
	// during transaction validation. High values may indicate database performance issues. Units: seconds.
	prometheusTransactionSpendUtxos prometheus.Histogram

	// getTransactionInputBlockHeights measures the time taken to retrieve UTXO block heights.
	// This histogram tracks database queries to determine the block height of transaction inputs,
	// which is required for certain consensus rules and validation checks. Units: seconds.
	getTransactionInputBlockHeights prometheus.Histogram

	// prometheusTransaction2PhaseCommit measures the time spent in 2-phase commit operations.
	// This histogram tracks the duration of distributed transaction coordination, including
	// prepare and commit phases for ensuring data consistency across multiple services. Units: seconds.
	prometheusTransaction2PhaseCommit prometheus.Histogram

	// prometheusValidateTransaction measures the overall time spent processing transactions.
	// This histogram captures the complete transaction processing pipeline from receipt through
	// final validation, including all validation steps, database operations, and result handling. Units: seconds.
	prometheusValidateTransaction prometheus.Histogram

	// prometheusTransactionSize tracks the distribution of transaction sizes processed by the validator.
	// This histogram provides insights into transaction size patterns, helping optimize memory allocation
	// and processing strategies for different transaction types. Units: bytes.
	prometheusTransactionSize prometheus.Histogram

	// prometheusValidatorSendToBlockAssembly measures the time spent sending validated transactions to block assembly.
	// This histogram tracks the duration of communication with the block assembly service, including
	// message serialization, network transmission, and acknowledgment processing. Units: seconds.
	prometheusValidatorSendToBlockAssembly prometheus.Histogram

	// prometheusValidatorSendToBlockValidationKafka measures Kafka publishing operations for block validation events.
	// This histogram tracks the time required to publish validation results to Kafka topics used for
	// block validation coordination and inter-service communication. Units: seconds.
	prometheusValidatorSendToBlockValidationKafka prometheus.Histogram

	// prometheusValidatorSendToP2PKafka measures Kafka publishing operations for peer-to-peer network events.
	// This histogram tracks the duration of publishing transaction validation results to P2P Kafka topics,
	// enabling efficient propagation of validation status across the network. Units: seconds.
	prometheusValidatorSendToP2PKafka prometheus.Histogram

	// prometheusValidatorSetTxMeta measures the time spent updating transaction metadata.
	// This histogram tracks database operations for storing and updating transaction metadata,
	// including validation status, processing timestamps, and related transaction information. Units: seconds.
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

	prometheusTransactionExtend = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "validator",
			Name:      "transactions_extend",
			Help:      "Histogram of transaction extension operations",
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
