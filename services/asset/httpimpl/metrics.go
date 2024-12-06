// Package httpimpl provides HTTP handlers for blockchain data retrieval and processing,
// including Prometheus metrics for monitoring.
package httpimpl

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Prometheus metrics variables for tracking various API operations.
// Each metric is a counter vector with "function" and "operation" labels.
var (
	// prometheusAssetHttpGetTransaction tracks individual transaction retrievals
	prometheusAssetHttpGetTransaction *prometheus.CounterVec

	// prometheusAssetHttpGetTransactions tracks batch transaction retrievals
	prometheusAssetHttpGetTransactions *prometheus.CounterVec

	// prometheusAssetHttpGetSubtree tracks subtree retrievals
	prometheusAssetHttpGetSubtree *prometheus.CounterVec

	// prometheusAssetHttpGetBlockHeader tracks block header retrievals
	prometheusAssetHttpGetBlockHeader *prometheus.CounterVec

	// prometheusAssetHttpGetBestBlockHeader tracks best block header retrievals
	prometheusAssetHttpGetBestBlockHeader *prometheus.CounterVec

	// prometheusAssetHttpGetBlock tracks block retrievals
	prometheusAssetHttpGetBlock *prometheus.CounterVec

	// prometheusAssetHttpGetBlockLegacy tracks legacy format block retrievals
	prometheusAssetHttpGetBlockLegacy *prometheus.CounterVec

	// prometheusAssetHttpGetLastNBlocks tracks multiple block retrievals
	prometheusAssetHttpGetLastNBlocks *prometheus.CounterVec

	// prometheusAssetHttpGetUTXO tracks UTXO retrievals
	prometheusAssetHttpGetUTXO *prometheus.CounterVec
)

// prometheusMetricsInitOnce ensures metrics are initialized exactly once
var (
	prometheusMetricsInitOnce sync.Once
)

// initPrometheusMetrics safely initializes all Prometheus metrics using sync.Once
// to ensure thread-safe single initialization.
func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

// _initPrometheusMetrics creates and registers all Prometheus metrics.
// All metrics follow the pattern:
//   - Namespace: "teranode"
//   - Subsystem: "asset"
//   - Labels:
//   - "function": Function tracking the operation
//   - "operation": Type of operation achieved
//
// Metric Types:
//   - All metrics are counters that track the number of operations
//   - Each counter tracks successful and failed operations separately
//
// Usage Example:
//
//	prometheusAssetHttpGetTransaction.WithLabelValues("OK", "200").Inc()
//
// Available Metrics:
//   - http_get_transaction: Individual transaction retrievals
//   - http_get_transactions: Batch transaction retrievals
//   - http_get_subtree: Subtree data retrievals
//   - http_get_block_header: Block header retrievals
//   - http_get_best_block_header: Best block header retrievals
//   - http_get_block: Block retrievals
//   - http_get_block_legacy: Legacy format block retrievals
//   - http_get_last_n_blocks: Multiple block retrievals
//   - http_get_utxo: UTXO retrievals
func _initPrometheusMetrics() {
	prometheusAssetHttpGetTransaction = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "asset",
			Name:      "http_get_transaction",
			Help:      "Number of Get transactions ops",
		},
		[]string{
			"function",  // function tracking the operation
			"operation", // type of operation achieved
		},
	)

	prometheusAssetHttpGetTransactions = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "asset",
			Name:      "http_get_transactions",
			Help:      "Number of Get transactions ops",
		},
		[]string{
			"function",  // function tracking the operation
			"operation", // type of operation achieved
		},
	)

	prometheusAssetHttpGetSubtree = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "asset",
			Name:      "http_get_subtree",
			Help:      "Number of Get subtree ops",
		},
		[]string{
			"function",  // function tracking the operation
			"operation", // type of operation achieved
		},
	)

	prometheusAssetHttpGetBlockHeader = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "asset",
			Name:      "http_get_block_header",
			Help:      "Number of Get block header ops",
		},
		[]string{
			"function",  // function tracking the operation
			"operation", // type of operation achieved
		},
	)

	prometheusAssetHttpGetLastNBlocks = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "asset",
			Name:      "http_get_last_n_blocks",
			Help:      "Number of Get last N blocks ops",
		},
		[]string{
			"function",  // function tracking the operation
			"operation", // type of operation achieved
		},
	)

	prometheusAssetHttpGetBestBlockHeader = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "asset",
			Name:      "http_get_best_block_header",
			Help:      "Number of Get best block header ops",
		},
		[]string{
			"function",  // function tracking the operation
			"operation", // type of operation achieved
		},
	)

	prometheusAssetHttpGetBlock = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "asset",
			Name:      "http_get_block",
			Help:      "Number of Get block ops",
		},
		[]string{
			"function",  // function tracking the operation
			"operation", // type of operation achieved
		},
	)

	prometheusAssetHttpGetBlockLegacy = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "asset",
			Name:      "http_get_block_legacy",
			Help:      "Number of Get legacy block ops",
		},
		[]string{
			"function",  // function tracking the operation
			"operation", // type of operation achieved
		},
	)

	prometheusAssetHttpGetUTXO = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "asset",
			Name:      "http_get_utxo",
			Help:      "Number of Get UTXO ops",
		},
		[]string{
			"function",  // function tracking the operation
			"operation", // type of operation achieved
		},
	)
}
