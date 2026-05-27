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
	// prometheusAssetHTTPGetTransaction tracks individual transaction retrievals
	prometheusAssetHTTPGetTransaction *prometheus.CounterVec

	// prometheusAssetHTTPGetTransactions tracks batch transaction retrievals
	prometheusAssetHTTPGetTransactions *prometheus.CounterVec

	// prometheusAssetHTTPGetSubtree tracks subtree retrievals
	prometheusAssetHTTPGetSubtree *prometheus.CounterVec

	// prometheusAssetHTTPGetBlockHeader tracks block header retrievals
	prometheusAssetHTTPGetBlockHeader *prometheus.CounterVec

	// prometheusAssetHTTPGetBestBlockHeader tracks best block header retrievals
	prometheusAssetHTTPGetBestBlockHeader *prometheus.CounterVec

	// prometheusAssetHTTPGetBlock tracks block retrievals
	prometheusAssetHTTPGetBlock *prometheus.CounterVec

	// prometheusAssetHTTPGetBlockLegacy tracks legacy format block retrievals
	prometheusAssetHTTPGetBlockLegacy *prometheus.CounterVec

	// prometheusAssetHTTPGetSubtreeData tracks subtree data retrievals
	prometheusAssetHTTPGetSubtreeData *prometheus.CounterVec

	// prometheusAssetHTTPGetLastNBlocks tracks multiple block retrievals
	prometheusAssetHTTPGetLastNBlocks *prometheus.CounterVec

	// prometheusAssetHTTPGetUTXO tracks UTXO retrievals
	prometheusAssetHTTPGetUTXO *prometheus.CounterVec

	// prometheusAssetHTTPGetMerkleProof tracks merkle proof retrievals
	prometheusAssetHTTPGetMerkleProof *prometheus.CounterVec

	// prometheusAssetHTTPProxyPropagationTx tracks proxied transaction submissions
	prometheusAssetHTTPProxyPropagationTx *prometheus.CounterVec

	// prometheusAssetHTTPRequestDuration tracks HTTP request duration by route
	prometheusAssetHTTPRequestDuration *prometheus.HistogramVec

	// prometheusAssetHTTPResponseSize tracks HTTP response sizes by route
	prometheusAssetHTTPResponseSize *prometheus.HistogramVec

	// prometheusAssetHTTPRateLimited counts rate-limited requests by scope (global/heavy)
	prometheusAssetHTTPRateLimited *prometheus.CounterVec

	// prometheusAssetHTTPPeerAuthResult counts the outcome of peer-auth attempts.
	// Labels: "ok" | "expired" | "bad_sig" | "bad_digest" | "replay" | "unknown_key" | "not_allowlisted".
	// Operators watch the non-ok rates to spot clock drift, key rotation, or
	// flooding attacks; absence of "ok" counts after deploy means no signed
	// peers are reaching the middleware.
	prometheusAssetHTTPPeerAuthResult *prometheus.CounterVec

	// prometheusAssetHTTPInFlight tracks in-flight HTTP requests
	prometheusAssetHTTPInFlight prometheus.Gauge
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
//   - http_get_merkle_proof: Merkle proof retrievals
func _initPrometheusMetrics() {
	prometheusAssetHTTPGetTransaction = promauto.NewCounterVec(
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

	prometheusAssetHTTPGetTransactions = promauto.NewCounterVec(
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

	prometheusAssetHTTPGetSubtree = promauto.NewCounterVec(
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

	prometheusAssetHTTPGetBlockHeader = promauto.NewCounterVec(
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

	prometheusAssetHTTPGetLastNBlocks = promauto.NewCounterVec(
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

	prometheusAssetHTTPGetBestBlockHeader = promauto.NewCounterVec(
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

	prometheusAssetHTTPGetBlock = promauto.NewCounterVec(
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

	prometheusAssetHTTPGetBlockLegacy = promauto.NewCounterVec(
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

	prometheusAssetHTTPGetSubtreeData = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "asset",
			Name:      "http_get_subtree_data",
			Help:      "Number of Get subtree data ops",
		},
		[]string{
			"function",  // function tracking the operation
			"operation", // type of operation achieved
		},
	)

	prometheusAssetHTTPGetUTXO = promauto.NewCounterVec(
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

	prometheusAssetHTTPGetMerkleProof = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "asset",
			Name:      "http_get_merkle_proof",
			Help:      "Number of Get merkle proof ops",
		},
		[]string{
			"function",  // function tracking the operation
			"operation", // type of operation achieved
		},
	)

	prometheusAssetHTTPProxyPropagationTx = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "asset",
			Name:      "http_proxy_propagation_tx",
			Help:      "Number of proxied transaction submissions to propagation service",
		},
		[]string{
			"function",  // function tracking the operation
			"operation", // type of operation achieved
		},
	)

	prometheusAssetHTTPRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "asset",
			Name:      "http_request_duration_seconds",
			Help:      "HTTP request duration in seconds",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"method", "path", "status"},
	)

	prometheusAssetHTTPResponseSize = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "asset",
			Name:      "http_response_size_bytes",
			Help:      "HTTP response size in bytes",
			Buckets:   prometheus.ExponentialBuckets(256, 4, 10), // 256B to ~64MB
		},
		[]string{"method", "path", "status"},
	)

	prometheusAssetHTTPRateLimited = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "asset",
			Name:      "http_rate_limited_total",
			Help:      "Number of rate-limited HTTP requests",
		},
		[]string{"scope"}, // "global" or "heavy" — identifies which rate limiter triggered
	)

	prometheusAssetHTTPPeerAuthResult = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "asset",
			Name:      "http_peer_auth_result_total",
			Help:      "Outcome of peer-auth attempts on the asset HTTP API",
		},
		[]string{"result"}, // ok | expired | bad_sig | bad_digest | replay | unknown_key | not_allowlisted
	)

	prometheusAssetHTTPInFlight = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "asset",
			Name:      "http_in_flight_requests",
			Help:      "Number of currently in-flight HTTP requests",
		},
	)
}
