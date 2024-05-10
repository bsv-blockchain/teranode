package http_impl

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"sync"
)

var (
	prometheusAssetHttpGetTransaction     *prometheus.CounterVec
	prometheusAssetHttpGetTransactions    *prometheus.CounterVec
	prometheusAssetHttpGetSubtree         *prometheus.CounterVec
	prometheusAssetHttpGetBlockHeader     *prometheus.CounterVec
	prometheusAssetHttpGetBestBlockHeader *prometheus.CounterVec
	prometheusAssetHttpGetBlock           *prometheus.CounterVec
	prometheusAssetHttpGetBlockLegacy     *prometheus.CounterVec
	prometheusAssetHttpGetLastNBlocks     *prometheus.CounterVec
	prometheusAssetHttpGetUTXO            *prometheus.CounterVec
)

var (
	prometheusMetricsInitOnce sync.Once
)

func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

func _initPrometheusMetrics() {
	prometheusAssetHttpGetTransaction = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "Asset",
			Name:      "http_get_transaction",
			Help:      "Number of Get transactions ops",
		},
		[]string{
			"function",  //function tracking the operation
			"operation", // type of operation achieved
		},
	)

	prometheusAssetHttpGetTransactions = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "Asset",
			Name:      "http_get_transactions",
			Help:      "Number of Get transactions ops",
		},
		[]string{
			"function",  //function tracking the operation
			"operation", // type of operation achieved
		},
	)

	prometheusAssetHttpGetSubtree = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "Asset",
			Name:      "http_get_subtree",
			Help:      "Number of Get subtree ops",
		},
		[]string{
			"function",  //function tracking the operation
			"operation", // type of operation achieved
		},
	)

	prometheusAssetHttpGetBlockHeader = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "Asset",
			Name:      "http_get_block_header",
			Help:      "Number of Get block header ops",
		},
		[]string{
			"function",  //function tracking the operation
			"operation", // type of operation achieved
		},
	)

	prometheusAssetHttpGetLastNBlocks = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "Asset",
			Name:      "http_get_last_n_blocks",
			Help:      "Number of Get last N blocks ops",
		},
		[]string{
			"function",  //function tracking the operation
			"operation", // type of operation achieved
		},
	)

	prometheusAssetHttpGetBestBlockHeader = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "Asset",
			Name:      "http_get_best_block_header",
			Help:      "Number of Get best block header ops",
		},
		[]string{
			"function",  //function tracking the operation
			"operation", // type of operation achieved
		},
	)

	prometheusAssetHttpGetBlock = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "Asset",
			Name:      "http_get_block",
			Help:      "Number of Get block ops",
		},
		[]string{
			"function",  //function tracking the operation
			"operation", // type of operation achieved
		},
	)

	prometheusAssetHttpGetBlockLegacy = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "Asset",
			Name:      "http_get_block_legacy",
			Help:      "Number of Get legacy block ops",
		},
		[]string{
			"function",  //function tracking the operation
			"operation", // type of operation achieved
		},
	)

	prometheusAssetHttpGetUTXO = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "Asset",
			Name:      "http_get_utxo",
			Help:      "Number of Get UTXO ops",
		},
		[]string{
			"function",  //function tracking the operation
			"operation", // type of operation achieved
		},
	)
}
