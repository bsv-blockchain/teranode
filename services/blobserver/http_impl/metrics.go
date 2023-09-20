package http_impl

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prometheusBlobServerHttpGetTransaction     *prometheus.CounterVec
	prometheusBlobServerHttpGetSubtree         *prometheus.CounterVec
	prometheusBlobServerHttpGetBlockHeader     *prometheus.CounterVec
	prometheusBlobServerHttpGetBestBlockHeader *prometheus.CounterVec
	prometheusBlobServerHttpGetBlock           *prometheus.CounterVec
	prometheusBlobServerHttpGetLastNBlocks     *prometheus.CounterVec
	prometheusBlobServerHttpGetUTXO            *prometheus.CounterVec
)

var prometheusMetricsInitialized = false

func initPrometheusMetrics() {
	if prometheusMetricsInitialized {
		return
	}

	prometheusBlobServerHttpGetTransaction = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "blobserver",
			Name:      "http_get_transaction",
			Help:      "Number of Get transactions ops",
		},
		[]string{
			"function",  //function tracking the operation
			"operation", // type of operation achieved
		},
	)

	prometheusBlobServerHttpGetSubtree = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "blobserver",
			Name:      "http_get_subtree",
			Help:      "Number of Get subtree ops",
		},
		[]string{
			"function",  //function tracking the operation
			"operation", // type of operation achieved
		},
	)

	prometheusBlobServerHttpGetBlockHeader = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "blobserver",
			Name:      "http_get_block_header",
			Help:      "Number of Get block header ops",
		},
		[]string{
			"function",  //function tracking the operation
			"operation", // type of operation achieved
		},
	)

	prometheusBlobServerHttpGetLastNBlocks = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "blobserver",
			Name:      "http_get_last_n_blocks",
			Help:      "Number of Get last N blocks ops",
		},
		[]string{
			"function",  //function tracking the operation
			"operation", // type of operation achieved
		},
	)

	prometheusBlobServerHttpGetBestBlockHeader = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "blobserver",
			Name:      "http_get_best_block_header",
			Help:      "Number of Get best block header ops",
		},
		[]string{
			"function",  //function tracking the operation
			"operation", // type of operation achieved
		},
	)

	prometheusBlobServerHttpGetBlock = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "blobserver",
			Name:      "http_get_block",
			Help:      "Number of Get block ops",
		},
		[]string{
			"function",  //function tracking the operation
			"operation", // type of operation achieved
		},
	)

	prometheusBlobServerHttpGetUTXO = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "blobserver",
			Name:      "http_get_utxo",
			Help:      "Number of Get UTXO ops",
		},
		[]string{
			"function",  //function tracking the operation
			"operation", // type of operation achieved
		},
	)

	prometheusMetricsInitialized = true
}
