package grpc_impl

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prometheusBlobServerGRPCHealth             prometheus.Counter
	prometheusBlobServerGRPCGetBlockHeader     prometheus.Counter
	prometheusBlobServerGRPCGetBlockHeaders    prometheus.Counter
	prometheusBlobServerGRPCGetBestBlockHeader prometheus.Counter
	prometheusBlobServerGRPCGetBlock           prometheus.Counter
	prometheusBlobServerGRPCGetNodes           prometheus.Counter
	prometheusBlobServerGRPCSubscribe          prometheus.Counter
)

var prometheusMetricsInitialized = false

func initPrometheusMetrics() {
	if prometheusMetricsInitialized {
		return
	}

	prometheusBlobServerGRPCHealth = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blobserver",
			Name:      "grpc_health",
			Help:      "Number of calls to the health endpoint of the blobserver service",
		},
	)

	prometheusBlobServerGRPCGetBlockHeader = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blobserver",
			Name:      "grpc_get_block_header",
			Help:      "Number of Get block header ops",
		},
	)

	prometheusBlobServerGRPCGetBlockHeaders = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blobserver",
			Name:      "grpc_get_block_headers",
			Help:      "Number of Get block headers ops",
		},
	)

	prometheusBlobServerGRPCGetBestBlockHeader = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blobserver",
			Name:      "grpc_get_best_block_header",
			Help:      "Number of Get best block header ops",
		},
	)

	prometheusBlobServerGRPCGetBlock = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blobserver",
			Name:      "grpc_get_block",
			Help:      "Number of Get block ops",
		},
	)

	prometheusBlobServerGRPCGetNodes = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blobserver",
			Name:      "grpc_get_nodes",
			Help:      "Number of get nodes ops",
		},
	)

	prometheusBlobServerGRPCSubscribe = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blobserver",
			Name:      "grpc_subscribe",
			Help:      "Number of subscription ops",
		},
	)

	prometheusMetricsInitialized = true
}
