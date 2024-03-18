package grpc_impl

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"sync"
)

var (
	prometheusAssetGRPCHealth             prometheus.Counter
	prometheusAssetGRPCGetBlockHeader     prometheus.Counter
	prometheusAssetGRPCGetBlockHeaders    prometheus.Counter
	prometheusAssetGRPCGetBestBlockHeader prometheus.Counter
	prometheusAssetGRPCGetBlock           prometheus.Counter
	prometheusAssetGRPCGetNodes           prometheus.Counter
	prometheusAssetGRPCSubscribe          prometheus.Counter
)

var (
	prometheusMetricsInitOnce sync.Once
)

func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

func _initPrometheusMetrics() {
	prometheusAssetGRPCHealth = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "Asset",
			Name:      "grpc_health",
			Help:      "Number of calls to the health endpoint of the Asset service",
		},
	)

	prometheusAssetGRPCGetBlockHeader = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "Asset",
			Name:      "grpc_get_block_header",
			Help:      "Number of Get block header ops",
		},
	)

	prometheusAssetGRPCGetBlockHeaders = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "Asset",
			Name:      "grpc_get_block_headers",
			Help:      "Number of Get block headers ops",
		},
	)

	prometheusAssetGRPCGetBestBlockHeader = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "Asset",
			Name:      "grpc_get_best_block_header",
			Help:      "Number of Get best block header ops",
		},
	)

	prometheusAssetGRPCGetBlock = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "Asset",
			Name:      "grpc_get_block",
			Help:      "Number of Get block ops",
		},
	)

	prometheusAssetGRPCGetNodes = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "Asset",
			Name:      "grpc_get_nodes",
			Help:      "Number of get nodes ops",
		},
	)

	prometheusAssetGRPCSubscribe = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "Asset",
			Name:      "grpc_subscribe",
			Help:      "Number of subscription ops",
		},
	)
}
