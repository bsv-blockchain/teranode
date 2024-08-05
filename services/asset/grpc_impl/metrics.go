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
	prometheusAssetGRPCGetBlockStats      prometheus.Counter
	prometheusAssetGRPCGetBlockGraphData  prometheus.Counter
	prometheusAssetGRPCGet                prometheus.Counter
	prometheusAssetGRPCExists             prometheus.Counter
	prometheusAssetGRPCSet                prometheus.Counter
	prometheusAssetGRPCSetTTL             prometheus.Counter
	prometheusAssetGRPCAddHttpSubscriber  prometheus.Counter
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

	prometheusAssetGRPCGetBlockStats = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "Asset",
			Name:      "grpc_get_block_stats",
			Help:      "Number of Get block stats ops",
		},
	)

	prometheusAssetGRPCGetBlockGraphData = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "Asset",
			Name:      "grpc_get_block_graph_data",
			Help:      "Number of Get block graph data ops",
		},
	)

	prometheusAssetGRPCGet = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "Asset",
			Name:      "grpc_get",
			Help:      "Number of Get subtree ops",
		},
	)

	prometheusAssetGRPCExists = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "Asset",
			Name:      "grpc_exists",
			Help:      "Number of Exists subtree ops",
		},
	)

	prometheusAssetGRPCSet = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "Asset",
			Name:      "grpc_set",
			Help:      "Number of Set subtree ops",
		},
	)

	prometheusAssetGRPCSetTTL = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "Asset",
			Name:      "grpc_set_ttl",
			Help:      "Number of Set TTL subtree ops",
		},
	)

	prometheusAssetGRPCAddHttpSubscriber = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "Asset",
			Name:      "grpc_add_http_subscriber",
			Help:      "Number of Add HTTP subscriber ops",
		},
	)
}
