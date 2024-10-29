package rpc

import (
	"sync"

	"github.com/bitcoin-sv/ubsv/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// prometheusHealth                     prometheus.Counter
	prometheusHandleGetBlock             prometheus.Histogram
	prometheusHandleGetBlockByHeight     prometheus.Histogram
	prometheusHandleGetBlockHash         prometheus.Histogram
	prometheusHandleGetBlockHeader       prometheus.Histogram
	prometheusHandleGetBestBlockHash     prometheus.Histogram
	prometheusHandleGetRawTransaction    prometheus.Histogram
	prometheusHandleCreateRawTransaction prometheus.Histogram
	prometheusHandleSendRawTransaction   prometheus.Histogram
	prometheusHandleGenerate             prometheus.Histogram
	prometheusHandleGetMiningCandidate   prometheus.Histogram
	prometheusHandleSubmitMiningSolution prometheus.Histogram
	prometheusHandleGetpeerinfo          prometheus.Histogram
	prometheusHandleGetblockchaininfo    prometheus.Histogram
	prometheusHandleGetinfo              prometheus.Histogram
	prometheusHandleGetDifficulty        prometheus.Histogram
	prometheusHandleInvalidateBlock      prometheus.Histogram
	prometheusHandleReconsiderBlock      prometheus.Histogram
	prometheusHandleHelp                 prometheus.Histogram
	prometheusHandleSetBan               prometheus.Histogram
)

var (
	prometheusMetricsInitOnce sync.Once
)

func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

func _initPrometheusMetrics() {
	// prometheusHealth = promauto.NewCounter(
	//	prometheus.CounterOpts{
	//		Namespace: "rpc",
	//		Name:      "health",
	//		Help:      "Number of calls to the health endpoint of the rpc service",
	//	},
	//)
	prometheusHandleGetBlock = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "rpc",
			Name:      "get_block",
			Help:      "Histogram of calls to handleGetBlock in the rpc service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusHandleGetBlockByHeight = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "rpc",
			Name:      "get_block_by_height",
			Help:      "Histogram of calls to handleGetBlockByHeight in the rpc service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusHandleGetBlockHash = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "rpc",
			Name:      "get_block_hash",
			Help:      "Histogram of calls to handleGetBlockHash in the rpc service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusHandleGetBlockHeader = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "rpc",
			Name:      "get_block_header",
			Help:      "Histogram of calls to handleGetBlockHeader in the rpc service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusHandleGetBestBlockHash = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "rpc",
			Name:      "get_best_block_hash",
			Help:      "Histogram of calls to handleGetBestBlockHash in the rpc service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusHandleGetRawTransaction = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "rpc",
			Name:      "get_raw_transaction",
			Help:      "Histogram of calls to handleGetRawTransaction in the rpc service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusHandleCreateRawTransaction = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "rpc",
			Name:      "create_raw_transaction",
			Help:      "Histogram of calls to handleCreateRawTransaction in the rpc service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusHandleSendRawTransaction = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "rpc",
			Name:      "send_raw_transaction",
			Help:      "Histogram of calls to handleSendRawTransaction in the rpc service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusHandleGenerate = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "rpc",
			Name:      "generate",
			Help:      "Histogram of calls to handleGenerate in the rpc service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusHandleGetMiningCandidate = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "rpc",
			Name:      "get_mining_candidate",
			Help:      "Histogram of calls to handleGetMiningCandidate in the rpc service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusHandleSubmitMiningSolution = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "rpc",
			Name:      "submit_mining_solution",
			Help:      "Histogram of calls to handleSubmitMiningSolution in the rpc service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusHandleGetpeerinfo = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "rpc",
			Name:      "get_peer_info",
			Help:      "Histogram of calls to handleGetpeerinfo in the rpc service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusHandleGetblockchaininfo = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "rpc",
			Name:      "get_blockchain_info",
			Help:      "Histogram of calls to handleGetblockchaininfo in the rpc service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusHandleGetinfo = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "rpc",
			Name:      "get_info",
			Help:      "Histogram of calls to handleGetinfo in the rpc service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusHandleGetDifficulty = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "rpc",
			Name:      "get_difficulty",
			Help:      "Histogram of calls to handleGetDifficulty in the rpc service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusHandleInvalidateBlock = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "rpc",
			Name:      "invalidate_block",
			Help:      "Histogram of calls to handleInvalidateBlock in the rpc service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusHandleReconsiderBlock = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "rpc",
			Name:      "reconsider_block",
			Help:      "Histogram of calls to handleReconsiderBlock in the rpc service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusHandleHelp = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "rpc",
			Name:      "help",
			Help:      "Histogram of calls to handleHelp in the rpc service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusHandleSetBan = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "rpc",
			Name:      "set_ban",
			Help:      "Histogram of calls to handleSetBan in the rpc service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
}
