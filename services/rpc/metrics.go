// Package rpc implements the Bitcoin JSON-RPC API service for Teranode.
package rpc

// The metrics.go file implements a comprehensive performance monitoring system for the RPC service
// using Prometheus. It tracks latency distributions for all supported RPC commands, enabling
// detailed analysis of API performance, throughput, and error rates.
//
// The metrics system provides:
// - Per-command latency histograms with configurable buckets
// - Consistent naming patterns for easy dashboard creation
// - Thread-safe initialization through sync.Once
// - Integration with Teranode's global Prometheus registry
//
// These metrics enable operators to:
// - Identify performance bottlenecks in specific RPC commands
// - Track changes in API usage patterns over time
// - Detect anomalies that might indicate problems
// - Plan capacity based on actual usage patterns
//
// The metrics are exported through Teranode's standard Prometheus endpoint
// and can be visualized using Grafana or similar monitoring tools.
import (
	"sync"

	"github.com/bitcoin-sv/teranode/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Prometheus metrics for monitoring individual RPC command performance and latency.
// Each histogram tracks the processing time for a specific Bitcoin JSON-RPC command,
// enabling detailed performance analysis and identification of bottlenecks.
//
// The metrics cover all major RPC command categories:
//   - Block operations: GetBlock, GetBlockByHeight, GetBlockHash, GetBlockHeader, GetBestBlockHash
//   - Transaction operations: GetRawTransaction, CreateRawTransaction, SendRawTransaction
//   - Mining operations: Generate, GenerateToAddress, GetMiningCandidate, SubmitMiningSolution, GetMiningInfo
//   - Network operations: GetPeerInfo, SetBan, IsBanned, ListBanned, ClearBanned
//   - Blockchain info: GetBlockchainInfo, GetInfo, GetDifficulty
//   - Block management: InvalidateBlock, ReconsiderBlock
//   - UTXO operations: Freeze, Unfreeze, Reassign
//   - Help system: Help command
//
// All histograms use consistent bucket definitions optimized for RPC response times,
// typically ranging from sub-millisecond to several seconds depending on command complexity.
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
	prometheusHandleGenerateToAddress    prometheus.Histogram
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
	prometheusHandleIsBanned             prometheus.Histogram
	prometheusHandleListBanned           prometheus.Histogram
	prometheusHandleClearBanned          prometheus.Histogram
	prometheusHandleGetMiningInfo        prometheus.Histogram
	prometheusHandleFreeze               prometheus.Histogram
	prometheusHandleUnfreeze             prometheus.Histogram
	prometheusHandleReassign             prometheus.Histogram
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
	prometheusHandleGenerateToAddress = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "rpc",
			Name:      "generate_to_address",
			Help:      "Histogram of calls to handleGenerateToAddress in the rpc service",
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

	prometheusHandleIsBanned = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "rpc",
			Name:      "is_banned",
			Help:      "Histogram of calls to handleIsBanned in the rpc service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusHandleListBanned = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "rpc",
			Name:      "list_banned",
			Help:      "Histogram of calls to handleListBanned in the rpc service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusHandleClearBanned = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "rpc",
			Name:      "clear_banned",
			Help:      "Histogram of calls to handleClearBanned in the rpc service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusHandleFreeze = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "rpc",
			Name:      "freeze",
			Help:      "Histogram of calls to handleFreeze in the rpc service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusHandleUnfreeze = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "rpc",
			Name:      "unfreeze",
			Help:      "Histogram of calls to handleUnfreeze in the rpc service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusHandleReassign = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "rpc",
			Name:      "reassign",
			Help:      "Histogram of calls to handleReassign in the rpc service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
}
