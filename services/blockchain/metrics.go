// Package blockchain provides Prometheus metrics for blockchain operations.
//
// This file defines the Prometheus metrics used to monitor the performance and health of
// the blockchain service. These metrics cover various aspects of blockchain operations
// including block processing, retrievals, state management, and API request handling.
//
// The metrics are registered with Prometheus through the promauto factory to ensure proper
// initialization and registration with the metrics registry. They are designed to track:
// - Request latency for various operations (histograms)
// - Call counts for health checks (counters)
// - Current service state (gauges)
//
// These metrics enable comprehensive monitoring of the blockchain service behavior in
// production environments and help diagnose performance issues.
package blockchain

import (
	"sync"

	"github.com/bsv-blockchain/teranode/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prometheusBlockchainHealth                               prometheus.Counter
	prometheusBlockchainAddBlock                             prometheus.Histogram
	prometheusBlockchainGetBlock                             prometheus.Histogram
	prometheusBlockchainGetBlockStats                        prometheus.Histogram
	prometheusBlockchainGetBlockGraphData                    prometheus.Histogram
	prometheusBlockchainGetLastNBlocks                       prometheus.Histogram
	prometheusBlockchainGetSuitableBlock                     prometheus.Histogram
	prometheusBlockchainGetHashOfAncestorBlock               prometheus.Histogram
	prometheusBlockchainGetLatestBlockHeaderFromBlockLocator prometheus.Histogram
	prometheusBlockchainGetBlockHeadersFromOldest            prometheus.Histogram
	prometheusBlockchainGetNextWorkRequired                  prometheus.Histogram
	prometheusBlockchainGetBlockExists                       prometheus.Histogram
	prometheusBlockchainGetBestBlockHeader                   prometheus.Histogram
	prometheusBlockchainCheckBlockIsInCurrentChain           prometheus.Histogram
	prometheusBlockchainCheckBlockIsAncestorOfBlock          prometheus.Histogram
	prometheusBlockchainGetChainTips                         prometheus.Histogram
	prometheusBlockchainGetBlockHeader                       prometheus.Histogram
	prometheusBlockchainGetBlockHeaders                      prometheus.Histogram
	prometheusBlockchainGetBlockHeadersFromHeight            prometheus.Histogram
	prometheusBlockchainGetBlockHeadersByHeight              prometheus.Histogram
	prometheusBlockchainGetBlocksByHeight                    prometheus.Histogram
	prometheusBlockchainSubscribe                            prometheus.Histogram
	prometheusBlockchainGetState                             prometheus.Histogram
	prometheusBlockchainSetState                             prometheus.Histogram
	prometheusBlockchainGetBlockHeaderIDs                    prometheus.Histogram
	prometheusBlockchainInvalidateBlock                      prometheus.Histogram
	prometheusBlockchainRevalidateBlock                      prometheus.Histogram
	prometheusBlockchainSendNotification                     prometheus.Histogram
	prometheusBlockchainGetBlockIsMined                      prometheus.Histogram
	prometheusBlockchainSetBlockMinedSet                     prometheus.Histogram
	prometheusBlockchainGetBlocksMinedNotSet                 prometheus.Histogram
	prometheusBlockchainSetBlockPersistedAt                  prometheus.Histogram
	prometheusBlockchainGetBlocksNotPersisted                prometheus.Histogram
	prometheusBlockchainSetBlockSubtreesSet                  prometheus.Histogram
	prometheusBlockchainGetBlocksSubtreesNotSet              prometheus.Histogram
	prometheusBlockchainFSMCurrentState                      prometheus.Gauge
	prometheusBlockchainGetFSMCurrentState                   prometheus.Histogram
	prometheusBlockchainGetBlockLocator                      prometheus.Histogram
	prometheusBlockchainLocateBlockHeaders                   prometheus.Histogram
	// prometheusExportBlockDb                        prometheus.Histogram
)

var (
	prometheusMetricsInitOnce sync.Once
)

// initPrometheusMetrics initializes all Prometheus metrics.
// This function is called once during package initialization.
func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

// _initPrometheusMetrics is the actual implementation of metrics initialization.
// It's called by initPrometheusMetrics through sync.Once to ensure single initialization.
func _initPrometheusMetrics() {
	prometheusBlockchainHealth = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "blockchain",
			Name:      "health",
			Help:      "Histogram of calls to the health endpoint of the blockchain service",
		},
	)

	prometheusBlockchainAddBlock = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockchain",
			Name:      "add_block",
			Help:      "Histogram of block added to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainGetBlock = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockchain",
			Name:      "get_block",
			Help:      "Histogram of Get block calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainGetBlockStats = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockchain",
			Name:      "get_block_stats",
			Help:      "Histogram of Get block stats calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainGetBlockGraphData = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockchain",
			Name:      "get_block_graph_data",
			Help:      "Histogram of Get block graph data calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainGetLastNBlocks = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockchain",
			Name:      "get_last_n_block",
			Help:      "Histogram of GetLastNBlocks calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainGetSuitableBlock = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockchain",
			Name:      "get_suitable_block",
			Help:      "Histogram of GetSuitableBlock calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusBlockchainGetHashOfAncestorBlock = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockchain",
			Name:      "get_hash_of_ancestor_block",
			Help:      "Histogram of GetHashOfAncestorBlock calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusBlockchainGetLatestBlockHeaderFromBlockLocator = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockchain",
			Name:      "get_latest_block_header_from_block_locator",
			Help:      "Histogram of GetLatestBlockHeaderFromBlockLocator calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusBlockchainGetBlockHeadersFromOldest = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockchain",
			Name:      "get_block_headers_from_oldest",
			Help:      "Histogram of GetBlockHeadersFromOldest calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusBlockchainGetNextWorkRequired = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockchain",
			Name:      "get_next_work_required",
			Help:      "Histogram of GetNextWorkRequired calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainGetBlockExists = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockchain",
			Name:      "get_block_exists",
			Help:      "Histogram of GetBlockExists calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainGetBestBlockHeader = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockchain",
			Name:      "get_get_best_block_header",
			Help:      "Histogram of GetBestBlockHeader calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainCheckBlockIsInCurrentChain = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockchain",
			Name:      "check_block_is_in_current_chain",
			Help:      "Histogram of CheckBlockIsInCurrentChain calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainCheckBlockIsAncestorOfBlock = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockchain",
			Name:      "check_block_is_ancestor_of_block",
			Help:      "Histogram of CheckBlockIsAncestorOfBlock calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainGetChainTips = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockchain",
			Name:      "get_chain_tips",
			Help:      "Histogram of GetChainTips calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainGetBlockHeader = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockchain",
			Name:      "get_get_block_header",
			Help:      "Histogram of GetBlockHeader calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainGetBlockHeaders = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockchain",
			Name:      "get_get_block_headers",
			Help:      "Histogram of GetBlockHeaders calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainGetBlockHeadersFromHeight = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockchain",
			Name:      "get_get_block_headers_from_height",
			Help:      "Histogram of GetBlockHeadersFromHeight calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainGetBlockHeadersByHeight = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockchain",
			Name:      "get_get_block_headers_by_height",
			Help:      "Histogram of GetBlockHeadersByHeight calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainGetBlocksByHeight = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockchain",
			Name:      "get_blocks_by_height",
			Help:      "Histogram of GetBlocksByHeight calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainSubscribe = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockchain",
			Name:      "subscribe",
			Help:      "Histogram of Subscribe calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainGetState = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockchain",
			Name:      "get_state",
			Help:      "Histogram of GetState calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainSetState = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockchain",
			Name:      "set_state",
			Help:      "Histogram of SetState calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainGetBlockHeaderIDs = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockchain",
			Name:      "get_block_header_ids",
			Help:      "Histogram of GetBlockHeaderIDs calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainInvalidateBlock = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockchain",
			Name:      "invalidate_block",
			Help:      "Histogram of InvalidateBlock calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainRevalidateBlock = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockchain",
			Name:      "revalidate_block",
			Help:      "Histogram of RevalidateBlock calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainSendNotification = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockchain",
			Name:      "send_notification",
			Help:      "Histogram of SendNotification calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainGetBlockIsMined = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockchain",
			Name:      "get_block_is_mined",
			Help:      "Histogram of GetBlockIsMined calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainSetBlockMinedSet = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockchain",
			Name:      "set_block_mined_set",
			Help:      "Histogram of SetBlockMinedSet calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainGetBlocksMinedNotSet = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockchain",
			Name:      "get_blocks_mined_not_set",
			Help:      "Histogram of GetBlocksMinedNotSet calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainSetBlockPersistedAt = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockchain",
			Name:      "set_block_persisted_at",
			Help:      "Histogram of SetBlockPersistedAt calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainGetBlocksNotPersisted = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockchain",
			Name:      "get_blocks_not_persisted",
			Help:      "Histogram of GetBlocksNotPersisted calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainSetBlockSubtreesSet = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockchain",
			Name:      "set_block_subtrees_set",
			Help:      "Histogram of SetBlockSubtreesSet calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainGetBlocksSubtreesNotSet = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockchain",
			Name:      "get_blocks_subtrees_not_set",
			Help:      "Histogram of GetBlocksSubtreesNotSet calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainFSMCurrentState = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "blockchain",
			Name:      "fsm_current_state",
			Help:      "Current state of the blockchain FSM",
		},
	)

	prometheusBlockchainGetFSMCurrentState = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockchain",
			Name:      "get_fsm_current_state",
			Help:      "Histogram of GetFSMCurrentState calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainGetBlockLocator = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockchain",
			Name:      "get_block_locator",
			Help:      "Histogram of GetBlockLocator calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainLocateBlockHeaders = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockchain",
			Name:      "locate_block_headers",
			Help:      "Histogram of LocateBlockHeaders calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
}

// prometheusExportBlockDb = promauto.NewHistogram(
//	prometheus.HistogramOpts{
//		Namespace: "blockchain",
//		Name:      "export_block_db",
//		Help:      "Histogram of ExportBlockDB calls to the blockchain service",
//		Buckets:   util.MetricsBucketsMilliSeconds,
//	},
// )
