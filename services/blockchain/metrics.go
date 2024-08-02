package blockchain

import (
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"sync"
)

var (
	prometheusBlockchainHealth                    prometheus.Counter
	prometheusBlockchainAddBlock                  prometheus.Histogram
	prometheusBlockchainGetBlock                  prometheus.Histogram
	prometheusBlockchainGetBlockStats             prometheus.Histogram
	prometheusBlockchainGetBlockGraphData         prometheus.Histogram
	prometheusBlockchainGetLastNBlocks            prometheus.Histogram
	prometheusBlockchainGetSuitableBlock          prometheus.Histogram
	prometheusBlockchainGetHashOfAncestorBlock    prometheus.Histogram
	prometheusBlockchainGetNextWorkRequired       prometheus.Histogram
	prometheusBlockchainGetBlockExists            prometheus.Histogram
	prometheusBlockchainGetBestBlockHeader        prometheus.Histogram
	prometheusBlockchainGetBlockHeader            prometheus.Histogram
	prometheusBlockchainGetBlockHeaders           prometheus.Histogram
	prometheusBlockchainGetBlockHeadersFromHeight prometheus.Histogram
	prometheusBlockchainSubscribe                 prometheus.Histogram
	prometheusBlockchainGetState                  prometheus.Histogram
	prometheusBlockchainSetState                  prometheus.Histogram
	prometheusBlockchainGetBlockHeaderIDs         prometheus.Histogram
	prometheusBlockchainInvalidateBlock           prometheus.Histogram
	prometheusBlockchainRevalidateBlock           prometheus.Histogram
	prometheusBlockchainSendNotification          prometheus.Histogram
	prometheusBlockchainSetBlockMinedSet          prometheus.Histogram
	prometheusBlockchainGetBlocksMinedNotSet      prometheus.Histogram
	prometheusBlockchainSetBlockSubtreesSet       prometheus.Histogram
	prometheusBlockchainGetBlocksSubtreesNotSet   prometheus.Histogram
	prometheusBlockchainGetFSMCurrentState        prometheus.Histogram
	prometheusBlockchainGetBlockLocator           prometheus.Histogram
	prometheusBlockchainLocateBlockHeaders        prometheus.Histogram
)

var (
	prometheusMetricsInitOnce sync.Once
)

func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

func _initPrometheusMetrics() {
	prometheusBlockchainHealth = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockchain",
			Name:      "health",
			Help:      "Histogram of calls to the health endpoint of the blockchain service",
		},
	)

	prometheusBlockchainAddBlock = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockchain",
			Name:      "add_block",
			Help:      "Histogram of block added to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainGetBlock = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockchain",
			Name:      "get_block",
			Help:      "Histogram of Get block calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainGetBlockStats = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockchain",
			Name:      "get_block_stats",
			Help:      "Histogram of Get block stats calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainGetBlockGraphData = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockchain",
			Name:      "get_block_graph_data",
			Help:      "Histogram of Get block graph data calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainGetLastNBlocks = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockchain",
			Name:      "get_last_n_block",
			Help:      "Histogram of GetLastNBlocks calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainGetSuitableBlock = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockchain",
			Name:      "get_suitable_block",
			Help:      "Histogram of GetSuitableBlock calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusBlockchainGetHashOfAncestorBlock = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockchain",
			Name:      "get_hash_of_ancestor_block",
			Help:      "Histogram of GetHashOfAncestorBlock calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusBlockchainGetNextWorkRequired = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockchain",
			Name:      "get_next_work_required",
			Help:      "Histogram of GetNextWorkRequired calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainGetBlockExists = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockchain",
			Name:      "get_block_exists",
			Help:      "Histogram of GetBlockExists calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainGetBestBlockHeader = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockchain",
			Name:      "get_get_best_block_header",
			Help:      "Histogram of GetBestBlockHeader calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainGetBlockHeader = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockchain",
			Name:      "get_get_block_header",
			Help:      "Histogram of GetBlockHeader calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainGetBlockHeaders = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockchain",
			Name:      "get_get_block_headers",
			Help:      "Histogram of GetBlockHeaders calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainGetBlockHeadersFromHeight = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockchain",
			Name:      "get_get_block_headers_from_height",
			Help:      "Histogram of GetBlockHeadersFromHeight calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainSubscribe = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockchain",
			Name:      "subscribe",
			Help:      "Histogram of Subscribe calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainGetState = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockchain",
			Name:      "get_state",
			Help:      "Histogram of GetState calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainSetState = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockchain",
			Name:      "set_state",
			Help:      "Histogram of SetState calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainGetBlockHeaderIDs = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockchain",
			Name:      "get_block_header_ids",
			Help:      "Histogram of GetBlockHeaderIDs calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainInvalidateBlock = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockchain",
			Name:      "invalidate_block",
			Help:      "Histogram of InvalidateBlock calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainRevalidateBlock = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockchain",
			Name:      "revalidate_block",
			Help:      "Histogram of RevalidateBlock calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainSendNotification = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockchain",
			Name:      "send_notification",
			Help:      "Histogram of SendNotification calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainSetBlockMinedSet = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockchain",
			Name:      "set_block_mined_set",
			Help:      "Histogram of SetBlockMinedSet calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainGetBlocksMinedNotSet = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockchain",
			Name:      "get_blocks_mined_not_set",
			Help:      "Histogram of GetBlocksMinedNotSet calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainSetBlockSubtreesSet = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockchain",
			Name:      "set_block_subtrees_set",
			Help:      "Histogram of SetBlockSubtreesSet calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainGetBlocksSubtreesNotSet = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockchain",
			Name:      "get_blocks_subtrees_not_set",
			Help:      "Histogram of GetBlocksSubtreesNotSet calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainGetFSMCurrentState = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockchain",
			Name:      "get_fsm_current_state",
			Help:      "Histogram of GetFSMCurrentState calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainGetBlockLocator = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockchain",
			Name:      "get_block_locator",
			Help:      "Histogram of GetBlockLocator calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockchainLocateBlockHeaders = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockchain",
			Name:      "locate_block_headers",
			Help:      "Histogram of LocateBlockHeaders calls to the blockchain service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
}
