package blockassembly

import (
	"sync"

	"github.com/bitcoin-sv/teranode/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// in Server
	prometheusBlockAssemblyHealth                     prometheus.Counter
	prometheusBlockAssemblyAddTx                      prometheus.Histogram
	prometheusBlockAssemblyRemoveTx                   prometheus.Histogram
	prometheusBlockAssemblyGetMiningCandidateDuration prometheus.Histogram
	prometheusBlockAssemblySubmitMiningSolutionCh     prometheus.Gauge
	prometheusBlockAssemblySubmitMiningSolution       prometheus.Histogram
	prometheusBlockAssemblyUpdateSubtreesTTL          prometheus.Histogram

	// in BlockAssembler
	prometheusBlockAssemblerGetMiningCandidate prometheus.Counter
	prometheusBlockAssemblerSubtreeCreated     prometheus.Counter
	prometheusBlockAssemblerTransactions       prometheus.Gauge
	prometheusBlockAssemblerQueuedTransactions prometheus.Gauge
	prometheusBlockAssemblerSubtrees           prometheus.Gauge
	prometheusBlockAssemblerTxMetaGetDuration  prometheus.Histogram

	prometheusBlockAssemblerReorg                  prometheus.Counter
	prometheusBlockAssemblerReorgDuration          prometheus.Histogram
	prometheusBlockAssemblerGetReorgBlocksDuration prometheus.Histogram
	prometheusBlockAssemblerUpdateBestBlock        prometheus.Histogram
	prometheusBlockAssemblyBestBlockHeight         prometheus.Gauge
	prometheusBlockAssemblyCurrentBlockHeight      prometheus.Gauge
	prometheusBlockAssemblerGenerateBlocks         prometheus.Histogram
)

var (
	prometheusMetricsInitOnce sync.Once
)

func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

func _initPrometheusMetrics() {
	prometheusBlockAssemblyHealth = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "blockassembly",
			Name:      "health",
			Help:      "Number of calls to the health endpoint of the blockassembly service",
		},
	)

	prometheusBlockAssemblyAddTx = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockassembly",
			Name:      "add_tx",
			Help:      "Histogram of AddTx in the blockassembly service",
			Buckets:   util.MetricsBucketsMicroSeconds,
		},
	)

	prometheusBlockAssemblyRemoveTx = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockassembly",
			Name:      "remove_tx",
			Help:      "Histogram of RemoveTx in the blockassembly service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockAssemblyGetMiningCandidateDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockassembly",
			Name:      "get_mining_candidate_duration",
			Help:      "Histogram of GetMiningCandidate in the blockassembly service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockAssemblySubmitMiningSolutionCh = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "blockassembly",
			Name:      "submit_mining_solution_ch",
			Help:      "Number of items in the SubmitMiningSolution channel in the blockassembly service",
		},
	)

	prometheusBlockAssemblySubmitMiningSolution = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockassembly",
			Name:      "submit_mining_solution",
			Help:      "Histogram of SubmitMiningSolution in the blockassembly service",
			Buckets:   util.MetricsBucketsSeconds,
		},
	)

	prometheusBlockAssemblyUpdateSubtreesTTL = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockassembly",
			Name:      "update_subtrees_ttl",
			Help:      "Histogram of updating subtrees TTL in the blockassembly service",
			Buckets:   util.MetricsBucketsSeconds,
		},
	)

	prometheusBlockAssemblerGetMiningCandidate = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "blockassembly",
			Name:      "block_assembler_get_mining_candidate",
			Help:      "Number of calls to GetMiningCandidate in the block assembler",
		},
	)

	prometheusBlockAssemblerSubtreeCreated = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "blockassembly",
			Name:      "subtree_created",
			Help:      "Number of subtrees created in the block assembler",
		},
	)

	prometheusBlockAssemblerTransactions = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "blockassembly",
			Name:      "transactions",
			Help:      "Number of transactions currently in the block assembler subtree processor",
		},
	)

	prometheusBlockAssemblerQueuedTransactions = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "blockassembly",
			Name:      "queued_transactions",
			Help:      "Number of transactions currently queued in the block assembler subtree processor",
		},
	)

	prometheusBlockAssemblerSubtrees = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "blockassembly",
			Name:      "subtrees",
			Help:      "Number of subtrees currently in the block assembler subtree processor",
		},
	)

	prometheusBlockAssemblerTxMetaGetDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockassembly",
			Name:      "tx_meta_get",
			Help:      "Histogram of reading tx meta data from txmeta store in block assembler",
			Buckets:   util.MetricsBucketsMicroSeconds,
		},
	)

	prometheusBlockAssemblerReorg = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "blockassembly",
			Name:      "reorg",
			Help:      "Number of reorgs in block assembler",
		},
	)

	prometheusBlockAssemblerReorgDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockassembly",
			Name:      "reorg_duration",
			Help:      "Histogram of reorg in block assembler",
			Buckets:   util.MetricsBucketsSeconds,
		},
	)

	prometheusBlockAssemblerGetReorgBlocksDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockassembly",
			Name:      "get_reorg_blocks_duration",
			Help:      "Histogram of GetReorgBlocks in block assembler",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockAssemblerUpdateBestBlock = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockassembly",
			Name:      "update_best_block",
			Help:      "Histogram of updating best block in block assembler",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockAssemblyBestBlockHeight = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "blockassembly",
			Name:      "best_block_height",
			Help:      "Best block height in block assembly",
		},
	)

	prometheusBlockAssemblyCurrentBlockHeight = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "blockassembly",
			Name:      "current_block_height",
			Help:      "Current block height in block assembly",
		},
	)

	prometheusBlockAssemblerGenerateBlocks = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockassembly",
			Name:      "generate_blocks",
			Help:      "Histogram of generating blocks in block assembler",
			Buckets:   util.MetricsBucketsSeconds,
		},
	)
}
