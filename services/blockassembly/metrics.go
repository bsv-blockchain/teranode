package blockassembly

import (
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"sync"
)

var (

	// in Server
	prometheusBlockAssemblyHealth                       prometheus.Counter
	prometheusBlockAssemblyAddTxDuration                prometheus.Histogram
	prometheusBlockAssemblyRemoveTx                     prometheus.Counter
	prometheusBlockAssemblyRemoveTxDuration             prometheus.Histogram
	prometheusBlockAssemblyGetMiningCandidate           prometheus.Counter
	prometheusBlockAssemblyGetMiningCandidateDuration   prometheus.Histogram
	prometheusBlockAssemblySubmitMiningSolutionCh       prometheus.Gauge
	prometheusBlockAssemblySubmitMiningSolution         prometheus.Counter
	prometheusBlockAssemblySubmitMiningSolutionDuration prometheus.Histogram
	prometheusBlockAssemblyUpdateSubtreesTTL            prometheus.Histogram
	//prometheusBlockAssemblyUpdateTxMinedStatus          prometheus.Histogram

	// in BlockAssembler
	prometheusBlockAssemblerGetMiningCandidate prometheus.Counter
	prometheusBlockAssemblerSubtreeCreated     prometheus.Counter
	prometheusBlockAssemblerTransactions       prometheus.Gauge
	prometheusBlockAssemblerQueuedTransactions prometheus.Gauge
	prometheusBlockAssemblerSubtrees           prometheus.Gauge
	prometheusBlockAssemblerTxMetaGetDuration  prometheus.Histogram
	//prometheusBlockAssemblerUtxoStoreDuration  prometheus.Histogram
	prometheusBlockAssemblerReorg             prometheus.Counter
	prometheusBlockAssemblerReorgDuration     prometheus.Histogram
	prometheusBlockAssemblyBestBlockHeight    prometheus.Gauge
	prometheusBlockAssemblyCurrentBlockHeight prometheus.Gauge
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
			Namespace: "blockassembly",
			Name:      "health",
			Help:      "Number of calls to the health endpoint of the blockassembly service",
		},
	)

	prometheusBlockAssemblyAddTxDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockassembly",
			Name:      "add_tx_duration_seconds",
			Help:      "Duration of AddTx in the blockassembly service",
			Buckets:   util.MetricsBucketsMicroSeconds,
		},
	)

	prometheusBlockAssemblyRemoveTx = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockassembly",
			Name:      "remove_tx",
			Help:      "Number of txs removed to the blockassembly service",
		},
	)

	prometheusBlockAssemblyRemoveTxDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockassembly",
			Name:      "remove_tx_duration_millis",
			Help:      "Duration of RemoveTx in the blockassembly service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockAssemblyGetMiningCandidate = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockassembly",
			Name:      "get_mining_candidate",
			Help:      "Number of calls to GetMiningCandidate in the blockassembly service",
		},
	)

	prometheusBlockAssemblyGetMiningCandidateDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockassembly",
			Name:      "get_mining_candidate_duration_millis",
			Help:      "Duration of GetMiningCandidate in the blockassembly service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockAssemblySubmitMiningSolutionCh = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "blockassembly",
			Name:      "submit_mining_solution_ch",
			Help:      "Number of items in the SubmitMiningSolution channel in the blockassembly service",
		},
	)

	prometheusBlockAssemblySubmitMiningSolution = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockassembly",
			Name:      "submit_mining_solution",
			Help:      "Number of calls to SubmitMiningSolution in the blockassembly service",
		},
	)

	prometheusBlockAssemblySubmitMiningSolutionDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockassembly",
			Name:      "submit_mining_solution_duration_seconds",
			Help:      "Duration of SubmitMiningSolution in the blockassembly service",
			Buckets:   util.MetricsBucketsSeconds,
		},
	)

	prometheusBlockAssemblyUpdateSubtreesTTL = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockassembly",
			Name:      "update_subtrees_ttl_duration_seconds",
			Help:      "Duration of updating subtrees TTL in the blockassembly service",
			Buckets:   util.MetricsBucketsSeconds,
		},
	)

	//prometheusBlockAssemblyUpdateTxMinedStatus = promauto.NewHistogram(
	//	prometheus.HistogramOpts{
	//		Namespace: "blockassembly",
	//		Name:      "update_tx_mined_status_duration",
	//		Help:      "Duration of updating tx mined status in the blockassembly service",
	//		Buckets:   util.MetricsBuckets,
	//	},
	//)

	prometheusBlockAssemblerGetMiningCandidate = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockassembly",
			Name:      "block_assembler_get_mining_candidate",
			Help:      "Number of calls to GetMiningCandidate in the block assembler",
		},
	)

	prometheusBlockAssemblerSubtreeCreated = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockassembly",
			Name:      "subtree_created",
			Help:      "Number of subtrees created in the block assembler",
		},
	)

	prometheusBlockAssemblerTransactions = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "blockassembly",
			Name:      "transactions",
			Help:      "Number of transactions currently in the block assembler subtree processor",
		},
	)

	prometheusBlockAssemblerQueuedTransactions = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "blockassembly",
			Name:      "queued_transactions",
			Help:      "Number of transactions currently queued in the block assembler subtree processor",
		},
	)

	prometheusBlockAssemblerSubtrees = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "blockassembly",
			Name:      "subtrees",
			Help:      "Number of subtrees currently in the block assembler subtree processor",
		},
	)

	prometheusBlockAssemblerTxMetaGetDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockassembly",
			Name:      "tx_meta_get_duration_micros",
			Help:      "Duration of reading tx meta data from txmeta store in block assembler",
			Buckets:   util.MetricsBucketsMicroSeconds,
		},
	)

	//prometheusBlockAssemblerUtxoStoreDuration = promauto.NewHistogram(
	//	prometheus.HistogramOpts{
	//		Namespace: "blockassembly",
	//		Name:      "utxo_store_duration_v2",
	//		Help:      "Duration of storing new utxos by block assembler",
	//	},
	//)

	prometheusBlockAssemblerReorg = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockassembly",
			Name:      "reorg",
			Help:      "Number of reorgs in block assembler",
		},
	)

	prometheusBlockAssemblerReorgDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockassembly",
			Name:      "reorg_duration_seconds",
			Help:      "Duration of reorg in block assembler",
			Buckets:   util.MetricsBucketsSeconds,
		},
	)

	prometheusBlockAssemblyBestBlockHeight = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "blockassembly",
			Name:      "best_block_height",
			Help:      "Best block height in block assembly",
		},
	)

	prometheusBlockAssemblyCurrentBlockHeight = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "blockassembly",
			Name:      "current_block_height",
			Help:      "Current block height in block assembly",
		},
	)
}
