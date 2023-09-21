package blockassembly

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prometheusBlockAssemblyHealth                       prometheus.Counter
	prometheusBlockAssemblyAddTx                        prometheus.Counter
	prometheusBlockAssemblyAddTxDuration                prometheus.Histogram
	prometheusBlockAssemblyGetMiningCandidate           prometheus.Counter
	prometheusBlockAssemblyGetMiningCandidateDuration   prometheus.Histogram
	prometheusBlockAssemblySubmitMiningSolution         prometheus.Counter
	prometheusBlockAssemblySubmitMiningSolutionDuration prometheus.Histogram

	// in BlockAssembler
	prometheusBlockAssemblerAddTx                       prometheus.Counter
	prometheusBlockAssemblerGetMiningCandidate          prometheus.Counter
	prometheusBlockAssemblerSubtreeCreated              prometheus.Counter
	prometheusBlockAssemblerTransactions                prometheus.Gauge
	prometheusBlockAssemblerTxMetaGetDuration           prometheus.Histogram
	prometheusBlockAssemblerUtxoStoreDuration           prometheus.Histogram
	prometheusBlockAssemblerSubtreeAddToChannelDuration prometheus.Histogram
	prometheusBlockAssemblerReorg                       prometheus.Counter
	prometheusBlockAssemblerReorgDuration               prometheus.Histogram
)

var prometheusMetricsInitialized = false

func initPrometheusMetrics() {
	if prometheusMetricsInitialized {
		return
	}

	prometheusBlockAssemblyHealth = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockassembly",
			Name:      "health",
			Help:      "Number of calls to the health endpoint of the blockassembly service",
		},
	)

	prometheusBlockAssemblyAddTx = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockassembly",
			Name:      "add_tx",
			Help:      "Number of txs added to the blockassembly service",
		},
	)

	prometheusBlockAssemblyAddTxDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockassembly",
			Name:      "add_tx_duration",
			Help:      "Duration of AddTx in the blockassembly service",
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
			Name:      "get_mining_candidate_duration",
			Help:      "Duration of GetMiningCandidate in the blockassembly service",
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
			Name:      "submit_mining_solution_duration",
			Help:      "Duration of SubmitMiningSolution in the blockassembly service",
		},
	)

	prometheusBlockAssemblerAddTx = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockassembly",
			Name:      "block_assembler_add_tx",
			Help:      "Number of txs added to the block assembler",
		},
	)

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

	prometheusBlockAssemblerTxMetaGetDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockassembly",
			Name:      "tx_meta_get_duration",
			Help:      "Duration of reading tx meta data from txmeta store in block assembler",
		},
	)

	prometheusBlockAssemblerUtxoStoreDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockassembly",
			Name:      "utxo_store_duration",
			Help:      "Duration of storing new utxos by block assembler",
		},
	)

	prometheusBlockAssemblerSubtreeAddToChannelDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockassembly",
			Name:      "add_tx_to_channel_duration",
			Help:      "Duration of writing tx to subtree processor channel by block assembler",
		},
	)

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
			Name:      "reorg_duration",
			Help:      "Duration of reorg in block assembler",
		},
	)

	prometheusMetricsInitialized = true
}
