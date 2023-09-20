package blockassembly

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prometheusBlockAssemblyHealth               prometheus.Counter
	prometheusBlockAssemblyAddTx                prometheus.Counter
	prometheusBlockAssemblyGetMiningCandidate   prometheus.Counter
	prometheusBlockAssemblySubmitMiningSolution prometheus.Counter

	// in BlockAssembler
	prometheusBlockAssemblySubtreeCreated prometheus.Counter
	prometheusBlockAssemblyTransactions   prometheus.Gauge
	prometheusTxMetaGetDuration           prometheus.Histogram
	prometheusUtxoStoreDuration           prometheus.Histogram
	prometheusSubtreeAddToChannelDuration prometheus.Histogram
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

	prometheusBlockAssemblyGetMiningCandidate = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockassembly",
			Name:      "get_mining_candidate",
			Help:      "Number of calls to GetMiningCandidate in the blockassembly service",
		},
	)

	prometheusBlockAssemblySubmitMiningSolution = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockassembly",
			Name:      "submit_mining_solution",
			Help:      "Number of calls to SubmitMiningSolution in the blockassembly service",
		},
	)

	prometheusBlockAssemblySubtreeCreated = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockassembly",
			Name:      "subtree_created",
			Help:      "Number of subtrees created in the block assembly service",
		},
	)

	prometheusBlockAssemblyTransactions = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "blockassembly",
			Name:      "transactions",
			Help:      "Number of transactions currently in the block assembler subtree processor",
		},
	)

	prometheusTxMetaGetDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockassembly",
			Name:      "tx_meta_get_duration",
			Help:      "Duration of reading tx meta data from txmeta store",
		},
	)

	prometheusUtxoStoreDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockassembly",
			Name:      "utxo_store_duration",
			Help:      "Duration of storing new utxos by BlockAssembly",
		},
	)

	prometheusSubtreeAddToChannelDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockassembly",
			Name:      "add_tx_to_channel_duration",
			Help:      "Duration of writing tx to subtree processor channel",
		},
	)

	prometheusMetricsInitialized = true
}
