// Package blockassembly provides functionality for assembling Bitcoin blocks in Teranode.
package blockassembly

import (
	"sync"

	"github.com/bsv-blockchain/teranode/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Prometheus metrics variables for monitoring block assembly operations
// These metrics provide observability into the performance, throughput, and health
// of the block assembly service. They are used for monitoring system behavior,
// detecting anomalies, and analyzing performance patterns across various operations.
var (
	// prometheusBlockAssemblyHealth tracks health check calls
	prometheusBlockAssemblyHealth prometheus.Counter

	// prometheusBlockAssemblyAddTx measures transaction addition time
	prometheusBlockAssemblyAddTx prometheus.Histogram

	// prometheusBlockAssemblyAddCounter counts number of transactions added
	prometheusBlockAssemblyAddTxCounter prometheus.Counter

	// prometheusBlockAssemblyRemoveTx measures transaction removal time
	prometheusBlockAssemblyRemoveTx prometheus.Histogram

	// prometheusBlockAssemblyGetMiningCandidateDuration measures mining candidate retrieval time
	prometheusBlockAssemblyGetMiningCandidateDuration prometheus.Histogram

	// prometheusBlockAssemblerCacheHits tracks cache hits for mining candidates
	prometheusBlockAssemblerCacheHits prometheus.Counter

	// prometheusBlockAssemblerCacheMisses tracks cache misses for mining candidates
	prometheusBlockAssemblerCacheMisses prometheus.Counter

	// prometheusBlockAssemblySubmitMiningSolutionCh tracks mining solution submission queue size
	prometheusBlockAssemblySubmitMiningSolutionCh prometheus.Gauge

	// prometheusBlockAssemblySubmitMiningSolution measures mining solution submission time
	prometheusBlockAssemblySubmitMiningSolution prometheus.Histogram

	// prometheusBlockAssemblyUpdateSubtreesDAH measures subtree DAH update time
	prometheusBlockAssemblyUpdateSubtreesDAH prometheus.Histogram

	// Additional metrics for block assembler operations
	prometheusBlockAssemblerGetMiningCandidate          prometheus.Counter
	prometheusBlockAssemblerSubtreeCreated              prometheus.Counter
	prometheusBlockAssemblerTransactions                prometheus.Gauge
	prometheusBlockAssemblerQueuedTransactions          prometheus.Gauge
	prometheusBlockAssemblerSubtrees                    prometheus.Gauge
	prometheusBlockAssemblerTxMetaGetDuration           prometheus.Histogram
	prometheusBlockAssemblerReorg                       prometheus.Counter
	prometheusBlockAssemblerReorgDuration               prometheus.Histogram
	prometheusBlockAssemblerGetReorgBlocksDuration      prometheus.Histogram
	prometheusBlockAssemblerUpdateBestBlock             prometheus.Histogram
	prometheusBlockAssemblyBestBlockHeight              prometheus.Gauge
	prometheusBlockAssemblyCurrentBlockHeight           prometheus.Gauge
	prometheusBlockAssemblerCurrentState                prometheus.Gauge
	prometheusBlockAssemblerStateTransitions            *prometheus.CounterVec
	prometheusBlockAssemblerStateDuration               *prometheus.HistogramVec
	prometheusBlockAssemblerGenerateBlocks              prometheus.Histogram
	prometheusBlockAssemblerUtxoIndexReady              *prometheus.GaugeVec
	prometheusBlockAssemblerUtxoIndexWaitDuration       *prometheus.HistogramVec
	prometheusBlockAssemblerGetUnminedTxIteratorTime    *prometheus.HistogramVec
	prometheusBlockAssemblerIteratorProcessingTime      *prometheus.HistogramVec
	prometheusBlockAssemblerIteratorTransactionsTotal   *prometheus.CounterVec
	prometheusBlockAssemblerIteratorTransactionsStats   *prometheus.CounterVec
	prometheusBlockAssemblerMarkTransactionsTime        prometheus.Histogram
	prometheusBlockAssemblerMarkTransactionsCount       prometheus.Counter
	prometheusBlockAssemblerSortTransactionsTime        *prometheus.HistogramVec
	prometheusBlockAssemblerValidateParentChainTime     prometheus.Histogram
	prometheusBlockAssemblerValidateParentChainFiltered prometheus.Counter
	prometheusBlockAssemblerAddDirectlyTime             prometheus.Histogram
	prometheusBlockAssemblerAddDirectlyTotal            prometheus.Counter
	prometheusBlockAssemblerAddDirectlyBatchTime        prometheus.Histogram
	prometheusBlockAssemblerSubtreeStoredHist           prometheus.Histogram
)

var (
	prometheusMetricsInitOnce sync.Once
)

// initPrometheusMetrics initializes all Prometheus metrics.
// This function is called once during package initialization to set up
// all required counters, gauges, and histograms with appropriate labels,
// buckets, and descriptions for monitoring the block assembly service.
// It ensures all metrics are correctly registered with the Prometheus registry.
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

	prometheusBlockAssemblyAddTxCounter = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "blockassembly",
			Name:      "add_tx_counter",
			Help:      "Number of transactions added in the blockassembly service",
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

	prometheusBlockAssemblyUpdateSubtreesDAH = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockassembly",
			Name:      "update_subtrees_dah",
			Help:      "Histogram of updating subtrees DAH in the blockassembly service",
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

	prometheusBlockAssemblerSubtreeStoredHist = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockassembly",
			Name:      "subtree_stored",
			Help:      "Histogram of subtree stored duration in block assembler",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockAssemblerCacheHits = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "blockassembly",
			Name:      "cache_hits",
			Help:      "Number of cache hits for mining candidates",
		},
	)

	prometheusBlockAssemblerCacheMisses = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "blockassembly",
			Name:      "cache_misses",
			Help:      "Number of cache misses for mining candidates",
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

	prometheusBlockAssemblerCurrentState = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "blockassembly",
			Name:      "current_state",
			Help:      "Current state of the block assembly process",
		},
	)

	prometheusBlockAssemblerStateTransitions = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "blockassembly",
			Name:      "state_transitions_total",
			Help:      "Total number of state transitions",
		},
		[]string{"from", "to"},
	)

	prometheusBlockAssemblerStateDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockassembly",
			Name:      "state_duration_seconds",
			Help:      "Time spent in each state",
			Buckets:   []float64{0.001, 0.01, 0.1, 0.5, 1, 2, 5, 10, 30, 60},
		},
		[]string{"state"},
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

	prometheusBlockAssemblerUtxoIndexReady = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "blockassembly",
			Name:      "utxo_index_ready",
			Help:      "Status of UTXO index readiness (0=not ready, 1=ready, 2=error)",
		},
		[]string{"index_name"},
	)

	prometheusBlockAssemblerUtxoIndexWaitDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockassembly",
			Name:      "utxo_index_wait_seconds",
			Help:      "Time taken waiting for UTXO index to become ready",
			Buckets:   []float64{0.1, 0.5, 1, 5, 10, 30, 60, 120, 300},
		},
		[]string{"index_name", "status"},
	)

	prometheusBlockAssemblerGetUnminedTxIteratorTime = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockassembly",
			Name:      "get_unmined_tx_iterator_seconds",
			Help:      "Time taken to get unmined transaction iterator from UTXO store",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30},
		},
		[]string{"full_scan", "status"},
	)

	prometheusBlockAssemblerIteratorProcessingTime = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockassembly",
			Name:      "iterator_processing_seconds",
			Help:      "Time taken to process all transactions from the unmined iterator",
			Buckets:   []float64{0.1, 0.5, 1, 5, 10, 30, 60, 120, 300, 600},
		},
		[]string{"full_scan"},
	)

	prometheusBlockAssemblerIteratorTransactionsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "blockassembly",
			Name:      "iterator_transactions_total",
			Help:      "Total number of transactions processed from the unmined iterator",
		},
		[]string{"full_scan"},
	)

	prometheusBlockAssemblerIteratorTransactionsStats = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "blockassembly",
			Name:      "iterator_transactions_stats",
			Help:      "Statistics about transactions processed from the iterator",
		},
		[]string{"full_scan", "category"},
	)

	prometheusBlockAssemblerMarkTransactionsTime = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockassembly",
			Name:      "mark_transactions_on_longest_chain_seconds",
			Help:      "Time taken to mark transactions as mined on longest chain",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockAssemblerMarkTransactionsCount = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "blockassembly",
			Name:      "mark_transactions_on_longest_chain_total",
			Help:      "Total number of transactions marked as mined on longest chain",
		},
	)

	prometheusBlockAssemblerSortTransactionsTime = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockassembly",
			Name:      "sort_transactions_seconds",
			Help:      "Time taken to sort unmined transactions by createdAt",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10},
		},
		[]string{"transaction_count_bucket"},
	)

	prometheusBlockAssemblerValidateParentChainTime = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockassembly",
			Name:      "validate_parent_chain_seconds",
			Help:      "Time taken to validate parent chain for unmined transactions",
			Buckets:   util.MetricsBucketsSeconds,
		},
	)

	prometheusBlockAssemblerValidateParentChainFiltered = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "blockassembly",
			Name:      "validate_parent_chain_filtered_total",
			Help:      "Total number of transactions filtered out during parent chain validation",
		},
	)

	prometheusBlockAssemblerAddDirectlyTime = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockassembly",
			Name:      "add_directly_seconds",
			Help:      "Time taken for individual AddDirectly calls to subtree processor",
			Buckets:   util.MetricsBucketsMicroSeconds,
		},
	)

	prometheusBlockAssemblerAddDirectlyTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "blockassembly",
			Name:      "add_directly_total",
			Help:      "Total number of transactions added directly to subtree processor",
		},
	)

	prometheusBlockAssemblerAddDirectlyBatchTime = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockassembly",
			Name:      "add_directly_batch_seconds",
			Help:      "Time taken to add all unmined transactions to subtree processor",
			Buckets:   util.MetricsBucketsSeconds,
		},
	)
}
