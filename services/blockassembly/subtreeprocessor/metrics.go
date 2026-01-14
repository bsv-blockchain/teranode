// Package subtreeprocessor provides functionality for processing transaction subtrees in Teranode.
package subtreeprocessor

import (
	"sync"

	"github.com/bsv-blockchain/teranode/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Prometheus metrics variables for monitoring subtree processor operations
// These metrics provide comprehensive observability into the performance and behavior
// of the subtree processor component, including transaction processing rates,
// block movement operations, and state transitions. They are essential for
// performance monitoring, troubleshooting, and capacity planning.
var (
	prometheusSubtreeProcessorAddTx                    prometheus.Counter
	prometheusSubtreeProcessorMoveForwardBlock         prometheus.Counter
	prometheusSubtreeProcessorMoveForwardBlockDuration prometheus.Histogram
	prometheusSubtreeProcessorMoveBackBlock            prometheus.Counter
	// prometheusSubtreeProcessorMoveBackBlocks               prometheus.Counter
	prometheusSubtreeProcessorMoveBackBlockDuration prometheus.Histogram
	// prometheusSubtreeProcessorMoveBackBlocksDuration       prometheus.Histogram
	prometheusSubtreeProcessorProcessCoinbaseTx            prometheus.Counter
	prometheusSubtreeProcessorProcessCoinbaseTxDuration    prometheus.Histogram
	prometheusSubtreeProcessorCreateTransactionMap         prometheus.Counter
	prometheusSubtreeProcessorCreateTransactionMapDuration prometheus.Histogram
	prometheusSubtreeProcessorRemoveTx                     prometheus.Histogram
	prometheusSubtreeProcessorReset                        prometheus.Histogram
	prometheusSubtreeProcessorDynamicSubtreeSize           prometheus.Gauge
	prometheusSubtreeProcessorCurrentState                 prometheus.Gauge
	prometheusBlockAssemblySubtreeCompleteHist             prometheus.Histogram
	prometheusSubtreeProcessorDequeuedTxs                  prometheus.Counter
)

var (
	prometheusMetricsInitOnce sync.Once
)

// initPrometheusMetrics initializes all Prometheus metrics for the subtree processor.
// This function is called once during package initialization to ensure that
// all metrics are properly registered before any operations occur.
// It leverages sync.Once to guarantee that the initialization happens exactly once,
// even in concurrent environments.
func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

// _initPrometheusMetrics is the actual implementation of metrics initialization.
// It creates and registers all Prometheus metrics used by the subtree processor,
// configuring each with appropriate namespace, subsystem, name, and help text.
// The metrics cover various aspects of subtree processing including transaction
// addition, block movements, and performance timing for critical operations.
func _initPrometheusMetrics() {
	prometheusSubtreeProcessorAddTx = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "subtreeprocessor",
			Name:      "add_tx",
			Help:      "Number of times a tx is added in subtree processor",
		},
	)

	prometheusSubtreeProcessorMoveForwardBlock = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "subtreeprocessor",
			Name:      "move_forward",
			Help:      "Number of times a block is moved up in subtree processor",
		},
	)

	prometheusSubtreeProcessorMoveForwardBlockDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "subtreeprocessor",
			Name:      "move_forward_duration",
			Help:      "Histogram of moving up block in subtree processor",
			Buckets:   util.MetricsBucketsSeconds,
		},
	)

	prometheusSubtreeProcessorMoveBackBlock = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "subtreeprocessor",
			Name:      "move_back",
			Help:      "Number of times a block is moved down in subtree processor",
		},
	)

	prometheusSubtreeProcessorMoveBackBlockDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "subtreeprocessor",
			Name:      "move_back_duration",
			Help:      "Histogram of moving down block in subtree processor",
			Buckets:   util.MetricsBucketsSeconds,
		},
	)

	// prometheusSubtreeProcessorMoveBackBlocks = promauto.NewCounter(
	// 	prometheus.CounterOpts{
	// 		Namespace: "subtreeprocessor",
	// 		Name:      "move_back_blocks",
	// 		Help:      "Number of times multple blocks moved down in subtree processor",
	// 	},
	// )

	// prometheusSubtreeProcessorMoveBackBlocksDuration = promauto.NewHistogram(
	// 	prometheus.HistogramOpts{
	// 		Namespace: "subtreeprocessor",
	// 		Name:      "move_back_blocks_duration_seconds",
	// 		Help:      "Duration of moving down blocks in subtree processor",
	// 		Buckets:   util.MetricsBucketsSeconds,
	// 	},
	// )

	prometheusSubtreeProcessorProcessCoinbaseTx = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "subtreeprocessor",
			Name:      "process_coinbase_tx",
			Help:      "Number of times a coinbase tx is processed in subtree processor",
		},
	)

	prometheusSubtreeProcessorProcessCoinbaseTxDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "subtreeprocessor",
			Name:      "process_coinbase_tx_duration",
			Help:      "Duration of processing coinbase tx in subtree processor",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusSubtreeProcessorCreateTransactionMap = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "subtreeprocessor",
			Name:      "transaction_map",
			Help:      "Number of times a transaction map is created in subtree processor",
		},
	)

	prometheusSubtreeProcessorCreateTransactionMapDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "subtreeprocessor",
			Name:      "transaction_map_duration",
			Help:      "Duration of creating transaction map in subtree processor",
			Buckets:   util.MetricsBucketsSeconds,
		},
	)

	prometheusSubtreeProcessorRemoveTx = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "subtreeprocessor",
			Name:      "remove_tx",
			Help:      "Duration of removing tx in subtree processor",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusSubtreeProcessorReset = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "subtreeprocessor",
			Name:      "reset",
			Help:      "Duration of resetting subtree processor",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusSubtreeProcessorDynamicSubtreeSize = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "subtreeprocessor",
			Name:      "dynamic_subtree_size",
			Help:      "Dynamic subtree size in number of transactions",
		},
	)

	prometheusSubtreeProcessorCurrentState = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "subtreeprocessor",
			Name:      "current_state",
			Help:      "Current state of the block assembly process",
		},
	)

	prometheusBlockAssemblySubtreeCompleteHist = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockassembly",
			Name:      "subtree_complete",
			Help:      "Histogram of subtree completion duration in block assembly",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusSubtreeProcessorDequeuedTxs = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "subtreeprocessor",
			Name:      "dequeued_txs",
			Help:      "Number of transactions dequeued from subtree processor",
		},
	)
}
