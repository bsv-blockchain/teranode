package subtreeprocessor

import (
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prometheusSubtreeProcessorAddTx                        prometheus.Counter
	prometheusSubtreeProcessorMoveUpBlock                  prometheus.Counter
	prometheusSubtreeProcessorMoveUpBlockDuration          prometheus.Histogram
	prometheusSubtreeProcessorMoveDownBlock                prometheus.Counter
	prometheusSubtreeProcessorMoveDownBlockDuration        prometheus.Histogram
	prometheusSubtreeProcessorProcessCoinbaseTx            prometheus.Counter
	prometheusSubtreeProcessorProcessCoinbaseTxDuration    prometheus.Histogram
	prometheusSubtreeProcessorCreateTransactionMap         prometheus.Counter
	prometheusSubtreeProcessorCreateTransactionMapDuration prometheus.Histogram
)

var prometheusMetricsInitialized = false

func initPrometheusMetrics() {
	if prometheusMetricsInitialized {
		return
	}

	prometheusSubtreeProcessorAddTx = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "subtreeprocessor",
			Name:      "add_tx",
			Help:      "Number of times a tx is added in subtree processor",
		},
	)

	prometheusSubtreeProcessorMoveUpBlock = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "subtreeprocessor",
			Name:      "move_up",
			Help:      "Number of times a block is moved up in subtree processor",
		},
	)

	prometheusSubtreeProcessorMoveUpBlockDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "subtreeprocessor",
			Name:      "move_up_duration_v2",
			Help:      "Duration of moving up block in subtree processor",
			Buckets:   util.MetricsBucketsSeconds,
		},
	)

	prometheusSubtreeProcessorMoveDownBlock = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "subtreeprocessor",
			Name:      "move_down",
			Help:      "Number of times a block is moved up in subtree processor",
		},
	)

	prometheusSubtreeProcessorMoveDownBlockDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "subtreeprocessor",
			Name:      "move_down_duration_v2",
			Help:      "Duration of moving down block in subtree processor",
			Buckets:   util.MetricsBucketsSeconds,
		},
	)

	prometheusSubtreeProcessorProcessCoinbaseTx = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "subtreeprocessor",
			Name:      "process_coinbase_tx",
			Help:      "Number of times a coinbase tx is processed in subtree processor",
		},
	)

	prometheusSubtreeProcessorProcessCoinbaseTxDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "subtreeprocessor",
			Name:      "process_coinbase_tx_duration_v2",
			Help:      "Duration of processing coinbase tx in subtree processor",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusSubtreeProcessorCreateTransactionMap = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "subtreeprocessor",
			Name:      "transaction_map",
			Help:      "Number of times a transaction map is created in subtree processor",
		},
	)

	prometheusSubtreeProcessorCreateTransactionMapDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "subtreeprocessor",
			Name:      "transaction_map_duration_v2",
			Help:      "Duration of creating transaction map in subtree processor",
			Buckets:   util.MetricsBucketsSeconds,
		},
	)

	prometheusMetricsInitialized = true
}
