package subtreeprocessor

import (
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
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

	prometheusSubtreeProcessorMoveUpBlock = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "subtreeprocessor",
			Name:      "move_up",
			Help:      "Number of times a block is moved up in block assembler",
		},
	)

	prometheusSubtreeProcessorMoveUpBlockDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "subtreeprocessor",
			Name:      "move_up_duration_v2",
			Help:      "Duration of moving up block in block assembler",
			Buckets:   util.MetricsBuckets,
		},
	)

	prometheusSubtreeProcessorMoveDownBlock = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "subtreeprocessor",
			Name:      "move_down",
			Help:      "Number of times a block is moved up in block assembler",
		},
	)

	prometheusSubtreeProcessorMoveDownBlockDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "subtreeprocessor",
			Name:      "move_down_duration_v2",
			Help:      "Duration of moving down block in block assembler",
			Buckets:   util.MetricsBuckets,
		},
	)

	prometheusSubtreeProcessorProcessCoinbaseTx = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "subtreeprocessor",
			Name:      "process_coinbase_tx",
			Help:      "Number of times a coinbase tx is processed in block assembler",
		},
	)

	prometheusSubtreeProcessorProcessCoinbaseTxDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "subtreeprocessor",
			Name:      "process_coinbase_tx_duration_v2",
			Help:      "Duration of processing coinbase tx in block assembler",
			Buckets:   util.MetricsBuckets,
		},
	)

	prometheusSubtreeProcessorCreateTransactionMap = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "subtreeprocessor",
			Name:      "transaction_map",
			Help:      "Number of times a transaction map is created in block assembler",
		},
	)

	prometheusSubtreeProcessorCreateTransactionMapDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "subtreeprocessor",
			Name:      "transaction_map_duration_v2",
			Help:      "Duration of creating transaction map in block assembler",
			Buckets:   util.MetricsBuckets,
		},
	)

	prometheusMetricsInitialized = true
}
