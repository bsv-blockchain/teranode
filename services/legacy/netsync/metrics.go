package netsync

import (
	"sync"

	"github.com/bsv-blockchain/teranode/util"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	prometheusLegacyNetsyncBlockHeight               prometheus.Gauge
	prometheusLegacyNetsyncHandleTxMsg               prometheus.Histogram
	prometheusLegacyNetsyncHandleTxMsgValidate       prometheus.Histogram
	prometheusLegacyNetsyncProcessOrphanTransactions prometheus.Histogram
	prometheusLegacyNetsyncHandleBlockDirect         prometheus.Histogram
	prometheusLegacyNetsyncProcessBlock              prometheus.Histogram
	prometheusLegacyNetsyncOrphans                   prometheus.Gauge
	prometheusLegacyNetsyncParkedBlocks              prometheus.Gauge
	prometheusLegacyNetsyncParkedBytes               prometheus.Gauge
	prometheusLegacyNetsyncOrphanTime                prometheus.Histogram

	// prometheusLegacyNetsyncFrontierRaces counts blocks asked of a second peer because the
	// chain was about to wait on them from a slow one. See frontier_race.go.
	prometheusLegacyNetsyncFrontierRaces prometheus.Counter

	prometheusMetricsInitOnce sync.Once
)

func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

func _initPrometheusMetrics() {
	prometheusLegacyNetsyncFrontierRaces = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "frontier_races_total",
		Help:      "Blocks asked of a second peer because the chain was about to wait on them from a slow one",
	})
	prometheus.MustRegister(prometheusLegacyNetsyncFrontierRaces)

	prometheusLegacyNetsyncBlockHeight = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "block_height",
		Help:      "The height of the block being processed",
	})
	prometheus.MustRegister(prometheusLegacyNetsyncBlockHeight)

	prometheusLegacyNetsyncHandleTxMsg = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "handle_tx_msg",
		Help:      "The time taken to handle a tx message",
		Buckets:   util.MetricsBucketsMilliSeconds,
	})
	prometheus.MustRegister(prometheusLegacyNetsyncHandleTxMsg)

	prometheusLegacyNetsyncHandleTxMsgValidate = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "handle_tx_msg_validate",
		Help:      "The time taken to validate a tx message",
		Buckets:   util.MetricsBucketsMilliSeconds,
	})
	prometheus.MustRegister(prometheusLegacyNetsyncHandleTxMsgValidate)

	prometheusLegacyNetsyncProcessOrphanTransactions = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "process_orphan_transactions",
		Help:      "The time taken to process orphan transactions",
		Buckets:   util.MetricsBucketsMilliSeconds,
	})
	prometheus.MustRegister(prometheusLegacyNetsyncProcessOrphanTransactions)

	prometheusLegacyNetsyncHandleBlockDirect = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "handle_block_direct",
		Help:      "The time taken to handle a block directly",
		Buckets:   util.MetricsBucketsSeconds,
	})
	prometheus.MustRegister(prometheusLegacyNetsyncHandleBlockDirect)

	prometheusLegacyNetsyncProcessBlock = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "process_block",
		Help:      "The time taken to process a block",
		Buckets:   util.MetricsBucketsSeconds,
	})
	prometheus.MustRegister(prometheusLegacyNetsyncProcessBlock)

	prometheusLegacyNetsyncOrphans = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "orphans",
		Help:      "The number of orphan transactions",
	})
	prometheus.MustRegister(prometheusLegacyNetsyncOrphans)

	prometheusLegacyNetsyncParkedBlocks = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "parked_blocks",
		Help:      "The number of downloaded blocks held on disk waiting for their parent",
	})
	prometheus.MustRegister(prometheusLegacyNetsyncParkedBlocks)

	prometheusLegacyNetsyncParkedBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "parked_bytes",
		Help:      "The serialized bytes of downloaded blocks held on disk waiting for their parent",
	})
	prometheus.MustRegister(prometheusLegacyNetsyncParkedBytes)

	prometheusLegacyNetsyncOrphanTime = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "legacy_netsync",
		Name:      "orphan_time",
		Help:      "The time taken to process an orphan transaction",
		Buckets:   util.MetricsBucketsSeconds,
	})
	prometheus.MustRegister(prometheusLegacyNetsyncOrphanTime)

}
