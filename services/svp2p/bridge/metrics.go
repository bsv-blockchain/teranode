package bridge

import (
	"sync"

	"github.com/bsv-blockchain/teranode/util"
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics relocated from the legacy netsync package's metrics.go, trimmed to
// the subset handle_block.go and subtree_partition.go actually observe, and
// renamed/re-subsystemed so a coexisting legacy netsync process cannot collide
// with this package's registrations under the same metric name.
//
// The arithmetic, corrected here per the phase-2 ledger's Task 8 nit ("report
// metric arithmetic 18/4 should read 19/5") and re-counted against
// services/legacy/netsync/metrics.go rather than carried over: legacy
// registers 19, this package kept 14 and dropped 5. The five are the
// tx-message and orphan-pool ones — HandleTxMsg, HandleTxMsgValidate,
// ProcessOrphanTransactions, Orphans and OrphanTime. It registers 15, the 14
// plus its own OrphanEvictionQueueDrops below.
//
// RESIDUAL, recorded by Task 25 rather than acted on: those five were dropped
// because Phase 2 had no relocated consumer for them (Decision 1, svp2p Phase
// 2 plan). Phase 3 built the consumers — IngestTx (Task 14, ingest_tx.go) and
// the orphan transaction pool (Task 15, orphans.go) — so the reason no longer
// holds and the ingest and orphan paths run uninstrumented apart from
// OrphanEvictionQueueDrops. Re-adding them is new instrumentation with its own
// naming and label decisions, not a minors-sweep row, so it is booked as a
// follow-up instead of smuggled in here.
//
// DECISION, Task 25, on the ledger line "Metrics series split if both ingest
// entries reachable" (Task 10 note): NO series split. IngestBlock deliberately
// shares handle_block_direct with HandleBlockDirect (ingest.go:375-378), which
// is sound only while one of the two entries is unreachable in a running node.
// Re-checked at the end of Phase 3, and it still is: HandleBlockDirect is not
// on the Bridge interface (bridge.go) at all, and the only callers anywhere in
// the tree are this package's own tests. Nothing this phase added reaches it —
// the transport hands bridge a reader, so protocol calls IngestBlock. The
// series therefore stays unambiguous and the shared histogram stays. WHOEVER
// GIVES HandleBlockDirect A PRODUCTION CALLER must split the series in the
// same change, or the two entries will silently share one histogram.
var (
	prometheusSvp2pBridgeBlockHeight                    prometheus.Gauge
	prometheusSvp2pBridgeHandleBlockDirect              prometheus.Histogram
	prometheusSvp2pBridgeProcessBlock                   prometheus.Histogram
	prometheusSvp2pBridgePrepareSubtrees                prometheus.Histogram
	prometheusSvp2pBridgeValidateTransactionsLegacyMode prometheus.Histogram
	prometheusSvp2pBridgePreValidateTransactions        prometheus.Histogram
	prometheusSvp2pBridgeValidateTransactions           prometheus.Histogram
	prometheusSvp2pBridgeExtendTransactions             prometheus.Histogram
	prometheusSvp2pBridgeCreateUtxos                    prometheus.Histogram
	prometheusSvp2pBridgeBlockTxSize                    prometheus.Histogram
	prometheusSvp2pBridgeBlockTxNrInputs                prometheus.Histogram
	prometheusSvp2pBridgeBlockTxNrOutputs               prometheus.Histogram
	prometheusSvp2pBridgeBlockTxValidate                prometheus.Histogram

	// prometheusSvp2pBridgePrewarmErrors counts validator errors observed during the
	// pre-warm path in validateTransactions, labelled by error class. The pre-warm runs
	// before full subtree validation and intentionally drops errors (real validation
	// catches consensus violations on its own), but ops still need a signal so they
	// can detect bursts of service/processing failures that would otherwise be silent.
	// Class labels: tx_invalid, service, processing, policy, other.
	prometheusSvp2pBridgePrewarmErrors *prometheus.CounterVec

	// prometheusSvp2pBridgeOrphanEvictionQueueDrops counts final-validation
	// attempts lost because the orphan pool's eviction-hand-off queue
	// (orphans.go, orphanEvictionQueueSize) was full when onEvict tried to
	// hand one off — fix round 1, Issue I1's best-effort trade, surfaced as
	// a metric per fix round 2's Minor 2 rather than left to a Debugf line
	// only: a sustained drop rate is invisible at info level, and the
	// package already registers other metrics here for this reason.
	prometheusSvp2pBridgeOrphanEvictionQueueDrops prometheus.Counter

	// The five tx-side netsync metrics Phase 2 dropped and Phase 3 gave
	// consumers again (see the RESIDUAL note above): IngestTx and the orphan
	// pool observe them under this package's subsystem.
	prometheusSvp2pBridgeHandleTxMsg               prometheus.Histogram
	prometheusSvp2pBridgeHandleTxMsgValidate       prometheus.Histogram
	prometheusSvp2pBridgeProcessOrphanTransactions prometheus.Histogram
	prometheusSvp2pBridgeOrphans                   prometheus.Gauge
	prometheusSvp2pBridgeOrphanTime                prometheus.Histogram

	prometheusMetricsInitOnce sync.Once
)

func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

func _initPrometheusMetrics() {
	prometheusSvp2pBridgeBlockHeight = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "teranode",
		Subsystem: "svp2p_bridge",
		Name:      "block_height",
		Help:      "The height of the block being processed",
	})
	prometheus.MustRegister(prometheusSvp2pBridgeBlockHeight)

	prometheusSvp2pBridgeHandleBlockDirect = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "svp2p_bridge",
		Name:      "handle_block_direct",
		Help:      "The time taken to handle a block directly",
		Buckets:   util.MetricsBucketsSeconds,
	})
	prometheus.MustRegister(prometheusSvp2pBridgeHandleBlockDirect)

	prometheusSvp2pBridgeProcessBlock = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "svp2p_bridge",
		Name:      "process_block",
		Help:      "The time taken to process a block",
		Buckets:   util.MetricsBucketsSeconds,
	})
	prometheus.MustRegister(prometheusSvp2pBridgeProcessBlock)

	prometheusSvp2pBridgePrepareSubtrees = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "svp2p_bridge",
		Name:      "prepare_subtrees",
		Help:      "The time taken to prepare the subtrees",
		Buckets:   util.MetricsBucketsMilliLongSeconds,
	})
	prometheus.MustRegister(prometheusSvp2pBridgePrepareSubtrees)

	prometheusSvp2pBridgeValidateTransactionsLegacyMode = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "svp2p_bridge",
		Name:      "validate_transactions_legacy_mode",
		Help:      "The time taken to validate transactions in legacy mode",
		Buckets:   util.MetricsBucketsMilliLongSeconds,
	})
	prometheus.MustRegister(prometheusSvp2pBridgeValidateTransactionsLegacyMode)

	prometheusSvp2pBridgeExtendTransactions = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "svp2p_bridge",
		Name:      "extend_transactions",
		Help:      "The time taken to extend transactions",
		Buckets:   util.MetricsBucketsMilliLongSeconds,
	})
	prometheus.MustRegister(prometheusSvp2pBridgeExtendTransactions)

	prometheusSvp2pBridgePreValidateTransactions = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "svp2p_bridge",
		Name:      "pre_validate_transactions",
		Help:      "The time taken to pre-validate transactions",
		Buckets:   util.MetricsBucketsMilliLongSeconds,
	})
	prometheus.MustRegister(prometheusSvp2pBridgePreValidateTransactions)

	prometheusSvp2pBridgeValidateTransactions = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "svp2p_bridge",
		Name:      "validate_transactions",
		Help:      "The time taken to validate transactions",
		Buckets:   util.MetricsBucketsMilliLongSeconds,
	})
	prometheus.MustRegister(prometheusSvp2pBridgeValidateTransactions)

	prometheusSvp2pBridgeCreateUtxos = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "svp2p_bridge",
		Name:      "create_utxos",
		Help:      "The time taken to create UTXOs",
		Buckets:   util.MetricsBucketsMilliLongSeconds,
	})
	prometheus.MustRegister(prometheusSvp2pBridgeCreateUtxos)

	prometheusSvp2pBridgeBlockTxSize = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "svp2p_bridge",
		Name:      "block_tx_size",
		Help:      "The size of the transactions in the block being processed",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 20),
	})
	prometheus.MustRegister(prometheusSvp2pBridgeBlockTxSize)

	prometheusSvp2pBridgeBlockTxNrInputs = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "svp2p_bridge",
		Name:      "block_tx_nr_inputs",
		Help:      "The number of inputs in the block being processed",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 20),
	})
	prometheus.MustRegister(prometheusSvp2pBridgeBlockTxNrInputs)

	prometheusSvp2pBridgeBlockTxNrOutputs = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "svp2p_bridge",
		Name:      "block_tx_nr_outputs",
		Help:      "The number of outputs in the block being processed",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 20),
	})
	prometheus.MustRegister(prometheusSvp2pBridgeBlockTxNrOutputs)

	prometheusSvp2pBridgeBlockTxValidate = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "svp2p_bridge",
		Name:      "block_tx_validate",
		Help:      "The time taken to validate a transaction",
		Buckets:   util.MetricsBucketsMilliSeconds,
	})
	prometheus.MustRegister(prometheusSvp2pBridgeBlockTxValidate)

	prometheusSvp2pBridgePrewarmErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "teranode",
		Subsystem: "svp2p_bridge",
		Name:      "prewarm_validation_errors_total",
		Help:      "Number of validator errors observed during the pre-warm path in validateTransactions, by class",
	}, []string{"class"})
	prometheus.MustRegister(prometheusSvp2pBridgePrewarmErrors)

	prometheusSvp2pBridgeOrphanEvictionQueueDrops = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "teranode",
		Subsystem: "svp2p_bridge",
		Name:      "orphan_eviction_queue_drops_total",
		Help:      "Number of orphan final-validation attempts dropped because the eviction hand-off queue was full",
	})
	prometheus.MustRegister(prometheusSvp2pBridgeOrphanEvictionQueueDrops)
	prometheusSvp2pBridgeHandleTxMsg = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "svp2p_bridge",
		Name:      "handle_tx_msg",
		Help:      "The time taken to ingest a tx message from a peer",
		Buckets:   util.MetricsBucketsMilliSeconds,
	})
	prometheus.MustRegister(prometheusSvp2pBridgeHandleTxMsg)

	prometheusSvp2pBridgeHandleTxMsgValidate = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "svp2p_bridge",
		Name:      "handle_tx_msg_validate",
		Help:      "The time taken by the validator for a tx message from a peer",
		Buckets:   util.MetricsBucketsMilliSeconds,
	})
	prometheus.MustRegister(prometheusSvp2pBridgeHandleTxMsgValidate)

	prometheusSvp2pBridgeProcessOrphanTransactions = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "svp2p_bridge",
		Name:      "process_orphan_transactions",
		Help:      "The time taken to release orphan transactions after a parent is accepted",
		Buckets:   util.MetricsBucketsMilliSeconds,
	})
	prometheus.MustRegister(prometheusSvp2pBridgeProcessOrphanTransactions)

	prometheusSvp2pBridgeOrphans = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "teranode",
		Subsystem: "svp2p_bridge",
		Name:      "orphans",
		Help:      "The number of orphan transactions in the pool",
	})
	prometheus.MustRegister(prometheusSvp2pBridgeOrphans)

	prometheusSvp2pBridgeOrphanTime = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "svp2p_bridge",
		Name:      "orphan_time",
		Help:      "The time an orphan transaction waited in the pool before it was released",
		Buckets:   util.MetricsBucketsSeconds,
	})
	prometheus.MustRegister(prometheusSvp2pBridgeOrphanTime)

}
