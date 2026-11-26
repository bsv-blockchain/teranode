package lanereader

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prometheusLanePrunerVerdicts *prometheus.CounterVec
	prometheusLanePrunerLookups  *prometheus.CounterVec
	prometheusLanePrunerReads    *prometheus.CounterVec
	prometheusLanePrunerUnjudged *prometheus.CounterVec
	prometheusLanePrunerSeconds  *prometheus.CounterVec

	pruneMetricsInitOnce sync.Once
)

func initPruneMetrics() {
	pruneMetricsInitOnce.Do(func() {
		prometheusLanePrunerVerdicts = promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "lane_pruner",
			Name:      "verdicts_total",
			Help:      "Pruning decisions judged by the lane rule beside today's rule, at utxostore_laneMigration shadow, by store (utxoset, aerospike), today's decision (delete; keep; keep_undo, a utxoset journal row carried only for a reorg because its spender is mined), the lane rule's (delete, keep, cannot_judge) and reason (settled; parent_ or spender_ unmined, off_chain, shallow; conflicting; no_position, not_ready, not_loaded, no_rows, spender_unread, coinbase). today=keep with lanes=delete is the direction that must never happen. Only today's decision is acted on",
		}, []string{"store", "today", "lanes", "reason"})
		prometheusLanePrunerLookups = promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "lane_pruner",
			Name:      "lookups_total",
			Help:      "Positions the pruner's lane rule looked up in the lane index, by store and what answered (row, floor, none)",
		}, []string{"store", "via"})
		prometheusLanePrunerReads = promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "lane_pruner",
			Name:      "extra_reads_total",
			Help:      "Records or rows the pruner read only for the lane rule, beyond what today's pruner reads, by store and kind (aerospike: page, a page record read for its slots; page_error, a page whose batch read failed, its master counted spender_unread; page_skipped, a page not read because an earlier page read outlived its bound and still runs, its master counted spender_unread; and utxos_bin, a record the query returned with the utxos bin it reads only for the rule; utxoset: journal_row, position)",
		}, []string{"store", "kind"})
		prometheusLanePrunerUnjudged = promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "lane_pruner",
			Name:      "unjudged_total",
			Help:      "Pruning decisions the lane rule did not judge, by store and reason (aerospike: record_error, a record the query returned with an error or no txid; chunk_error, the records of a chunk whose processing failed; and page_without_master, a page today deletes whose master the chunk does not hold, an upper bound on pages deleted without their master, since a master is often judged in another chunk; utxoset: backlog, one per retiring journal partition of a pass that retires more than one, none of which is judged; leaf_error, one per retiring journal partition whose judging stopped early; budget, one per partition the pass's time budget left unjudged; and not_ready, one per retiring partition before the lane index's first snapshot)",
		}, []string{"store", "reason"})
		prometheusLanePrunerSeconds = promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "lane_pruner",
			Name:      "judge_seconds_total",
			Help:      "Wall time the pruner spent in the lane rule, by store, not counting the extra reads; that work does no I/O, so its rate approximates the cores the rule adds",
		}, []string{"store"})
	})
}
