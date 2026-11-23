package lanereader

import (
	"sync"

	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prometheusLaneReaderAnswers       *prometheus.CounterVec
	prometheusLaneReaderLookupSeconds prometheus.Histogram
	prometheusLaneReaderIndexBytes    prometheus.Gauge
	prometheusLaneReaderIndexRows     prometheus.Gauge
	prometheusLaneReaderIndexBlocks   prometheus.Gauge
	prometheusLaneReaderIndexListed   prometheus.Gauge
	prometheusLaneReaderTipHeight     prometheus.Gauge
	prometheusLaneReaderAnchorHeight  prometheus.Gauge
	prometheusLaneReaderLoads         *prometheus.CounterVec
	prometheusLaneReaderRebuilds      *prometheus.CounterVec
	prometheusLaneReaderFollowErrors  prometheus.Counter

	// answerCounters holds every reader, kind and via's counter, resolved once, so counting an
	// answer is an atomic add rather than a label lookup.
	answerCounters map[string]map[string][3]prometheus.Counter

	metricsInitOnce sync.Once
)

func initMetrics() {
	metricsInitOnce.Do(func() {
		prometheusLaneReaderAnswers = promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "lane_reader",
			Name:      "answers_total",
			Help:      "Mined-state checks answered from the lane index beside today's answer, at utxostore_laneMigration shadow, by reader (validator, subtree_prefetch, subtree_already_mined, block_parent, block_duplicate), kind (agree, lanes_only, today_only, different_block, no_position, no_rows, not_loaded, not_ready, off_chain) and what answered on the lanes side (row, floor, none). Nothing acts on the lanes answer",
		}, []string{"reader", "kind", "via"})
		prometheusLaneReaderLookupSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "lane_reader",
			Name:      "lookup_seconds",
			Help:      "Time of one lookup in the lane index, one lookup in 64 timed",
			Buckets:   prometheus.ExponentialBuckets(10e-9, 2, 14),
		})
		prometheusLaneReaderIndexBytes = promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "lane_reader",
			Name:      "index_bytes",
			Help:      "Memory the lane index's rows and floors hold, as Roaring reports its bitmaps plus each entry; the Go heap behind them is somewhat larger",
		})
		prometheusLaneReaderIndexRows = promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "lane_reader",
			Name:      "index_rows",
			Help:      "Rows the lane index holds, those of blocks not yet folded into the floors",
		})
		prometheusLaneReaderIndexBlocks = promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "lane_reader",
			Name:      "index_blocks",
			Help:      "Blocks the lane index knows, folded ones included",
		})
		prometheusLaneReaderIndexListed = promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "lane_reader",
			Name:      "index_listed_positions",
			Help:      "Positions below the lane floors that no folded row took, over every lane: holes, unmined and burnt positions, and those below the first row the index saw",
		})
		prometheusLaneReaderTipHeight = promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "lane_reader",
			Name:      "tip_height",
			Help:      "Height of the main chain's tip as the lane index holds it",
		})
		prometheusLaneReaderAnchorHeight = promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "lane_reader",
			Name:      "anchor_height",
			Help:      "Height of the newest block folded into the lane floors",
		})
		prometheusLaneReaderLoads = promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "lane_reader",
			Name:      "block_loads_total",
			Help:      "Blocks whose lane map rows the lane index read, by outcome (ok, error)",
		}, []string{"outcome"})
		prometheusLaneReaderRebuilds = promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "lane_reader",
			Name:      "rebuilds_total",
			Help:      "Times the lane index was built from scratch, by reason: start, gap (no held block within 1,000 of the tip) and deep_reorg (a reorg past rows already folded)",
		}, []string{"reason"})
		prometheusLaneReaderFollowErrors = promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "lane_reader",
			Name:      "follow_errors_total",
			Help:      "Failed steps of the lane index following the main chain; each is retried",
		})

		answerCounters = make(map[string]map[string][3]prometheus.Counter, len(readers))

		for _, reader := range readers {
			byKind := make(map[string][3]prometheus.Counter, len(kinds))

			for _, kind := range kinds {
				var c [3]prometheus.Counter

				for _, via := range []lane.Via{lane.ViaNone, lane.ViaRow, lane.ViaFloor} {
					c[via] = prometheusLaneReaderAnswers.WithLabelValues(reader, kind, via.String())
				}

				byKind[kind] = c
			}

			answerCounters[reader] = byKind
		}
	})
}

// count counts one answer.
func count(reader, kind string, via lane.Via) {
	answerCounters[reader][kind][via].Inc()
}
