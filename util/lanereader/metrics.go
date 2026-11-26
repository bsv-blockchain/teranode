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

	prometheusLaneReaderPrepareFailures *prometheus.CounterVec

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
			Help:      "Mined-state checks answered from the lane index beside today's answer, at utxostore_laneMigration shadow, by reader (validator, subtree_prefetch, subtree_already_mined, block_parent, block_duplicate), kind (agree, lanes_only, today_only, different_block, no_position, no_rows, not_loaded, not_ready, off_chain, coinbase, mined_pending: a lanes_only answer from the row of the best block at the follower's last poll while its mined write is unfinished, for at most a minute after the block was stored, seeded: a row the seeder wrote answers mined while today's block list names only the seeder's stand-in block ID 0) and what answered on the lanes side (row, floor, none). Nothing acts on the lanes answer",
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
			Help:      "Positions between each lane's first folded row and its floor that no folded row took, over every lane: holes, unmined and burnt positions. A lane's history before the first row the index folded is not held and not counted",
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
			Help:      "Times the lane index was built from scratch, by reason: start, gap (no held block within 1,024 of the tip) and deep_reorg (a reorg past rows already folded)",
		}, []string{"reason"})
		prometheusLaneReaderFollowErrors = promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "lane_reader",
			Name:      "follow_errors_total",
			Help:      "Failed steps of the lane index following the main chain, and failed reads of the best block's mined_set flag; each is retried",
		})

		prometheusLaneReaderPrepareFailures = promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "lane_reader",
			Name:      "prepare_failures_total",
			Help:      "Failed loads of a block's ancestry for block validation's lanes shadow, by reason: read_error (a header or row read failed) and no_held_ancestor (no block the index holds within 1,024 blocks back; the block's checks count not_loaded until the tip moves)",
		}, []string{"reason"})

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
