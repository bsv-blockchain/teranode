package lane

import (
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// The Phase 1 measurements of the block gate, at utxostore_laneMigration shadow. They live here,
// not in each service, because the subtree writers, block validation and the blockchain store all
// report into the same series.

// Gate stages: building a block's rows from its subtrees' bitmaps, in the block's writer, and in
// the blockchain store checking them in memory before the block's database transaction begins,
// then inserting them inside it. Their sum is a block's gate time.
const (
	GateStageBuild  = "build"
	GateStageInsert = "insert"
)

// Why a block's rows were not built.
const (
	// MissingBitmapsAbsent: a subtree of the block has no lane bitmaps file.
	MissingBitmapsAbsent = "absent"
	// MissingBitmapsUnreadable: a subtree's lane bitmaps file could not be read or decoded.
	MissingBitmapsUnreadable = "unreadable"
	// MissingBitmapsTimedOut: reading a block's lane bitmaps files ran past its deadline or was
	// cancelled.
	MissingBitmapsTimedOut = "timed_out"
	// MissingBitmapsWriteFailed: subtree validation failed to write a subtree's lane bitmaps file.
	// It is counted when the write fails, so a block that later lists the subtree counts it again,
	// as absent.
	MissingBitmapsWriteFailed = "write_failed"
)

// Directions of a count mismatch: the rows hold fewer members than the block has transactions
// besides its coinbase, or more.
const (
	MismatchFewer = "fewer"
	MismatchMore  = "more"
)

var (
	prometheusLaneGateSeconds      *prometheus.HistogramVec
	prometheusLaneRowBytes         *prometheus.HistogramVec
	prometheusLaneRowHoles         prometheus.Histogram
	prometheusLaneRowMismatches    *prometheus.CounterVec
	prometheusLaneMissingBitmaps   *prometheus.CounterVec
	prometheusLaneTxsNoPosition    *prometheus.CounterVec
	prometheusLaneRowInsertFailure prometheus.Counter

	rowsMetricsInitOnce sync.Once
)

func initRowsMetrics() {
	rowsMetricsInitOnce.Do(func() {
		prometheusLaneGateSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "lane",
			Name:      "gate_seconds",
			Help:      "Time per block of the lane gate at utxostore_laneMigration shadow, by stage: build (reading and merging the subtrees' lane bitmaps into rows, in block validation) and insert (in the blockchain store, checking the rows in memory before the block's database transaction begins, then inserting them inside it, observed once the block is stored and only when its rows were, with the insert of the attempt that stored it); a block's gate time is the sum. The block's own INSERT and the commit are not included",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 4, 12),
		}, []string{"stage"})
		prometheusLaneRowBytes = promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "lane",
			Name:      "row_bytes",
			Help:      "Bytes one block's map rows of one lane store, 36 per row plus its hole data, by lane",
			Buckets:   prometheus.ExponentialBuckets(32, 4, 13),
		}, []string{"lane"})
		prometheusLaneRowHoles = promauto.NewHistogram(prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "lane",
			Name:      "row_holes",
			Help:      "Positions between a map row's first and last that its block did not take, one observation per row",
			Buckets:   append([]float64{0}, prometheus.ExponentialBuckets(1, 4, 15)...),
		})
		prometheusLaneRowMismatches = promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "lane",
			Name:      "row_count_mismatches_total",
			Help:      "Blocks whose map rows hold fewer or more members than the block's transaction count less its coinbase; counted and logged, and whatever rows were built still go with the block. Counted only when every subtree's lane bitmaps file was read: one whose files hold no position has no rows and, with transactions besides its coinbase, counts as fewer. A block with any file absent, unreadable or not read in time gets no rows and is counted in teranode_lane_missing_bitmaps_total instead",
		}, []string{"direction"})
		prometheusLaneMissingBitmaps = promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "lane",
			Name:      "missing_bitmaps_total",
			Help:      "Subtrees without a usable lane bitmaps file, by reason. absent, unreadable and timed_out are counted by block validation for the subtrees of a block it stores without lane rows; write_failed is counted by subtree validation when it fails to write a subtree's file, and a block that later lists that subtree counts it again as absent",
		}, []string{"reason"})
		prometheusLaneTxsNoPosition = promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "lane",
			Name:      "txs_without_position_total",
			Help:      "Transactions a subtree's lane bitmaps leave out because no valid lane position was known for them, by the service that built the bitmaps; counted each time a subtree is validated, so a transaction in several subtrees, as peers announce the same unmined transactions again, counts once for each. The coinbase placeholder is not counted",
		}, []string{"service"})
		prometheusLaneRowInsertFailure = promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "lane",
			Name:      "row_insert_failures_total",
			Help:      "Blocks whose attempted map rows were not stored, so the block's store could go on: the rows could not be built (block validation), parsed (the blockchain service) or checked (the blockchain store), the block ID does not fit the map, or their insert failed and was rolled back to its savepoint. Rows already stored for that block ID are left in place. In the blockchain store a block is counted once it is stored, from the attempt that stored it, so a retried store counts once and a store that fails counts nothing; a failure of the savepoint itself aborts the block's transaction and is not counted",
		})
	})
}

// ObserveGate records one block's time in a gate stage.
func ObserveGate(stage string, d time.Duration) {
	initRowsMetrics()
	prometheusLaneGateSeconds.WithLabelValues(stage).Observe(d.Seconds())
}

// ObserveBlockRows records the size and holes of one block's rows.
func ObserveBlockRows(rows BlockRows) {
	initRowsMetrics()

	for _, s := range rows.Lanes {
		prometheusLaneRowBytes.WithLabelValues(strconv.Itoa(int(s.Lane))).Observe(float64(s.Bytes))

		for _, h := range s.Holes {
			prometheusLaneRowHoles.Observe(float64(h))
		}
	}
}

// CountRowMismatch counts a block whose rows hold members other than its transactions less the
// coinbase, in direction MismatchFewer or MismatchMore.
func CountRowMismatch(direction string) {
	initRowsMetrics()
	prometheusLaneRowMismatches.WithLabelValues(direction).Inc()
}

// CountMissingBitmaps counts n subtrees of a block without usable lane bitmaps, for reason.
func CountMissingBitmaps(reason string, n int) {
	if n <= 0 {
		return
	}

	initRowsMetrics()
	prometheusLaneMissingBitmaps.WithLabelValues(reason).Add(float64(n))
}

// CountTxsWithoutPosition counts n transactions left out of a subtree's bitmaps by service. It is
// called once per subtree validated, so a transaction counts again in every subtree it is in.
func CountTxsWithoutPosition(service string, n int) {
	if n <= 0 {
		return
	}

	initRowsMetrics()
	prometheusLaneTxsNoPosition.WithLabelValues(service).Add(float64(n))
}

// CountRowInsertFailure counts a block whose attempted rows were not stored, so its store could go
// on: they could not be built, parsed or checked, its block ID does not fit the map, or their
// insert failed and was rolled back to its savepoint, which leaves rows already stored for the
// block ID in place. The blockchain store calls it once the block is stored, once per block.
func CountRowInsertFailure() {
	initRowsMetrics()
	prometheusLaneRowInsertFailure.Inc()
}
