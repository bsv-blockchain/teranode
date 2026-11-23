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
	// MissingBitmapsAbsent: a subtree of a peer's block has no lane bitmaps file, or a subtree of a
	// block this node mined has no bitmaps kept and no storage running for it.
	MissingBitmapsAbsent = "absent"
	// MissingBitmapsUnreadable: a subtree's lane bitmaps file could not be read or decoded (peers'
	// blocks only).
	MissingBitmapsUnreadable = "unreadable"
	// MissingBitmapsTimedOut: reading a peer block's lane bitmaps files ran past its deadline or was
	// cancelled, or a subtree of a block this node mined was still being stored when its wait
	// ended.
	MissingBitmapsTimedOut = "timed_out"
)

// Whose block the measurement is of, the block label of every series here: a peer's block, whose
// rows block validation builds, or a block this node mined, whose rows block assembly builds.
const (
	BlockPeer = "peer"
	BlockOwn  = "own"
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
	prometheusLaneRowHoles         *prometheus.HistogramVec
	prometheusLaneRowMismatches    *prometheus.CounterVec
	prometheusLaneMissingBitmaps   *prometheus.CounterVec
	prometheusLaneTxsNoPosition    *prometheus.CounterVec
	prometheusLaneRowInsertFailure *prometheus.CounterVec

	rowsMetricsInitOnce sync.Once
)

func initRowsMetrics() {
	rowsMetricsInitOnce.Do(func() {
		prometheusLaneGateSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "lane",
			Name:      "gate_seconds",
			Help:      "Time per block of the lane gate at utxostore_laneMigration shadow, by stage: build (merging the subtrees' lane bitmaps into rows, in block validation for a peer's block, after reading them from the subtree store, and in block assembly for its own, from the bitmaps it kept when it stored each subtree) and insert (in the blockchain store, checking the rows in memory before the block's database transaction begins, then inserting them inside it, observed once the block is stored and only when its rows were, with the insert of the attempt that stored it); a block's gate time is the sum. The block's own INSERT and the commit are not included. block is peer or own",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 4, 12),
		}, []string{"block", "stage"})
		prometheusLaneRowBytes = promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "lane",
			Name:      "row_bytes",
			Help:      "Bytes one block's map rows of one lane store, 36 per row plus its hole data, by lane; block is peer or own",
			Buckets:   prometheus.ExponentialBuckets(32, 4, 13),
		}, []string{"block", "lane"})
		prometheusLaneRowHoles = promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "lane",
			Name:      "row_holes",
			Help:      "Positions between a map row's first and last that its block did not take, one observation per row; block is peer or own",
			Buckets:   append([]float64{0}, prometheus.ExponentialBuckets(1, 4, 15)...),
		}, []string{"block"})
		prometheusLaneRowMismatches = promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "lane",
			Name:      "row_count_mismatches_total",
			Help:      "Blocks whose map rows hold fewer or more members than the block's transaction count less its coinbase; counted and logged, and whatever rows were built still go with the block. Counted only when every subtree's lane bitmaps were found: one whose bitmaps hold no position has no rows and, with transactions besides its coinbase, counts as fewer. A block with any subtree's bitmaps absent, unreadable or not read in time gets no rows and is counted in teranode_lane_missing_bitmaps_total instead. For a peer's block the bitmaps are its subtrees' lane bitmaps files; for a block this node mined they are the ones block assembly kept. block is peer or own",
		}, []string{"block", "direction"})
		prometheusLaneMissingBitmaps = promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "lane",
			Name:      "missing_bitmaps_total",
			Help:      "Subtrees of a block with no usable lane bitmaps, by reason; the block's rows are not built and the block is stored as it is without lanes. For a peer's block the bitmaps are its subtrees' lane bitmaps files: absent, unreadable, or timed_out when not read by the deadline. For a block this node mined (block own) they are the bitmaps block assembly kept in memory, not read from a file: timed_out when the subtree was still being stored as the wait ended, absent when nothing was kept and no storage was running",
		}, []string{"block", "reason"})
		prometheusLaneTxsNoPosition = promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "lane",
			Name:      "txs_without_position_total",
			Help:      "Transactions a subtree's lane bitmaps leave out because no valid lane position was known for them, by the service that built the bitmaps; counted each time a subtree is validated (subtreevalidation) or stored (blockassembly), so a transaction in several subtrees, as peers announce the same unmined transactions again or block assembly rebuilds its subtrees after a block, counts once for each. The coinbase placeholder is not counted",
		}, []string{"service"})
		prometheusLaneRowInsertFailure = promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "lane",
			Name:      "row_insert_failures_total",
			Help:      "Blocks whose attempted map rows were not stored, so the block's store could go on: the rows could not be built (block validation or block assembly), parsed (the blockchain service) or checked (the blockchain store), the block ID does not fit the map, or their insert failed and was rolled back to its savepoint. Rows already stored for that block ID are left in place. In the blockchain store a block is counted once it is stored, from the attempt that stored it, so a retried store counts once and a store that fails counts nothing; a failure of the savepoint itself aborts the block's transaction and is not counted. block is peer or own",
		}, []string{"block"})
	})
}

// ObserveGate records one block's time in a gate stage; block is BlockPeer or BlockOwn.
func ObserveGate(block, stage string, d time.Duration) {
	initRowsMetrics()
	prometheusLaneGateSeconds.WithLabelValues(block, stage).Observe(d.Seconds())
}

// ObserveBlockRows records the size and holes of one block's rows; block is BlockPeer or BlockOwn.
func ObserveBlockRows(block string, rows BlockRows) {
	initRowsMetrics()

	holes := prometheusLaneRowHoles.WithLabelValues(block)

	for _, s := range rows.Lanes {
		prometheusLaneRowBytes.WithLabelValues(block, strconv.Itoa(int(s.Lane))).Observe(float64(s.Bytes))

		for _, h := range s.Holes {
			holes.Observe(float64(h))
		}
	}
}

// CountRowMismatch counts a block whose rows hold members other than its transactions less the
// coinbase, in direction MismatchFewer or MismatchMore; block is BlockPeer or BlockOwn.
func CountRowMismatch(block, direction string) {
	initRowsMetrics()
	prometheusLaneRowMismatches.WithLabelValues(block, direction).Inc()
}

// CountMissingBitmaps counts n subtrees of a block without usable lane bitmaps, for reason; block
// is BlockPeer or BlockOwn.
func CountMissingBitmaps(block, reason string, n int) {
	if n <= 0 {
		return
	}

	initRowsMetrics()
	prometheusLaneMissingBitmaps.WithLabelValues(block, reason).Add(float64(n))
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
// block ID in place. The blockchain store calls it once the block is stored, once per block. block
// is BlockPeer or BlockOwn.
func CountRowInsertFailure(block string) {
	initRowsMetrics()
	prometheusLaneRowInsertFailure.WithLabelValues(block).Inc()
}
