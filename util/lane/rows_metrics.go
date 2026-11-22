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

// Gate stages: building a block's rows from its subtrees' bitmaps, in the block's writer, and
// inserting them in the block's database transaction, in the blockchain store. Their sum is a
// block's gate time.
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
			Help:      "Time per block of the lane gate at utxostore_laneMigration shadow, by stage: build (merging the subtrees' lane bitmaps into rows) and insert (writing the rows in the block's database transaction); a block's gate time is the sum",
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
			Help:      "Blocks whose map rows hold fewer or more members than the block's transaction count less its coinbase; counted and logged, the block is stored as it is without lanes",
		}, []string{"direction"})
		prometheusLaneMissingBitmaps = promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "lane",
			Name:      "missing_bitmaps_total",
			Help:      "Subtrees of a block with no usable lane bitmaps file, by reason; the block's rows are not built and the block is stored as it is without lanes",
		}, []string{"reason"})
		prometheusLaneTxsNoPosition = promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "lane",
			Name:      "txs_without_position_total",
			Help:      "Transactions a subtree's lane bitmaps leave out because no valid lane position was known for them, by the service that built the bitmaps; the coinbase placeholder is not counted",
		}, []string{"service"})
		prometheusLaneRowInsertFailure = promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "lane",
			Name:      "row_insert_failures_total",
			Help:      "Blocks whose map rows the blockchain store failed to insert; the rows are rolled back and the block is stored without them",
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

// CountTxsWithoutPosition counts n transactions left out of a subtree's bitmaps by service.
func CountTxsWithoutPosition(service string, n int) {
	if n <= 0 {
		return
	}

	initRowsMetrics()
	prometheusLaneTxsNoPosition.WithLabelValues(service).Add(float64(n))
}

// CountRowInsertFailure counts a block whose rows the store could not insert.
func CountRowInsertFailure() {
	initRowsMetrics()
	prometheusLaneRowInsertFailure.Inc()
}
