package postgres

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prometheusDirectCreateDuration prometheus.Histogram
	prometheusDirectSpendDuration  prometheus.Histogram
	prometheusDirectMinedDuration  prometheus.Histogram
	prometheusDirectConflicts      prometheus.Counter
	prometheusDirectCreate         prometheus.Counter
	prometheusDirectSpend          prometheus.Counter

	// proc-mode DAH sweep (runDAHCursorProc)
	prometheusDAHSweepCallDuration prometheus.Histogram
	prometheusDAHSweepRowsStamped  prometheus.Counter
	prometheusDAHSweepWatermarkLag prometheus.Gauge
	prometheusDAHSweepErrors       prometheus.Counter

	// stagnation monitor (runDAHStagnationMonitor) — the sweep's only
	// remaining "timeout"; it alarms, it never cancels.
	prometheusDAHSweepStalled *prometheus.GaugeVec

	// reconciliation backstop (runDAHReconcile)
	prometheusDAHReconcileCorrected  prometheus.Counter
	prometheusDAHDirtyParentsDrained prometheus.Counter

	// best-effort watermark rewind on reorg (unsetMinedMulti); a non-zero value
	// flags the acknowledged disk-leak risk of a lost rewind (see mined.go).
	prometheusDAHWatermarkRewindFailures prometheus.Counter

	prometheusMetricsInitOnce sync.Once
)

func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(doInitPrometheusMetrics)
}

func doInitPrometheusMetrics() {
	prometheusDirectCreateDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "postgres_utxo",
		Name:      "create_duration_seconds",
		Help:      "Duration of Create() calls in seconds",
		Buckets:   prometheus.DefBuckets,
	})

	prometheusDirectSpendDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "postgres_utxo",
		Name:      "spend_duration_seconds",
		Help:      "Duration of Spend() per-input calls in seconds",
		Buckets:   prometheus.DefBuckets,
	})

	prometheusDirectMinedDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "postgres_utxo",
		Name:      "mined_duration_seconds",
		Help:      "Duration of SetMinedMulti() calls in seconds",
		Buckets:   prometheus.DefBuckets,
	})

	prometheusDirectConflicts = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "teranode",
		Subsystem: "postgres_utxo",
		Name:      "spend_conflicts_total",
		Help:      "Total number of spend conflicts detected",
	})

	prometheusDirectCreate = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "teranode",
		Subsystem: "postgres_utxo",
		Name:      "create_total",
		Help:      "Total number of Create calls",
	})

	prometheusDirectSpend = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "teranode",
		Subsystem: "postgres_utxo",
		Name:      "spend_total",
		Help:      "Total number of Spend calls",
	})

	prometheusDAHSweepCallDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "teranode",
		Subsystem: "postgres_utxo",
		Name:      "dah_sweep_call_duration_seconds",
		Help:      "Duration of a proc-mode dah_sweep_batch() CALL in seconds",
		Buckets:   prometheus.DefBuckets,
	})

	prometheusDAHSweepRowsStamped = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "teranode",
		Subsystem: "postgres_utxo",
		Name:      "dah_sweep_rows_stamped_total",
		Help:      "Total rows stamped by the proc-mode DAH sweep",
	})

	prometheusDAHSweepWatermarkLag = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "teranode",
		Subsystem: "postgres_utxo",
		Name:      "dah_sweep_watermark_lag",
		Help:      "Heights between the DAH watermark and the safe tip (backlog)",
	})

	prometheusDAHSweepErrors = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "teranode",
		Subsystem: "postgres_utxo",
		Name:      "dah_sweep_errors_total",
		Help:      "Total proc-mode DAH sweep CALL errors",
	})

	prometheusDAHSweepStalled = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "teranode",
		Subsystem: "postgres_utxo",
		Name:      "dah_sweep_stalled",
		Help:      "1 when the partition's DAH sweep watermark has been frozen past the stall threshold with backlog present",
	}, []string{"partition"})

	prometheusDAHReconcileCorrected = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "teranode",
		Subsystem: "postgres_utxo",
		Name:      "dah_reconcile_corrected_total",
		Help:      "Total txs rows whose spent_bits/last_spend_height were corrected by the reconciliation backstop",
	})

	prometheusDAHDirtyParentsDrained = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "teranode",
		Subsystem: "postgres_utxo",
		Name:      "dah_dirty_parents_drained_total",
		Help:      "Total dirty-parent heal-queue rows drained by the reconciliation backstop",
	})

	prometheusDAHWatermarkRewindFailures = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "teranode",
		Subsystem: "postgres_utxo",
		Name:      "dah_watermark_rewind_failures_total",
		Help:      "Total best-effort DAH watermark rewinds that failed on reorg (lost rewind = disk-leak risk until the reconciler rotation heals it)",
	})
}
