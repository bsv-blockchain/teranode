package pruner

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prunerDuration *prometheus.HistogramVec
	prunerSkipped  *prometheus.CounterVec
	prunerErrors   *prometheus.CounterVec

	// Pruner operation metrics
	prunerUpdatingParents  prometheus.Counter
	prunerDeletingChildren prometheus.Counter
	prunerCurrentHeight    prometheus.Gauge
	prunerActive           prometheus.Gauge

	// Blob deletion metrics
	blobDeletionScheduledTotal  *prometheus.CounterVec
	blobDeletionCancelledTotal  *prometheus.CounterVec
	blobDeletionProcessedTotal  prometheus.Counter
	blobDeletionNotFoundTotal   *prometheus.CounterVec
	blobDeletionErrorsTotal     *prometheus.CounterVec
	blobDeletionDurationSeconds *prometheus.HistogramVec
	blobDeletionPendingGauge    prometheus.Gauge

	// Stamp worker metrics. Section 12 of the block-facts design names each one.
	stampDrains           *prometheus.CounterVec // by outcome
	stampWakesSkipped     *prometheus.CounterVec // by reason
	stampAncestryRejected *prometheus.CounterVec // by the check that failed
	stampDrainsAbandoned  prometheus.Counter
	stampStaleAnchorHints prometheus.Counter
	stampTimerDrains      prometheus.Counter
	stampResidualLag      prometheus.Gauge
	stampDrainStarted     prometheus.Gauge
	stampWindowsPerDrain  prometheus.Histogram
	stampPagesPerDrain    prometheus.Histogram
	stampDrainDuration    prometheus.Histogram

	prometheusMetricsInitOnce sync.Once
)

// initPrometheusMetrics initializes all Prometheus metrics for the pruner service.
// This function uses sync.Once to ensure metrics are only initialized once,
// regardless of how many times it's called, preventing duplicate metric registration errors.
func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

// _initPrometheusMetrics is the internal implementation that registers all Prometheus metrics
// used by the pruner service. Metrics track:
// - Duration of pruner operations (preserve_parents, expire_preservations, dah_pruner)
// - Operations skipped due to various conditions
// - Successfully processed operations
// - Errors during pruner operations
func _initPrometheusMetrics() {
	prunerDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "pruner",
			Name:      "duration_seconds",
			Help:      "Duration of pruner operations in seconds",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 10), // 1s to ~17 minutes
		},
		[]string{"operation"}, // "preserve_parents", "expire_preservations", "dah_pruner"
	)

	prunerSkipped = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "pruner",
			Name:      "skipped_total",
			Help:      "Number of pruner operations skipped",
		},
		[]string{"reason"}, // "block_assembly_timeout", "below_min_height", "fsm_error", "catchup_mode"
	)

	prunerErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "pruner",
			Name:      "errors_total",
			Help:      "Total number of pruner errors",
		},
		[]string{"operation"}, // "preserve_parents", "expire_preservations", "dah_pruner"
	)

	prunerUpdatingParents = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "pruner",
			Name:      "updating_parents_total",
			Help:      "Total number of unmined transactions whose parents were preserved",
		},
	)

	prunerDeletingChildren = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "pruner",
			Name:      "deleting_children_total",
			Help:      "Total number of records deleted by the DAH pruner",
		},
	)

	prunerCurrentHeight = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "pruner",
			Name:      "current_height",
			Help:      "Current block height reached by the pruner",
		},
	)

	prunerActive = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "pruner",
			Name:      "active",
			Help:      "Whether the pruner is currently active (1) or idle (0)",
		},
	)

	// Blob deletion metrics
	blobDeletionScheduledTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "pruner",
			Name:      "blob_deletion_scheduled_total",
			Help:      "Total blob deletions scheduled",
		},
		[]string{"store_id"},
	)

	blobDeletionCancelledTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "pruner",
			Name:      "blob_deletion_cancelled_total",
			Help:      "Total blob deletions cancelled",
		},
		[]string{"store_id"},
	)

	blobDeletionProcessedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "pruner",
			Name:      "blob_deletion_processed_total",
			Help:      "Total blobs successfully deleted from disk",
		},
	)

	blobDeletionNotFoundTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "pruner",
			Name:      "blob_deletion_not_found_total",
			Help:      "Total blob deletions where the file was already absent from disk (idempotent success). A sustained high rate may indicate a volume mount misconfiguration.",
		},
		[]string{"store_id"},
	)

	blobDeletionErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "pruner",
			Name:      "blob_deletion_errors_total",
			Help:      "Total blob deletion errors",
		},
		[]string{"store_id"},
	)

	blobDeletionDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "pruner",
			Name:      "blob_deletion_duration_seconds",
			Help:      "Blob deletion duration",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 15),
		},
		[]string{"store_id"},
	)

	blobDeletionPendingGauge = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "pruner",
			Name:      "blob_deletion_pending",
			Help:      "Number of pending deletions in queue",
		},
	)

	stampDrains = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pruner_stamp_drains_total",
			Help: "Stamp drains run by the pruner's stamp worker, by how each one ended",
		},
		[]string{"outcome"}, // completed, nothing_stampable, abandoned, ancestry_rejected, error
	)

	stampWakesSkipped = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pruner_stamp_wakes_skipped_total",
			Help: "Stamp worker wakes that ran no drain, by reason",
		},
		[]string{"reason"}, // no_chain_client, lock_held, below_min_height, catchup_mode, fsm_error, block_assembly_timeout
	)

	stampAncestryRejected = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pruner_stamp_ancestry_rejected_total",
			Help: "Chain answers the stamp worker could not prove, by the check that failed",
		},
		[]string{"check"},
	)

	stampDrainsAbandoned = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "pruner_stamp_drains_abandoned_total",
			Help: "Stamp drains abandoned mid-drain because the best chain switched branches",
		},
	)

	stampStaleAnchorHints = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "pruner_stamp_stale_anchor_hints_total",
			Help: "Notifications whose block was no longer on the best chain when the drain started",
		},
	)

	stampTimerDrains = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "pruner_stamp_timer_drains_total",
			Help: "Stamp drains started by the retry timer rather than a notification; healthy value at the tip is zero",
		},
	)

	stampResidualLag = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "pruner_stamp_residual_lag_windows",
			Help: "Windows stampable at the live tip with no completion record, read as the last drain exited",
		},
	)

	stampDrainStarted = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "pruner_stamp_drain_started_seconds",
			Help: "Unix time the drain in progress started, or zero when no drain is open",
		},
	)

	stampWindowsPerDrain = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "pruner_stamp_windows_per_drain",
			Help:    "Windows completed by one drain",
			Buckets: prometheus.ExponentialBuckets(1, 2, 8), // 1 to 128
		},
	)

	stampPagesPerDrain = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "pruner_stamp_pages_per_drain",
			Help:    "Pages committed by one drain",
			Buckets: prometheus.ExponentialBuckets(1, 4, 8), // 1 to 16384
		},
	)

	stampDrainDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "pruner_stamp_drain_seconds",
			Help:    "Wall time of one drain, open to close",
			Buckets: prometheus.ExponentialBuckets(0.1, 2, 14), // 0.1s to ~14 minutes
		},
	)
}
