package pruner

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prunerDuration  *prometheus.HistogramVec
	prunerSkipped   *prometheus.CounterVec
	prunerProcessed prometheus.Counter
	prunerErrors    *prometheus.CounterVec

	// Blob deletion metrics
	blobDeletionScheduledTotal  *prometheus.CounterVec
	blobDeletionCancelledTotal  *prometheus.CounterVec
	blobDeletionProcessedTotal  prometheus.Counter
	blobDeletionErrorsTotal     *prometheus.CounterVec
	blobDeletionDurationSeconds *prometheus.HistogramVec
	blobDeletionPendingGauge    prometheus.Gauge

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
// - Duration of pruner operations (preserve_parents, dah_pruner)
// - Operations skipped due to various conditions
// - Successfully processed operations
// - Errors during pruner operations
func _initPrometheusMetrics() {
	prunerDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "pruner_duration_seconds",
			Help:    "Duration of pruner operations in seconds",
			Buckets: prometheus.ExponentialBuckets(1, 2, 10), // 1s to ~17 minutes
		},
		[]string{"operation"}, // "preserve_parents" or "dah_pruner"
	)

	prunerSkipped = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pruner_skipped_total",
			Help: "Number of pruner operations skipped",
		},
		[]string{"reason"}, // "not_running", "no_new_height", "already_in_progress"
	)

	prunerProcessed = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "pruner_processed_total",
			Help: "Total number of successful pruner operations",
		},
	)

	prunerErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pruner_errors_total",
			Help: "Total number of pruner errors",
		},
		[]string{"operation"}, // "preserve_parents", "dah_pruner", "poll"
	)

	// Blob deletion metrics
	blobDeletionScheduledTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pruner_blob_deletion_scheduled_total",
			Help: "Total blob deletions scheduled",
		},
		[]string{"store_id"},
	)

	blobDeletionCancelledTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pruner_blob_deletion_cancelled_total",
			Help: "Total blob deletions cancelled",
		},
		[]string{"store_id"},
	)

	blobDeletionProcessedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "pruner_blob_deletion_processed_total",
			Help: "Total blobs successfully deleted",
		},
	)

	blobDeletionErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pruner_blob_deletion_errors_total",
			Help: "Total blob deletion errors",
		},
		[]string{"store_id"},
	)

	blobDeletionDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "pruner_blob_deletion_duration_seconds",
			Help:    "Blob deletion duration",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 15),
		},
		[]string{"store_id"},
	)

	blobDeletionPendingGauge = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "pruner_blob_deletion_pending",
			Help: "Number of pending deletions in queue",
		},
	)
}
