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
}
