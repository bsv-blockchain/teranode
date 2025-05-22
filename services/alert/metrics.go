// Package alert implements the Bitcoin SV alert system server and related functionality.
// This file contains the metrics-related code for the alert service, which tracks
// and exposes operational metrics via Prometheus for monitoring and observability.
package alert

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Prometheus metrics for the alert service.
var (
	// prometheusHealth tracks the number of health check requests processed.
	// This metric provides visibility into how frequently health checks are being performed,
	// essential for monitoring system stability and external monitoring activity.
	prometheusHealth prometheus.Counter
)

// Initialization synchronization variables.
var (
	// prometheusMetricsInitOnce ensures that Prometheus metrics are initialized exactly once,
	// even if initialization is requested from multiple goroutines concurrently.
	// This prevents duplicate metric registration which would cause Prometheus to panic.
	prometheusMetricsInitOnce sync.Once
)

// initPrometheusMetrics initializes all Prometheus metrics for the alert service.
// This function uses sync.Once to ensure that metrics are registered exactly once,
// regardless of how many times this function is called. This is important because
// duplicate registration of Prometheus metrics would cause a panic.
//
// This function should be called during service initialization and before any
// metrics are used. It delegates the actual initialization work to the internal
// _initPrometheusMetrics function.
func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

// _initPrometheusMetrics is the internal function that actually initializes the metrics.
// It should not be called directly - use initPrometheusMetrics instead.
//
// This function registers all Prometheus metrics used by the alert service with the
// appropriate namespaces, subsystems, and help text. Each metric is created using
// the promauto factory, which automatically registers metrics with the global
// Prometheus registry.
//
// Current metrics include:
// - health: Counter that tracks the number of health check requests
//
// When adding new metrics, they should be initialized here and follow the same
// naming and organization patterns for consistency.
func _initPrometheusMetrics() {
	prometheusHealth = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "alert",
			Name:      "health",
			Help:      "Number of calls to the Health endpoint",
		},
	)
}
