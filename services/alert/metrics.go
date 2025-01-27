// Package alert implements the Bitcoin SV alert system server and related functionality.
package alert

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prometheusHealth prometheus.Counter
)

var (
	prometheusMetricsInitOnce sync.Once
)

// initPrometheusMetrics initializes all Prometheus metrics using sync.Once.
func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

// _initPrometheusMetrics is the internal function that actually initializes the metrics.
// It should not be called directly - use initPrometheusMetrics instead.
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
