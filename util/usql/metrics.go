package usql

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// prometheusQueryRetries tracks total number of retry attempts by retry count
	prometheusQueryRetries *prometheus.CounterVec

	// prometheusQueryRetrySuccess tracks successful queries after retry
	prometheusQueryRetrySuccess prometheus.Counter

	// prometheusQueryRetryExhausted tracks queries that exhausted all retry attempts
	prometheusQueryRetryExhausted prometheus.Counter

	// prometheusCircuitBreakerState tracks the current circuit breaker state (0=closed, 1=open, 2=half-open)
	prometheusCircuitBreakerState prometheus.Gauge

	// prometheusCircuitBreakerOpened tracks total number of times the circuit breaker opened
	prometheusCircuitBreakerOpened prometheus.Counter

	// prometheusCircuitBreakerFastFailed tracks requests rejected while circuit is open
	prometheusCircuitBreakerFastFailed prometheus.Counter

	// prometheusMetricsInitOnce ensures metrics are initialized only once
	prometheusMetricsInitOnce sync.Once
)

// initPrometheusMetrics initializes all Prometheus metrics for database retry and circuit breaker operations
func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(func() {
		prometheusQueryRetries = promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "teranode",
				Subsystem: "db",
				Name:      "query_retries_total",
				Help:      "Total number of database query retry attempts by retry count",
			},
			[]string{"retry_attempt"},
		)

		prometheusQueryRetrySuccess = promauto.NewCounter(
			prometheus.CounterOpts{
				Namespace: "teranode",
				Subsystem: "db",
				Name:      "query_retry_success",
				Help:      "Number of database queries that succeeded after retry",
			},
		)

		prometheusQueryRetryExhausted = promauto.NewCounter(
			prometheus.CounterOpts{
				Namespace: "teranode",
				Subsystem: "db",
				Name:      "query_retry_exhausted",
				Help:      "Number of database queries that exhausted all retry attempts",
			},
		)

		// Circuit breaker metrics
		prometheusCircuitBreakerState = promauto.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "teranode",
				Subsystem: "db",
				Name:      "circuit_breaker_state",
				Help:      "Current circuit breaker state (0=closed, 1=open, 2=half-open)",
			},
		)

		prometheusCircuitBreakerOpened = promauto.NewCounter(
			prometheus.CounterOpts{
				Namespace: "teranode",
				Subsystem: "db",
				Name:      "circuit_breaker_opened_total",
				Help:      "Total number of times the circuit breaker opened",
			},
		)

		prometheusCircuitBreakerFastFailed = promauto.NewCounter(
			prometheus.CounterOpts{
				Namespace: "teranode",
				Subsystem: "db",
				Name:      "circuit_breaker_fast_failed_total",
				Help:      "Total number of requests rejected while circuit breaker is open",
			},
		)
	})
}

func init() {
	initPrometheusMetrics()
}
