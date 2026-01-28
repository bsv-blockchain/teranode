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

	// prometheusMetricsInitOnce ensures metrics are initialized only once
	prometheusMetricsInitOnce sync.Once
)

// initPrometheusMetrics initializes all Prometheus metrics for database retry operations
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
	})
}

func init() {
	initPrometheusMetrics()
}
