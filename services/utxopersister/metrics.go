package utxopersister

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prometheusUTXOPersisterLivenessHeartbeatAge prometheus.Gauge

	prometheusMetricsInitOnce sync.Once
)

// initPrometheusMetrics registers the UTXO persister's metrics once per process.
func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(func() {
		prometheusUTXOPersisterLivenessHeartbeatAge = promauto.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "teranode",
				Subsystem: "utxopersister",
				Name:      "liveness_heartbeat_age_seconds",
				Help:      "Seconds since the UTXO persister main loop last beat its liveness heartbeat (0 before the loop's first beat and after it stops). This is the number utxoPersister_livenessStallTimeout is compared against, so alert on it and learn its worst case before arming a restart on it (issue 1447).",
			},
		)
	})
}
