package miner

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prometheusBlockMined         prometheus.Counter
	prometheusBlockMinedDuration prometheus.Histogram
)

var prometheusMetricsInitialized = false

func initPrometheusMetrics() {
	if prometheusMetricsInitialized {
		return
	}

	prometheusBlockMined = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "miner",
			Name:      "block_mined",
			Help:      "Number of calls to the health endpoint of the miner service",
		},
	)

	prometheusBlockMinedDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "miner",
			Name:      "block_mined_duration",
			Help:      "Duration of block mining",
		},
	)

	prometheusMetricsInitialized = true
}
