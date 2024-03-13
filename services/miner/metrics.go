package miner

import (
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"sync"
)

var (
	prometheusBlockMined         prometheus.Counter
	prometheusBlockMinedDuration prometheus.Histogram
)

var (
	prometheusMetricsInitOnce sync.Once
)

func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

func _initPrometheusMetrics() {
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
			Name:      "block_mined_duration_seconds",
			Help:      "Duration of block mining",
			Buckets:   util.MetricsBucketsSeconds,
		},
	)
}
