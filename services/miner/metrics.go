package miner

import (
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"sync"
)

var (
	prometheusBlockMined prometheus.Histogram
)

var (
	prometheusMetricsInitOnce sync.Once
)

func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

func _initPrometheusMetrics() {
	prometheusBlockMined = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "miner",
			Name:      "block_mined",
			Help:      "Histogram of block mining",
			Buckets:   util.MetricsBucketsSeconds,
		},
	)
}
