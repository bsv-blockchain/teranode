package blockpersister

import (
	"sync"

	"github.com/bitcoin-sv/ubsv/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prometheusBlockPersisterBlocks   prometheus.Histogram
	prometheusBlockPersisterSubtrees prometheus.Histogram
)

var (
	prometheusMetricsInitOnce sync.Once
)

func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

func _initPrometheusMetrics() {
	prometheusBlockPersisterBlocks = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockpersister",
			Name:      "blocks_duration_millis",
			Help:      "Duration of block processing by the subtree assembly service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockPersisterSubtrees = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "blockpersister",
			Name:      "subtrees_duration_millis",
			Help:      "Duration of subtree processing by the subtree assembly service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
}
