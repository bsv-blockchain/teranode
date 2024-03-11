package subtreeassembly

import (
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"sync"
)

var (
	prometheusSubtreeAssemblyBlocks   prometheus.Histogram
	prometheusSubtreeAssemblySubtrees prometheus.Histogram
)

var (
	prometheusMetricsInitOnce sync.Once
)

func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

func _initPrometheusMetrics() {
	prometheusSubtreeAssemblyBlocks = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "subtreeassembly",
			Name:      "blocks_duration_millis",
			Help:      "Duration of block processing by the subtree assembly service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusSubtreeAssemblySubtrees = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "subtreeassembly",
			Name:      "subtrees_duration_millis",
			Help:      "Duration of subtree processing by the subtree assembly service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
}
