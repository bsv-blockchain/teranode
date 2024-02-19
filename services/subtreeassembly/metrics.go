package subtreeassembly

import (
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prometheusSubtreeAssemblyBlocks   prometheus.Histogram
	prometheusSubtreeAssemblySubtrees prometheus.Histogram
)

var prometheusMetricsInitialized = false

func initPrometheusMetrics() {
	if prometheusMetricsInitialized {
		return
	}

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

	prometheusMetricsInitialized = true
}
