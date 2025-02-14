package model

import (
	"sync"

	"github.com/bitcoin-sv/teranode/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prometheusBlockFromBytes              prometheus.Histogram
	prometheusBlockValid                  prometheus.Histogram
	prometheusBlockCheckMerkleRoot        prometheus.Histogram
	prometheusBlockGetSubtrees            prometheus.Histogram
	prometheusBlockGetAndValidateSubtrees prometheus.Histogram
	prometheusBloomQueryCounter           prometheus.Gauge
	prometheusBloomPositiveCounter        prometheus.Gauge
	prometheusBloomFalsePositiveCounter   prometheus.Gauge
)

var (
	prometheusMetricsInitOnce sync.Once
)

func init() {
	initPrometheusMetrics()
}

func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

func _initPrometheusMetrics() {
	prometheusBlockFromBytes = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "block",
			Name:      "from_bytes",
			Help:      "Histogram of Block.FromBytes",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockValid = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "block",
			Name:      "valid",
			Help:      "Histogram of Block.Valid",
			Buckets:   util.MetricsBucketsSeconds,
		},
	)

	prometheusBlockCheckMerkleRoot = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "block",
			Name:      "check_merkle_root",
			Help:      "Histogram of Block.CheckMerkleRoot",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockGetSubtrees = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "block",
			Name:      "get_subtrees",
			Help:      "Histogram of Block.GetSubtrees",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockGetAndValidateSubtrees = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "block",
			Name:      "get_and_validate_subtrees",
			Help:      "Histogram of Block.GetAndValidateSubtrees",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBloomQueryCounter = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "block",
			Name:      "bloom_filter_query_counter",
			Help:      "Number of queries to the bloom filter",
		},
	)

	prometheusBloomPositiveCounter = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "block",
			Name:      "bloom_filter_positive_counter",
			Help:      "Number of positive from the bloom filter",
		},
	)

	prometheusBloomFalsePositiveCounter = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "block",
			Name:      "bloom_filter_false_positive_counter",
			Help:      "Number of false positives from the bloom filter",
		},
	)
}
