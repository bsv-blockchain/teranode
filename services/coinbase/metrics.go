package coinbase

import (
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"sync"
)

var (
	prometheusHealth                prometheus.Counter
	prometheusRequestFunds          prometheus.Histogram
	prometheusDistributeTransaction prometheus.Histogram
	prometheusGetBalance            prometheus.Histogram
)

var (
	prometheusMetricsInitOnce sync.Once
)

func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

func _initPrometheusMetrics() {
	prometheusHealth = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "coinbase",
			Name:      "health",
			Help:      "Number of calls to the Health endpoint",
		},
	)

	prometheusRequestFunds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "coinbase",
			Name:      "request_funds",
			Help:      "Histogram of calls to the RequestFunds endpoint",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusDistributeTransaction = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "coinbase",
			Name:      "distribute_transaction",
			Help:      "Histogram of calls to the DistributeTransaction endpoint",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusGetBalance = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "coinbase",
			Name:      "get_balance",
			Help:      "Histogram of calls to the GetBalance endpoint",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
}
