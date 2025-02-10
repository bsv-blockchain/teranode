package coinbase

import (
	"sync"

	"github.com/bitcoin-sv/teranode/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prometheusHealth                 prometheus.Counter
	prometheusRequestFunds           prometheus.Histogram
	prometheusDistributeTransaction  prometheus.Histogram
	prometheusGetBalance             prometheus.Histogram
	prometheusSetMalformedUTXOConfig prometheus.Histogram
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
			Namespace: "teranode",
			Subsystem: "coinbase",
			Name:      "health",
			Help:      "Number of calls to the Health endpoint",
		},
	)

	prometheusRequestFunds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "coinbase",
			Name:      "request_funds",
			Help:      "Histogram of calls to the RequestFunds endpoint",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusDistributeTransaction = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "coinbase",
			Name:      "distribute_transaction",
			Help:      "Histogram of calls to the DistributeTransaction endpoint",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusGetBalance = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "coinbase",
			Name:      "get_balance",
			Help:      "Histogram of calls to the GetBalance endpoint",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusSetMalformedUTXOConfig = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "coinbase",
			Name:      "set_malformed_utxo_config",
			Help:      "Histogram of calls to the SetMalformedUTXOConfig endpoint",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
}
