package rpc

import (
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"sync"
)

var (
	//prometheusHealth                     prometheus.Counter
	prometheusHandleGetBlock             prometheus.Histogram
	prometheusHandleGetBestBlockHash     prometheus.Histogram
	prometheusHandleCreateRawTransaction prometheus.Histogram
	prometheusHandleSendRawTransaction   prometheus.Histogram
	prometheusHandleGenerate             prometheus.Histogram
	prometheusHandleGetMiningCandidate   prometheus.Histogram
	prometheusHandleSubmitMiningSolution prometheus.Histogram
)

var (
	prometheusMetricsInitOnce sync.Once
)

func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

func _initPrometheusMetrics() {
	//prometheusHealth = promauto.NewCounter(
	//	prometheus.CounterOpts{
	//		Namespace: "rpc",
	//		Name:      "health",
	//		Help:      "Number of calls to the health endpoint of the rpc service",
	//	},
	//)
	prometheusHandleGetBlock = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "rpc",
			Name:      "get_block",
			Help:      "Histogram of calls to handleGetBlock in the rpc service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusHandleGetBestBlockHash = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "rpc",
			Name:      "get_best_block_hash",
			Help:      "Histogram of calls to handleGetBestBlockHash in the rpc service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusHandleCreateRawTransaction = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "rpc",
			Name:      "create_raw_transaction",
			Help:      "Histogram of calls to handleCreateRawTransaction in the rpc service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusHandleSendRawTransaction = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "rpc",
			Name:      "send_raw_transaction",
			Help:      "Histogram of calls to handleSendRawTransaction in the rpc service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusHandleGenerate = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "rpc",
			Name:      "generate",
			Help:      "Histogram of calls to handleGenerate in the rpc service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusHandleGetMiningCandidate = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "rpc",
			Name:      "get_mining_candidate",
			Help:      "Histogram of calls to handleGetMiningCandidate in the rpc service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusHandleSubmitMiningSolution = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "rpc",
			Name:      "submit_mining_solution",
			Help:      "Histogram of calls to handleSubmitMiningSolution in the rpc service",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
}
