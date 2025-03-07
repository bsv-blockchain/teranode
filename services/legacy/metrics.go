// Package legacy implements a Bitcoin SV legacy protocol server that handles peer-to-peer communication
// and blockchain synchronization using the traditional Bitcoin network protocol.
package legacy

import (
	"sync"

	"github.com/bitcoin-sv/teranode/util"
	"github.com/prometheus/client_golang/prometheus"
)

var peerServerMetricHandlers = []string{
	"OnVersion",
	"OnProtoconf",
	"OnMemPool",
	"OnTx",
	"OnBlock",
	"OnInv",
	"OnHeaders",
	"OnGetData",
	"OnGetBlocks",
	"OnGetHeaders",
	"OnFeeFilter",
	"OnGetAddr",
	"OnAddr",
	"OnReject",
	"OnNotFound",
	"OnRead",
	"OnWrite",
}

var (
	peerServerMetrics = map[string]prometheus.Histogram{}

	prometheusMetricsInitOnce sync.Once
)

func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

func _initPrometheusMetrics() {
	for _, metric := range peerServerMetricHandlers {
		peerServerMetrics[metric] = prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "legacy_peer_server",
			Name:      metric,
			Help:      "The time taken to handle " + metric,
			Buckets:   util.MetricsBucketsMilliSeconds,
		})
		prometheus.MustRegister(peerServerMetrics[metric])
	}
}
