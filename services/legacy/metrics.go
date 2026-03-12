// Package legacy implements a Bitcoin SV legacy protocol server that handles peer-to-peer communication
// and blockchain synchronization using the traditional Bitcoin network protocol.
//
// The metrics.go file provides Prometheus instrumentation for the legacy package,
// allowing for comprehensive monitoring of peer server performance and behavior.
package legacy

import (
	"sync"

	"github.com/bsv-blockchain/teranode/util"
	"github.com/prometheus/client_golang/prometheus"
)

// peerServerMetricHandlers defines all the peer server message handling operations that will be measured.
// Each entry corresponds to a specific message type or I/O operation in the peer server, including:
// - Protocol handshake messages (Version, Protoconf)
// - Data exchange messages (Block, Tx, Inv, Headers)
// - Query messages (GetData, GetBlocks, GetHeaders, GetAddr)
// - Control messages (FeeFilter, Addr, Reject, NotFound)
// - Basic I/O operations (Read, Write)
//
// Each handler will have its execution time measured and reported via Prometheus metrics.
var peerServerMetricHandlers = []string{
	"OnVersion",      // Version message handler metrics
	"OnProtoconf",    // Protocol configuration message handler metrics
	"OnMemPool",      // Memory pool query handler metrics
	"OnTx",           // Transaction message handler metrics
	"OnBlock",        // Block message handler metrics
	"OnInv",          // Inventory message handler metrics
	"OnHeaders",      // Headers message handler metrics
	"OnGetData",      // GetData message handler metrics
	"OnGetBlocks",    // GetBlocks message handler metrics
	"OnGetHeaders",   // GetHeaders message handler metrics
	"OnFeeFilter",    // FeeFilter message handler metrics
	"OnGetAddr",      // GetAddr message handler metrics
	"OnAddr",         // Addr message handler metrics
	"OnReject",       // Reject message handler metrics
	"OnNotFound",     // NotFound message handler metrics
	"OnRead",         // General read operation metrics
	"OnWrite",        // General write operation metrics
	"OnCreateStream", // CreateStream message handler metrics
	"OnStreamAck",    // StreamAck message handler metrics
}

var (
	// peerServerMetrics stores the Prometheus histogram metrics for each peer server handler.
	// The map is keyed by handler name (from peerServerMetricHandlers) with histogram values
	// that track execution time distribution.
	peerServerMetrics = map[string]prometheus.Histogram{}

	// prometheusMetricsInitOnce ensures metrics initialization happens exactly once,
	// even if initPrometheusMetrics is called multiple times from different goroutines.
	prometheusMetricsInitOnce sync.Once
)

// initPrometheusMetrics initializes all Prometheus metrics for the legacy peer server.
// This function ensures thread-safe, one-time initialization using sync.Once to prevent
// duplicate registration of metrics which would cause Prometheus to panic.
//
// This should be called during server initialization before any metrics are recorded.
func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

// _initPrometheusMetrics is the actual implementation of metrics initialization.
// It creates and registers histogram metrics for each handler defined in peerServerMetricHandlers.
//
// Each metric:
// - Uses the teranode.legacy_peer_server.[handler_name] naming convention
// - Tracks time taken to process different message types
// - Uses standard millisecond-based time buckets defined in util.MetricsBucketsMilliSeconds
//
// This function should never be called directly; use initPrometheusMetrics instead.
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
