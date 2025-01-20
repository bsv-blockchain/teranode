// Package blockvalidation implements block validation for Bitcoin SV nodes in Teranode.
//
// This package provides the core functionality for validating Bitcoin blocks, managing block subtrees,
// and processing transaction metadata. It is designed for high-performance operation at scale,
// supporting features like:
//
// - Concurrent block validation with optimistic mining support
// - Subtree-based block organization and validation
// - Transaction metadata caching and management
// - Automatic chain catchup when falling behind
// - Integration with Kafka for distributed operation
//
// The package exposes gRPC interfaces for block validation operations,
// making it suitable for use in distributed Teranode deployments.
package blockvalidation

import (
	"sync"

	"github.com/bitcoin-sv/teranode/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prometheusBlockValidationHealth            prometheus.Counter
	prometheusBlockValidationBlockFoundCh      prometheus.Gauge
	prometheusBlockValidationBlockFound        prometheus.Histogram
	prometheusBlockValidationCatchupCh         prometheus.Gauge
	prometheusBlockValidationCatchup           prometheus.Histogram
	prometheusBlockValidationProcessBlockFound prometheus.Histogram
	prometheusBlockValidationSetTxMetaQueueCh  prometheus.Gauge

	// block validation
	prometheusBlockValidationValidateBlock      prometheus.Histogram
	prometheusBlockValidationReValidateBlock    prometheus.Histogram
	prometheusBlockValidationReValidateBlockErr prometheus.Histogram

	// tx meta cache stats
	prometheusBlockValidationSetTXMetaCache    prometheus.Counter
	prometheusBlockValidationSetTXMetaCacheDel prometheus.Counter
	prometheusBlockValidationSetMinedMulti     prometheus.Counter

	// expiring cache metrics
	prometheusBlockValidationLastValidatedBlocksCache prometheus.Gauge
	prometheusBlockValidationBlockExistsCache         prometheus.Gauge
	prometheusBlockValidationSubtreeExistsCache       prometheus.Gauge
)

var (
	prometheusMetricsInitOnce sync.Once
)

func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

func _initPrometheusMetrics() {
	prometheusBlockValidationHealth = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "blockvalidation",
			Name:      "health",
			Help:      "Number of health checks",
		},
	)

	prometheusBlockValidationBlockFoundCh = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "blockvalidation",
			Name:      "block_found_ch",
			Help:      "Number of blocks found buffered in the block found channel",
		},
	)

	prometheusBlockValidationBlockFound = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockvalidation",
			Name:      "block_found",
			Help:      "Histogram of calls to BlockFound method",
			Buckets:   util.MetricsBucketsSeconds,
		},
	)

	prometheusBlockValidationCatchupCh = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "blockvalidation",
			Name:      "catchup_ch",
			Help:      "Number of catchups buffered in the catchup channel",
		},
	)

	prometheusBlockValidationCatchup = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockvalidation",
			Name:      "catchup",
			Help:      "Histogram of catchup events",
			Buckets:   util.MetricsBucketsSeconds,
		},
	)

	prometheusBlockValidationProcessBlockFound = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockvalidation",
			Name:      "process_block_found",
			Help:      "Histogram of process block found",
			Buckets:   util.MetricsBucketsSeconds,
		},
	)

	prometheusBlockValidationSetTxMetaQueueCh = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "blockvalidation",
			Name:      "set_tx_meta_queue_ch",
			Help:      "Number of tx meta queue buffered in the set tx meta queue channel",
		},
	)

	// prometheusBlockValidationSetTxMetaQueueChWaitDuration = promauto.NewHistogram(
	//	prometheus.HistogramOpts{
	//		Namespace: "blockvalidation",
	//		Name:      "set_tx_meta_queue_ch_wait_duration_millis",
	//		Help:      "Duration of set tx meta queue channel wait",
	//		Buckets:   util.MetricsBucketsMilliSeconds,
	//	},
	//)
	//
	// prometheusBlockValidationSetTxMetaQueueDuration = promauto.NewHistogram(
	//	prometheus.HistogramOpts{
	//		Namespace: "blockvalidation",
	//		Name:      "set_tx_meta_queue_duration_millis",
	//		Help:      "Duration of set tx meta from queue",
	//		Buckets:   util.MetricsBucketsMilliSeconds,
	//	},
	//)

	prometheusBlockValidationValidateBlock = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockvalidation",
			Name:      "validate_block",
			Help:      "Histogram of calls to ValidateBlock method",
			Buckets:   util.MetricsBucketsSeconds,
		},
	)

	prometheusBlockValidationReValidateBlock = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockvalidation",
			Name:      "revalidate_block",
			Help:      "Histogram of re-validate block",
			Buckets:   util.MetricsBucketsSeconds,
		},
	)

	prometheusBlockValidationReValidateBlockErr = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockvalidation",
			Name:      "revalidate_block_err",
			Help:      "Number of blocks revalidated with error",
			Buckets:   util.MetricsBucketsSeconds,
		},
	)

	prometheusBlockValidationSetTXMetaCache = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "blockvalidation",
			Name:      "set_tx_meta_cache",
			Help:      "Number of tx meta cache sets",
		},
	)

	prometheusBlockValidationSetTXMetaCacheDel = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "blockvalidation",
			Name:      "del_tx_meta_cache",
			Help:      "Number of tx meta cache deletes",
		},
	)

	// prometheusBlockValidationSetMinedLocal = promauto.NewCounter(
	//	prometheus.CounterOpts{
	//		Namespace: "blockvalidation",
	//		Name:      "set_tx_mined_local",
	//		Help:      "Number of tx mined local sets",
	//	},
	//)

	prometheusBlockValidationSetMinedMulti = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "blockvalidation",
			Name:      "set_tx_mined_multi",
			Help:      "Number of tx mined multi sets",
		},
	)

	prometheusBlockValidationLastValidatedBlocksCache = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "blockvalidation",
			Name:      "last_validated_blocks_cache",
			Help:      "Number of blocks in the last validated blocks cache",
		},
	)

	prometheusBlockValidationBlockExistsCache = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "blockvalidation",
			Name:      "block_exists_cache",
			Help:      "Number of blocks in the block exists cache",
		},
	)

	prometheusBlockValidationSubtreeExistsCache = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "blockvalidation",
			Name:      "subtree_exists_cache",
			Help:      "Number of subtrees in the subtree exists cache",
		},
	)
}
