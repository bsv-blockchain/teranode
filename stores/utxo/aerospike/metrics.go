// //go:build aerospike

// Package aerospike provides an Aerospike-based implementation of the UTXO store interface.
// It offers high performance, distributed storage capabilities with support for large-scale
// UTXO sets and complex operations like freezing, reassignment, and batch processing.
//
// # Architecture
//
// The implementation uses a combination of Aerospike Key-Value store and Lua scripts
// for atomic operations. Transactions are stored with the following structure:
//   - Main Record: Contains transaction metadata and up to 20,000 UTXOs
//   - Pagination Records: Additional records for transactions with >20,000 outputs
//   - External Storage: Optional blob storage for large transactions
//
// # Features
//
//   - Efficient UTXO lifecycle management (create, spend, unspend)
//   - Support for batched operations with LUA scripting
//   - Automatic cleanup of spent UTXOs through TTL
//   - Alert system integration for freezing/unfreezing UTXOs
//   - Metrics tracking via Prometheus
//   - Support for large transactions through external blob storage
//
// # Usage
//
//	store, err := aerospike.New(ctx, logger, settings, &url.URL{
//	    Scheme: "aerospike",
//	    Host:   "localhost:3000",
//	    Path:   "/test/utxos",
//	    RawQuery: "expiration=3600&set=txmeta",
//	})
//
// # Database Structure
//
// Normal Transaction:
//   - inputs: Transaction input data
//   - outputs: Transaction output data
//   - utxos: List of UTXO hashes
//   - nrUtxos: Total number of UTXOs
//   - spentUtxos: Number of spent UTXOs
//   - blockIDs: Block references
//   - isCoinbase: Coinbase flag
//   - spendingHeight: Coinbase maturity height
//   - frozen: Frozen status
//
// Large Transaction with External Storage:
//   - Same as normal but with external=true
//   - Transaction data stored in blob storage
//   - Multiple records for >20k outputs
//
// # Thread Safety
//
// The implementation is fully thread-safe and supports concurrent access through:
//   - Atomic operations via Lua scripts
//   - Batched operations for better performance
//   - Lock-free reads with optimistic concurrency
package aerospike

import (
	"sync"

	"github.com/bitcoin-sv/teranode/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prometheusUtxoMapGet    prometheus.Counter
	prometheusUtxoMapSpend  prometheus.Counter
	prometheusUtxoMapReset  prometheus.Counter
	prometheusUtxoMapDelete prometheus.Counter
	prometheusUtxoMapErrors *prometheus.CounterVec

	prometheusUtxoCreateBatch     prometheus.Histogram
	prometheusUtxoCreateBatchSize prometheus.Histogram
	prometheusUtxoSpendBatch      prometheus.Histogram
	prometheusUtxoSpendBatchSize  prometheus.Histogram

	prometheusTxMetaAerospikeMapGet       prometheus.Counter
	prometheusUtxostoreCreate             prometheus.Counter
	prometheusTxMetaAerospikeMapSetMined  prometheus.Counter
	prometheusTxMetaAerospikeMapErrors    *prometheus.CounterVec
	prometheusTxMetaAerospikeMapGetMulti  prometheus.Counter
	prometheusTxMetaAerospikeMapGetMultiN prometheus.Counter

	prometheusTxMetaAerospikeMapSetMinedBatch     prometheus.Counter
	prometheusTxMetaAerospikeMapSetMinedBatchN    prometheus.Counter
	prometheusTxMetaAerospikeMapSetMinedBatchErrN prometheus.Counter

	// metrics for external transactions
	prometheusTxMetaAerospikeMapGetExternal prometheus.Histogram
	prometheusTxMetaAerospikeMapSetExternal prometheus.Histogram

	prometheusMetricsInitOnce sync.Once
)

func InitPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

func _initPrometheusMetrics() {
	prometheusTxMetaAerospikeMapGet = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "aerospike",
			Name:      "txmeta_get",
			Help:      "Number of txmeta get calls done to aerospike",
		},
	)
	prometheusUtxostoreCreate = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "aerospike",
			Name:      "utxo_store",
			Help:      "Number of Create calls done to aerospike",
		},
	)
	prometheusTxMetaAerospikeMapSetMined = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "aerospike",
			Name:      "txmeta_set_mined",
			Help:      "Number of txmeta set_mined calls done to aerospike",
		},
	)
	prometheusTxMetaAerospikeMapErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "aerospike",
			Name:      "txmeta_errors",
			Help:      "Number of txmeta map errors",
		},
		[]string{
			"function", // function raising the error
			"error",    // error returned
		},
	)
	prometheusTxMetaAerospikeMapGetMulti = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "aerospike",
			Name:      "txmeta_get_multi",
			Help:      "Number of txmeta get_multi calls done to aerospike map",
		},
	)
	prometheusTxMetaAerospikeMapGetMultiN = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "aerospike",
			Name:      "txmeta_get_multi_n",
			Help:      "Number of txmeta get_multi txs done to aerospike map",
		},
	)

	prometheusTxMetaAerospikeMapSetMinedBatch = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "aerospike",
			Name:      "txmeta_set_mined_batch",
			Help:      "Number of txmeta set_mined_batch calls done to aerospike map",
		},
	)
	prometheusTxMetaAerospikeMapSetMinedBatchN = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "aerospike",
			Name:      "txmeta_set_mined_batch_n",
			Help:      "Number of txmeta set_mined_batch txs done to aerospike map",
		},
	)
	prometheusTxMetaAerospikeMapSetMinedBatchErrN = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "aerospike",
			Name:      "txmeta_set_mined_batch_err_n",
			Help:      "Number of txmeta set_mined_batch txs errors to aerospike map",
		},
	)

	prometheusUtxoMapGet = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "aerospike",
			Name:      "utxo_get",
			Help:      "Number of utxo get calls done to aerospike",
		},
	)
	// prometheusUtxoMapStore = promauto.NewCounter(
	// 	prometheus.CounterOpts{
	// 		Name: "aerospike_map_utxo_store",
	// 		Help: "Number of utxo store calls done to aerospike",
	// 	},
	// )
	// prometheusUtxoMapStoreSpent = promauto.NewCounter(
	// 	prometheus.CounterOpts{
	// 		Name: "aerospike_map_utxo_store_spent",
	// 		Help: "Number of utxo store calls that were already spent to aerospike",
	// 	},
	// )
	// prometheusUtxoMapReStore = promauto.NewCounter(
	// 	prometheus.CounterOpts{
	// 		Name: "aerospike_map_utxo_restore",
	// 		Help: "Number of utxo restore calls done to aerospike",
	// 	},
	// )
	prometheusUtxoMapSpend = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "aerospike",
			Name:      "utxo_spend",
			Help:      "Number of utxo spend calls done to aerospike",
		},
	)

	prometheusUtxoMapReset = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "aerospike",
			Name:      "utxo_reset",
			Help:      "Number of utxo reset calls done to aerospike",
		},
	)
	prometheusUtxoMapDelete = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "aerospike",
			Name:      "utxo_delete",
			Help:      "Number of utxo delete calls done to aerospike",
		},
	)
	prometheusUtxoMapErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "aerospike",
			Name:      "utxo_errors",
			Help:      "Number of utxo errors",
		},
		[]string{
			"function", // function raising the error
			"error",    // error returned
		},
	)

	prometheusUtxoCreateBatch = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "aerospike",
			Name:      "utxo_create_batch",
			Help:      "Duration of utxo create batch",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusUtxoCreateBatchSize = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "aerospike",
			Name:      "utxo_create_batch_size",
			Help:      "Size of utxo create batch",
			Buckets:   util.MetricsBucketsSizeSmall,
		},
	)

	prometheusUtxoSpendBatch = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "aerospike",
			Name:      "utxo_spend_batch",
			Help:      "Duration of utxo spend batch",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusUtxoSpendBatchSize = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "aerospike",
			Name:      "utxo_spend_batch_size",
			Help:      "Size of utxo spend batch",
			Buckets:   util.MetricsBucketsSizeSmall,
		},
	)

	prometheusTxMetaAerospikeMapGetExternal = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "aerospike",
			Name:      "get_external",
			Help:      "Duration of getting an external transaction from the blob store",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusTxMetaAerospikeMapSetExternal = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "aerospike",
			Name:      "set_external",
			Help:      "Duration of setting an external transaction to the blob store",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)
}
