// //go:build aerospike

package aerospike2

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prometheusUtxoMapGet    prometheus.Counter
	prometheusUtxoMapSpend  prometheus.Counter
	prometheusUtxoMapReset  prometheus.Counter
	prometheusUtxoMapDelete prometheus.Counter
	prometheusUtxoMapErrors *prometheus.CounterVec

	prometheusTxMetaAerospikeMapGet       prometheus.Counter
	prometheusUtxostoreCreate             prometheus.Counter
	prometheusTxMetaAerospikeMapSetMined  prometheus.Counter
	prometheusTxMetaAerospikeMapErrors    *prometheus.CounterVec
	prometheusTxMetaAerospikeMapGetMulti  prometheus.Counter
	prometheusTxMetaAerospikeMapGetMultiN prometheus.Counter

	prometheusTxMetaAerospikeMapSetMinedBatch     prometheus.Counter
	prometheusTxMetaAerospikeMapSetMinedBatchN    prometheus.Counter
	prometheusTxMetaAerospikeMapSetMinedBatchErrN prometheus.Counter

	prometheusMetricsInitOnce sync.Once
)

func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

func _initPrometheusMetrics() {
	prometheusTxMetaAerospikeMapGet = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_map_txmeta_get",
			Help: "Number of txmeta get calls done to aerospike",
		},
	)
	prometheusUtxostoreCreate = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_utxo_store",
			Help: "Number of Create calls done to aerospike",
		},
	)
	prometheusTxMetaAerospikeMapSetMined = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_map_txmeta_set_mined",
			Help: "Number of txmeta set_mined calls done to aerospike",
		},
	)
	prometheusTxMetaAerospikeMapErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aerospike_map_txmeta_errors",
			Help: "Number of txmeta map errors",
		},
		[]string{
			"function", //function raising the error
			"error",    // error returned
		},
	)
	prometheusTxMetaAerospikeMapGetMulti = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_map_txmeta_get_multi",
			Help: "Number of txmeta get_multi calls done to aerospike map",
		},
	)
	prometheusTxMetaAerospikeMapGetMultiN = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_map_txmeta_get_multi_n",
			Help: "Number of txmeta get_multi txs done to aerospike map",
		},
	)

	prometheusTxMetaAerospikeMapSetMinedBatch = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_map_txmeta_set_mined_batch",
			Help: "Number of txmeta set_mined_batch calls done to aerospike map",
		},
	)
	prometheusTxMetaAerospikeMapSetMinedBatchN = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_map_txmeta_set_mined_batch_n",
			Help: "Number of txmeta set_mined_batch txs done to aerospike map",
		},
	)
	prometheusTxMetaAerospikeMapSetMinedBatchErrN = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_map_txmeta_set_mined_batch_err_n",
			Help: "Number of txmeta set_mined_batch txs errors to aerospike map",
		},
	)

	prometheusUtxoMapGet = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_map_utxo_get",
			Help: "Number of utxo get calls done to aerospike",
		},
	)
	//prometheusUtxoMapStore = promauto.NewCounter(
	//	prometheus.CounterOpts{
	//		Name: "aerospike_map_utxo_store",
	//		Help: "Number of utxo store calls done to aerospike",
	//	},
	//)
	//prometheusUtxoMapStoreSpent = promauto.NewCounter(
	//	prometheus.CounterOpts{
	//		Name: "aerospike_map_utxo_store_spent",
	//		Help: "Number of utxo store calls that were already spent to aerospike",
	//	},
	//)
	//prometheusUtxoMapReStore = promauto.NewCounter(
	//	prometheus.CounterOpts{
	//		Name: "aerospike_map_utxo_restore",
	//		Help: "Number of utxo restore calls done to aerospike",
	//	},
	//)
	prometheusUtxoMapSpend = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_map_utxo_spend",
			Help: "Number of utxo spend calls done to aerospike",
		},
	)

	prometheusUtxoMapReset = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_map_utxo_reset",
			Help: "Number of utxo reset calls done to aerospike",
		},
	)
	prometheusUtxoMapDelete = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_map_utxo_delete",
			Help: "Number of utxo delete calls done to aerospike",
		},
	)
	prometheusUtxoMapErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aerospike_map_utxo_errors",
			Help: "Number of utxo errors",
		},
		[]string{
			"function", //function raising the error
			"error",    // error returned
		},
	)
}
