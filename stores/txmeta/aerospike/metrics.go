package aerospike

import (
	asl "github.com/aerospike/aerospike-client-go/v7/logger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"sync"
)

var (
	prometheusTxMetaGet               prometheus.Counter
	prometheusTxMetaGetDuration       prometheus.Histogram
	prometheusTxMetaGetErr            prometheus.Counter
	prometheusTxMetaSet               prometheus.Counter
	prometheusTxMetaSetMined          prometheus.Counter
	prometheusTxMetaSetMinedBatch     prometheus.Counter
	prometheusTxMetaSetMinedBatchN    prometheus.Counter
	prometheusTxMetaSetMinedBatchErrN prometheus.Counter
	prometheusTxMetaGetMulti          prometheus.Counter
	prometheusTxMetaGetMultiN         prometheus.Counter
	prometheusTxMetaDelete            prometheus.Counter
)

var prometheusMetricsInitOnce sync.Once

func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(initPrometheusMetricsInternal)
}

func initPrometheusMetricsInternal() {
	prometheusTxMetaGet = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_txmeta_get",
			Help: "Number of txmeta get calls done to aerospike",
		},
	)
	prometheusTxMetaGetDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "aerospike_txmeta_get_duration",
			Help:    "Duration of txmeta get calls done to aerospike",
			Buckets: util.MetricsBucketsMilliSeconds,
		},
	)
	prometheusTxMetaGetErr = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_txmeta_get_err",
			Help: "Number of txmeta get errors",
		},
	)
	prometheusTxMetaSet = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_txmeta_set",
			Help: "Number of txmeta set calls done to aerospike",
		},
	)
	prometheusTxMetaSetMined = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_txmeta_set_mined",
			Help: "Number of txmeta set_mined calls done to aerospike",
		},
	)
	prometheusTxMetaSetMinedBatch = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_txmeta_set_mined_batch",
			Help: "Number of txmeta set_mined_batch calls done to aerospike",
		},
	)
	prometheusTxMetaSetMinedBatchN = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_txmeta_set_mined_batch_n",
			Help: "Number of txmeta set_mined_batch txs done to aerospike",
		},
	)
	prometheusTxMetaSetMinedBatchErrN = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_txmeta_set_mined_batch_err_n",
			Help: "Number of txmeta set_mined_batch txs errors to aerospike",
		},
	)
	prometheusTxMetaGetMulti = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_txmeta_get_multi",
			Help: "Number of txmeta get_multi calls done to aerospike",
		},
	)
	prometheusTxMetaGetMultiN = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_txmeta_get_multi_n",
			Help: "Number of txmeta get_multi txs done to aerospike",
		},
	)
	prometheusTxMetaDelete = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aerospike_txmeta_delete",
			Help: "Number of txmeta delete calls done to aerospike",
		},
	)

	if gocore.Config().GetBool("aerospike_debug", true) {
		asl.Logger.SetLevel(asl.DEBUG)
	}

}
