package txmeta

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prometheusTxMetaHealth   prometheus.Counter
	prometheusTxMetaSet      prometheus.Counter
	prometheusTxMetaSetMined prometheus.Counter
	prometheusTxMetaGet      prometheus.Counter
	prometheusTxMetaDelete   prometheus.Counter
)

var prometheusMetricsInitialised = false

func initPrometheusMetrics() {
	if prometheusMetricsInitialised {
		return
	}

	prometheusTxMetaHealth = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "txmeta",
			Name:      "health",
			Help:      "Number of calls done to txmeta health",
		},
	)
	prometheusTxMetaSet = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "txmeta",
			Name:      "set",
			Help:      "Number of calls done to txmeta set",
		},
	)
	prometheusTxMetaSetMined = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "txmeta",
			Name:      "set_mined",
			Help:      "Number of calls done to txmeta set mined",
		},
	)
	prometheusTxMetaGet = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "txmeta",
			Name:      "get",
			Help:      "Number of calls done to txmeta get",
		},
	)
	prometheusTxMetaDelete = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "txmeta",
			Name:      "delete",
			Help:      "Number of calls done to txmeta get",
		},
	)

	prometheusMetricsInitialised = true
}
