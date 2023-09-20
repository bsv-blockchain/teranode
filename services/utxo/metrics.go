package utxo

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prometheusUtxoGet   prometheus.Counter
	prometheusUtxoStore prometheus.Counter
	// prometheusUtxoReStore    prometheus.Counter
	// prometheusUtxoStoreSpent prometheus.Counter
	prometheusUtxoSpend prometheus.Counter
	// prometheusUtxoReSpend    prometheus.Counter
	// prometheusUtxoSpendSpent prometheus.Counter
	prometheusUtxoReset prometheus.Counter
)

var prometheusMetricsInitialised = false

func initPrometheusMetrics() {
	if prometheusMetricsInitialised {
		return
	}

	prometheusUtxoGet = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "utxostore_utxo_get",
			Help: "Number of utxo get calls done to utxostore",
		},
	)
	prometheusUtxoStore = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "utxostore_utxo_store",
			Help: "Number of utxo store calls done to utxostore",
		},
	)
	//prometheusUtxoStoreSpent = promauto.NewCounter(
	//	prometheus.CounterOpts{
	//		Name: "utxostore_utxo_store_spent",
	//		Help: "Number of utxo store calls that were already spent to utxostore",
	//	},
	//)
	//prometheusUtxoReStore = promauto.NewCounter(
	//	prometheus.CounterOpts{
	//		Name: "utxostore_utxo_restore",
	//		Help: "Number of utxo restore calls done to utxostore",
	//	},
	//)
	prometheusUtxoSpend = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "utxostore_utxo_spend",
			Help: "Number of utxo spend calls done to utxostore",
		},
	)
	//prometheusUtxoReSpend = promauto.NewCounter(
	//	prometheus.CounterOpts{
	//		Name: "utxostore_utxo_respend",
	//		Help: "Number of utxo respend calls done to utxostore",
	//	},
	//)
	//prometheusUtxoSpendSpent = promauto.NewCounter(
	//	prometheus.CounterOpts{
	//		Name: "utxostore_utxo_spend_spent",
	//		Help: "Number of utxo spend calls that were already spent done to utxostore",
	//	},
	//)
	prometheusUtxoReset = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "utxostore_utxo_reset",
			Help: "Number of utxo reset calls done to utxostore",
		},
	)

	prometheusMetricsInitialised = true
}
