package txmetacache

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// tx meta cache stats
	prometheusBlockValidationTxMetaCacheSize       prometheus.Gauge
	prometheusBlockValidationTxMetaCacheInsertions prometheus.Gauge
	prometheusBlockValidationTxMetaCacheHits       prometheus.Gauge
	prometheusBlockValidationTxMetaCacheMisses     prometheus.Gauge
	prometheusBlockValidationTxMetaCacheEvictions  prometheus.Gauge
)

var prometheusMetricsInitialised = false

func initPrometheusMetrics() {
	if prometheusMetricsInitialised {
		return
	}

	prometheusBlockValidationTxMetaCacheSize = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "blockvalidation",
			Name:      "tx_meta_cache_size",
			Help:      "Number of items in the tx meta cache",
		},
	)

	prometheusBlockValidationTxMetaCacheInsertions = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "blockvalidation",
			Name:      "tx_meta_cache_insertions",
			Help:      "Number of insertions into the tx meta cache",
		},
	)

	prometheusBlockValidationTxMetaCacheHits = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "blockvalidation",
			Name:      "tx_meta_cache_hits",
			Help:      "Number of hits in the tx meta cache",
		},
	)

	prometheusBlockValidationTxMetaCacheMisses = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "blockvalidation",
			Name:      "tx_meta_cache_misses",
			Help:      "Number of misses in the tx meta cache",
		},
	)

	prometheusBlockValidationTxMetaCacheEvictions = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "blockvalidation",
			Name:      "tx_meta_cache_evictions",
			Help:      "Number of evictions in the tx meta cache",
		},
	)

	prometheusMetricsInitialised = true
}
