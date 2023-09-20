package blockvalidation

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prometheusBlockValidationBlockFound   prometheus.Counter
	prometheusBlockValidationSubtreeFound prometheus.Counter
)

var prometheusMetricsInitialised = false

func initPrometheusMetrics() {
	if prometheusMetricsInitialised {
		return
	}

	prometheusBlockValidationBlockFound = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockvalidation",
			Name:      "block_found",
			Help:      "Number of blocks found",
		},
	)
	prometheusBlockValidationSubtreeFound = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "blockvalidation",
			Name:      "subtree_found",
			Help:      "Number of subtrees found",
		},
	)

	prometheusMetricsInitialised = true
}
