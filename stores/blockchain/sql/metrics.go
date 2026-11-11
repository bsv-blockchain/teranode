package sql

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// prometheusOnMainChainWriteFailures counts on_main_chain writes whose failure the
	// store swallowed, by the path that swallowed it. Each one leaves the column
	// inconsistent until the background repair lands.
	prometheusOnMainChainWriteFailures *prometheus.CounterVec
	// prometheusOnMainChainRepairPending counts the stores in this process that owe an
	// on_main_chain repair, so every flag reader of each is on its flag-free walk. Each
	// store adds one while it owes a repair, so one store cannot hide another's.
	prometheusOnMainChainRepairPending prometheus.Gauge

	prometheusMetricsInitOnce sync.Once
)

func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

func _initPrometheusMetrics() {
	prometheusOnMainChainWriteFailures = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "blockchain_sql",
			Name:      "on_main_chain_write_failures",
			Help:      "on_main_chain writes whose failure was swallowed, by path (invalidate_block|revalidate_block|startup_rebuild|startup_rebuild_overlap|store_block_fork_clear|store_block_best_lookup); startup_rebuild_overlap counts a startup rebuild that landed while a chain write committed and may have overwritten it; each one is repaired in the background",
		},
		[]string{"path"},
	)

	prometheusOnMainChainRepairPending = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "blockchain_sql",
			Name:      "on_main_chain_repair_pending",
			Help:      "Number of blockchain stores whose failed on_main_chain write still owes a repair; their flag readers use the flag-free walk until it lands",
		},
	)
}
