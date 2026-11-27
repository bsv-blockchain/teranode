package lane

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prometheusLaneCurrent       prometheus.Gauge
	prometheusLanePositions     prometheus.Counter
	prometheusLaneGrants        prometheus.Counter
	prometheusLaneWaits         prometheus.Counter
	prometheusLaneTakeFailures  prometheus.Counter
	prometheusLaneRefusedGrants prometheus.Counter
	prometheusLaneReused        prometheus.Counter

	prometheusLaneGiveBacksMissed prometheus.Counter

	prometheusMetricsInitOnce sync.Once
)

func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(func() {
		prometheusLaneCurrent = promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "lane",
			Name:      "current",
			Help:      "Lane of the lease this process opened last: 0 before its first lease, and not cleared when that lease closes or is released",
		})
		prometheusLanePositions = promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "lane",
			Name:      "positions_total",
			Help:      "Lane positions this process handed to new records, a returned position handed out again counted each time; minus teranode_lane_positions_reused_total, the new positions it used",
		})
		prometheusLaneGrants = promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "lane",
			Name:      "grants_total",
			Help:      "Grants of lane positions this process received from the blockchain service, the first grant of each lease included",
		})
		prometheusLaneWaits = promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "lane",
			Name:      "waits_total",
			Help:      "Takes of a lane position that had to wait because none was in hand",
		})
		prometheusLaneTakeFailures = promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "lane",
			Name:      "take_failures_total",
			Help:      "Takes of a lane position that failed, failing their create with service unavailable",
		})
		prometheusLaneRefusedGrants = promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "lane",
			Name:      "refused_grants_total",
			Help:      "Grants refused because they did not start above a position this process already gave out in that lane",
		})
		prometheusLaneReused = promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "lane",
			Name:      "positions_reused_total",
			Help:      "Lane positions handed to a new record a second time, after a store returned them because no record carried them. Each is also counted in positions_total",
		})
		prometheusLaneGiveBacksMissed = promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "lane",
			Name:      "give_backs_missed_total",
			Help:      "Grant requests that gave a spare back and either failed, so the retry goes without the give-back, or got a grant that does not continue the aged grant; the positions between stay unused. A blockchain service older than the give-back field, or a database role without DELETE on lane_handout, gives this on every give-back",
		})
	})
}
