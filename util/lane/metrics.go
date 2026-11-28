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
	prometheusLaneWaitSeconds   *prometheus.HistogramVec
	prometheusLaneTakeFailures  prometheus.Counter
	prometheusLaneRefusedGrants prometheus.Counter
	prometheusLaneReused        prometheus.Counter

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
		prometheusLaneWaitSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "lane",
			Name:      "wait_seconds",
			Help:      "How long each take counted in teranode_lane_waits_total waited, until it got a position or failed. reason is why none was in hand: aged_out, the grant outlived blockchain_laneGrantDuration with no next grant held; used_up, its last position was taken with no next grant held; no_lease, no lease is held or its last good renewal is older than blockchain_laneLeaseStopAfter",
			Buckets:   []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		}, []string{"reason"})
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
	})
}
