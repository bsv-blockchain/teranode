package lane

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prometheusLaneCurrent   prometheus.Gauge
	prometheusLanePositions prometheus.Counter
	prometheusLaneRanges    prometheus.Counter
	prometheusLaneExhausted prometheus.Counter

	prometheusMetricsInitOnce sync.Once
)

func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(func() {
		prometheusLaneCurrent = promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "lane",
			Name:      "current",
			Help:      "Lane this process is handing out positions in",
		})
		prometheusLanePositions = promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "lane",
			Name:      "positions_total",
			Help:      "Lane positions this process handed to new records",
		})
		prometheusLaneRanges = promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "lane",
			Name:      "ranges_total",
			Help:      "Ranges of lane positions this process received from the blockchain service",
		})
		prometheusLaneExhausted = promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "lane",
			Name:      "unstamped_total",
			Help:      "New records created without a lane position because no range was in hand",
		})
	})
}
