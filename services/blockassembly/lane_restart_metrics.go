package blockassembly

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// The restart dry run's metrics (lane_restart.go), registered on first use at
// utxostore_laneMigration shadow.
var (
	laneRestartMetricsOnce sync.Once

	prometheusLaneRestartRuns     *prometheus.CounterVec
	prometheusLaneRestartRecords  *prometheus.CounterVec
	prometheusLaneRestartBytes    prometheus.Counter
	prometheusLaneRestartSample   *prometheus.CounterVec
	prometheusLaneRestartLast     *prometheus.GaugeVec
	prometheusLaneRestartDuration prometheus.Histogram
)

// Measures of the last run (teranode_blockassembly_lane_restart_last).
const (
	laneLastReadRecords          = "read_records"
	laneLastReadBytes            = "read_bytes"
	laneLastReadSeconds          = "read_seconds"
	laneLastMarginRecords        = "margin_records"
	laneLastExternalRecords      = "external_records"
	laneLastTodayReadRecords     = "today_read_records"
	laneLastTodayKept            = "today_kept"
	laneLastWouldKeep            = "would_keep"
	laneLastReadRatio            = "read_ratio"
	laneLastCapBlocks            = "cap_blocks"
	laneLastCapBlocksWithoutRows = "cap_blocks_without_rows"
	laneLastSampleRate           = "sample_rate"
)

func initLaneRestartMetrics() {
	laneRestartMetricsOnce.Do(func() {
		prometheusLaneRestartRuns = promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "blockassembly",
			Name:      "lane_restart_runs_total",
			Help:      "Lanes restart dry runs by outcome: done, unsupported, no_qualifying_grant, not_ready, error, cancelled",
		}, []string{"outcome"})

		prometheusLaneRestartRecords = promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "blockassembly",
			Name:      "lane_restart_records_total",
			Help:      "Records the lanes restart dry run read, by how it judged them",
		}, []string{"outcome"})

		prometheusLaneRestartBytes = promauto.NewCounter(prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "blockassembly",
			Name:      "lane_restart_read_bytes_total",
			Help:      "Bytes of the values the lanes restart dry run read",
		})

		prometheusLaneRestartSample = promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "blockassembly",
			Name:      "lane_restart_sample_total",
			Help:      "Sampled transactions by how today's reloaded pool and the lanes restart window compare",
		}, []string{"kind"})

		prometheusLaneRestartLast = promauto.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "blockassembly",
			Name:      "lane_restart_last",
			Help:      "The last lanes restart dry run that ended done or no_qualifying_grant, by measure; a no_qualifying_grant run reads no window, so its read measures are 0",
		}, []string{"measure"})

		prometheusLaneRestartDuration = promauto.NewHistogram(prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "blockassembly",
			Name:      "lane_restart_duration_seconds",
			Help:      "How long a lanes restart dry run took, waiting for the lane index included",
			Buckets:   prometheus.ExponentialBuckets(0.1, 2, 16),
		})
	})
}

// publishLaneRestart counts one run.
func publishLaneRestart(res laneRestartResult) {
	initLaneRestartMetrics()

	prometheusLaneRestartRuns.WithLabelValues(res.outcome).Inc()
	prometheusLaneRestartDuration.Observe(res.duration.Seconds())

	if res.outcome != laneRunDone && res.outcome != laneRunNoGrant {
		return
	}

	for _, o := range laneRecOutcomes {
		prometheusLaneRestartRecords.WithLabelValues(o).Add(float64(res.records[o]))
	}

	for _, k := range laneSampleKinds {
		prometheusLaneRestartSample.WithLabelValues(k).Add(float64(res.sample[k]))
	}

	prometheusLaneRestartBytes.Add(float64(res.stats.Bytes))

	last := map[string]float64{
		laneLastReadRecords:          float64(res.stats.Records),
		laneLastReadBytes:            float64(res.stats.Bytes),
		laneLastReadSeconds:          res.stats.Duration.Seconds(),
		laneLastMarginRecords:        float64(res.stats.MarginRecords),
		laneLastExternalRecords:      float64(res.stats.External),
		laneLastTodayReadRecords:     float64(res.todayRead),
		laneLastTodayKept:            float64(res.todayKept),
		laneLastWouldKeep:            float64(res.records[laneRecKeep]),
		laneLastCapBlocks:            float64(res.capBlocks),
		laneLastCapBlocksWithoutRows: float64(res.capBlocksWithoutRows),
		laneLastSampleRate:           float64(res.sampleRate),
	}

	// With nothing read by today's reload there is no ratio; the raw counts above stand.
	if res.todayRead > 0 {
		last[laneLastReadRatio] = float64(res.stats.Records) / float64(res.todayRead)
	} else {
		prometheusLaneRestartLast.DeleteLabelValues(laneLastReadRatio)
	}

	for m, v := range last {
		prometheusLaneRestartLast.WithLabelValues(m).Set(v)
	}
}
