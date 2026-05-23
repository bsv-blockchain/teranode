package model

import (
	"sync"

	"github.com/bsv-blockchain/teranode/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prometheusBlockFromBytes              prometheus.Histogram
	prometheusBlockValid                  prometheus.Histogram
	prometheusBlockCheckMerkleRoot        prometheus.Histogram
	prometheusBlockGetSubtrees            prometheus.Histogram
	prometheusBlockGetAndValidateSubtrees prometheus.Histogram
	prometheusTxMapEntries                prometheus.Gauge
	prometheusTxMapFilterRAM              prometheus.Gauge
	prometheusTxMapDiskWritten            prometheus.Gauge
	prometheusParentSpendsMapEntries      prometheus.Gauge
	prometheusParentSpendsMapFilterRAM    prometheus.Gauge
	prometheusParentSpendsMapDiskWritten  prometheus.Gauge
)

var (
	prometheusMetricsInitOnce sync.Once
)

func init() {
	initPrometheusMetrics()
}

func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

func _initPrometheusMetrics() {
	prometheusBlockFromBytes = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "block",
			Name:      "from_bytes",
			Help:      "Histogram of Block.FromBytes",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockValid = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "block",
			Name:      "valid",
			Help:      "Histogram of Block.Valid",
			Buckets:   util.MetricsBucketsSeconds,
		},
	)

	prometheusBlockCheckMerkleRoot = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "block",
			Name:      "check_merkle_root",
			Help:      "Histogram of Block.CheckMerkleRoot",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockGetSubtrees = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "block",
			Name:      "get_subtrees",
			Help:      "Histogram of Block.GetSubtrees",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusBlockGetAndValidateSubtrees = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "teranode",
			Subsystem: "block",
			Name:      "get_and_validate_subtrees",
			Help:      "Histogram of Block.GetAndValidateSubtrees",
			Buckets:   util.MetricsBucketsMilliSeconds,
		},
	)

	prometheusTxMapEntries = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "block",
			Name:      "txmap_entries",
			Help:      "Number of entries in disk-backed txMap during block validation",
		},
	)

	prometheusTxMapFilterRAM = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "block",
			Name:      "txmap_filter_ram_bytes",
			Help:      "Cuckoo filter memory in bytes for disk-backed txMap during block validation",
		},
	)

	prometheusTxMapDiskWritten = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "block",
			Name:      "txmap_disk_written_bytes",
			Help:      "Data bytes written to disk for disk-backed txMap during block validation",
		},
	)

	prometheusParentSpendsMapEntries = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "block",
			Name:      "parentspends_entries",
			Help:      "Number of entries in disk-backed parentSpendsMap during block validation",
		},
	)

	prometheusParentSpendsMapFilterRAM = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "block",
			Name:      "parentspends_filter_ram_bytes",
			Help:      "Cuckoo filter memory in bytes for disk-backed parentSpendsMap during block validation",
		},
	)

	prometheusParentSpendsMapDiskWritten = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "block",
			Name:      "parentspends_disk_written_bytes",
			Help:      "Data bytes written to disk for disk-backed parentSpendsMap during block validation",
		},
	)
}

// ReportTxMapStats sets Prometheus gauges for the disk-backed txMap.
func ReportTxMapStats(stats DiskMapStats) {
	prometheusTxMapEntries.Set(float64(stats.Entries))
	prometheusTxMapFilterRAM.Set(float64(stats.FilterMemBytes))
	prometheusTxMapDiskWritten.Set(float64(stats.DiskBytesWritten))
}

// ClearTxMapStats zeroes Prometheus gauges for the disk-backed txMap.
func ClearTxMapStats() {
	prometheusTxMapEntries.Set(0)
	prometheusTxMapFilterRAM.Set(0)
	prometheusTxMapDiskWritten.Set(0)
}

// ReportParentSpendsMapStats sets Prometheus gauges for the disk-backed parentSpendsMap.
func ReportParentSpendsMapStats(stats DiskMapStats) {
	prometheusParentSpendsMapEntries.Set(float64(stats.Entries))
	prometheusParentSpendsMapFilterRAM.Set(float64(stats.FilterMemBytes))
	prometheusParentSpendsMapDiskWritten.Set(float64(stats.DiskBytesWritten))
}

// ClearParentSpendsMapStats zeroes Prometheus gauges for the disk-backed parentSpendsMap.
func ClearParentSpendsMapStats() {
	prometheusParentSpendsMapEntries.Set(0)
	prometheusParentSpendsMapFilterRAM.Set(0)
	prometheusParentSpendsMapDiskWritten.Set(0)
}
