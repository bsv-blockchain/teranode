// Package repository provides access to blockchain data storage and retrieval operations.
package repository

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Prometheus metrics variables for tracking repository operations.
var (
	// prometheusAssetSubtreeDataCreated tracks on-demand subtreeData file creation events
	prometheusAssetSubtreeDataCreated *prometheus.CounterVec
)

// prometheusMetricsInitOnce ensures metrics are initialized exactly once
var prometheusMetricsInitOnce sync.Once

// initPrometheusMetrics safely initializes all Prometheus metrics using sync.Once
// to ensure thread-safe single initialization.
func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

// _initPrometheusMetrics creates and registers all Prometheus metrics.
func _initPrometheusMetrics() {
	prometheusAssetSubtreeDataCreated = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "asset_repository",
			Name:      "subtree_data_created_total",
			Help:      "Number of on-demand subtreeData file creation events",
		},
		[]string{
			"result", // success or error
			"source", // Event source - see values below
			// Source values:
			//   Success cases:
			//     - on_demand_created: Created without quorum lock (single instance)
			//     - on_demand_created_locked: Created with quorum lock (multi-instance)
			//     - file_existed: File already existed when checked
			//     - file_appeared: File appeared during creation (rare race)
			//     - waited_for_other: Waited for another instance to create file (quorum)
			//   Error cases:
			//     - creation_failed: FileStorer creation failed
			//     - storer_creation_failed: FileStorer creation failed (with quorum)
			//     - write_failed: Write operation failed
			//     - close_failed: File close operation failed
			//     - quorum_lock_failed: Quorum lock acquisition failed, fell back to no-lock
		},
	)
}
