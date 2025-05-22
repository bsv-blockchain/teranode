// Package txmetacache provides a caching layer for transaction metadata to improve performance.
// This file specifically contains Prometheus metrics definitions for monitoring cache operations.
package txmetacache

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// tx meta cache stats - the following Prometheus metrics track key performance indicators
	// for the transaction metadata cache to help with monitoring and tuning
	
	// Current number of entries in the cache
	prometheusBlockValidationTxMetaCacheSize               prometheus.Gauge
	// Total count of cache insertions since startup
	prometheusBlockValidationTxMetaCacheInsertions         prometheus.Gauge
	// Count of successful cache retrievals (cache hits)
	prometheusBlockValidationTxMetaCacheHits               prometheus.Gauge
	// Count of unsuccessful cache retrievals (cache misses)
	prometheusBlockValidationTxMetaCacheMisses             prometheus.Gauge
	// Count of origin retrievals from the cache
	prometheusBlockValidationTxMetaCacheGetOrigin          prometheus.Gauge
	// Count of items evicted from the cache due to memory constraints
	prometheusBlockValidationTxMetaCacheEvictions          prometheus.Gauge
	// Count of trim operations performed on the cache
	prometheusBlockValidationTxMetaCacheTrims              prometheus.Gauge
	// Total size of all map buckets in the cache
	prometheusBlockValidationTxMetaCacheMapSize            prometheus.Gauge
	// Cumulative count of all elements ever added to the cache
	prometheusBlockValidationTxMetaCacheTotalElementsAdded prometheus.Gauge
	// Count of hits for transactions that were deemed too old to use
	prometheusBlockValidationTxMetaCacheHitOldTx           prometheus.Gauge
)

var (
	// prometheusMetricsInitOnce ensures that Prometheus metrics are initialized exactly once,
	// preventing duplicate metric registration which would cause a panic
	prometheusMetricsInitOnce sync.Once
)

// initPrometheusMetrics initializes all Prometheus metrics for the txmetacache package.
// It uses sync.Once to ensure metrics are registered only once with Prometheus,
// as attempting to register the same metric twice would cause a panic.
func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

// _initPrometheusMetrics is the internal implementation for initializing all Prometheus metrics.
// This function creates and registers each individual metric with the Prometheus registry.
// All metrics use the same namespace ("teranode") and subsystem ("tx_meta_cache") for consistency.
//
// Metrics overview:
// - size: Current number of entries in the cache
// - insertions: Total number of entries added to the cache since startup
// - hits: Number of successful retrievals from the cache
// - misses: Number of failed retrievals that had to fall back to the underlying store
// - get_origin: Number of transactions where origin information was retrieved from cache
// - evictions: Number of entries removed from the cache due to memory constraints
// - trims: Number of cache cleanup operations performed
// - map_size: Total size of all bucket maps in the cache
// - total_elements_added: Cumulative count of all elements ever added to the cache
// - hit_old_tx: Count of cache hits for transactions deemed too old to use
func _initPrometheusMetrics() {
	// Size metric tracks the current number of entries in the transaction metadata cache
	// This is a point-in-time measurement that indicates cache utilization level
	prometheusBlockValidationTxMetaCacheSize = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "tx_meta_cache",
			Name:      "size",
			Help:      "Number of items in the tx meta cache",
		},
	)

	// Insertions metric tracks the total number of items added to the transaction metadata cache
	// This counter increases monotonically and helps track write load on the cache
	prometheusBlockValidationTxMetaCacheInsertions = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "tx_meta_cache",
			Name:      "insertions",
			Help:      "Number of insertions into the tx meta cache",
		},
	)

	// Hits metric tracks successful cache retrievals where the item was found and returned
	// This counter is crucial for evaluating cache effectiveness and hit ratio when compared with misses
	prometheusBlockValidationTxMetaCacheHits = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "tx_meta_cache",
			Name:      "hits",
			Help:      "Number of hits in the tx meta cache",
		},
	)

	// Misses metric tracks failed cache retrievals where the item was not found
	// High miss rates may indicate insufficient cache size or premature eviction of useful entries
	prometheusBlockValidationTxMetaCacheMisses = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "tx_meta_cache",
			Name:      "misses",
			Help:      "Number of misses in the tx meta cache",
		},
	)

	// GetOrigin metric tracks how many times transaction origin information was requested
	// This metric helps monitor access patterns for transaction origin data, which is important
	// for transaction validation and provenance tracking
	prometheusBlockValidationTxMetaCacheGetOrigin = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "tx_meta_cache",
			Name:      "get_origin",
			Help:      "Number of get origins in the tx meta cache",
		},
	)

	// Evictions metric tracks the number of entries that were removed from the cache due to memory constraints
	// High eviction rates may indicate cache pressure and potential performance degradation,
	// as frequently used entries might be prematurely removed
	prometheusBlockValidationTxMetaCacheEvictions = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "tx_meta_cache",
			Name:      "evictions",
			Help:      "Number of evictions in the tx meta cache",
		},
	)

	// Trims metric tracks how many times the cache performed trim operations to manage memory
	// Trim operations are initiated when the cache needs to reclaim space for new entries.
	// Regular trim operations are normal, but a high frequency may indicate cache pressure
	prometheusBlockValidationTxMetaCacheTrims = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "tx_meta_cache",
			Name:      "trims",
			Help:      "Number of trim operations in the tx meta cache",
		},
	)

	// MapSize metric tracks the total size of all bucket maps in the cache, providing insight into memory usage patterns
	// This metric is valuable for monitoring the cache's memory footprint and can help identify
	// potential memory leaks or unexpected growth patterns
	prometheusBlockValidationTxMetaCacheMapSize = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "tx_meta_cache",
			Name:      "map_size",
			Help:      "Number of total elements in the improved cache's bucket maps",
		},
	)

	// TotalElementsAdded metric provides the cumulative count of all elements that have been added to the cache
	// This counter never decreases and helps track overall cache throughput over time
	prometheusBlockValidationTxMetaCacheTotalElementsAdded = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "tx_meta_cache",
			Name:      "total_elements_added",
			Help:      "Number of total number of elements added to the txmetacache",
		},
	)

	// HitOldTx metric tracks cache hits for transactions that were found but considered too old to use
	// This helps monitor the effectiveness of the cache expiration policy
	prometheusBlockValidationTxMetaCacheHitOldTx = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "tx_meta_cache",
			Name:      "hit_old_tx",
			Help:      "Number of hits on old txs in the tx meta cache",
		},
	)
}
