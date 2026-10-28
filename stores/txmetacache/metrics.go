// Package txmetacache provides a caching layer for transaction metadata to improve performance.
// This file specifically contains Prometheus metrics definitions for monitoring cache operations.
// The metrics track key performance indicators such as cache size, hit/miss rates, and memory usage
// to enable effective monitoring and tuning of the cache in production environments.
package txmetacache

import (
	"context"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// tx meta cache stats - the following Prometheus metrics track key performance indicators
	// for the transaction metadata cache to help with monitoring and tuning

	// Current number of entries in the cache; monitors utilization and capacity
	// prometheusBlockValidationTxMetaCacheSize prometheus.Gauge
	// Total count of cache insertions since startup; tracks write throughput
	prometheusBlockValidationTxMetaCacheInsertions prometheus.Gauge
	// Count of successful cache retrievals (cache hits); indicates cache effectiveness
	prometheusBlockValidationTxMetaCacheHits prometheus.Gauge
	// Count of unsuccessful cache retrievals (cache misses); helps identify sizing issues
	prometheusBlockValidationTxMetaCacheMisses prometheus.Gauge
	// Count of origin retrievals from the cache; tracks origin information usage
	prometheusBlockValidationTxMetaCacheGetOrigin prometheus.Gauge
	// Count of items evicted from the cache due to memory constraints; indicates pressure
	prometheusBlockValidationTxMetaCacheEvictions prometheus.Gauge
	// Count of trim operations performed on the cache; tracks memory management activity
	prometheusBlockValidationTxMetaCacheTrims     prometheus.Gauge
	prometheusBlockValidationTxMetaCacheLaneWaits prometheus.Gauge
	// Total size of all map buckets in the cache; monitors memory consumption
	prometheusBlockValidationTxMetaCacheMapSize prometheus.Gauge
	// Cumulative count of all elements ever added to the cache; tracks total throughput
	prometheusBlockValidationTxMetaCacheTotalElementsAdded prometheus.Gauge
	// Number of valid (readable) entries; equals current_gen_entries + previous_gen_entries
	prometheusBlockValidationTxMetaCacheValidEntriesCount prometheus.Gauge
	// Count of entries written in the current ring generation
	prometheusBlockValidationTxMetaCacheCurrentGenEntries prometheus.Gauge
	// Count of valid entries from the previous generation not yet overwritten
	prometheusBlockValidationTxMetaCachePreviousGenEntries prometheus.Gauge
	prometheusBlockValidationTxMetaCacheBytes              prometheus.Gauge
	prometheusBlockValidationTxMetaCacheCapacityBytes      prometheus.Gauge

	// prometheusTxMetaCacheTombstonedWritesEvicted counts writes of a tx that
	// DeleteDemoted had evicted: dropped before the write, or evicted right
	// after it when the DeleteDemoted ran during the write.
	prometheusTxMetaCacheTombstonedWritesEvicted prometheus.Counter
)

var (
	// prometheusMetricsInitOnce ensures that Prometheus metrics are initialized exactly once,
	// preventing duplicate metric registration which would cause a panic. This is essential
	// for thread-safety when multiple instances or components might initialize metrics.
	prometheusMetricsInitOnce sync.Once
)

var txMetaCacheMetricsRegistry = struct {
	mu     sync.Mutex
	caches map[*TxMetaCache]struct{}
	cancel context.CancelFunc
}{}

const txMetaCachePrometheusUpdateInterval = 5 * time.Second

func registerTxMetaCacheMetrics(cache *TxMetaCache) func() {
	var unregisterOnce sync.Once

	txMetaCacheMetricsRegistry.mu.Lock()
	if txMetaCacheMetricsRegistry.caches == nil {
		txMetaCacheMetricsRegistry.caches = make(map[*TxMetaCache]struct{})
	}
	txMetaCacheMetricsRegistry.caches[cache] = struct{}{}

	if txMetaCacheMetricsRegistry.cancel == nil {
		ctx, cancel := context.WithCancel(context.Background())
		txMetaCacheMetricsRegistry.cancel = cancel
		go runTxMetaCachePrometheusUpdater(ctx)
	}
	txMetaCacheMetricsRegistry.mu.Unlock()

	return func() {
		unregisterOnce.Do(func() {
			txMetaCacheMetricsRegistry.mu.Lock()
			delete(txMetaCacheMetricsRegistry.caches, cache)
			if len(txMetaCacheMetricsRegistry.caches) == 0 && txMetaCacheMetricsRegistry.cancel != nil {
				txMetaCacheMetricsRegistry.cancel()
				txMetaCacheMetricsRegistry.cancel = nil
			}
			txMetaCacheMetricsRegistry.mu.Unlock()
		})
	}
}

func runTxMetaCachePrometheusUpdater(ctx context.Context) {
	ticker := time.NewTicker(txMetaCachePrometheusUpdateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			updateTxMetaCachePrometheusMetrics()
		}
	}
}

func txMetaCacheMetricsSnapshot() []*TxMetaCache {
	txMetaCacheMetricsRegistry.mu.Lock()
	defer txMetaCacheMetricsRegistry.mu.Unlock()

	caches := make([]*TxMetaCache, 0, len(txMetaCacheMetricsRegistry.caches))
	for cache := range txMetaCacheMetricsRegistry.caches {
		caches = append(caches, cache)
	}

	return caches
}

func updateTxMetaCachePrometheusMetrics() {
	if prometheusBlockValidationTxMetaCacheInsertions == nil {
		return
	}

	caches := txMetaCacheMetricsSnapshot()

	var (
		insertions         uint64
		hits               uint64
		misses             uint64
		evictions          uint64
		getOrigin          uint64
		trimCount          uint64
		totalMapSize       uint64
		totalElementsAdded uint64
		validEntriesCount  uint64
		currentGenEntries  uint64
		previousGenEntries uint64
		laneWaits          uint64
		bytes              uint64
		capacityBytes      uint64
	)

	for _, cache := range caches {
		insertions += cache.metrics.insertions.Load()
		hits += cache.metrics.hits.Load()
		misses += cache.metrics.misses.Load()
		evictions += cache.metrics.evictions.Load()
		getOrigin += cache.metrics.getOrigin.Load()

		cacheStats := cache.GetCacheStats()
		trimCount += cacheStats.TrimCount
		totalMapSize += cacheStats.TotalMapSize
		totalElementsAdded += cacheStats.TotalElementsAdded
		validEntriesCount += cacheStats.ValidEntriesCount
		currentGenEntries += cacheStats.CurrentGenEntries
		previousGenEntries += cacheStats.PreviousGenEntries
		laneWaits += cacheStats.LaneWaits
		evictions += cacheStats.Evictions
		bytes += cacheStats.Bytes
		capacityBytes += cacheStats.CapacityBytes
	}

	prometheusBlockValidationTxMetaCacheInsertions.Set(float64(insertions))
	prometheusBlockValidationTxMetaCacheHits.Set(float64(hits))
	prometheusBlockValidationTxMetaCacheMisses.Set(float64(misses))
	prometheusBlockValidationTxMetaCacheGetOrigin.Set(float64(getOrigin))
	prometheusBlockValidationTxMetaCacheEvictions.Set(float64(evictions))
	prometheusBlockValidationTxMetaCacheTrims.Set(float64(trimCount))
	prometheusBlockValidationTxMetaCacheMapSize.Set(float64(totalMapSize))
	prometheusBlockValidationTxMetaCacheTotalElementsAdded.Set(float64(totalElementsAdded))
	prometheusBlockValidationTxMetaCacheValidEntriesCount.Set(float64(validEntriesCount))
	prometheusBlockValidationTxMetaCacheCurrentGenEntries.Set(float64(currentGenEntries))
	prometheusBlockValidationTxMetaCachePreviousGenEntries.Set(float64(previousGenEntries))
	prometheusBlockValidationTxMetaCacheLaneWaits.Set(float64(laneWaits))
	prometheusBlockValidationTxMetaCacheBytes.Set(float64(bytes))
	prometheusBlockValidationTxMetaCacheCapacityBytes.Set(float64(capacityBytes))
}

// initPrometheusMetrics initializes all Prometheus metrics for the txmetacache package.
// It uses sync.Once to ensure metrics are registered only once with Prometheus,
// as attempting to register the same metric twice would cause a panic. This function
// is called during TxMetaCache initialization to set up monitoring capabilities.
func initPrometheusMetrics() {
	prometheusMetricsInitOnce.Do(_initPrometheusMetrics)
}

// _initPrometheusMetrics is the internal implementation for initializing all Prometheus metrics.
// This function creates and registers each individual metric with the Prometheus registry.
// All metrics use the same namespace ("teranode") and subsystem ("tx_meta_cache") for consistency.
//
// Metrics are organized into functional categories:
// 1. Capacity metrics:
//   - size: Current number of entries in the cache
//   - map_size: Total size of all bucket maps in the cache
//
// 2. Performance metrics:
//   - hits: Number of successful retrievals from the cache
//   - misses: Number of failed retrievals that had to fall back to the underlying store
//
// 3. Throughput metrics:
//   - insertions: Total number of entries added to the cache since startup
//   - get_origin: Number of transactions where origin information was retrieved from cache
//   - total_elements_added: Cumulative count of all elements ever added to the cache
//
// 4. Memory management metrics:
//   - evictions: Number of entries removed from the cache: deletes, plus an estimate of ring-wrap evictions on the lockfree cache
//   - trims: Number of cache cleanup operations performed
func _initPrometheusMetrics() {
	// // Size metric tracks the current number of entries in the transaction metadata cache.
	// // This is a point-in-time measurement that indicates cache utilization level and
	// // helps identify potential capacity issues or unexpected cache clearing events.
	// prometheusBlockValidationTxMetaCacheSize = promauto.NewGauge(
	// 	prometheus.GaugeOpts{
	// 		Namespace: "teranode",
	// 		Subsystem: "tx_meta_cache",
	// 		Name:      "size",
	// 		Help:      "Number of items in the tx meta cache",
	// 	},
	// )

	// Insertions metric tracks the total number of items added to the transaction metadata cache.
	// This counter increases monotonically and helps track write load on the cache,
	// providing insights into transaction processing throughput and cache churn rate.
	prometheusBlockValidationTxMetaCacheInsertions = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "tx_meta_cache",
			Name:      "insertions",
			Help:      "Number of insertions into the tx meta cache",
		},
	)

	// Hits metric tracks successful cache retrievals where the item was found and returned.
	// This counter is crucial for evaluating cache effectiveness and hit ratio when compared with misses,
	// serving as a primary indicator of how well the cache is reducing database load.
	prometheusBlockValidationTxMetaCacheHits = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "tx_meta_cache",
			Name:      "hits",
			Help:      "Number of hits in the tx meta cache",
		},
	)

	// Misses metric tracks failed cache retrievals where the item was not found.
	// High miss rates may indicate insufficient cache size or premature eviction of useful entries,
	// and can help identify opportunities for cache tuning and optimization.
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

	// Evictions metric tracks the number of entries removed from the cache: explicit deletes
	// (mined or removed transactions) on every backend, plus, on the lockfree cache, entries
	// overwritten when a ring wraps (an estimate, which also counts superseded and deleted records). High eviction rates may indicate cache pressure.
	prometheusBlockValidationTxMetaCacheEvictions = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "tx_meta_cache",
			Name:      "evictions",
			Help:      "Number of entries removed from the tx meta cache (deletes, plus an estimate of ring-wrap evictions on the lockfree cache)",
		},
	)

	// LaneWaits counts the times a lockfree-cache writer found every lane busy
	// and had to wait. It should stay at 0; a rising value means more writers
	// run at once than the cache has lanes, which costs CPU in the wait loop.
	prometheusBlockValidationTxMetaCacheLaneWaits = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "tx_meta_cache",
			Name:      "lane_waits",
			Help:      "Number of times a lockfree txmeta cache writer found every lane busy",
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

	// ValidEntriesCount is the number of valid (readable) entries in the cache
	prometheusBlockValidationTxMetaCacheValidEntriesCount = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "tx_meta_cache",
			Name:      "valid_entries_count",
			Help:      "Number of valid (readable) entries in the tx meta cache",
		},
	)

	// CurrentGenEntries is the count of entries in the current ring generation
	prometheusBlockValidationTxMetaCacheCurrentGenEntries = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "tx_meta_cache",
			Name:      "current_gen_entries",
			Help:      "Number of entries in the current generation of the tx meta cache",
		},
	)

	// PreviousGenEntries is the count of valid entries from the previous generation
	prometheusBlockValidationTxMetaCachePreviousGenEntries = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "tx_meta_cache",
			Name:      "previous_gen_entries",
			Help:      "Number of valid entries from the previous generation in the tx meta cache",
		},
	)

	prometheusBlockValidationTxMetaCacheBytes = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "tx_meta_cache",
			Name:      "bytes",
			Help:      "Bytes of the tx meta cache capacity that hold entries",
		},
	)

	prometheusBlockValidationTxMetaCacheCapacityBytes = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "teranode",
			Subsystem: "tx_meta_cache",
			Name:      "capacity_bytes",
			Help:      "Usable capacity of the tx meta cache in bytes, the bound for teranode_tx_meta_cache_bytes",
		},
	)

	prometheusTxMetaCacheTombstonedWritesEvicted = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "teranode",
			Subsystem: "tx_meta_cache",
			Name:      "tombstoned_writes_evicted_total",
			Help:      "Writes of a tx demoted to conflicting that were dropped, or evicted right after landing: a late txmeta ADD or a read-through write that raced the demotion",
		},
	)
}
