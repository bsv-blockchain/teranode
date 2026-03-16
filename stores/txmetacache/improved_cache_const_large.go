//go:build !smalltxmetacache && !testtxmetacache

// Package txmetacache provides large-scale cache configuration for production environments.
// Uses build tags to select appropriate cache size for high-throughput production deployments.
package txmetacache

import "github.com/bsv-blockchain/teranode/ulogger"

// BucketsCount defines the number of cache buckets (8,192 for production environments).
// Also used as the shard count for SplitSwissLockFreeMapUint64 (bucketNative, bucketTrimmed).
const BucketsCount = 8 * 1024

// MapInitialCapacity is the expected total number of entries across the entire cache (1M for 24GB cache).
// Used as capacity hint when creating SplitSwissLockFreeMapUint64 index maps.
// This is for 24GB cache size. Our experience shows 1.5m total entries max for this size.
const MapInitialCapacity = 100_000

// ChunkSize defines the memory chunk size (maxValueSizeKB * 2 * 1024 = ~4MB per chunk).
const ChunkSize = maxValueSizeKB * 2 * 1024

// LogCacheConfig logs which cache configuration is active and its bucket/capacity constants.
func LogCacheConfig(bucketsCount, mapInitialCapacity int) {
	logger := ulogger.NewZeroLogger("improved_cache")
	logger.Debugf("Using improved_cache_const_large.go")
	logger.Infof("txmetacache: BucketsCount=%d MapInitialCapacity=%d", bucketsCount, mapInitialCapacity)
}
