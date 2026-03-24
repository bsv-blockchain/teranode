//go:build smalltxmetacache

// Package txmetacache provides small-scale cache configuration for development environments.
// Uses build tags to select appropriate cache size for development and testing.
package txmetacache

import "github.com/bsv-blockchain/teranode/ulogger"

// BucketsCount defines the number of cache buckets (32 for development environments).
// Also used as the shard count for SplitSwissLockFreeMapUint64 (bucketNative, bucketTrimmed).
const BucketsCount = 32

// MapInitialCapacity is the expected total number of entries across the entire cache (small dev env).
// Used as capacity hint when creating SplitSwissLockFreeMapUint64 index maps.
const MapInitialCapacity = 10_000

// ChunkSize defines the memory chunk size (maxValueSizeKB * 512 = ~1MB per chunk).
const ChunkSize = maxValueSizeKB * 512

// LogCacheConfig logs which cache configuration is active and its bucket/capacity constants.
func LogCacheConfig(bucketsCount, mapInitialCapacity int) {
	logger := ulogger.NewZeroLogger("improved_cache")
	logger.Debugf("Using improved_cache_const_small.go")
	logger.Infof("txmetacache: BucketsCount=%d MapInitialCapacity=%d", bucketsCount, mapInitialCapacity)
}
