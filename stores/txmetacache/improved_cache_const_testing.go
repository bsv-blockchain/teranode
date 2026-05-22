//go:build testtxmetacache

// Package txmetacache provides minimal cache configuration for testing environments.
// Uses build tags to select appropriate cache size for unit tests and automated testing.
package txmetacache

import "github.com/bsv-blockchain/teranode/ulogger"

// BucketsCount defines the number of hash buckets (8 for minimal memory usage).
const BucketsCount = 8

// MapInitialCapacity is the expected total number of entries across the entire cache (test env).
// Used as capacity hint when creating SplitSwissLockFreeMapUint64 index maps.
const MapInitialCapacity = 1

// ChunkSize defines the memory chunk size (maxValueSizeKB * 2 * 1024 bytes).
const ChunkSize = maxValueSizeKB * 2 * 1024

// LogCacheConfig logs which cache configuration is active and its bucket/capacity constants.
func LogCacheConfig(bucketsCount, mapInitialCapacity int) {
	logger := ulogger.NewZeroLogger("improved_cache")
	logger.Debugf("Using improved_cache_const_test.go")
	logger.Infof("txmetacache: BucketsCount=%d MapInitialCapacity=%d", bucketsCount, mapInitialCapacity)
}
