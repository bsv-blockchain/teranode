//go:build smalltxmetacache

// Package txmetacache provides small-scale cache configuration for development environments.
// Uses build tags to select appropriate cache size for development and testing.
package txmetacache

import "github.com/bitcoin-sv/teranode/ulogger"

// BucketsCount defines the number of cache buckets (32 for development environments).
const BucketsCount = 32

// ChunkSize defines the memory chunk size (maxValueSizeKB * 512 = ~1MB per chunk).
const ChunkSize = maxValueSizeKB * 512

// LogCacheSize logs which cache configuration is active for diagnostics.
func LogCacheSize() {
	logger := ulogger.NewZeroLogger("improved_cache")
	logger.Debugf("Using improved_cache_const_small.go")
}
