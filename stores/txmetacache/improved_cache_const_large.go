//go:build !smalltxmetacache && !testtxmetacache

// Package txmetacache provides large-scale cache configuration for production environments.
// Uses build tags to select appropriate cache size for high-throughput production deployments.
package txmetacache

import "github.com/bitcoin-sv/teranode/ulogger"

// BucketsCount defines the number of cache buckets (8,192 for production environments).
const BucketsCount = 8 * 1024

// ChunkSize defines the memory chunk size (maxValueSizeKB * 2 * 1024 = ~4MB per chunk).
const ChunkSize = maxValueSizeKB * 2 * 1024

// LogCacheSize logs which cache configuration is active for diagnostics.
func LogCacheSize() {
	logger := ulogger.NewZeroLogger("improved_cache")
	logger.Debugf("Using improved_cache_const_large.go")
}
