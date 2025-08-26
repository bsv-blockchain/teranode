//go:build testtxmetacache

// Package txmetacache provides minimal cache configuration for testing environments.
// Uses build tags to select appropriate cache size for unit tests and automated testing.
package txmetacache

import "github.com/bitcoin-sv/teranode/ulogger"

// BucketsCount defines the number of hash buckets (8 for minimal memory usage).
const BucketsCount = 8

// ChunkSize defines the memory chunk size (maxValueSizeKB * 2 * 1024 bytes).
const ChunkSize = maxValueSizeKB * 2 * 1024

// LogCacheSize logs which cache configuration is active for diagnostics.
func LogCacheSize() {
	logger := ulogger.NewZeroLogger("improved_cache")
	logger.Debugf("Using improved_cache_const_test.go")
}
