//go:build testtxmetacache

package txmetacache

import "github.com/bitcoin-sv/teranode/ulogger"

/*
These const values are suitable for a production machine that needs to manage 1m TPS
*/
const BucketsCount = 8
const ChunkSize = maxValueSizeKB * 2 * 1024

// LogCacheSize logs which cache configuration is being used
func LogCacheSize() {
	logger := ulogger.NewZeroLogger("improved_cache")
	logger.Debugf("Using improved_cache_const_test.go")
}
