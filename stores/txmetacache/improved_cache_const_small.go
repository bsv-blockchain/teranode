//go:build smalltxmetacache

package txmetacache

import "github.com/bitcoin-sv/teranode/ulogger"

/*
These const values are suitable for a dev machine that does NOT need to cope with 1m TPS
*/
const BucketsCount = 32
const ChunkSize = maxValueSizeKB * 512

// LogCacheSize logs which cache configuration is being used
func LogCacheSize() {
	logger := ulogger.NewZeroLogger("improved_cache")
	logger.Debugf("Using improved_cache_const_small.go")
}
