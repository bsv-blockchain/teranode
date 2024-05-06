//go:build smalltxmetacache

package txmetacache

import "github.com/bitcoin-sv/ubsv/ulogger"

/*
These const values are suitable for a dev machine that does NOT need to cope with 1m TPS
*/
const bucketsCount = 32
const chunkSize = maxValueSizeKB * 512

func LogCacheSize() {
	logger := ulogger.NewZeroLogger("improved_cache")
	logger.Infof("Using improved_cache_const_small.go")
}
