//go:build !smalltxmetacache && !testtxmetacache

package txmetacache

import "github.com/bitcoin-sv/ubsv/ulogger"

/*
These const values are suitable for a production machine that needs to manage 1m TPS
*/
const BucketsCount = 8
const chunkSize = maxValueSizeKB * 2 * 1024 // 4 KB

func LogCacheSize() {
	logger := ulogger.NewZeroLogger("improved_cache")
	logger.Infof("Using improved_cache_const_large.go")
}
