//go:build smalltxmetacache

package txmetacache

/*
These const values are suitable for a dev machine that does NOT need to cope with 1m TPS
*/
const bucketsCount = 32
const chunkSize = maxValueSizeKB * 512
