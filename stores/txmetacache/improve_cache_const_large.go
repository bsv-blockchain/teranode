//go:build !smalltxmetacache

package txmetacache

/*
These const values are suitable for a production machine that needs to manage 1m TPS
*/
const bucketsCount = 8 * 1024
const chunkSize = maxValueSizeKB * 2 * 1024 // 4 KB
