package txmetacache

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"testing"

	"github.com/cespare/xxhash"
	"github.com/stretchr/testify/require"
)

func TestTxMetaCache_Unallocated_Memory_1_2_3_GiB(t *testing.T) {
	// Heavy test: allocates up to 3GiB off-heap (via unix.Mmap) and writes pprof files.
	//
	// Run:
	//   go test ./stores/txmetacache -run '^TestTxMetaCache_Unallocated_Memory_1_2_3_GiB$' -count=1 -v -timeout=30m
	//
	// Artifacts:
	//   stores/txmetacache/testdata/pprof/*

	const giB = uint64(1024 * 1024 * 1024)

	tests := []struct {
		name         string
		cacheBytes   uint64
		overwriteMul uint64
	}{
		{name: "1GiB", cacheBytes: 1 * giB, overwriteMul: 2},
		{name: "2GiB", cacheBytes: 2 * giB, overwriteMul: 2},
		{name: "3GiB", cacheBytes: 3 * giB, overwriteMul: 2},
	}

	wd, err := os.Getwd()
	require.NoError(t, err)
	pprofDir := filepath.Join(wd, "testdata", "pprof")
	require.NoError(t, os.MkdirAll(pprofDir, 0o755))

	// Use a value size that makes each entry nearly fill a chunk, so we allocate chunks quickly.
	// kvLen = 4 + len(key) + len(val); must be < ChunkSize.
	// Also, bucketUnallocated rejects values >= 2048 bytes (maxValueSizeLog=11),
	// so keep val under that.
	key := make([]byte, 32)
	val := make([]byte, 2000) // < 2048, and yields ~2 entries per 4KB chunk

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache, err := New(int(tt.cacheBytes), Unallocated)
			require.NoError(t, err)
			defer cache.Reset()

			maxBucketBytes := tt.cacheBytes / uint64(BucketsCount)
			require.NotZero(t, maxBucketBytes)

			// Init() floors to whole chunks.
			maxChunksPerBucket := maxBucketBytes / uint64(ChunkSize)
			if maxChunksPerBucket == 0 {
				maxChunksPerBucket = 1
			}

			// With key=32 and val=2000: kvLen = 4+32+2000 = 2036 bytes -> 2 entries per 4096B chunk.
			entriesPerChunk := uint64(2)
			entriesToFullyAllocateBucket := maxChunksPerBucket * entriesPerChunk

			// Fill all buckets to their per-bucket chunk capacity.
			for bi := 0; bi < BucketsCount; bi++ {
				ub, ok := cache.buckets[bi].(*bucketUnallocated)
				require.True(t, ok, "bucket %d is not bucketUnallocated", bi)

				for ci := uint64(0); ci < entriesToFullyAllocateBucket; ci++ {
					// Deterministic unique key so xxhash changes, but we bypass bucket selection by calling ub.Set directly.
					key[0] = byte(bi)
					key[1] = byte(bi >> 8)
					key[2] = byte(bi >> 16)
					key[3] = byte(bi >> 24)
					key[4] = byte(ci)
					key[5] = byte(ci >> 8)
					key[6] = byte(ci >> 16)
					key[7] = byte(ci >> 24)

					h := xxhash.Sum64(key)
					require.NoError(t, ub.Set(key, val, h))
				}

				require.Equal(t, maxChunksPerBucket, ub.allocatedChunks, "bucket %d should be fully allocated", bi)
			}

			// Total off-heap chunk bytes mmapped is tracked by allocatedChunks.
			var totalOffHeapChunks uint64
			for bi := 0; bi < BucketsCount; bi++ {
				ub := cache.buckets[bi].(*bucketUnallocated)
				totalOffHeapChunks += ub.allocatedChunks
			}
			totalOffHeapBytes := totalOffHeapChunks * uint64(ChunkSize)

			// Overwrite multiple times; this should not increase allocatedChunks (no more mmaps).
			for bi := 0; bi < BucketsCount; bi++ {
				ub := cache.buckets[bi].(*bucketUnallocated)
				allocatedAtFull := ub.allocatedChunks

				// Overwrite enough entries to force wraparounds (exceed capacity in "entries"),
				// but it must never allocate more chunk buffers once fully allocated.
				overwrites := entriesToFullyAllocateBucket * tt.overwriteMul
				for ci := uint64(0); ci < overwrites; ci++ {
					key[0] = byte(bi)
					key[1] = byte(bi >> 8)
					key[2] = byte(bi >> 16)
					key[3] = byte(bi >> 24)
					key[4] = byte(ci)
					key[5] = byte(ci >> 8)
					key[6] = byte(ci >> 16)
					key[7] = byte(ci >> 24)

					h := xxhash.Sum64(key)
					_ = ub.Set(key, val, h)
				}

				require.Equal(t, allocatedAtFull, ub.allocatedChunks, "bucket %d should not allocate more after wrap/overwrite", bi)
			}

			// Force GC to make heap profile easier to interpret.
			runtime.GC()

			heapPath := filepath.Join(pprofDir, fmt.Sprintf("heap_txmetacache_unallocated_%s.pprof", tt.name))
			f, err := os.Create(heapPath)
			require.NoError(t, err)
			require.NoError(t, pprof.WriteHeapProfile(f))
			require.NoError(t, f.Close())

			allocsPath := filepath.Join(pprofDir, fmt.Sprintf("allocs_txmetacache_unallocated_%s.pprof", tt.name))
			af, err := os.Create(allocsPath)
			require.NoError(t, err)
			require.NoError(t, pprof.Lookup("allocs").WriteTo(af, 0))
			require.NoError(t, af.Close())

			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)

			reportPath := filepath.Join(pprofDir, fmt.Sprintf("report_txmetacache_unallocated_%s.txt", tt.name))
			report := fmt.Sprintf(
				"cache=%s\nBucketsCount=%d\nChunkSize=%d\nmaxBucketBytes=%d\nmaxChunksPerBucket=%d\n"+
					"offheap_allocatedChunks_total=%d\noffheap_bytes_total=%d\n"+
					"heap_Alloc=%d\nheap_HeapInuse=%d\nsys=%d\n\npid=%d\n\nheap_pprof=%s\nallocs_pprof=%s\n",
				tt.name,
				BucketsCount,
				ChunkSize,
				maxBucketBytes,
				maxChunksPerBucket,
				totalOffHeapChunks,
				totalOffHeapBytes,
				ms.Alloc,
				ms.HeapInuse,
				ms.Sys,
				os.Getpid(),
				heapPath,
				allocsPath,
			)
			require.NoError(t, os.WriteFile(reportPath, []byte(report), 0o600))

			t.Logf("wrote heap profile: %s", heapPath)
			t.Logf("wrote allocs profile: %s", allocsPath)
			t.Logf("wrote report: %s", reportPath)

			// // Optional: keep the process alive so you can inspect a live PID with vmmap/top.
			// // Example:
			// //   TERANODE_TXMETACACHE_MEMTEST=1 TERANODE_TXMETACACHE_MEMTEST_SLEEP_SECS=120 \
			// //     go test ./stores/txmetacache -run TestTxMetaCache_Unallocated_Memory_1_2_3_GiB -count=1 -v
			// if s := os.Getenv("TERANODE_TXMETACACHE_MEMTEST_SLEEP_SECS"); s != "" {
			// 	secs, convErr := strconv.Atoi(s)
			// 	require.NoError(t, convErr)
			// 	if secs > 0 {
			// 		t.Logf("sleeping %ds for live inspection; pid=%d", secs, os.Getpid())
			// 		time.Sleep(time.Duration(secs) * time.Second)
			// 	}
			// }
		})
	}
}
