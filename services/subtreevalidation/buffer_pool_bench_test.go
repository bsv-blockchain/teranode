package subtreevalidation

import (
	"bufio"
	"bytes"
	"io"
	"os"
	"sync"
	"testing"

	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/require"
)

// Benchmark_BufferPoolAllocation tests different buffer sizes for pool allocation overhead
func Benchmark_BufferPoolAllocation(b *testing.B) {
	sizes := []int{16 * 1024, 32 * 1024, 64 * 1024, 128 * 1024, 512 * 1024}

	for _, size := range sizes {
		b.Run(SizeName(size), func(b *testing.B) {
			pool := sync.Pool{
				New: func() interface{} {
					return bufio.NewReaderSize(nil, size)
				},
			}

			// Pre-populate some readers to simulate real usage
			readers := make([]*bufio.Reader, 10)
			for i := 0; i < 10; i++ {
				readers[i] = pool.Get().(*bufio.Reader)
			}
			for i := 0; i < 10; i++ {
				pool.Put(readers[i])
			}

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				r := pool.Get().(*bufio.Reader)
				r.Reset(bytes.NewReader(make([]byte, size)))
				// Simulate minimal read operation
				_, _ = r.ReadByte()
				r.Reset(nil)
				pool.Put(r)
			}
		})
	}
}

// Benchmark_BufferPoolConcurrent simulates concurrent access pattern similar to subtree validation
func Benchmark_BufferPoolConcurrent(b *testing.B) {
	sizes := []int{32 * 1024, 64 * 1024, 512 * 1024}
	concurrency := 32 // Matches typical CheckBlockSubtreesConcurrency

	for _, size := range sizes {
		b.Run(SizeName(size), func(b *testing.B) {
			pool := sync.Pool{
				New: func() interface{} {
					return bufio.NewReaderSize(nil, size)
				},
			}

			b.ReportAllocs()
			b.ResetTimer()

			b.RunParallel(func(pb *testing.PB) {
				data := make([]byte, size)
				for pb.Next() {
					r := pool.Get().(*bufio.Reader)
					r.Reset(bytes.NewReader(data))
					// Simulate reading some data
					_, _ = io.ReadAll(r)
					r.Reset(nil)
					pool.Put(r)
				}
			})

			_ = concurrency // Document the intent
		})
	}
}

// Benchmark_SubtreeDeserializationWithBufferSizes tests real subtree deserialization
// with different buffer sizes to ensure no I/O performance degradation
func Benchmark_SubtreeDeserializationWithBufferSizes(b *testing.B) {
	// Load real subtree data from testdata
	subtreeBytes, err := os.ReadFile("testdata/4d22d3ea8d618c6de784855bf4facd0760f4012852242adfd399cff700665f3d.subtree")
	if err != nil {
		b.Skip("Skipping: testdata file not found")
		return
	}

	sizes := []int{16 * 1024, 32 * 1024, 64 * 1024, 128 * 1024, 512 * 1024}

	for _, size := range sizes {
		b.Run(SizeName(size), func(b *testing.B) {
			pool := sync.Pool{
				New: func() interface{} {
					return bufio.NewReaderSize(nil, size)
				},
			}

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				reader := bytes.NewReader(subtreeBytes[8:]) // trim magic bytes
				bufferedReader := pool.Get().(*bufio.Reader)
				bufferedReader.Reset(reader)

				_, err := subtreepkg.NewSubtreeFromReader(bufferedReader)
				require.NoError(b, err)

				bufferedReader.Reset(nil)
				pool.Put(bufferedReader)
			}
		})
	}
}

// Benchmark_SubtreeDataDeserializationWithBufferSizes tests subtree data deserialization
func Benchmark_SubtreeDataDeserializationWithBufferSizes(b *testing.B) {
	// Load real subtree and data from testdata
	subtreeBytes, err := os.ReadFile("testdata/4d22d3ea8d618c6de784855bf4facd0760f4012852242adfd399cff700665f3d.subtree")
	if err != nil {
		b.Skip("Skipping: testdata file not found")
		return
	}

	subtreeDataBytes, err := os.ReadFile("testdata/4d22d3ea8d618c6de784855bf4facd0760f4012852242adfd399cff700665f3d.subtreeData")
	if err != nil {
		b.Skip("Skipping: testdata file not found")
		return
	}

	subtree, err := subtreepkg.NewSubtreeFromBytes(subtreeBytes[8:])
	require.NoError(b, err)

	sizes := []int{16 * 1024, 32 * 1024, 64 * 1024, 128 * 1024, 512 * 1024}

	for _, size := range sizes {
		b.Run(SizeName(size), func(b *testing.B) {
			pool := sync.Pool{
				New: func() interface{} {
					return bufio.NewReaderSize(nil, size)
				},
			}

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				reader := bytes.NewReader(subtreeDataBytes)
				bufferedReader := pool.Get().(*bufio.Reader)
				bufferedReader.Reset(reader)

				_, err := subtreepkg.NewSubtreeDataFromReader(subtree, bufferedReader)
				require.NoError(b, err)

				bufferedReader.Reset(nil)
				pool.Put(bufferedReader)
			}
		})
	}
}

// Benchmark_PooledVsNonPooled compares pooled vs non-pooled buffer allocation
func Benchmark_PooledVsNonPooled(b *testing.B) {
	size := 32 * 1024
	data := make([]byte, size)

	b.Run("Pooled", func(b *testing.B) {
		pool := sync.Pool{
			New: func() interface{} {
				return bufio.NewReaderSize(nil, size)
			},
		}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			r := pool.Get().(*bufio.Reader)
			r.Reset(bytes.NewReader(data))
			_, _ = r.ReadByte()
			r.Reset(nil)
			pool.Put(r)
		}
	})

	b.Run("NonPooled", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			r := bufio.NewReaderSize(bytes.NewReader(data), size)
			_, _ = r.ReadByte()
		}
	})
}

// Benchmark_MemoryFootprint measures approximate memory footprint for different scenarios
func Benchmark_MemoryFootprint(b *testing.B) {
	scenarios := []struct {
		name        string
		bufferSize  int
		concurrency int
	}{
		{"Current_512KB_32concurrent", 512 * 1024, 32},
		{"Proposed_32KB_32concurrent", 32 * 1024, 32},
		{"Alternative_64KB_32concurrent", 64 * 1024, 32},
	}

	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			pool := sync.Pool{
				New: func() interface{} {
					return bufio.NewReaderSize(nil, scenario.bufferSize)
				},
			}

			// Pre-allocate buffers to simulate concurrent usage
			buffers := make([]*bufio.Reader, scenario.concurrency)
			for i := 0; i < scenario.concurrency; i++ {
				buffers[i] = pool.Get().(*bufio.Reader)
			}

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				// Simulate work with all buffers
				for _, buf := range buffers {
					buf.Reset(bytes.NewReader(make([]byte, 1024)))
					_, _ = buf.ReadByte()
					buf.Reset(nil)
				}
			}

			// Clean up
			for i := 0; i < scenario.concurrency; i++ {
				pool.Put(buffers[i])
			}
		})
	}
}

// SizeName returns a human-readable name for buffer size
func SizeName(size int) string {
	return map[int]string{
		16 * 1024:  "16KB",
		32 * 1024:  "32KB",
		64 * 1024:  "64KB",
		128 * 1024: "128KB",
		512 * 1024: "512KB",
	}[size]
}
