package file

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// The BenchmarkCompare_ family exists to be run unchanged against two revisions and paired by
// benchstat. It therefore uses nothing but the store's public API and keeps the sub-benchmark
// names and parameter grids fixed; adding a parameter to one of these grids breaks the pairing
// for every row.

// benchCompareSource feeds a payload to a streaming write. It keeps its buffer in a named
// field so that neither io.WriterTo nor io.ReaderFrom is promoted onto it: with either of
// those present the copy would take a fast path and the benchmark would stop measuring the
// store's own copy loop, which is the thing under test.
type benchCompareSource struct {
	buf []byte
	off int
}

func (s *benchCompareSource) Read(p []byte) (int, error) {
	if s.off >= len(s.buf) {
		return 0, io.EOF
	}

	n := copy(p, s.buf[s.off:])
	s.off += n

	return n, nil
}

func (s *benchCompareSource) Close() error { return nil }

func (s *benchCompareSource) rewind() { s.off = 0 }

// BenchmarkCompare_SetInMemory measures the path taken by a caller that already holds the
// whole payload, which is where subtree blobs arrive.
func BenchmarkCompare_SetInMemory(b *testing.B) {
	for _, payloadSize := range []int{256, 4 * 1024, 64 * 1024, 1024 * 1024} {
		for _, mode := range []string{"full", "data", "none"} {
			b.Run(fmt.Sprintf("payload=%dB/mode=%s", payloadSize, mode), func(b *testing.B) {
				u, err := url.Parse("file://" + b.TempDir() + "?fsyncMode=" + mode)
				require.NoError(b, err)

				f, err := New(ulogger.TestLogger{}, u)
				require.NoError(b, err)

				payload := bytes.Repeat([]byte("x"), payloadSize)
				ctx := context.Background()

				b.SetBytes(int64(payloadSize))
				b.ReportAllocs()
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					key := []byte(fmt.Sprintf("bench-%d", i))
					if err := f.Set(ctx, key, fileformat.FileTypeTesting, payload); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// BenchmarkCompare_SetFromReaderStreaming measures the streaming path over a source with no
// fast path to offer. This is the only benchmark whose B/op speaks to the copy buffer pool:
// a buffer allocated per call rather than pooled shows up here as roughly a megabyte per op.
func BenchmarkCompare_SetFromReaderStreaming(b *testing.B) {
	for _, payloadSize := range []int{256, 4 * 1024, 64 * 1024, 1024 * 1024} {
		for _, mode := range []string{"full", "data", "none"} {
			b.Run(fmt.Sprintf("payload=%dB/mode=%s", payloadSize, mode), func(b *testing.B) {
				u, err := url.Parse("file://" + b.TempDir() + "?fsyncMode=" + mode)
				require.NoError(b, err)

				f, err := New(ulogger.TestLogger{}, u)
				require.NoError(b, err)

				source := &benchCompareSource{buf: bytes.Repeat([]byte("x"), payloadSize)}
				ctx := context.Background()

				b.SetBytes(int64(payloadSize))
				b.ReportAllocs()
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					source.rewind()

					key := []byte(fmt.Sprintf("bench-%d", i))
					if err := f.SetFromReader(ctx, key, fileformat.FileTypeTesting, source); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
