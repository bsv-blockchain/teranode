package filestorer

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob/file"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// benchComparePayload is the total number of bytes written per iteration. It is sixteen times
// the larger buffer and identical for every row of the grid, so the only thing the buffer size
// changes is how many caller writes are coalesced into one transfer. A payload that fits in a
// single buffer would measure nothing but the flush that Close performs either way.
const benchComparePayload = 4 * 1024 * 1024

// BenchmarkCompare_FileStorerWrite measures the utxopersister write path at both the old and
// the new buffer default. The buffer size is set explicitly in the settings literal so the row
// labels stay meaningful whatever the shipped default happens to be, and so the benchmark can
// be run unchanged against an earlier revision and paired by benchstat.
//
// fsync is switched off in the backing store deliberately: one publication per iteration costs
// the same at either buffer size, and leaving it in would bury the difference this benchmark
// exists to show.
//
// The sub-benchmark names here are deliberately left alone. Widening this grid rather than
// adding BenchmarkCompare_FileStorerSmallWrite alongside it would rename every row and break
// benchstat pairing against a run from an earlier revision. Note that the names preserve the
// pairing, not the numbers: any figure quoted for this path has to come from a run made after
// the buffers were pooled.
func BenchmarkCompare_FileStorerWrite(b *testing.B) {
	for _, recordSize := range []int{64, 512} {
		for _, bufferSize := range []string{"4KB", "256KB"} {
			b.Run(fmt.Sprintf("record=%dB/buffer=%s", recordSize, bufferSize), func(b *testing.B) {
				ctx := context.Background()
				logger := ulogger.TestLogger{}

				u, err := url.Parse("file://" + b.TempDir() + "?fsyncMode=none")
				require.NoError(b, err)

				store, err := file.New(logger, u)
				require.NoError(b, err)

				tSettings := &settings.Settings{
					Block: settings.BlockSettings{
						UTXOPersisterBufferSize: bufferSize,
					},
				}

				record := bytes.Repeat([]byte("u"), recordSize)
				records := benchComparePayload / recordSize

				b.SetBytes(benchComparePayload)
				b.ReportAllocs()
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					// A fresh key every iteration: NewFileStorer refuses a key that already exists.
					key := []byte(fmt.Sprintf("bench-%d-%s-%d", recordSize, bufferSize, i))

					fs, err := NewFileStorer(ctx, logger, tSettings, store, key, fileformat.FileTypeUtxoSet)
					if err != nil {
						b.Fatal(err)
					}

					for r := 0; r < records; r++ {
						// Write surfaces the background reader's error once that goroutine has
						// failed; an unchecked write would benchmark a storer that stopped
						// persisting.
						if _, err := fs.Write(record); err != nil {
							b.Fatal(err)
						}
					}

					// Close is inside the measured iteration: it flushes the buffer, closes the
					// pipe and waits for the blob to be published. Without it the benchmark
					// times buffered appends rather than completed persistence.
					if err := fs.Close(ctx); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// benchCompareSmallPayloads are payloads a good deal smaller than the larger buffer, so the
// buffer allocation is a visible share of each iteration rather than the ~6 % it is at
// BenchmarkCompare_FileStorerWrite's 4 MiB. This is the row that shows what one storer costs.
var benchCompareSmallPayloads = []int{256, 4 * 1024}

// BenchmarkCompare_FileStorerSmallWrite measures a single small blob per storer at both the
// old and the new buffer default. It exists for B/op and allocs/op: with one Write and one
// Close per iteration, per-storer allocation is most of what is left to measure.
//
// It is a sibling of BenchmarkCompare_FileStorerWrite rather than two more rows of that
// grid, because adding a dimension there would rename every existing sub-benchmark and break
// benchstat pairing against runs from an earlier revision.
//
// The backing store runs with fsync and checksumming both off, so neither a durability
// barrier nor a second create/write/rename cycle for the sidecar sits between the buffer and
// the number.
//
// The B/op it reports is a warm-pool best case. One buffer cycles through every iteration and
// a benchmark loop rarely collects, so an acquire almost always hits. sync.Pool drops what
// nobody is using across two collections, and a storer that finds the pool empty allocates
// its whole buffer exactly as the unpooled code did. Pooling is never worse than that; how
// much it saves on a syncing node depends on block rate against collection rate, which this
// benchmark cannot show.
func BenchmarkCompare_FileStorerSmallWrite(b *testing.B) {
	for _, payloadSize := range benchCompareSmallPayloads {
		for _, bufferSize := range []string{"4KB", "256KB"} {
			b.Run(fmt.Sprintf("payload=%dB/buffer=%s", payloadSize, bufferSize), func(b *testing.B) {
				ctx := context.Background()
				logger := ulogger.TestLogger{}

				u, err := url.Parse("file://" + b.TempDir() + "?fsyncMode=none&checksum=false")
				require.NoError(b, err)

				store, err := file.New(logger, u)
				require.NoError(b, err)

				tSettings := &settings.Settings{
					Block: settings.BlockSettings{
						UTXOPersisterBufferSize: bufferSize,
					},
				}

				// Built once, outside the measurement: the payload is not what is under test.
				payload := bytes.Repeat([]byte("u"), payloadSize)

				b.SetBytes(int64(payloadSize))
				b.ReportAllocs()
				b.ResetTimer()

				for i := 0; i < b.N; i++ {
					// A fresh key every iteration: NewFileStorer refuses a key that already exists.
					key := []byte(fmt.Sprintf("bench-small-%d-%s-%d", payloadSize, bufferSize, i))

					fs, err := NewFileStorer(ctx, logger, tSettings, store, key, fileformat.FileTypeUtxoSet)
					if err != nil {
						b.Fatal(err)
					}

					// Write surfaces the background reader's error once that goroutine has
					// failed; an unchecked write would benchmark a storer that stopped
					// persisting.
					if _, err := fs.Write(payload); err != nil {
						b.Fatal(err)
					}

					// Close is inside the measured iteration for the same reason as above:
					// without it the benchmark times a buffered append, not a published blob.
					if err := fs.Close(ctx); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
