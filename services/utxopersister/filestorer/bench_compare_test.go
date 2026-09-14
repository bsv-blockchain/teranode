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
