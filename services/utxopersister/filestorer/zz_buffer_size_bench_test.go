package filestorer

import (
	"bufio"
	"fmt"
	"io"
	"testing"
	"time"
)

// The real write pattern. go-bt's extended-transaction writer issues one Write
// per field, so the buffer sees a stream of small writes rather than large ones.
// That matters because bufio.Writer bypasses its buffer entirely for a write
// bigger than the buffer, so benchmarking with large chunks measures nothing
// about the buffer size at all.
//
// The reader also has to be slower than instant. In production it is
// SetFromReader writing to a file, so a discard reader understates the cost of
// each synchronous pipe handoff.
func BenchmarkPipeWriteRealisticFields(b *testing.B) {
	const payload = 16 << 20 // kept modest so the slow cases finish

	// Field sizes go-bt emits per input and output: version, counts, hashes,
	// indices, sequence, satoshis, script lengths and the scripts themselves.
	fields := [][]byte{
		make([]byte, 4), make([]byte, 32), make([]byte, 4),
		make([]byte, 8), make([]byte, 107), make([]byte, 4),
		make([]byte, 8), make([]byte, 25),
	}

	perTx := 0
	for _, f := range fields {
		perTx += len(f)
	}

	for _, bufSize := range []int{4 << 10, 64 << 10, 256 << 10} {
		b.Run(fmt.Sprintf("%dKB", bufSize>>10), func(b *testing.B) {
			b.SetBytes(payload)

			for i := 0; i < b.N; i++ {
				r, w := io.Pipe()

				done := make(chan struct{})

				go func() {
					// A reader that is not instant, standing in for a file write.
					buf := make([]byte, 32<<10)
					for {
						n, err := r.Read(buf)
						if n > 0 {
							time.Sleep(time.Microsecond)
						}
						if err != nil {
							break
						}
					}
					close(done)
				}()

				bw := bufio.NewWriterSize(w, bufSize)

				for written := 0; written < payload; written += perTx {
					for _, f := range fields {
						if _, err := bw.Write(f); err != nil {
							b.Fatal(err)
						}
					}
				}

				_ = bw.Flush()
				_ = w.Close()
				<-done
			}
		})
	}
}
