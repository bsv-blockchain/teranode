//go:build unix

package seeder

import (
	"bufio"
	"context"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// streamSnapshotThroughFIFO serves data through a named pipe, the way an
// operator streams a compressed snapshot into the seeder without unpacking it
// to disk, and returns the read end positioned past the per-file metadata. A
// pipe cannot seek, so the footer can only come from the stream itself.
func streamSnapshotThroughFIFO(t *testing.T, data []byte) (*os.File, *bufio.Reader) {
	t.Helper()

	f, err := os.Open(serveThroughFIFO(t, data))
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })

	reader := bufio.NewReader(f)

	_, err = fileformat.ReadHeader(reader)
	require.NoError(t, err)

	skip := make([]byte, 32+4+32) // block hash + height + previous block hash
	_, err = io.ReadFull(reader, skip)
	require.NoError(t, err)

	return f, reader
}

// serveThroughFIFO creates a named pipe and writes data into it once, for the
// first reader to open it. If nothing opens it, cleanup releases the writer.
func serveThroughFIFO(t *testing.T, data []byte) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "utxo-set.fifo")
	require.NoError(t, syscall.Mkfifo(path, 0o600))

	go func() {
		w, err := os.OpenFile(path, os.O_WRONLY, 0)
		if err != nil {
			return
		}

		_, _ = w.Write(data)
		_ = w.Close()
	}()

	t.Cleanup(func() {
		// A non-blocking read open lets a writer still waiting in open proceed.
		if r, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0); err == nil {
			_ = r.Close()
		}
	})

	return path
}

func buildStreamedSnapshot(t *testing.T, footerTxCount, footerUTXOCount uint64) []byte {
	t.Helper()

	metadata, wrapperBytes, _, _ := buildSnapshotWrappers(t)

	var footer [16]byte
	binary.LittleEndian.PutUint64(footer[0:8], footerTxCount)
	binary.LittleEndian.PutUint64(footer[8:16], footerUTXOCount)

	data := append([]byte{}, fileformat.NewHeader(fileformat.FileTypeUtxoSet).Bytes()...)
	data = append(data, metadata...)

	for _, w := range wrapperBytes {
		data = append(data, w...)
	}

	return append(data, footer[:]...)
}

// TestReadUTXOFrames_StreamedSnapshot_Succeeds: a complete snapshot read from
// a pipe must be accepted. The footer is the last 16 bytes of the stream, so
// the reader has it in hand when the records end and must not need to seek.
func TestReadUTXOFrames_StreamedSnapshot_Succeeds(t *testing.T) {
	_, _, txCount, utxoCount := buildSnapshotWrappers(t)
	f, reader := streamSnapshotThroughFIFO(t, buildStreamedSnapshot(t, txCount, utxoCount))

	frameCh := make(chan []byte, 10)

	var received int

	done := make(chan struct{})

	go func() {
		defer close(done)

		for range frameCh {
			received++
		}
	}()

	err := readUTXOFrames(context.Background(), ulogger.TestLogger{}, f, reader, frameCh, "all", nil)
	<-done

	require.NoError(t, err, "a complete snapshot streamed through a pipe must not be rejected")
	require.Equal(t, int(txCount), received)
}

// TestReadUTXOFrames_StreamedSnapshot_FooterMismatch_ReturnsError: reading the
// footer from the stream must keep the truncation check exactly as strict as
// the seek did.
func TestReadUTXOFrames_StreamedSnapshot_FooterMismatch_ReturnsError(t *testing.T) {
	_, _, txCount, utxoCount := buildSnapshotWrappers(t)
	f, reader := streamSnapshotThroughFIFO(t, buildStreamedSnapshot(t, txCount+1, utxoCount))

	frameCh := make(chan []byte, 10)

	go func() {
		for range frameCh { //nolint:revive // drain channel
		}
	}()

	err := readUTXOFrames(context.Background(), ulogger.TestLogger{}, f, reader, frameCh, "all", nil)
	require.ErrorContains(t, err, "snapshot file truncated")
}

// TestReadUTXOFrames_StreamedSnapshot_TruncatedIsRejected: a pipe cannot seek
// to the footer, so a stream that does not end in exactly one footer after the
// last record must be reported as truncated, however it was cut.
func TestReadUTXOFrames_StreamedSnapshot_TruncatedIsRejected(t *testing.T) {
	_, wrapperBytes, txCount, utxoCount := buildSnapshotWrappers(t)
	full := buildStreamedSnapshot(t, txCount, utxoCount)
	recordsEnd := len(full) - 16

	tests := []struct {
		name string
		data []byte
	}{
		{"cut mid-record", full[:recordsEnd-len(wrapperBytes[len(wrapperBytes)-1])/2]},
		{"cut at a record boundary with no footer", full[:recordsEnd]},
		{"8 bytes of the footer", full[:recordsEnd+8]},
		{"15 bytes of the footer", full[:recordsEnd+15]},
		{"footer followed by 8 bytes of garbage", append(append([]byte{}, full...), make([]byte, 8)...)},
		{"footer followed by 40 bytes of garbage", append(append([]byte{}, full...), make([]byte, 40)...)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, reader := streamSnapshotThroughFIFO(t, tt.data)

			frameCh := make(chan []byte, 10)

			go func() {
				for range frameCh { //nolint:revive // drain channel
				}
			}()

			err := readUTXOFrames(context.Background(), ulogger.TestLogger{}, f, reader, frameCh, "all", nil)
			require.ErrorContains(t, err, "snapshot file truncated")
		})
	}
}

// TestImportUTXOSet_PipeWithTwoPasses_Refused: two passes would each open the
// pipe and split one byte stream between them, so each would parse the other's
// gaps as records. The import must refuse before reading or storing anything.
func TestImportUTXOSet_PipeWithTwoPasses_Refused(t *testing.T) {
	_, _, txCount, utxoCount := buildSnapshotWrappers(t)
	path := serveThroughFIFO(t, buildStreamedSnapshot(t, txCount, utxoCount))

	store := &txidRecordingStore{seen: map[chainhash.Hash]int{}}
	opts := testImportOptions()
	opts.utxoBatchSize = 128

	done := make(chan error, 1)

	go func() {
		done <- importUTXOSet(context.Background(), ulogger.TestLogger{}, store, path, opts)
	}()

	select {
	case err := <-done:
		require.ErrorContains(t, err, "named pipe")
	case <-time.After(10 * time.Second):
		t.Fatal("importUTXOSet opened the pipe for two passes instead of refusing it")
	}

	require.Empty(t, store.seen, "nothing may be stored from a refused pipe")
}

// TestImportUTXOSet_PipeWithOnePass_StoresEveryRecord: with one pass a pipe is
// read end to end, and every record reaches the store exactly once.
func TestImportUTXOSet_PipeWithOnePass_StoresEveryRecord(t *testing.T) {
	_, _, txCount, utxoCount := buildSnapshotWrappers(t)
	path := serveThroughFIFO(t, buildStreamedSnapshot(t, txCount, utxoCount))

	store := &txidRecordingStore{seen: map[chainhash.Hash]int{}}
	opts := testImportOptions()
	opts.utxoBatchSize = 0

	require.NoError(t, importUTXOSet(context.Background(), ulogger.TestLogger{}, store, path, opts))

	require.Len(t, store.seen, int(txCount))

	for txid, n := range store.seen {
		require.Equal(t, 1, n, "tx %s", txid)
	}
}
