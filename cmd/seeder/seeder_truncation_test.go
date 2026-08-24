package seeder

import (
	"bufio"
	"context"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/utxopersister"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// buildSnapshotWrappers builds the well-formed per-file metadata (current
// block hash, height, previous block hash) and two UTXOWrapper records, in
// exactly the layout CreateUTXOSet writes (services/utxopersister/UTXOSet.go)
// and processUTXOs reads back. The footer is deliberately not included here:
// callers that want a genuinely truncated file splice in a partial second
// record instead of the real footer.
func buildSnapshotWrappers(t *testing.T) (metadata []byte, wrapperBytes [][]byte, txCount, utxoCount uint64) {
	t.Helper()

	var blockHash chainhash.Hash
	blockHash[0] = 0xaa

	metadata = append(metadata, blockHash[:]...)

	heightBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(heightBytes, 100)
	metadata = append(metadata, heightBytes...)

	var prevHash [32]byte
	prevHash[0] = 0xbb

	metadata = append(metadata, prevHash[:]...)

	wrappers := []*utxopersister.UTXOWrapper{
		{
			Height: 100,
			UTXOs: []*utxopersister.UTXO{
				{Index: 0, Value: 5000000000, Script: []byte{0x76, 0xa9}},
			},
		},
		{
			Height: 100,
			UTXOs: []*utxopersister.UTXO{
				{Index: 0, Value: 1234, Script: []byte{0x51}},
				{Index: 1, Value: 5678, Script: []byte{0x52, 0x53}},
			},
		},
	}
	wrappers[0].TxID[0] = 0x01
	wrappers[1].TxID[0] = 0x02

	for _, w := range wrappers {
		wrapperBytes = append(wrapperBytes, w.Bytes())
		txCount++
		utxoCount += uint64(len(w.UTXOs))
	}

	return metadata, wrapperBytes, txCount, utxoCount
}

// openForReading opens path and positions a bufio.Reader past the per-file
// header/hash/height/prevHash metadata, exactly as processUTXOs does before
// handing off to readUTXOWrapperFile.
func openForReading(t *testing.T, path string) (*os.File, *bufio.Reader) {
	t.Helper()

	f, err := os.Open(path)
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

// TestReadUTXOWrapperFile_TruncatedFile_ReturnsError is the regression test
// for the silent-truncation bug: a snapshot file cut off partway through a
// UTXOWrapper record - here, mid-way through the second record's 32-byte
// TxID, well short of the real footer - must be rejected outright rather
// than misread as a clean end-of-records boundary.
//
// This exact cut point matters: NewUTXOWrapperFromReader's short read on the
// TxID field is wrapped into a *errors.Error whose rendered message is
// "...unexpected EOF", and the seeder's old `errors.Is(err, io.EOF)` check
// matched it via this package's substring-matching Is() fallback (io.EOF's
// message "EOF" is a substring of "unexpected EOF") - so the truncation was
// silently treated as a successful, complete import.
func TestReadUTXOWrapperFile_TruncatedFile_ReturnsError(t *testing.T) {
	metadata, wrapperBytes, _, _ := buildSnapshotWrappers(t)
	require.Len(t, wrapperBytes, 2)

	header := fileformat.NewHeader(fileformat.FileTypeUtxoSet)

	data := make([]byte, 0, len(header.Bytes())+len(metadata)+len(wrapperBytes[0])+10)
	data = append(data, header.Bytes()...)
	data = append(data, metadata...)
	data = append(data, wrapperBytes[0]...)
	// Cut off 10 bytes into the second record's 32-byte TxID: a genuine
	// mid-record truncation, with no footer appended at all.
	data = append(data, wrapperBytes[1][:10]...)

	path := filepath.Join(t.TempDir(), "utxo-set-truncated.dat")
	require.NoError(t, os.WriteFile(path, data, 0o600))

	f, reader := openForReading(t, path)

	utxoWrapperCh := make(chan *utxopersister.UTXOWrapper, 10)

	go func() {
		for range utxoWrapperCh { //nolint:revive // drain channel
		}
	}()

	err := readUTXOWrapperFile(context.Background(), ulogger.TestLogger{}, f, reader, utxoWrapperCh)
	require.Error(t, err, "a truncated snapshot file must not be reported as a successful import")
}
