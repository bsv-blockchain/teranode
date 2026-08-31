package utxopersister

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetFooter_Success(t *testing.T) {
	// Create a temporary file with valid footer data
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test-utxo.dat")

	f, err := os.Create(tmpFile)
	require.NoError(t, err)
	defer f.Close()

	// Write some dummy content
	_, err = f.Write([]byte("dummy content for testing"))
	require.NoError(t, err)

	// Write a 32-byte EOF marker (simulating the actual file format)
	eofMarker := make([]byte, 32)
	_, err = f.Write(eofMarker)
	require.NoError(t, err)

	// Write footer: txCount (8 bytes) + utxoCount (8 bytes)
	txCount := uint64(12345)
	utxoCount := uint64(67890)

	footer := make([]byte, 16)
	binary.LittleEndian.PutUint64(footer[0:8], txCount)
	binary.LittleEndian.PutUint64(footer[8:16], utxoCount)
	_, err = f.Write(footer)
	require.NoError(t, err)

	// Close and reopen for reading
	f.Close()

	f, err = os.Open(tmpFile)
	require.NoError(t, err)
	defer f.Close()

	// Test GetFooter
	readTxCount, readUtxoCount, err := GetFooter(f)
	require.NoError(t, err)
	assert.Equal(t, txCount, readTxCount)
	assert.Equal(t, utxoCount, readUtxoCount)
}

func TestGetFooter_NonSeekableReader(t *testing.T) {
	// Test with a non-seekable reader (bytes.Buffer)
	buffer := bytes.NewBuffer([]byte("test data"))

	txCount, utxoCount, err := GetFooter(buffer)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "seek is not supported")
	assert.Equal(t, uint64(0), txCount)
	assert.Equal(t, uint64(0), utxoCount)
	assert.True(t, errors.Is(err, errors.ErrProcessing))
}

func TestGetFooter_SeekError(t *testing.T) {
	// Create a file that's too short to seek -16 bytes from end
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "too-short.dat")

	f, err := os.Create(tmpFile)
	require.NoError(t, err)

	// Write only 10 bytes (less than the required 16)
	_, err = f.Write([]byte("short"))
	require.NoError(t, err)
	f.Close()

	f, err = os.Open(tmpFile)
	require.NoError(t, err)
	defer f.Close()

	txCount, utxoCount, err := GetFooter(f)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "error seeking to footer")
	assert.Equal(t, uint64(0), txCount)
	assert.Equal(t, uint64(0), utxoCount)
	assert.True(t, errors.Is(err, errors.ErrProcessing))
}

func TestGetFooter_ZeroCounts(t *testing.T) {
	// Test with zero transaction and UTXO counts
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "zero-counts.dat")

	f, err := os.Create(tmpFile)
	require.NoError(t, err)
	defer f.Close()

	// Write EOF marker
	eofMarker := make([]byte, 32)
	_, err = f.Write(eofMarker)
	require.NoError(t, err)

	// Write footer with zero counts
	footer := make([]byte, 16)
	binary.LittleEndian.PutUint64(footer[0:8], 0)
	binary.LittleEndian.PutUint64(footer[8:16], 0)
	_, err = f.Write(footer)
	require.NoError(t, err)

	f.Close()

	f, err = os.Open(tmpFile)
	require.NoError(t, err)
	defer f.Close()

	txCount, utxoCount, err := GetFooter(f)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), txCount)
	assert.Equal(t, uint64(0), utxoCount)
}

func TestGetFooter_MaxUint64Values(t *testing.T) {
	// Test with maximum uint64 values
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "max-values.dat")

	f, err := os.Create(tmpFile)
	require.NoError(t, err)
	defer f.Close()

	// Write EOF marker
	eofMarker := make([]byte, 32)
	_, err = f.Write(eofMarker)
	require.NoError(t, err)

	// Write footer with max uint64 values
	maxUint64 := ^uint64(0)
	footer := make([]byte, 16)
	binary.LittleEndian.PutUint64(footer[0:8], maxUint64)
	binary.LittleEndian.PutUint64(footer[8:16], maxUint64)
	_, err = f.Write(footer)
	require.NoError(t, err)

	f.Close()

	f, err = os.Open(tmpFile)
	require.NoError(t, err)
	defer f.Close()

	txCount, utxoCount, err := GetFooter(f)
	require.NoError(t, err)
	assert.Equal(t, maxUint64, txCount)
	assert.Equal(t, maxUint64, utxoCount)
}

func TestGetFooter_LargeFile(t *testing.T) {
	// Test with a larger file to ensure seeking works correctly
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "large-file.dat")

	f, err := os.Create(tmpFile)
	require.NoError(t, err)
	defer f.Close()

	// Write 1MB of dummy data
	dummyData := make([]byte, 1024*1024)
	for i := range dummyData {
		dummyData[i] = byte(i % 256)
	}
	_, err = f.Write(dummyData)
	require.NoError(t, err)

	// Write EOF marker
	eofMarker := make([]byte, 32)
	_, err = f.Write(eofMarker)
	require.NoError(t, err)

	// Write footer
	txCount := uint64(999999)
	utxoCount := uint64(888888)
	footer := make([]byte, 16)
	binary.LittleEndian.PutUint64(footer[0:8], txCount)
	binary.LittleEndian.PutUint64(footer[8:16], utxoCount)
	_, err = f.Write(footer)
	require.NoError(t, err)

	f.Close()

	f, err = os.Open(tmpFile)
	require.NoError(t, err)
	defer f.Close()

	readTxCount, readUtxoCount, err := GetFooter(f)
	require.NoError(t, err)
	assert.Equal(t, txCount, readTxCount)
	assert.Equal(t, utxoCount, readUtxoCount)
}

func TestGetFooter_DifferentValues(t *testing.T) {
	// Table-driven test with various tx and utxo counts
	tests := []struct {
		name      string
		txCount   uint64
		utxoCount uint64
	}{
		{
			name:      "small values",
			txCount:   1,
			utxoCount: 1,
		},
		{
			name:      "different values",
			txCount:   123456789,
			utxoCount: 987654321,
		},
		{
			name:      "txCount zero, utxoCount non-zero",
			txCount:   0,
			utxoCount: 12345,
		},
		{
			name:      "txCount non-zero, utxoCount zero",
			txCount:   54321,
			utxoCount: 0,
		},
		{
			name:      "large values",
			txCount:   1000000000000,
			utxoCount: 2000000000000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			tmpFile := filepath.Join(tmpDir, "test.dat")

			f, err := os.Create(tmpFile)
			require.NoError(t, err)

			// Write EOF marker
			eofMarker := make([]byte, 32)
			_, err = f.Write(eofMarker)
			require.NoError(t, err)

			// Write footer
			footer := make([]byte, 16)
			binary.LittleEndian.PutUint64(footer[0:8], tt.txCount)
			binary.LittleEndian.PutUint64(footer[8:16], tt.utxoCount)
			_, err = f.Write(footer)
			require.NoError(t, err)

			f.Close()

			f, err = os.Open(tmpFile)
			require.NoError(t, err)
			defer f.Close()

			readTxCount, readUtxoCount, err := GetFooter(f)
			require.NoError(t, err)
			assert.Equal(t, tt.txCount, readTxCount)
			assert.Equal(t, tt.utxoCount, readUtxoCount)
		})
	}
}

func TestGetFooter_EmptyFile(t *testing.T) {
	// Test with an empty file (0 bytes)
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "empty.dat")

	f, err := os.Create(tmpFile)
	require.NoError(t, err)
	f.Close()

	f, err = os.Open(tmpFile)
	require.NoError(t, err)
	defer f.Close()

	txCount, utxoCount, err := GetFooter(f)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "error seeking to footer")
	assert.Equal(t, uint64(0), txCount)
	assert.Equal(t, uint64(0), utxoCount)
}

func TestGetFooter_ExactlyFooterSize(t *testing.T) {
	// Test with a file that has exactly 16 bytes (just the footer, no EOF marker)
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "exact-size.dat")

	f, err := os.Create(tmpFile)
	require.NoError(t, err)

	// Write exactly 16 bytes
	footer := make([]byte, 16)
	binary.LittleEndian.PutUint64(footer[0:8], 111)
	binary.LittleEndian.PutUint64(footer[8:16], 222)
	_, err = f.Write(footer)
	require.NoError(t, err)

	f.Close()

	f, err = os.Open(tmpFile)
	require.NoError(t, err)
	defer f.Close()

	txCount, utxoCount, err := GetFooter(f)
	require.NoError(t, err)
	assert.Equal(t, uint64(111), txCount)
	assert.Equal(t, uint64(222), utxoCount)
}

func TestGetFooter_NonFileReader(t *testing.T) {
	// Test various non-file reader types
	tests := []struct {
		name   string
		reader io.Reader
	}{
		{
			name:   "bytes.Buffer",
			reader: bytes.NewBuffer([]byte("test")),
		},
		{
			name:   "bytes.Reader",
			reader: bytes.NewReader([]byte("test")),
		},
		{
			name:   "custom reader",
			reader: &customReader{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			txCount, utxoCount, err := GetFooter(tt.reader)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "seek is not supported")
			assert.Equal(t, uint64(0), txCount)
			assert.Equal(t, uint64(0), utxoCount)
		})
	}
}

// customReader is a custom reader type for testing
type customReader struct{}

func (c *customReader) Read(p []byte) (n int, err error) {
	return 0, io.EOF
}
