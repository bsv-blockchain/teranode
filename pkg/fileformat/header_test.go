package fileformat

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHeaderWriteRead_RoundTrip(t *testing.T) {
	types := []FileType{FileTypeUtxoAdditions, FileTypeBlock, FileTypeTx, FileTypeSubtreeMeta}

	for _, ft := range types {
		h := NewHeader(ft)

		buf := &bytes.Buffer{}

		if err := h.Write(buf); err != nil {
			t.Fatalf("Header.Write failed: %v", err)
		}

		var h2 Header
		if err := h2.Read(buf); err != nil {
			t.Fatalf("Header.Read failed: %v", err)
		}

		if h.magic != h2.magic {
			t.Errorf("magic mismatch: got %q, want %q", h2.magic, h.magic)
		}
	}
}

func TestHeader_ReadHeaderFunc(t *testing.T) {
	ft := FileTypeTx

	h := NewHeader(ft)

	buf := &bytes.Buffer{}

	if err := h.Write(buf); err != nil {
		t.Fatalf("Header.Write failed: %v", err)
	}

	h2, err := ReadHeader(buf)
	if err != nil {
		t.Fatalf("ReadHeader failed: %v", err)
	}

	if h.magic != h2.magic {
		t.Errorf("Header mismatch after ReadHeader: got %+v, want %+v", h2, h)
	}
}

func TestHeader_InvalidMagic(t *testing.T) {
	buf := &bytes.Buffer{}
	buf.Write([]byte("INVALID\x00\x00")) // 8 bytes, not a known magic
	buf.Write(make([]byte, 36))          // pad for hash and hNumber

	var h Header

	if err := h.Read(buf); err == nil {
		t.Error("expected error for unknown magic, got nil")
	}
}

func TestHeader_ShortRead(t *testing.T) {
	buf := &bytes.Buffer{}
	buf.Write([]byte("U-A-1.0")) // only 7 bytes, too short

	var h Header

	if err := h.Read(buf); err == nil {
		t.Error("expected error for short read, got nil")
	}
}

// TestNewHeader_AllFileTypes tests NewHeader with all supported file types
func TestNewHeader_AllFileTypes(t *testing.T) {
	allTypes := []FileType{
		FileTypeUtxoAdditions,
		FileTypeUtxoDeletions,
		FileTypeUtxoHeaders,
		FileTypeUtxoSet,
		FileTypeBlock,
		FileTypeSubtree,
		FileTypeSubtreeToCheck,
		FileTypeSubtreeData,
		FileTypeSubtreeMeta,
		FileTypeTx,
		FileTypeOutputs,
		FileTypeBloomFilter,
		FileTypeMsgBlock,
		FileTypeDat,
		FileTypeTesting,
		FileTypeBatchData,
		FileTypeBatchKeys,
		FileTypePreserveUntil,
	}

	for _, fileType := range allTypes {
		t.Run(string(fileType), func(t *testing.T) {
			header := NewHeader(fileType)
			assert.Equal(t, fileType, header.FileType())
			assert.Equal(t, 8, header.Size())
		})
	}
}

// TestNewHeader_EmptyFileType tests NewHeader with empty file type
func TestNewHeader_EmptyFileType(t *testing.T) {
	assert.Panics(t, func() {
		NewHeader(FileTypeUnknown)
	})
}

// TestNewHeader_InvalidFileType tests NewHeader with invalid file type
func TestNewHeader_InvalidFileType(t *testing.T) {
	assert.Panics(t, func() {
		NewHeader(FileType("invalid-type"))
	})
}

// TestHeader_Size tests the Size method
func TestHeader_Size(t *testing.T) {
	header := NewHeader(FileTypeBlock)
	assert.Equal(t, 8, header.Size())
}

// TestHeader_Bytes tests the Bytes method
func TestHeader_Bytes(t *testing.T) {
	header := NewHeader(FileTypeBlock)
	bytes := header.Bytes()
	assert.Len(t, bytes, 8)
	assert.Equal(t, magicBlock[:], bytes)
}

// TestHeader_Write tests the Write method
func TestHeader_Write(t *testing.T) {
	header := NewHeader(FileTypeUtxoSet)
	buf := &bytes.Buffer{}

	err := header.Write(buf)
	assert.NoError(t, err)
	assert.Equal(t, 8, buf.Len())
	assert.Equal(t, magicUtxoSet[:], buf.Bytes())
}

// TestHeader_Write_Error tests Write with a failing writer
func TestHeader_Write_Error(t *testing.T) {
	header := NewHeader(FileTypeBlock)
	failingWriter := &failingWriter{}

	err := header.Write(failingWriter)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "error writing magic")
}

// TestHeader_Read tests the Read method
func TestHeader_Read(t *testing.T) {
	header := NewHeader(FileTypeUtxoAdditions)
	buf := &bytes.Buffer{}
	require.NoError(t, header.Write(buf))

	var readHeader Header
	err := readHeader.Read(bytes.NewReader(buf.Bytes()))
	assert.NoError(t, err)
	assert.Equal(t, header.magic, readHeader.magic)
}

// TestHeader_Read_ShortRead tests Read with insufficient data
func TestHeader_Read_ShortRead(t *testing.T) {
	var header Header
	shortData := []byte("SHORT")
	err := header.Read(bytes.NewReader(shortData))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "expected to read 8 bytes")
}

// TestHeader_Read_EOF tests Read with EOF
func TestHeader_Read_EOF(t *testing.T) {
	var header Header
	emptyReader := &bytes.Buffer{}
	err := header.Read(emptyReader)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "error reading magic")
}

// TestHeader_Read_UnknownMagic tests Read with unknown magic
func TestHeader_Read_UnknownMagic(t *testing.T) {
	var header Header
	unknownMagic := []byte("UNKNOWN")
	err := header.Read(bytes.NewReader(unknownMagic))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "expected to read 8 bytes")
}

// TestHeader_Read_BackwardCompatibility tests Read with trailing nulls
func TestHeader_Read_BackwardCompatibility(t *testing.T) {
	var header Header
	// Create magic with trailing nulls
	magicWithNulls := [8]byte{'B', '-', '1', '.', '0', 0, 0, 0}
	err := header.Read(bytes.NewReader(magicWithNulls[:]))
	assert.NoError(t, err)
	assert.Equal(t, FileTypeBlock, header.FileType())
}

// TestReadHeader tests the ReadHeader function
func TestReadHeader(t *testing.T) {
	originalHeader := NewHeader(FileTypeSubtree)
	buf := &bytes.Buffer{}
	require.NoError(t, originalHeader.Write(buf))

	readHeader, err := ReadHeader(bytes.NewReader(buf.Bytes()))
	assert.NoError(t, err)
	assert.Equal(t, originalHeader.magic, readHeader.magic)
	assert.Equal(t, FileTypeSubtree, readHeader.FileType())
}

// TestReadHeader_Error tests ReadHeader with error
func TestReadHeader_Error(t *testing.T) {
	shortData := []byte("SHORT")
	header, err := ReadHeader(bytes.NewReader(shortData))
	assert.Error(t, err)
	assert.Equal(t, Header{}, header)
}

// TestReadHeaderFromBytes tests the ReadHeaderFromBytes function
func TestReadHeaderFromBytes(t *testing.T) {
	originalHeader := NewHeader(FileTypeTx)
	headerBytes := originalHeader.Bytes()

	readHeader, err := ReadHeaderFromBytes(headerBytes)
	assert.NoError(t, err)
	assert.Equal(t, originalHeader.magic, readHeader.magic)
	assert.Equal(t, FileTypeTx, readHeader.FileType())
}

// TestReadHeaderFromBytes_ShortData tests ReadHeaderFromBytes with insufficient data
func TestReadHeaderFromBytes_ShortData(t *testing.T) {
	shortData := []byte("SHORT")
	header, err := ReadHeaderFromBytes(shortData)
	assert.Error(t, err)
	assert.Equal(t, Header{}, header)
	assert.Contains(t, err.Error(), "not enough bytes")
}

// TestReadHeaderFromBytes_UnknownMagic tests ReadHeaderFromBytes with unknown magic
func TestReadHeaderFromBytes_UnknownMagic(t *testing.T) {
	unknownMagic := []byte("UNKNOWN")
	header, err := ReadHeaderFromBytes(unknownMagic)
	assert.Error(t, err)
	assert.Equal(t, Header{}, header)
	assert.Contains(t, err.Error(), "not enough bytes")
}

// TestHeader_FileType tests the FileType method
func TestHeader_FileType(t *testing.T) {
	testCases := []struct {
		fileType FileType
		magic    [8]byte
	}{
		{FileTypeUtxoAdditions, magicUtxoAdditions},
		{FileTypeUtxoDeletions, magicUtxoDeletions},
		{FileTypeUtxoHeaders, magicUtxoHeaders},
		{FileTypeUtxoSet, magicUtxoSet},
		{FileTypeBlock, magicBlock},
		{FileTypeSubtree, magicSubtree},
		{FileTypeSubtreeToCheck, magicSubtreeToCheck},
		{FileTypeSubtreeData, magicSubtreeData},
		{FileTypeSubtreeMeta, magicSubtreeMeta},
		{FileTypeTx, magicTx},
		{FileTypeOutputs, magicOutputs},
		{FileTypeBloomFilter, magicBloomFilter},
		{FileTypeMsgBlock, magicMsgBlock},
		{FileTypeDat, magicDat},
		{FileTypeTesting, magicTesting},
		{FileTypeBatchData, magicBatchData},
		{FileTypeBatchKeys, magicBatchKeys},
		{FileTypePreserveUntil, magicPreserveUntil},
	}

	for _, tc := range testCases {
		t.Run(string(tc.fileType), func(t *testing.T) {
			header := Header{magic: tc.magic}
			assert.Equal(t, tc.fileType, header.FileType())
		})
	}
}

// TestFileType_String tests the String method
func TestFileType_String(t *testing.T) {
	fileType := FileTypeBlock
	assert.Equal(t, "block", fileType.String())
}

// TestFileType_ToMagicBytes tests the ToMagicBytes method
func TestFileType_ToMagicBytes(t *testing.T) {
	fileType := FileTypeUtxoSet
	magicBytes := fileType.ToMagicBytes()
	assert.Equal(t, magicUtxoSet, magicBytes)
}

// TestFileTypeFromExtension tests the FileTypeFromExtension function
func TestFileTypeFromExtension(t *testing.T) {
	testCases := []struct {
		extension string
		expected  FileType
		expectErr bool
	}{
		{"utxo-additions", FileTypeUtxoAdditions, false},
		{"utxo-deletions", FileTypeUtxoDeletions, false},
		{"utxo-headers", FileTypeUtxoHeaders, false},
		{"utxo-set", FileTypeUtxoSet, false},
		{"block", FileTypeBlock, false},
		{"subtree", FileTypeSubtree, false},
		{"subtreeToCheck", FileTypeSubtreeToCheck, false},
		{"subtreeData", FileTypeSubtreeData, false},
		{"subtreeMeta", FileTypeSubtreeMeta, false},
		{"tx", FileTypeTx, false},
		{"outputs", FileTypeOutputs, false},
		{"bloomfilter", FileTypeBloomFilter, false},
		{"msgBlock", FileTypeMsgBlock, false},
		{"dat", FileTypeDat, false},
		{"testing", FileTypeTesting, false},
		{"batch-data", FileTypeBatchData, false},
		{"batch-keys", FileTypeBatchKeys, false},
		{"preserveUntil", FileTypePreserveUntil, false},
		{"invalid-extension", "", true},
		{"", "", true},
	}

	for _, tc := range testCases {
		t.Run(tc.extension, func(t *testing.T) {
			result, err := FileTypeFromExtension(tc.extension)
			if tc.expectErr {
				assert.Error(t, err)
				assert.Equal(t, FileType(""), result)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}

// TestMagicConstants tests that all magic constants are properly defined
func TestMagicConstants(t *testing.T) {
	expectedMagics := map[FileType][8]byte{
		FileTypeUtxoAdditions:  magicUtxoAdditions,
		FileTypeUtxoDeletions:  magicUtxoDeletions,
		FileTypeUtxoHeaders:    magicUtxoHeadersV2,
		FileTypeUtxoSet:        magicUtxoSet,
		FileTypeBlock:          magicBlock,
		FileTypeSubtree:        magicSubtree,
		FileTypeSubtreeToCheck: magicSubtreeToCheck,
		FileTypeSubtreeData:    magicSubtreeData,
		FileTypeSubtreeMeta:    magicSubtreeMeta,
		FileTypeTx:             magicTx,
		FileTypeOutputs:        magicOutputs,
		FileTypeBloomFilter:    magicBloomFilter,
		FileTypeMsgBlock:       magicMsgBlock,
		FileTypeDat:            magicDat,
		FileTypeTesting:        magicTesting,
		FileTypeBatchData:      magicBatchData,
		FileTypeBatchKeys:      magicBatchKeys,
		FileTypePreserveUntil:  magicPreserveUntil,
	}

	for fileType, expectedMagic := range expectedMagics {
		t.Run(string(fileType), func(t *testing.T) {
			header := NewHeader(fileType)
			assert.Equal(t, expectedMagic, header.magic)
		})
	}
}

// TestFileTypeToMagicMapping tests the fileTypeToMagic mapping
func TestFileTypeToMagicMapping(t *testing.T) {
	for fileType, expectedMagic := range fileTypeToMagic {
		t.Run(string(fileType), func(t *testing.T) {
			magicBytes := fileType.ToMagicBytes()
			assert.Equal(t, expectedMagic, magicBytes)
		})
	}
}

// TestMagicToFileTypeMapping tests the magicToFileType mapping
func TestMagicToFileTypeMapping(t *testing.T) {
	for magic, expectedFileType := range magicToFileType {
		t.Run(string(expectedFileType), func(t *testing.T) {
			header := Header{magic: magic}
			assert.Equal(t, expectedFileType, header.FileType())
		})
	}
}

// TestHeader_RoundTripAllTypes tests round-trip serialization for all file types
func TestHeader_RoundTripAllTypes(t *testing.T) {
	allTypes := []FileType{
		FileTypeUtxoAdditions,
		FileTypeUtxoDeletions,
		FileTypeUtxoHeaders,
		FileTypeUtxoSet,
		FileTypeBlock,
		FileTypeSubtree,
		FileTypeSubtreeToCheck,
		FileTypeSubtreeData,
		FileTypeSubtreeMeta,
		FileTypeTx,
		FileTypeOutputs,
		FileTypeBloomFilter,
		FileTypeMsgBlock,
		FileTypeDat,
		FileTypeTesting,
		FileTypeBatchData,
		FileTypeBatchKeys,
		FileTypePreserveUntil,
	}

	for _, fileType := range allTypes {
		t.Run(string(fileType), func(t *testing.T) {
			// Create header
			originalHeader := NewHeader(fileType)

			// Serialize
			buf := &bytes.Buffer{}
			err := originalHeader.Write(buf)
			require.NoError(t, err)

			// Deserialize
			var readHeader Header
			err = readHeader.Read(bytes.NewReader(buf.Bytes()))
			require.NoError(t, err)

			// Verify
			assert.Equal(t, originalHeader.magic, readHeader.magic)
			assert.Equal(t, fileType, readHeader.FileType())
			assert.Equal(t, 8, readHeader.Size())
		})
	}
}

// TestHeader_EdgeCases tests edge cases for header operations
func TestHeader_EdgeCases(t *testing.T) {
	t.Run("empty magic", func(t *testing.T) {
		var header Header
		emptyMagic := [8]byte{}
		header.magic = emptyMagic

		// Should return unknown file type
		assert.Equal(t, FileTypeUnknown, header.FileType())
	})

	t.Run("partial magic", func(t *testing.T) {
		var header Header
		partialMagic := [8]byte{'B', '-', '1', '.', '0', ' ', ' ', ' '}
		header.magic = partialMagic

		// Should work for valid partial magic
		assert.Equal(t, FileTypeBlock, header.FileType())
	})
}

// failingWriter is a writer that always fails
type failingWriter struct{}

func (fw *failingWriter) Write(p []byte) (n int, err error) {
	return 0, io.ErrShortWrite
}
