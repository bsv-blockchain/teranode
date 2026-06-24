package fileformat

import (
	"bytes"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/require"
)

// validMagics returns the 8-byte magic for every registered file type, used as
// valid fuzz seeds and as the basis for the consistency checks below.
func validMagics() [][]byte {
	out := make([][]byte, 0, len(fileTypeToMagic))
	for ft := range fileTypeToMagic {
		m := NewHeader(ft).Bytes()
		out = append(out, m)
	}

	return out
}

// TestHeaderRead_AcceptsChunkedReader pins that Header.Read accepts a valid
// 8-byte magic even when the reader delivers it in pieces. io.Reader.Read is
// permitted to return fewer than len(p) bytes with a nil error (network
// sockets, TLS conns, io.Pipe, bufio over a slow source all do this). The old
// implementation used a single r.Read and rejected anything that did not return
// all 8 bytes in one call — so a valid header arriving in chunks failed with
// "expected to read 8 bytes". Reading must use io.ReadFull.
func TestHeaderRead_AcceptsChunkedReader(t *testing.T) {
	for _, m := range validMagics() {
		var h Header
		// iotest.OneByteReader forces one byte per Read — the worst-case chunking.
		err := h.Read(iotest.OneByteReader(bytes.NewReader(m)))
		require.NoError(t, err, "valid magic %q must parse even when delivered one byte at a time", m)

		fromBytes, err := ReadHeaderFromBytes(m)
		require.NoError(t, err)
		require.Equal(t, fromBytes.FileType(), h.FileType())
	}
}

// TestReadHeaderFromBytes_BackwardCompatibility pins that ReadHeaderFromBytes
// applies the same trailing-0x00 -> 0x20 normalization that Header.Read does
// (see TestHeader_Read_BackwardCompatibility). Older files padded the magic
// with NULs rather than spaces; without this, the streaming reader accepts such
// a file while the byte-slice reader rejects it as "unknown magic" — the same
// kind of reader/writer disagreement that has crashed the core sidecar before.
func TestReadHeaderFromBytes_BackwardCompatibility(t *testing.T) {
	magicWithNulls := []byte{'B', '-', '1', '.', '0', 0, 0, 0}

	h, err := ReadHeaderFromBytes(magicWithNulls)
	require.NoError(t, err)
	require.Equal(t, FileTypeBlock, h.FileType())
}

// FuzzHeaderParsersAgree is a differential fuzz: the byte-slice parser
// (ReadHeaderFromBytes) and the streaming parser (Header.Read) must agree on
// whether arbitrary input is a valid header and, when it is, on the file type —
// even when the stream is delivered one byte at a time. This catches both
// reader/writer normalization drift and short-read mishandling.
func FuzzHeaderParsersAgree(f *testing.F) {
	for _, m := range validMagics() {
		f.Add(m)
	}
	f.Add([]byte{'B', '-', '1', '.', '0', 0, 0, 0}) // NUL-padded (backward-compat)
	f.Add([]byte("UNKNOWN!"))                       // 8 bytes, unknown magic
	f.Add([]byte("SHORT"))                          // too short
	f.Add([]byte{})                                 // empty

	f.Fuzz(func(t *testing.T, data []byte) {
		fromBytes, errBytes := ReadHeaderFromBytes(data)
		fromReader, errReader := ReadHeader(iotest.OneByteReader(bytes.NewReader(data)))

		require.Equal(t, errBytes == nil, errReader == nil,
			"parsers disagree on validity of %q: bytes err=%v, reader err=%v", data, errBytes, errReader)

		if errBytes == nil && errReader == nil {
			require.Equal(t, fromBytes.FileType(), fromReader.FileType(),
				"parsers agree on validity but disagree on file type for %q", data)
		}
	})
}

// FuzzReadHeaderFromBytes asserts the byte-slice header parser never panics on
// arbitrary input.
func FuzzReadHeaderFromBytes(f *testing.F) {
	for _, m := range validMagics() {
		f.Add(m)
	}
	f.Add([]byte{})
	f.Add(make([]byte, 8))

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = ReadHeaderFromBytes(data)
	})
}
