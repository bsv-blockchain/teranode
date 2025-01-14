package helper

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/bitcoin-sv/teranode/stores/blob/options"
)

// mockReadCloser implements io.ReadCloser for testing
type mockReadCloser struct {
	*bytes.Reader
	closed bool
}

func newMockReadCloser(data []byte) *mockReadCloser {
	return &mockReadCloser{
		Reader: bytes.NewReader(data),
	}
}

func (m *mockReadCloser) Close() error {
	m.closed = true
	return nil
}

func TestReaderWithHeaderAndFooterRemoved(t *testing.T) {
	tests := []struct {
		name        string
		data        []byte
		header      []byte
		footer      *options.Footer
		wantData    []byte
		wantErr     error
		readSize    int
		description string
	}{
		{
			name:        "No header or footer",
			data:        []byte("hello world"),
			header:      nil,
			footer:      nil,
			wantData:    []byte("hello world"),
			wantErr:     nil,
			readSize:    32,
			description: "Should return original data when no header/footer specified",
		},
		{
			name:        "With header only",
			data:        []byte("HDRhello world"),
			header:      []byte("HDR"),
			footer:      nil,
			wantData:    []byte("hello world"),
			wantErr:     nil,
			readSize:    32,
			description: "Should strip header only",
		},
		{
			name:        "With footer only",
			data:        []byte("hello worldFTR"),
			header:      nil,
			footer:      options.NewFooter(3, []byte("FTR"), nil),
			wantData:    []byte("hello world"),
			wantErr:     nil,
			readSize:    32,
			description: "Should strip footer only",
		},
		{
			name:        "With header and footer",
			data:        []byte("HDRhello worldFTR"),
			header:      []byte("HDR"),
			footer:      options.NewFooter(3, []byte("FTR"), nil),
			wantData:    []byte("hello world"),
			wantErr:     nil,
			readSize:    32,
			description: "Should strip both header and footer",
		},
		{
			name:        "Invalid header",
			data:        []byte("XXXhello world"),
			header:      []byte("HDR"),
			footer:      nil,
			wantData:    nil,
			wantErr:     ErrInvalidHeader,
			readSize:    32,
			description: "Should return error for invalid header",
		},
		{
			name:        "Invalid footer",
			data:        []byte("hello worldXXX"),
			header:      nil,
			footer:      options.NewFooter(3, []byte("FTR"), nil),
			wantData:    nil,
			wantErr:     ErrInvalidFooter,
			readSize:    32,
			description: "Should return error for invalid footer",
		},
		{
			name:        "Small read buffer",
			data:        []byte("HDRhello worldFTR"),
			header:      []byte("HDR"),
			footer:      options.NewFooter(3, []byte("FTR"), nil),
			wantData:    []byte("hello world"),
			wantErr:     nil,
			readSize:    3,
			description: "Should handle small read buffer sizes correctly",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := newMockReadCloser(tt.data)

			r, err := ReaderWithHeaderAndFooterRemoved(reader, tt.header, tt.footer)
			if err != nil {
				t.Fatalf("ReaderWithHeaderAndFooterRemoved() error = %v", err)
			}

			buf := make([]byte, 0)
			readBuf := make([]byte, tt.readSize)

			for {
				n, err := r.Read(readBuf)
				if n > 0 {
					buf = append(buf, readBuf[:n]...)
				}

				if err == io.EOF {
					break
				}

				if err != nil {
					if tt.wantErr != nil && err == tt.wantErr {
						return
					}

					t.Fatalf("unexpected error during read: %v", err)
				}
			}

			if tt.wantErr == nil {
				if !bytes.Equal(buf, tt.wantData) {
					t.Errorf("got %q, want %q", buf, tt.wantData)
				}
			}

			err = r.Close()
			if err != nil {
				t.Errorf("Close() error = %v", err)
			}

			if !reader.closed {
				t.Error("underlying reader was not closed")
			}
		})
	}
}

func TestBytesWithHeadAndFooterRemoved(t *testing.T) {
	tests := []struct {
		name        string
		input       []byte
		header      []byte
		footer      *options.Footer
		want        []byte
		wantErr     bool
		description string
	}{
		{
			name:        "Empty input",
			input:       []byte{},
			header:      nil,
			footer:      nil,
			want:        []byte{},
			wantErr:     false,
			description: "Should handle empty input",
		},
		{
			name:        "With header and footer",
			input:       []byte("HDRcontentFTR"),
			header:      []byte("HDR"),
			footer:      options.NewFooter(3, []byte("FTR"), nil),
			want:        []byte("content"),
			wantErr:     false,
			description: "Should remove both header and footer",
		},
		{
			name:        "Input shorter than header",
			input:       []byte("12"),
			header:      []byte("123"),
			footer:      nil,
			want:        nil,
			wantErr:     true,
			description: "Should error when input is shorter than header",
		},
		{
			name:        "Input shorter than footer",
			input:       []byte("12"),
			header:      nil,
			footer:      options.NewFooter(3, []byte("123"), nil),
			want:        nil,
			wantErr:     true,
			description: "Should error when input is shorter than footer",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := BytesWithHeadAndFooterRemoved(tt.input, tt.header, tt.footer)
			if (err != nil) != tt.wantErr {
				t.Errorf("BytesWithHeadAndFooterRemoved() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && !bytes.Equal(got, tt.want) {
				t.Errorf("BytesWithHeadAndFooterRemoved() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReadLastNBytes(t *testing.T) {
	// Create a temporary file for testing
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.txt")
	content := []byte("Hello World!")

	err := os.WriteFile(tmpFile, content, 0644) //nolint:gosec
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	tests := []struct {
		name        string
		size        int64
		want        []byte
		wantErr     bool
		description string
	}{
		{
			name:        "Read last 5 bytes",
			size:        5,
			want:        []byte("orld!"),
			wantErr:     false,
			description: "Should read last 5 bytes correctly",
		},
		{
			name:        "Read more than file size",
			size:        20,
			want:        nil,
			wantErr:     true,
			description: "Should error when requested size is larger than file",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			file, err := os.Open(tmpFile)
			if err != nil {
				t.Fatalf("Failed to open test file: %v", err)
			}
			defer file.Close()

			got, err := ReadLastNBytes(file, tt.size)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadLastNBytes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && !bytes.Equal(got, tt.want) {
				t.Errorf("ReadLastNBytes() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHeaderAndFooterMetaData(t *testing.T) {
	// Create a temporary file for testing
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "metadata_test.txt")
	header := []byte("HDR")
	content := []byte("content")
	footer := []byte("FTR")
	metadata := []byte("x")
	fullContent := append(append(append(header, content...), footer...), metadata...)

	err := os.WriteFile(tmpFile, fullContent, 0644) //nolint:gosec
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	t.Run("Test HeaderMetaData", func(t *testing.T) {
		file, err := os.Open(tmpFile)
		if err != nil {
			t.Fatalf("Failed to open test file: %v", err)
		}
		defer file.Close()

		got, err := HeaderMetaData(file, len(header))
		if err != nil {
			t.Errorf("HeaderMetaData() error = %v", err)
			return
		}

		if !bytes.Equal(got, header) {
			t.Errorf("HeaderMetaData() = %v, want %v", got, header)
		}
	})

	t.Run("Test FooterMetaData", func(t *testing.T) {
		file, err := os.Open(tmpFile)
		if err != nil {
			t.Fatalf("Failed to open test file: %v", err)
		}
		defer file.Close()

		metadata := []byte("x")

		footerOpt := options.NewFooter(4, footer, func() []byte {
			return metadata
		})

		got, err := FooterMetaData(file, footerOpt)
		if err != nil {
			t.Errorf("FooterMetaData() error = %v", err)
			return
		}

		if !bytes.Equal(got, metadata) {
			t.Errorf("FooterMetaData() = %v, want %v", got, metadata)
		}
	})
}
