package options

import (
	"bytes"
	"testing"
)

func TestNewFooter(t *testing.T) {
	eofMarker := []byte("EOF")
	metaData := []byte("meta")
	fetchMeta := func() []byte { return metaData }

	footer := NewFooter(7, eofMarker, fetchMeta)

	if footer.size != 7 {
		t.Errorf("Expected size 7, got %d", footer.size)
	}

	if !bytes.Equal(footer.eofMarker, eofMarker) {
		t.Errorf("Expected EOF marker %v, got %v", eofMarker, footer.eofMarker)
	}

	if actual := footer.fetchMetaData(); !bytes.Equal(actual, metaData) {
		t.Errorf("Expected metadata %v, got %v", metaData, actual)
	}
}

func TestFooter_GetFooter(t *testing.T) {
	tests := []struct {
		name       string
		size       int
		eofMarker  []byte
		fetchMeta  func() []byte
		wantFooter []byte
		wantErr    bool
	}{
		{
			name:       "valid footer with metadata",
			size:       7,
			eofMarker:  []byte("EOF"),
			fetchMeta:  func() []byte { return []byte("meta") },
			wantFooter: []byte("EOFmeta"),
			wantErr:    false,
		},
		{
			name:       "valid footer without metadata",
			size:       3,
			eofMarker:  []byte("EOF"),
			fetchMeta:  nil,
			wantFooter: []byte("EOF"),
			wantErr:    false,
		},
		{
			name:       "size mismatch error",
			size:       5,
			eofMarker:  []byte("EOF"),
			fetchMeta:  func() []byte { return []byte("meta") },
			wantFooter: nil,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			footer := NewFooter(tt.size, tt.eofMarker, tt.fetchMeta)
			got, err := footer.GetFooter()

			if tt.wantErr {
				if err == nil {
					t.Error("Expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}

				if !bytes.Equal(got, tt.wantFooter) {
					t.Errorf("Expected footer %v, got %v", tt.wantFooter, got)
				}
			}
		})
	}
}

func TestFooter_IsFooter(t *testing.T) {
	eofMarker := []byte("EOF")
	footer := NewFooter(7, eofMarker, nil)

	tests := []struct {
		name     string
		input    []byte
		expected bool
	}{
		{
			name:     "valid footer",
			input:    []byte("EOFmeta"),
			expected: true,
		},
		{
			name:     "too short",
			input:    []byte("EO"),
			expected: false,
		},
		{
			name:     "different prefix",
			input:    []byte("FOOmeta"),
			expected: false,
		},
		{
			name:     "exact size match",
			input:    []byte("EOFdata"),
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := footer.IsFooter(tt.input); got != tt.expected {
				t.Errorf("IsFooter() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestFooter_GetMetaData(t *testing.T) {
	eofMarker := []byte("EOF")
	footer := NewFooter(7, eofMarker, nil)

	tests := []struct {
		name         string
		footerBytes  []byte
		wantMetadata []byte
	}{
		{
			name:         "extract metadata",
			footerBytes:  []byte("EOFmeta"),
			wantMetadata: []byte("meta"),
		},
		{
			name:         "empty metadata",
			footerBytes:  []byte("EOF"),
			wantMetadata: []byte{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := footer.GetFooterMetaData(tt.footerBytes)
			if !bytes.Equal(got, tt.wantMetadata) {
				t.Errorf("GetMetaData() = %v, want %v", got, tt.wantMetadata)
			}
		})
	}
}
