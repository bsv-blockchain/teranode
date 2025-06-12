package btcleveldb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestVarInt128Read tests the VarInt128Read function
func TestVarInt128Read(t *testing.T) {
	tests := []struct {
		name     string
		bytes    []byte
		offset   int
		expected []byte
		n        int
	}{
		{
			name:     "single byte, no continuation",
			bytes:    []byte{0x7F, 0x00},
			offset:   0,
			expected: []byte{0x7F},
			n:        1,
		},
		{
			name:     "multi‑byte varInt",
			bytes:    []byte{0x81, 0x01, 0xFF},
			offset:   0,
			expected: []byte{0x81, 0x01},
			n:        2,
		},
		{
			name:     "offset into slice",
			bytes:    []byte{0xFF, 0x7F},
			offset:   1,
			expected: []byte{0x7F},
			n:        1,
		},
		{
			name:     "unterminated varInt returns n == 0",
			bytes:    []byte{0x81, 0x80},
			offset:   0,
			expected: []byte{0x81, 0x80},
			n:        0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, bytesRead := VarInt128Read(tt.bytes, tt.offset)
			require.Equal(t, tt.expected, got)
			require.Equal(t, tt.n, bytesRead)
		})
	}
}

// TestVarInt128Decode tests the VarInt128Decode function
func TestVarInt128Decode(t *testing.T) {
	tests := []struct {
		name     string
		bytes    []byte
		expected int64
	}{
		{
			name:     "257",
			bytes:    []byte{0x81, 0x01},
			expected: 257,
		},
		{
			name:     "127 single byte",
			bytes:    []byte{0x7F},
			expected: 127,
		},
		{
			name:     "value one (0x80)",
			bytes:    []byte{0x80},
			expected: 1,
		},
		{
			name:     "higher value 389",
			bytes:    []byte{0x82, 0x05},
			expected: 389,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decoded := VarInt128Decode(tt.bytes)
			require.Equal(t, tt.expected, decoded)
		})
	}
}

// TestDecompressValue tests the DecompressValue function
func TestDecompressValue(t *testing.T) {
	tests := []struct {
		name     string
		x        int64
		expected int64
	}{
		{
			name:     "zero",
			x:        0,
			expected: 0,
		},
		{
			name:     "one",
			x:        1,
			expected: 1,
		},
		{
			name:     "compressed 27 -> 3,000,000",
			x:        27,
			expected: 3000000,
		},
		{
			name:     "compressed 10 -> 1,000,000,000",
			x:        10,
			expected: 1000000000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DecompressValue(tt.x)
			require.Equal(t, tt.expected, got)
		})
	}
}
