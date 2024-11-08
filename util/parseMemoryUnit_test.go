package util

import (
	"testing"

	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

func TestParseMemoryUnit(t *testing.T) {
	testCases := []struct {
		input    string
		expected uint64
		hasError bool
	}{
		{"4B", 4, false},
		{"4 B", 4, false},
		{"4.5 KB", 4608, false},
		{"4.5 KiB", 4608, false},
		{"4MB", 4194304, false},
		{"4 MiB", 4194304, false},
		{"4GB", 4294967296, false},
		{"4 GiB", 4294967296, false},
		{"4TB", 4398046511104, false},
		{"4 TiB", 4398046511104, false},
		{"4PB", 4503599627370496, false},
		{"4 PiB", 4503599627370496, false},
		{"1024", 1024, false},
		{"1024.5", 1024, false},
		{"0", 0, false},
		{"", 0, true},
		{"4 XB", 0, true},
		{"4 KB MB", 0, true},
		{"4KB4MB", 0, true},
		{"4.5.6 MB", 0, true},
		{"4 KiB MB", 0, true},
	}

	for _, tc := range testCases {
		result, err := ParseMemoryUnit(tc.input)
		if tc.hasError {
			if err == nil {
				t.Errorf("Expected an error for input: %s", tc.input)
			}
		} else {
			if err != nil {
				t.Errorf("Unexpected error for input: %s, error: %v", tc.input, err)
			}
			if result != tc.expected {
				t.Errorf("Expected %d bytes for input: %s, but got %d bytes", tc.expected, tc.input, result)
			}
		}
	}

}
func TestParseConfig(t *testing.T) {
	blockMaxSize, ok := gocore.Config().Get("blockmaxsize", "4GB")
	require.Equal(t, true, ok)

	blockMaxSizeInt, err := ParseMemoryUnit(blockMaxSize)
	require.NoError(t, err)
	require.Equal(t, uint64(4294967296), blockMaxSizeInt)
}
