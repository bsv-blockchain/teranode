package util

import (
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVarintSize(t *testing.T) {
	tests := []struct {
		name     string
		input    uint64
		expected uint64
	}{
		// Single byte values (0 to 252)
		{
			name:     "zero value",
			input:    0,
			expected: 1,
		},
		{
			name:     "small value",
			input:    1,
			expected: 1,
		},
		{
			name:     "medium single byte",
			input:    100,
			expected: 1,
		},
		{
			name:     "max single byte minus one",
			input:    0xfc,
			expected: 1,
		},

		// Three byte values (253 to 65535)
		{
			name:     "min three byte",
			input:    0xfd,
			expected: 3,
		},
		{
			name:     "three byte value",
			input:    1000,
			expected: 3,
		},
		{
			name:     "max three byte",
			input:    0xffff,
			expected: 3,
		},

		// Five byte values (65536 to 4294967295)
		{
			name:     "min five byte",
			input:    0x10000,
			expected: 5,
		},
		{
			name:     "five byte value",
			input:    1000000,
			expected: 5,
		},
		{
			name:     "max five byte",
			input:    0xffffffff,
			expected: 5,
		},

		// Nine byte values (4294967296 and above)
		{
			name:     "min nine byte",
			input:    0x100000000,
			expected: 9,
		},
		{
			name:     "large nine byte",
			input:    1000000000000,
			expected: 9,
		},
		{
			name:     "max uint64",
			input:    math.MaxUint64,
			expected: 9,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := VarintSize(tt.input)
			assert.Equal(t, tt.expected, result, "VarintSize(%d) should return %d", tt.input, tt.expected)
		})
	}
}

func TestVarintSizeBoundaries(t *testing.T) {
	// Test exact boundary values
	boundaries := []struct {
		value    uint64
		expected uint64
		desc     string
	}{
		{0xfc, 1, "0xfc (252) - last single byte"},
		{0xfd, 3, "0xfd (253) - first three byte"},
		{0xffff, 3, "0xffff (65535) - last three byte"},
		{0x10000, 5, "0x10000 (65536) - first five byte"},
		{0xffffffff, 5, "0xffffffff (4294967295) - last five byte"},
		{0x100000000, 9, "0x100000000 (4294967296) - first nine byte"},
	}

	for _, b := range boundaries {
		t.Run(b.desc, func(t *testing.T) {
			result := VarintSize(b.value)
			assert.Equal(t, b.expected, result)
		})
	}
}

func TestVarintSizeEdgeCases(t *testing.T) {
	t.Run("values around boundaries", func(t *testing.T) {
		// Test values just before and after boundaries
		testCases := []struct {
			value    uint64
			expected uint64
		}{
			// Around 0xfd boundary
			{0xfb, 1},
			{0xfc, 1},
			{0xfd, 3},
			{0xfe, 3},

			// Around 0xffff boundary
			{0xfffe, 3},
			{0xffff, 3},
			{0x10000, 5},
			{0x10001, 5},

			// Around 0xffffffff boundary
			{0xfffffffe, 5},
			{0xffffffff, 5},
			{0x100000000, 9},
			{0x100000001, 9},
		}

		for _, tc := range testCases {
			result := VarintSize(tc.value)
			assert.Equal(t, tc.expected, result, "VarintSize(%x) should return %d", tc.value, tc.expected)
		}
	})
}

func BenchmarkVarintSize(b *testing.B) {
	b.Run("SingleByte", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = VarintSize(100)
		}
	})

	b.Run("ThreeByte", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = VarintSize(1000)
		}
	})

	b.Run("FiveByte", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = VarintSize(100000)
		}
	})

	b.Run("NineByte", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = VarintSize(10000000000)
		}
	})

	b.Run("Mixed", func(b *testing.B) {
		values := []uint64{10, 1000, 100000, 10000000000}
		for i := 0; i < b.N; i++ {
			_ = VarintSize(values[i%len(values)])
		}
	})
}

// TestVarintSizeTable tests using a table of known values
func TestVarintSizeTable(t *testing.T) {
	// These values are based on Bitcoin's varint encoding specification
	knownValues := map[uint64]uint64{
		0:              1,
		1:              1,
		252:            1,
		253:            3,
		254:            3,
		255:            3,
		256:            3,
		65535:          3,
		65536:          5,
		65537:          5,
		4294967295:     5,
		4294967296:     9,
		4294967297:     9,
		math.MaxUint64: 9,
	}

	for value, expected := range knownValues {
		t.Run(formatValue(value), func(t *testing.T) {
			result := VarintSize(value)
			assert.Equal(t, expected, result)
		})
	}
}

// Helper function to format large numbers for test names
func formatValue(v uint64) string {
	switch {
	case v < 1000:
		return strconv.FormatUint(v, 10)
	case v == math.MaxUint64:
		return "MaxUint64"
	default:
		return strconv.FormatUint(v, 10)
	}
}

// TestVarintSizeConsistency ensures the function behaves consistently
func TestVarintSizeConsistency(t *testing.T) {
	// Test that calling the function multiple times returns the same result
	testValues := []uint64{0, 252, 253, 65535, 65536, 4294967295, 4294967296, math.MaxUint64}

	for _, value := range testValues {
		firstResult := VarintSize(value)

		// Call multiple times
		for i := 0; i < 10; i++ {
			result := VarintSize(value)
			assert.Equal(t, firstResult, result, "VarintSize should return consistent results for value %d", value)
		}
	}
}

// TestVarintSizeSequential tests sequential values to ensure no gaps in logic
func TestVarintSizeSequential(t *testing.T) {
	// Test ranges around boundaries
	ranges := []struct {
		start, end uint64
		expected   uint64
	}{
		{250, 255, 0},     // Mixed: some 1-byte, some 3-byte
		{65530, 65540, 0}, // Mixed: some 3-byte, some 5-byte
	}

	for _, r := range ranges {
		for v := r.start; v <= r.end; v++ {
			result := VarintSize(v)

			// Verify result is valid
			assert.True(t, result == 1 || result == 3 || result == 5 || result == 9,
				"VarintSize(%d) returned invalid size: %d", v, result)

			// Verify transitions happen at correct boundaries
			if v < 0xfd {
				assert.Equal(t, uint64(1), result, "Value %d should use 1 byte", v)
			} else if v <= 0xffff {
				assert.Equal(t, uint64(3), result, "Value %d should use 3 bytes", v)
			} else if v <= 0xffffffff {
				assert.Equal(t, uint64(5), result, "Value %d should use 5 bytes", v)
			} else {
				assert.Equal(t, uint64(9), result, "Value %d should use 9 bytes", v)
			}
		}
	}
}
