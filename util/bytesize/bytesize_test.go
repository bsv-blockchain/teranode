package bytesize

import (
	"fmt"
	"testing"
)

// Test helpers
func assertByteSize(t *testing.T, got, want ByteSize) {
	t.Helper()
	if got != want {
		t.Errorf("got %d, want %d", got, want)
	}
}

func assertError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Error("expected error, got nil")
	}
}

func assertNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

// Core functionality tests
func TestParse_BasicUnits(t *testing.T) {
	tests := []struct {
		input string
		want  ByteSize
	}{
		// Bytes
		{"1B", 1},
		{"100B", 100},
		{"1024B", 1024},

		// Kilobytes
		{"1KB", 1024},
		{"1K", 1024},
		{"2KB", 2048},

		// Megabytes
		{"1MB", 1048576},
		{"1M", 1048576},

		// Gigabytes
		{"1GB", 1073741824},
		{"1G", 1073741824},

		// Terabytes
		{"1TB", 1099511627776},
		{"1T", 1099511627776},

		// No unit (assumes bytes)
		{"1024", 1024},
		{"0", 0},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := Parse(tt.input)
			assertNoError(t, err)
			assertByteSize(t, got, tt.want)
		})
	}
}

func TestParse_DecimalValues(t *testing.T) {
	tests := []struct {
		input string
		want  ByteSize
	}{
		{"0.5KB", 512},
		{"1.5KB", 1536},
		{"0.25MB", 262144},
		{"1.5GB", 1610612736},
		{"2.5GB", 2684354560},
		{"1.5TB", 1649267441664},
		{"0.0GB", 0},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := Parse(tt.input)
			assertNoError(t, err)
			assertByteSize(t, got, tt.want)
		})
	}
}

func TestParse_Whitespace(t *testing.T) {
	tests := []struct {
		input string
		want  ByteSize
	}{
		{" 1GB", 1073741824},
		{"1GB ", 1073741824},
		{" 1GB ", 1073741824},
		{"\t1GB\n", 1073741824},
		{"1 KB", 1024},      // Space between number and unit
		{"1  KB", 1024},     // Multiple spaces
		{"  1  KB  ", 1024}, // Spaces everywhere
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%q", tt.input), func(t *testing.T) {
			got, err := Parse(tt.input)
			assertNoError(t, err)
			assertByteSize(t, got, tt.want)
		})
	}
}

func TestParse_CaseInsensitive(t *testing.T) {
	tests := []struct {
		input string
		want  ByteSize
	}{
		{"1kb", 1024},
		{"1Kb", 1024},
		{"1kB", 1024},
		{"1KB", 1024},
		{"1mb", 1048576},
		{"1Mb", 1048576},
		{"1mB", 1048576},
		{"1MB", 1048576},
		{"1gb", 1073741824},
		{"1Gb", 1073741824},
		{"1gB", 1073741824},
		{"1GB", 1073741824},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := Parse(tt.input)
			assertNoError(t, err)
			assertByteSize(t, got, tt.want)
		})
	}
}

func TestParse_InvalidInput(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"empty string", ""},
		{"only whitespace", "   "},
		{"no number", "KB"},
		{"invalid number", "abc"},
		{"invalid unit", "1XB"},
		{"unsupported unit", "1PB"},
		{"multiple decimals", "1.5.5GB"},
		{"scientific notation", "1E5"},
		{"negative number", "-1KB"}, // '-' is treated as unit separator
		{"comma decimal", "1,5GB"},  // ',' is treated as unit separator
		{"special chars", "++1GB"},
		{"invalid decimal", "1..5"},
		{"just sign", "+"},
		{"just decimal", "."},
		{"wrong unit name", "1BYTE"},
		{"plural unit", "1BYTES"},
		{"full unit name", "1KILOBYTE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.input)
			assertError(t, err)
		})
	}
}

func TestString_Formatting(t *testing.T) {
	tests := []struct {
		input ByteSize
		want  string
	}{
		// Bytes (< 1KB)
		{0, "0 B"},
		{1, "1 B"},
		{512, "512 B"},
		{1023, "1023 B"},

		// Kilobytes
		{1024, "1.00 KB"},
		{1536, "1.50 KB"},
		{1025, "1.00 KB"},
		{1075, "1.05 KB"},

		// Megabytes
		{1048576, "1.00 MB"},
		{1572864, "1.50 MB"},
		{1536000, "1.46 MB"},

		// Gigabytes
		{1073741824, "1.00 GB"},
		{1610612736, "1.50 GB"},

		// Terabytes
		{1099511627776, "1.00 TB"},
		{1649267441664, "1.50 TB"},

		// Large values
		{1099511627776 * 999, "999.00 TB"},

		// Negative values
		{-1, "-1 B"},
		{-512, "-512 B"},
		{-1024, "-1.00 KB"},
		{-1536, "-1.50 KB"},
		{-1048576, "-1.00 MB"},
		{-1073741824, "-1.00 GB"},
		{-1099511627776, "-1.00 TB"},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d", tt.input), func(t *testing.T) {
			got := tt.input.String()
			if got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestString_BoundaryValues(t *testing.T) {
	tests := []struct {
		name  string
		input ByteSize
		want  string
	}{
		// Values just under unit boundaries
		{"just under 1KB", 1023, "1023 B"},
		{"just under 1MB", 1048575, "1024.00 KB"},
		{"just under 1GB", 1073741823, "1024.00 MB"},
		{"just under 1TB", 1099511627775, "1024.00 GB"},

		// Exact boundaries
		{"exactly 1KB", 1024, "1.00 KB"},
		{"exactly 1MB", 1048576, "1.00 MB"},
		{"exactly 1GB", 1073741824, "1.00 GB"},
		{"exactly 1TB", 1099511627776, "1.00 TB"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.input.String()
			if got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestInt_Conversion(t *testing.T) {
	tests := []struct {
		input ByteSize
		want  int
	}{
		{0, 0},
		{1, 1},
		{1024, 1024},
		{1048576, 1048576},
		{-1024, -1024},
		{-1048576, -1048576},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d", tt.input), func(t *testing.T) {
			got := tt.input.Int()
			if got != tt.want {
				t.Errorf("Int() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestConstants(t *testing.T) {
	tests := []struct {
		name     string
		constant ByteSize
		want     ByteSize
	}{
		{"B", B, 1},
		{"KB", KB, 1024},
		{"MB", MB, 1024 * 1024},
		{"GB", GB, 1024 * 1024 * 1024},
		{"TB", TB, 1024 * 1024 * 1024 * 1024},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.constant != tt.want {
				t.Errorf("%s = %d, want %d", tt.name, tt.constant, tt.want)
			}
		})
	}
}

func TestRoundTrip(t *testing.T) {
	// Test that common values can be parsed and formatted consistently
	inputs := []string{
		"1KB",
		"1.5MB",
		"2GB",
		"0.5TB",
		"1024B",
	}

	for _, input := range inputs {
		t.Run(input, func(t *testing.T) {
			// Parse the input
			parsed, err := Parse(input)
			assertNoError(t, err)

			// Convert to string and parse again
			str := parsed.String()
			reparsed, err := Parse(str)
			assertNoError(t, err)

			// Should be equal (allowing for small rounding differences)
			diff := parsed - reparsed
			if diff < 0 {
				diff = -diff
			}

			// Allow up to 0.1% difference due to rounding
			tolerance := ByteSize(float64(parsed) * 0.001)
			if tolerance < 1 {
				tolerance = 1
			}

			if diff > tolerance {
				t.Errorf("round trip failed: %s -> %d -> %s -> %d (diff: %d)",
					input, parsed, str, reparsed, diff)
			}
		})
	}
}

func TestParse_EdgeCases(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  ByteSize
		err   bool
	}{
		// Zero values
		{"zero bytes", "0", 0, false},
		{"zero with unit", "0KB", 0, false},
		{"zero decimal", "0.0GB", 0, false},

		// Large values
		{"large TB", "999TB", 999 * TB, false},
		{"max safe value", "8191TB", 8191 * TB, false},

		// Parsing quirks
		{"space in unit", "1 KB", 1024, false},
		{"negative fails", "-1KB", 0, true},
		{"incomplete json", "{1GB", 0, true},
		{"number with text", "1ABC2", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parse(tt.input)
			if tt.err {
				assertError(t, err)
			} else {
				assertNoError(t, err)
				assertByteSize(t, got, tt.want)
			}
		})
	}
}
