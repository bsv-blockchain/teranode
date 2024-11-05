package bytesize

import (
	"testing"
)

func TestParseByteSize(t *testing.T) {
	tests := []struct {
		input    string
		expected ByteSize
		hasError bool
	}{
		{"1B", 1, false},
		{"1KB", 1024, false},
		{"1MB", 1024 * 1024, false},
		{"1GB", 1024 * 1024 * 1024, false},
		{"1TB", 1024 * 1024 * 1024 * 1024, false},
		{"1.5GB", 1610612736, false},
		{"1K", 1024, false},
		{"1M", 1048576, false},
		{"1G", 1073741824, false},
		{"1T", 1099511627776, false},
		{"1024", 1024, false},
		{"  1.5 GB  ", 1610612736, false},
		{"1.5 gb", 1610612736, false},
		{"invalid", 0, true},
		{"1XB", 0, true},
		{"GB", 0, true},
	}

	for _, test := range tests {
		result, err := Parse(test.input)
		if test.hasError {
			if err == nil {
				t.Errorf("ParseByteSize(%q) expected an error, but got none", test.input)
			}
		} else {
			if err != nil {
				t.Errorf("ParseByteSize(%q) unexpected error: %v", test.input, err)
			}

			if result != test.expected {
				t.Errorf("ParseByteSize(%q) = %v, expected %v", test.input, result, test.expected)
			}
		}
	}
}

func TestString(t *testing.T) {
	tests := []struct {
		input    ByteSize
		expected string
	}{
		{1024, "1.00 KB"},
		{1024 * 1024, "1.00 MB"},
		{1024 * 1024 * 1024, "1.00 GB"},
		{1024 * 1024 * 1024 * 1024, "1.00 TB"},
		{-1024, "-1.00 KB"},
		{-1024 * 1024, "-1.00 MB"},
		{-1024 * 1024 * 1024, "-1.00 GB"},
		{-1024 * 1024 * 1024 * 1024, "-1.00 TB"},
	}

	for _, test := range tests {
		result := test.input.String()
		if result != test.expected {
			t.Errorf("String(%v) = %q, expected %q", test.input, result, test.expected)
		}
	}
}
