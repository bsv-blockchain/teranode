package seeder

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestFormatNumber tests the formatNumber function
func TestFormatNumber(t *testing.T) {
	tests := []struct {
		input    uint64
		expected string
	}{
		{0, "0"},
		{123, "123"},
		{1234, "1,234"},
		{1234567, "1,234,567"},
		{1234567890, "1,234,567,890"},
	}

	for _, test := range tests {
		result := formatNumber(test.input)
		assert.Equal(t, test.expected, result, "formatNumber(%d) = %s; want %s", test.input, result, test.expected)
	}
}
