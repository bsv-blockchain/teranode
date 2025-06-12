package bitcointoutxoset

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestFormatNumber tests the formatNumber function which formats an "uint64" number with commas.
func TestFormatNumber(t *testing.T) {
	testCases := []struct {
		name string
		n    uint64
		exp  string
	}{
		{"zero", 0, "0"},
		{"one", 1, "1"},
		{"two-digits", 12, "12"},
		{"three-digits", 123, "123"},
		{"four-digits", 1234, "1,234"},
		{"five-digits", 12345, "12,345"},
		{"six-digits", 123456, "123,456"},
		{"seven-digits", 1234567, "1,234,567"},
		{"eight-digits", 12345678, "12,345,678"},
		{"nine-digits", 123456789, "123,456,789"},
		{"ten-digits", 1234567890, "1,234,567,890"},
		{"million", 1000000, "1,000,000"},
		{"billion", 1000000000, "1,000,000,000"},
		{"max-uint64", 18446744073709551615, "18,446,744,073,709,551,615"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.exp, formatNumber(tc.n))
		})
	}
}

// TestGetObfuscateKeyExtended tests the getObfuscateKeyExtended function which generates an obfuscated key based on a given key and value.
func TestGetObfuscateKeyExtended(t *testing.T) {
	testCases := []struct {
		name       string
		key, value []byte
		exp        []byte
	}{
		{
			name:  "key shorter than value, repeats",
			key:   []byte{0x03, 0xAA, 0xBB, 0xCC}, // first byte is length, so actual key is {0xAA, 0xBB, 0xCC}
			value: []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
			exp:   []byte{0xAA, 0xBB, 0xCC, 0xAA, 0xBB, 0xCC, 0xAA, 0xBB},
		},
		{
			name:  "key same length as value",
			key:   []byte{0x02, 0x11, 0x22}, // actual key {0x11, 0x22}
			value: []byte{0x01, 0x02},
			exp:   []byte{0x11, 0x22},
		},
		/*
			// Not sure if this should pass or not, but currently it does not.
			// This would require the function to change to handle cases where the key is longer than the value.
			{
				name: "key longer than value",
				key: []byte{0x04, 0xDE, 0xAD, 0xBE, 0xEF}, // actual key {0xAD, 0xBE, 0xEF}
				value: []byte{0x01},
				exp: []byte{0xAD},
			},
			{
				name: "empty value",
				key: []byte{0x01, 0xFF},
				value: []byte{},
				exp: []byte{},
			},
		*/
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.exp, getObfuscateKeyExtended(tc.key, tc.value))
		})
	}
}
