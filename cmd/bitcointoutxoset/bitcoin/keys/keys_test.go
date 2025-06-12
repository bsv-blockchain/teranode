package keys

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDecompressPublicKey tests the DecompressPublicKey function
func TestDecompressPublicKey(t *testing.T) {
	tests := []struct {
		name          string
		compressedHex string
		expectedHex   string
	}{
		{
			name:          "generator even y (prefix 02)",
			compressedHex: "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798",
			expectedHex:   "0479be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798483ada7726a3c4655da4fbfc0e1108a8fd17b448a68554199c47d08ffb10d4b8",
		},
		{
			name:          "generator odd y (prefix 03)",
			compressedHex: "0379be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798",
			expectedHex:   "0479be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798b7c52588d95c3b9aa25b0403f1eef75702e84bb7597aabe663b82f6f04ef2777",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compressed, err := hex.DecodeString(tt.compressedHex)
			require.NoError(t, err)

			var expected []byte
			expected, err = hex.DecodeString(tt.expectedHex)
			require.NoError(t, err)

			result := DecompressPublicKey(compressed)
			require.Equal(t, expected, result)
		})
	}
}
