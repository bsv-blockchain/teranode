package bitcoin

import (
	"encoding/hex"
	"testing"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/services/utxopersister"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDeserializeBlockIndex_WithRecords tests the DeserializeBlockIndex function with various block index records.
func TestDeserializeBlockIndex_WithRecords(t *testing.T) {
	tests := []struct {
		name           string
		data           string
		expectedLen    int
		expectedErr    error
		expectedHeight uint32
		expectedHash   string
	}{
		{
			name:           "Record138",
			data:           "af93d20081cb40801d010083efd05df7986401000000ce5fc77cd8545040bd2b91cd363f4239f1f1e617b90cbcd490daa338000000006915b0d5b08e51b52984b2a3c6f9dfda643ab777f45d2552075884b45970da490fca864be5b3431c95fbd220875f003cb21695ed87f371b73befbe7317ee2f3ad5daf02f858e74f5fd6714e4d700000000000000",
			expectedLen:    138,
			expectedErr:    nil,
			expectedHeight: 42560,
			expectedHash:   "000000002c4f382be70290c5bf02dac879a73ab3e0e6f7ececbf7858ebd14500",
		},
		{
			name:        "Record89",
			data:        "af949350a1c57b020000000020af7148cbd10eff33f8ceb37c4428e57a9a33553d14a0bd04000000000000000065ee2a3525933e0c8c0a983f8d1c02b80c208d34d56af07d648bc20173878cb99491445ca0240518013d1e83",
			expectedLen: 89,
			expectedErr: errors.ErrBlockInvalid,
		},
		{
			name:           "Record141",
			data:           "af93d20093a702801d81600d83b3bfa15eceb3a51602000000fb759231e1fa5f80c3508e3a59ebf301930257d04aa492070000000000000000c11c6bc67af8264be7979db45043f5f5c1e8d2060082af4ce7957658a22147e30bf97f54747b1b187d1eac41788ec3b45e0355ee249249a7bf92ca0216f690585e53c92e76032bc82d20db6ed7f6020000000000",
			expectedLen:    141,
			expectedErr:    nil,
			expectedHeight: 332802,
			expectedHash:   "0000000000000000169cdec8dcfa2e408f59e0d50b1a228f65d8f5480f990000",
		},
		{
			name:           "Record137",
			data:           "af93d200c931801d01008085e93798990801000000d71b1aa9127ab8461b8242f171ee9b4700bc3e43b46452340819bfa9000000002b0213be2b6c3a42f870474bd4e20ac11381ab97168090303eff112fd0760760d52dd449ffff001d065603e5cc0326252ac99205d7f4e2b2d5ab9a44600b5b2b0b0bb32080ed5a1400b07649d800000000000000",
			expectedLen:    137,
			expectedErr:    nil,
			expectedHeight: 9521,
			expectedHash:   "00000000659e1402f1cdf3d2fd94065369e5cde43d6a00b6b5edcff01db50000",
		},
		{
			name:           "Record859490",
			data:           "af949350b3b962801d57ad7d85e6dcce3cfacd824e00e02332ac37fcce8ebbc48273c7782e3cd56dc63a3208eeabb443040000000000000000aee57815cf86c83c0df538b6557bcadd92d9bf830e5c1a9ba37032c825ee55fb40e3cd66924b0d18206119a089e31014c908b6913f6926207f8cbd07351d0be948c76a459f8d00ab9b24bcf58eac000000000000",
			expectedLen:    141,
			expectedErr:    nil,
			expectedHeight: 859490,
			expectedHash:   "0000000000000000093d722ea52a23d599f1cacad9498dcab07d800cc4509054",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := hex.DecodeString(tt.data)
			require.NoError(t, err)

			assert.Len(t, b, tt.expectedLen)

			var bi *utxopersister.BlockIndex
			bi, err = DeserializeBlockIndex(b)
			// Ensure only one cuddle assignment before the if statement
			if tt.expectedErr != nil {
				assert.ErrorIs(t, err, tt.expectedErr)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedHeight, bi.Height)
				assert.Equal(t, tt.expectedHash, bi.BlockHeader.String())
			}
		})
	}
}

// encodeVarIntForIndex encodes an integer into a variable-length format used by Bitcoin's index.
func encodeVarIntForIndex(n int) []byte {
	// Produces the var‑int encoding used by Bitcoin's index (reverse of DecodeVarIntForIndex)
	if n == 0 {
		return []byte{0x00}
	}

	var tmp []byte

	val := n

	for {
		b := byte(val & 0x7f)
		tmp = append(tmp, b)
		val >>= 7

		if val == 0 {
			break
		}

		val -= 1
	}

	// Reverse for MSB‑first order and set continuation bits
	encoded := make([]byte, len(tmp))

	for i := 0; i < len(tmp); i++ {
		j := len(tmp) - 1 - i
		encoded[i] = tmp[j]

		if i < len(tmp)-1 { // all but last need a continuation flag
			encoded[i] |= 0x80
		}
	}

	return encoded
}

// TestDecodeVarIntForIndex tests the DecodeVarIntForIndex function with various inputs.
func TestDecodeVarIntForIndex(t *testing.T) {
	tests := []struct {
		name  string
		value int
	}{
		{name: "zero", value: 0},
		{name: "small 127", value: 127},
		{name: "exact 128", value: 128},
		{name: "larger 256", value: 256},
		{name: "big 500000", value: 500000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := encodeVarIntForIndex(tt.value)
			decoded, bytesRead := DecodeVarIntForIndex(encoded)
			require.Equal(t, tt.value, decoded)
			require.Equal(t, len(encoded), bytesRead)
		})
	}
}

// TestIntToLittleEndianBytes tests the IntToLittleEndianBytes function with various integer types.
func TestIntToLittleEndianBytes(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected []byte
	}{
		{name: "uint8", input: uint8(0xAB), expected: []byte{0xAB}},
		{name: "uint16", input: uint16(0xBEEF), expected: []byte{0xEF, 0xBE}},
		{name: "uint32", input: uint32(0xDEADBEEF), expected: []byte{0xEF, 0xBE, 0xAD, 0xDE}},
		{name: "uint64", input: uint64(0x1122334455667788), expected: []byte{0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11}},
		{name: "int32", input: int32(0x7F010203), expected: []byte{0x03, 0x02, 0x01, 0x7F}},
		{name: "int64", input: int64(0x0102030405060708), expected: []byte{0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IntToLittleEndianBytes(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}

	t.Run("unsupported type panics", func(t *testing.T) {
		require.Panics(t, func() {
			_ = IntToLittleEndianBytes(float32(1.23))
		})
	})
}

// TestDeserializeFileIndex tests the DeserializeFileIndex function to ensure it correctly deserializes a FileIndex from a byte slice.
func TestDeserializeFileIndex(t *testing.T) {
	expected := &FileIndex{
		NBlocks:      600,
		NSize:        2048,
		NUndoSize:    1024,
		NHeightFirst: 700000,
		NHeightLast:  700599,
		NTimeFirst:   1700000000,
		NTimeLast:    1700000599,
	}

	// Build serialized data
	parts := [][]byte{
		encodeVarIntForIndex(expected.NBlocks),
		encodeVarIntForIndex(expected.NSize),
		encodeVarIntForIndex(expected.NUndoSize),
		encodeVarIntForIndex(expected.NHeightFirst),
		encodeVarIntForIndex(expected.NHeightLast),
		encodeVarIntForIndex(expected.NTimeFirst),
		encodeVarIntForIndex(expected.NTimeLast),
	}

	var data []byte
	for _, p := range parts {
		data = append(data, p...)
	}

	result := DeserializeFileIndex(data)
	require.Equal(t, expected, result)
}
