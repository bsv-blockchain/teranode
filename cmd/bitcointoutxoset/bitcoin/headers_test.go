package bitcoin

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// buildSerializedBlockIndex constructs a serialized block index for testing purposes.
func buildSerializedBlockIndex(height, txs, status int, header []byte) []byte {
	parts := [][]byte{
		encodeVarIntForIndex(0),      // val (unused)
		encodeVarIntForIndex(height), // height
		encodeVarIntForIndex(status), // status
		encodeVarIntForIndex(txs),    // tx count
	}

	// The C++ logic reads extra varInts depending on status bits
	if status&(BlockHaveData|BlockHaveUndo) != 0 {
		parts = append(parts, encodeVarIntForIndex(0)) // file number
	}

	if status&BlockHaveData != 0 {
		parts = append(parts, encodeVarIntForIndex(0)) // data pos
	}

	if status&BlockHaveUndo != 0 {
		parts = append(parts, encodeVarIntForIndex(0)) // undo pos
	}

	var data []byte
	for _, p := range parts {
		data = append(data, p...)
	}

	data = append(data, header...)

	return data
}

// TestDeserializeBlockIndex tests the DeserializeBlockIndex function with a valid serialized block index.
func TestDeserializeBlockIndex(t *testing.T) {
	// Minimal 80‑byte header: version=1 little‑endian, rest zeros.
	header := make([]byte, 80)
	header[0] = 0x01

	// Choose a status that is fully valid and has both data & undo bits set.
	statusValid := (BlockValidTree + 1) | BlockHaveData | BlockHaveUndo

	serialized := buildSerializedBlockIndex(750000, 2048, statusValid, header)

	bi, err := DeserializeBlockIndex(serialized)
	require.NoError(t, err)
	require.Equal(t, uint32(750000), bi.Height)
	require.Equal(t, uint64(2048), bi.TxCount)
	require.NotNil(t, bi.BlockHeader)
}

// TestDeserializeBlockIndexErrors tests various error cases for DeserializeBlockIndex.
func TestDeserializeBlockIndexErrors(t *testing.T) {
	header := make([]byte, 80)
	header[0] = 0x01

	t.Run("invalid status", func(t *testing.T) {
		statusInvalid := BlockValidTree // <= BlockValidTree should fail
		data := buildSerializedBlockIndex(100, 1, statusInvalid, header)
		_, err := DeserializeBlockIndex(data)
		require.Error(t, err)
		require.Contains(t, err.Error(), "not in active chain")
	})

	t.Run("short header", func(t *testing.T) {
		statusValid := (BlockValidTree + 1) | BlockHaveData
		shortHeader := make([]byte, 10) // too short
		data := buildSerializedBlockIndex(100, 1, statusValid, shortHeader)
		_, err := DeserializeBlockIndex(data)
		require.Error(t, err)
		require.Contains(t, err.Error(), "block header length is less than 80")
	})
}
