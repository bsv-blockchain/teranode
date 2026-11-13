package txmetacache

import (
	"encoding/binary"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// decodeDeleteBatchLikeConsumer mirrors the v1 branch of the subtreevalidation
// txmetaHandler: [count LE32] then per entry [32 hash][1 action][LE32 len][len bytes].
// It returns the hashes of DELETE entries and fails on any other action.
func decodeDeleteBatchLikeConsumer(t *testing.T, data []byte) []chainhash.Hash {
	t.Helper()

	// The consumer only treats a message as v2 when the full header signature
	// matches AND the count fits the buffer at the v2 entry size. A v1 DELETE
	// batch must never satisfy that.
	if len(data) >= WireV2HeaderLen && data[0] == WireV2Magic && data[1] == WireV2Version && data[2] == 0 && data[3] == 0 {
		candidate := binary.LittleEndian.Uint32(data[4:])
		require.Greater(t, uint64(candidate)*uint64(WireV2MinEntrySize), uint64(len(data)-WireV2HeaderLen),
			"v1 delete batch must not be detectable as v2")
	}

	count := binary.LittleEndian.Uint32(data[:4])
	offset := 4

	out := make([]chainhash.Hash, 0, count)

	for i := uint32(0); i < count; i++ {
		var h chainhash.Hash

		copy(h[:], data[offset:offset+32])
		offset += 32

		require.Equal(t, WireActionDELETE, data[offset])
		offset++

		require.Zero(t, binary.LittleEndian.Uint32(data[offset:]), "DELETE carries no content")
		offset += 4

		out = append(out, h)
	}

	require.Equal(t, len(data), offset, "no trailing bytes")

	return out
}

func TestEncodeDeleteBatch(t *testing.T) {
	t.Run("round trips every hash as a DELETE entry", func(t *testing.T) {
		hashes := []chainhash.Hash{
			chainhash.HashH([]byte("a")),
			chainhash.HashH([]byte("b")),
			chainhash.HashH([]byte("c")),
		}

		data := EncodeDeleteBatch(hashes)

		require.Len(t, data, 4+len(hashes)*WireV1MinEntrySize)
		require.Equal(t, hashes, decodeDeleteBatchLikeConsumer(t, data))
	})

	t.Run("255 entries is not mistaken for v2", func(t *testing.T) {
		// count 255 has a little-endian low byte of 0xFF, the v2 magic.
		hashes := make([]chainhash.Hash, 255)
		for i := range hashes {
			hashes[i] = chainhash.HashH([]byte{byte(i), 1})
		}

		require.Equal(t, hashes, decodeDeleteBatchLikeConsumer(t, EncodeDeleteBatch(hashes)))
	})
}
