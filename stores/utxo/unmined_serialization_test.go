package utxo

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSerializeDeserialize_Roundtrip(t *testing.T) {
	// Create a hash for testing
	hash, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	require.NoError(t, err)

	parentHash, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000002")
	require.NoError(t, err)

	txInpoints := &subtree.TxInpoints{
		ParentTxHashes: []chainhash.Hash{*parentHash},
		Idxs:           [][]uint32{{0}}, // Must match ParentTxHashes length
	}

	original := &UnminedTransaction{
		Node: &subtree.Node{
			Hash:        *hash,
			Fee:         12345,
			SizeInBytes: 250,
		},
		TxInpoints:   txInpoints,
		CreatedAt:    1700000000,
		Locked:       true,
		Skip:         false,
		UnminedSince: 1000,
		BlockIDs:     []uint32{100, 200, 300},
	}

	// Serialize
	data, err := SerializeUnminedTransaction(original)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	// Deserialize
	result, err := DeserializeUnminedTransaction(data)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Verify all fields
	assert.Equal(t, original.Node.Hash, result.Node.Hash)
	assert.Equal(t, original.Node.Fee, result.Node.Fee)
	assert.Equal(t, original.Node.SizeInBytes, result.Node.SizeInBytes)
	assert.Equal(t, original.CreatedAt, result.CreatedAt)
	assert.Equal(t, original.Locked, result.Locked)
	assert.Equal(t, original.Skip, result.Skip)
	assert.Equal(t, original.UnminedSince, result.UnminedSince)
	assert.Equal(t, original.BlockIDs, result.BlockIDs)
	assert.NotNil(t, result.TxInpoints)
	assert.Equal(t, len(original.TxInpoints.ParentTxHashes), len(result.TxInpoints.ParentTxHashes))
	assert.Equal(t, original.TxInpoints.ParentTxHashes[0], result.TxInpoints.ParentTxHashes[0])
}

func TestSerializeDeserialize_EmptyBlockIDs(t *testing.T) {
	hash, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000003")
	require.NoError(t, err)

	original := &UnminedTransaction{
		Node: &subtree.Node{
			Hash:        *hash,
			Fee:         100,
			SizeInBytes: 200,
		},
		TxInpoints:   nil,
		CreatedAt:    12345,
		Locked:       false,
		Skip:         true,
		UnminedSince: 500,
		BlockIDs:     []uint32{},
	}

	data, err := SerializeUnminedTransaction(original)
	require.NoError(t, err)

	result, err := DeserializeUnminedTransaction(data)
	require.NoError(t, err)

	assert.Equal(t, original.Node.Hash, result.Node.Hash)
	assert.Equal(t, original.Skip, result.Skip)
	assert.Empty(t, result.BlockIDs)
}

func TestSerializeDeserialize_Flags(t *testing.T) {
	hash, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000004")
	require.NoError(t, err)

	testCases := []struct {
		name   string
		locked bool
		skip   bool
	}{
		{"neither", false, false},
		{"locked_only", true, false},
		{"skip_only", false, true},
		{"both", true, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			original := &UnminedTransaction{
				Node: &subtree.Node{
					Hash: *hash,
				},
				Locked:   tc.locked,
				Skip:     tc.skip,
				BlockIDs: []uint32{},
			}

			data, err := SerializeUnminedTransaction(original)
			require.NoError(t, err)

			result, err := DeserializeUnminedTransaction(data)
			require.NoError(t, err)

			assert.Equal(t, tc.locked, result.Locked)
			assert.Equal(t, tc.skip, result.Skip)
		})
	}
}

func TestDeserialize_TooShort(t *testing.T) {
	// Data shorter than minimum (61 bytes)
	shortData := make([]byte, 50)

	_, err := DeserializeUnminedTransaction(shortData)
	assert.ErrorIs(t, err, ErrInvalidSerializedData)
}

func TestSerializeDeserialize_LargeBlockIDs(t *testing.T) {
	hash, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000005")
	require.NoError(t, err)

	// Create many block IDs
	blockIDs := make([]uint32, 100)
	for i := range blockIDs {
		blockIDs[i] = uint32(i * 1000)
	}

	original := &UnminedTransaction{
		Node: &subtree.Node{
			Hash:        *hash,
			Fee:         999999,
			SizeInBytes: 888888,
		},
		CreatedAt:    1700000000,
		UnminedSince: 5000,
		BlockIDs:     blockIDs,
	}

	data, err := SerializeUnminedTransaction(original)
	require.NoError(t, err)

	result, err := DeserializeUnminedTransaction(data)
	require.NoError(t, err)

	assert.Equal(t, len(blockIDs), len(result.BlockIDs))
	for i, id := range blockIDs {
		assert.Equal(t, id, result.BlockIDs[i])
	}
}

func TestSerializeDeserialize_MultipleParents(t *testing.T) {
	hash, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000006")
	require.NoError(t, err)

	parent1, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000007")
	require.NoError(t, err)
	parent2, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000008")
	require.NoError(t, err)
	parent3, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000009")
	require.NoError(t, err)

	txInpoints := &subtree.TxInpoints{
		ParentTxHashes: []chainhash.Hash{*parent1, *parent2, *parent3},
		Idxs:           [][]uint32{{0}, {1}, {2}}, // Must match ParentTxHashes length
	}

	original := &UnminedTransaction{
		Node: &subtree.Node{
			Hash: *hash,
		},
		TxInpoints: txInpoints,
		BlockIDs:   []uint32{},
	}

	data, err := SerializeUnminedTransaction(original)
	require.NoError(t, err)

	result, err := DeserializeUnminedTransaction(data)
	require.NoError(t, err)

	require.NotNil(t, result.TxInpoints)
	assert.Len(t, result.TxInpoints.ParentTxHashes, 3)
	assert.Equal(t, *parent1, result.TxInpoints.ParentTxHashes[0])
	assert.Equal(t, *parent2, result.TxInpoints.ParentTxHashes[1])
	assert.Equal(t, *parent3, result.TxInpoints.ParentTxHashes[2])
}
