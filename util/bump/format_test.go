package bump

import (
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/util/merkleproof"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConvertToBUMP(t *testing.T) {
	t.Run("valid merkle proof conversion", func(t *testing.T) {
		// Create test hashes
		txHash, err := chainhash.NewHashFromStr("abc1234567890123456789012345678901234567890123456789012345678901")
		require.NoError(t, err)

		blockHash, err := chainhash.NewHashFromStr("def4567890123456789012345678901234567890123456789012345678901234")
		require.NoError(t, err)

		merkleRoot, err := chainhash.NewHashFromStr("fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321")
		require.NoError(t, err)

		subtreeRoot, err := chainhash.NewHashFromStr("111222333444555666777888999aaabbbcccdddeeefffaaabbbcccdddeeeaaa")
		require.NoError(t, err)

		sibling1, err := chainhash.NewHashFromStr("aaa1111222233334444555566667777888899990000aaaabbbbccccddddeee")
		require.NoError(t, err)

		sibling2, err := chainhash.NewHashFromStr("bbb2222333344445555666677778888999900001111bbbbccccddddeeeeaaa")
		require.NoError(t, err)

		blockSibling, err := chainhash.NewHashFromStr("ccc3333444455556666777788889999000011112222ccccddddeeeeaaaaabb")
		require.NoError(t, err)

		// Create test merkle proof
		proof := &merkleproof.MerkleProof{
			TxID:             *txHash,
			BlockHash:        *blockHash,
			BlockHeight:      12345,
			MerkleRoot:       *merkleRoot,
			SubtreeIndex:     2,
			TxIndexInSubtree: 5,
			SubtreeRoot:      *subtreeRoot,
			SubtreeProof:     []chainhash.Hash{*sibling1, *sibling2},
			BlockProof:       []chainhash.Hash{*blockSibling},
			Flags:            []int{0, 1, 1, 0},
		}

		// Convert to BUMP format
		bump, err := ConvertToBUMP(proof)
		assert.NoError(t, err)
		assert.NotNil(t, bump)

		// Verify basic structure
		assert.Equal(t, uint32(12345), bump.BlockHeight)
		assert.Equal(t, 3, len(bump.Path)) // 2 subtree levels + 1 block level

		// Verify first subtree level (level 0)
		level0 := bump.Path[0]
		assert.Equal(t, 1, len(level0))
		assert.Equal(t, uint32(4), level0[0].Offset) // 5 XOR 1 = 4 (sibling of index 5)
		assert.Equal(t, sibling1.String(), level0[0].Hash)

		// Verify second subtree level (level 1)
		level1 := bump.Path[1]
		assert.Equal(t, 1, len(level1))
		assert.Equal(t, uint32(3), level1[0].Offset) // (5>>1) XOR 1 = 2 XOR 1 = 3
		assert.Equal(t, sibling2.String(), level1[0].Hash)

		// Verify block level
		blockLevel := bump.Path[2]
		assert.Equal(t, 1, len(blockLevel))
		assert.Equal(t, uint32(3), blockLevel[0].Offset) // 2 XOR 1 = 3
		assert.Equal(t, blockSibling.String(), blockLevel[0].Hash)
	})

	t.Run("nil proof returns error", func(t *testing.T) {
		bump, err := ConvertToBUMP(nil)
		assert.Error(t, err)
		assert.Nil(t, bump)
		assert.Contains(t, err.Error(), "proof cannot be nil")
	})

	t.Run("empty proof paths", func(t *testing.T) {
		txHash, _ := chainhash.NewHashFromStr("abc1234567890123456789012345678901234567890123456789012345678901")
		blockHash, _ := chainhash.NewHashFromStr("def4567890123456789012345678901234567890123456789012345678901234")

		proof := &merkleproof.MerkleProof{
			TxID:             *txHash,
			BlockHash:        *blockHash,
			BlockHeight:      100,
			SubtreeIndex:     0,
			TxIndexInSubtree: 0,
			SubtreeProof:     []chainhash.Hash{}, // Empty
			BlockProof:       []chainhash.Hash{}, // Empty
		}

		bump, err := ConvertToBUMP(proof)
		assert.NoError(t, err)
		assert.NotNil(t, bump)
		assert.Equal(t, uint32(100), bump.BlockHeight)
		assert.Equal(t, 0, len(bump.Path))
	})
}

func TestBUMPFormat_EncodeBinary(t *testing.T) {
	t.Run("basic binary encoding", func(t *testing.T) {
		bump := &Format{
			BlockHeight: 12345,
			Path: []Level{
				{
					{
						Offset: 4,
						Hash:   "aaa11112222333344445555666677778888999900000aaaabbbbccccddddeeee",
					},
				},
				{
					{
						Offset: 3,
						Hash:   "bbb22223333444455556666777788889999000011111bbbbccccddddeeeeffff",
					},
				},
			},
		}

		binary, err := bump.EncodeBinary()
		assert.NoError(t, err)
		assert.NotEmpty(t, binary)

		// Verify the binary starts with block height as VarInt
		// 12345 (0x3039) should be encoded as 0xFD 0x39 0x30 (little-endian)
		assert.Equal(t, byte(0xFD), binary[0]) // VarInt prefix for 16-bit
		assert.Equal(t, byte(0x39), binary[1]) // Low byte of 12345
		assert.Equal(t, byte(0x30), binary[2]) // High byte of 12345

		// Verify tree height
		assert.Equal(t, byte(2), binary[3]) // 2 levels
	})

	t.Run("with duplicate flag", func(t *testing.T) {
		bump := &Format{
			BlockHeight: 100,
			Path: []Level{
				{
					{
						Offset:    1,
						Duplicate: true,
					},
				},
			},
		}

		binary, err := bump.EncodeBinary()
		assert.NoError(t, err)
		assert.NotEmpty(t, binary)

		// Should contain the duplicate flag (0x01) and no hash data
		flagPosition := 4 // After block height (1 byte) + tree height (1 byte) + level count (1 byte) + offset (1 byte)
		assert.Equal(t, byte(0x01), binary[flagPosition])
	})

	t.Run("with txid flag", func(t *testing.T) {
		bump := &Format{
			BlockHeight: 50,
			Path: []Level{
				{
					{
						Offset: 2,
						Hash:   "abc1234567890123456789012345678901234567890123456789012345678901",
						TxID:   true,
					},
				},
			},
		}

		binary, err := bump.EncodeBinary()
		assert.NoError(t, err)
		assert.NotEmpty(t, binary)

		// Should contain the txid flag (0x02) followed by 32 bytes of hash
		flagPosition := 4 // After block height (1 byte) + tree height (1 byte) + level count (1 byte) + offset (1 byte)
		assert.Equal(t, byte(0x02), binary[flagPosition])

		// Should be followed by 32 bytes of hash data
		assert.Equal(t, 37, len(binary)) // 1 byte blockHeight + 1 byte treeHeight + 1 byte levelCount + 1 byte offset + 1 byte flag + 32 bytes hash
	})

	t.Run("invalid hash should return error", func(t *testing.T) {
		bump := &Format{
			BlockHeight: 100,
			Path: []Level{
				{
					{
						Offset: 1,
						Hash:   "invalid-hex",
					},
				},
			},
		}

		_, err := bump.EncodeBinary()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid hash hex")
	})

	t.Run("wrong hash length should return error", func(t *testing.T) {
		bump := &Format{
			BlockHeight: 100,
			Path: []Level{
				{
					{
						Offset: 1,
						Hash:   "abc123", // Too short
					},
				},
			},
		}

		_, err := bump.EncodeBinary()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "hash must be 32 bytes")
	})
}

func TestBUMPFormat_EncodeHex(t *testing.T) {
	t.Run("valid hex encoding", func(t *testing.T) {
		bump := &Format{
			BlockHeight: 255,
			Path: []Level{
				{
					{
						Offset: 1,
						Hash:   "abc1234567890123456789012345678901234567890123456789012345678901",
					},
				},
			},
		}

		hexStr, err := bump.EncodeHex()
		assert.NoError(t, err)
		assert.NotEmpty(t, hexStr)

		// Should be valid hex
		_, err = hex.DecodeString(hexStr)
		assert.NoError(t, err)

		// Should start with block height encoding
		assert.True(t, len(hexStr) >= 4) // At least 2 hex chars for basic structure
	})
}

func TestValidateBUMP(t *testing.T) {
	t.Run("valid BUMP structure", func(t *testing.T) {
		bump := &Format{
			BlockHeight: 12345,
			Path: []Level{
				{
					{
						Offset: 4,
						Hash:   "aaa11112222333344445555666677778888999900000aaaabbbbccccddddeeee",
					},
				},
			},
		}

		err := Validate(bump)
		assert.NoError(t, err)
	})

	t.Run("nil BUMP returns error", func(t *testing.T) {
		err := Validate(nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "BUMP structure cannot be nil")
	})

	t.Run("empty path returns error", func(t *testing.T) {
		bump := &Format{
			BlockHeight: 100,
			Path:        []Level{},
		}

		err := Validate(bump)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "BUMP path cannot be empty")
	})

	t.Run("too many levels returns error", func(t *testing.T) {
		// Create 65 levels (over the limit of 64)
		path := make([]Level, 65)
		for i := range path {
			path[i] = Level{
				{
					Offset: uint32(i),
					Hash:   "abc1234567890123456789012345678901234567890123456789012345678901",
				},
			}
		}

		bump := &Format{
			BlockHeight: 100,
			Path:        path,
		}

		err := Validate(bump)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "BUMP path too long")
	})

	t.Run("empty level returns error", func(t *testing.T) {
		bump := &Format{
			BlockHeight: 100,
			Path: []Level{
				{}, // Empty level
			},
		}

		err := Validate(bump)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "level 0 cannot be empty")
	})

	t.Run("invalid hash length returns error", func(t *testing.T) {
		bump := &Format{
			BlockHeight: 100,
			Path: []Level{
				{
					{
						Offset: 1,
						Hash:   "abc123", // Too short
					},
				},
			},
		}

		err := Validate(bump)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid hash length")
	})

	t.Run("invalid hash hex returns error", func(t *testing.T) {
		bump := &Format{
			BlockHeight: 100,
			Path: []Level{
				{
					{
						Offset: 1,
						Hash:   "xyz1234567890123456789012345678901234567890123456789012345678901", // Invalid hex
					},
				},
			},
		}

		err := Validate(bump)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid hash hex")
	})

	t.Run("duplicate with hash returns error", func(t *testing.T) {
		bump := &Format{
			BlockHeight: 100,
			Path: []Level{
				{
					{
						Offset:    1,
						Hash:      "abc1234567890123456789012345678901234567890123456789012345678901",
						Duplicate: true, // Should not be combined with hash
					},
				},
			},
		}

		err := Validate(bump)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate flag cannot be combined with hash")
	})
}

func TestBUMPFormat_JSONSerialization(t *testing.T) {
	t.Run("JSON marshaling and unmarshaling", func(t *testing.T) {
		original := &Format{
			BlockHeight: 12345,
			Path: []Level{
				{
					{
						Offset: 4,
						Hash:   "aaa11112222333344445555666677778888999900000aaaabbbbccccddddeeee",
					},
				},
				{
					{
						Offset: 3,
						Hash:   "bbb22223333444455556666777788889999000011111bbbbccccddddeeeeffff",
					},
				},
			},
		}

		// Marshal to JSON
		jsonData, err := json.Marshal(original)
		assert.NoError(t, err)

		// Unmarshal back
		var restored Format
		err = json.Unmarshal(jsonData, &restored)
		assert.NoError(t, err)

		// Compare
		assert.Equal(t, original.BlockHeight, restored.BlockHeight)
		assert.Equal(t, len(original.Path), len(restored.Path))

		for i, level := range original.Path {
			assert.Equal(t, len(level), len(restored.Path[i]))
			for j, node := range level {
				assert.Equal(t, node.Offset, restored.Path[i][j].Offset)
				assert.Equal(t, node.Hash, restored.Path[i][j].Hash)
				assert.Equal(t, node.TxID, restored.Path[i][j].TxID)
				assert.Equal(t, node.Duplicate, restored.Path[i][j].Duplicate)
			}
		}
	})

	t.Run("JSON with optional fields", func(t *testing.T) {
		bump := &Format{
			BlockHeight: 100,
			Path: []Level{
				{
					{
						Offset: 1,
						Hash:   "abc1234567890123456789012345678901234567890123456789012345678901",
						TxID:   true,
					},
					{
						Offset:    2,
						Duplicate: true,
					},
				},
			},
		}

		jsonData, err := json.Marshal(bump)
		assert.NoError(t, err)

		// Verify JSON contains expected fields
		jsonStr := string(jsonData)
		assert.Contains(t, jsonStr, "\"txid\":true")
		assert.Contains(t, jsonStr, "\"duplicate\":true")
		assert.Contains(t, jsonStr, "\"blockHeight\":100")
	})
}
