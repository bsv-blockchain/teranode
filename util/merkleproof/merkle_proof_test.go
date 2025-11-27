package merkleproof

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockMerkleProofConstructor is a mock implementation of MerkleProofConstructor for testing
type MockMerkleProofConstructor struct {
	txMeta      *TxMetaData
	block       *model.Block
	blockHeader *model.BlockHeader
	subtrees    map[string]*subtree.Subtree
}

func (m *MockMerkleProofConstructor) GetTxMeta(txHash *chainhash.Hash) (*TxMetaData, error) {
	return m.txMeta, nil
}

func (m *MockMerkleProofConstructor) GetBlockByID(id uint64) (*model.Block, error) {
	return m.block, nil
}

func (m *MockMerkleProofConstructor) GetBlockHeader(blockHash *chainhash.Hash) (*model.BlockHeader, error) {
	return m.blockHeader, nil
}

func (m *MockMerkleProofConstructor) GetSubtree(subtreeHash *chainhash.Hash) (*subtree.Subtree, error) {
	if st, ok := m.subtrees[subtreeHash.String()]; ok {
		return st, nil
	}
	return nil, nil
}

func (m *MockMerkleProofConstructor) FindBlocksContainingSubtree(subtreeHash *chainhash.Hash) ([]uint32, []uint32, []int, error) {
	// Return mock data for testing
	return []uint32{1}, []uint32{100}, []int{0}, nil
}

func TestConstructMerkleProof(t *testing.T) {
	t.Run("simple transaction in single subtree block", func(t *testing.T) {
		// Create test transaction hashes
		coinbaseHash, _ := chainhash.NewHashFromStr("abc1234567890123456789012345678901234567890123456789012345678901")
		tx1Hash, _ := chainhash.NewHashFromStr("1111111111111111111111111111111111111111111111111111111111111111")

		// Create a subtree with two transactions
		st, err := subtree.NewTreeByLeafCount(2)
		require.NoError(t, err)
		st.Nodes = []subtree.Node{
			{Hash: *coinbaseHash},
			{Hash: *tx1Hash},
		}

		// Calculate subtree root
		subtreeRoot := st.RootHash()

		// Create mock block
		block := &model.Block{
			Subtrees: []*chainhash.Hash{subtreeRoot},
		}

		// Create mock block header
		blockHeader := &model.BlockHeader{
			HashMerkleRoot: subtreeRoot, // For single subtree, merkle root equals subtree root
		}

		// Setup mock repository
		mock := &MockMerkleProofConstructor{
			txMeta: &TxMetaData{
				BlockIDs:     []uint32{1},
				BlockHeights: []uint32{100},
				SubtreeIdxs:  []int{0},
			},
			block:       block,
			blockHeader: blockHeader,
			subtrees: map[string]*subtree.Subtree{
				subtreeRoot.String(): st,
			},
		}

		// Construct proof for tx1
		proof, err := ConstructMerkleProof(tx1Hash, mock)
		require.NoError(t, err)
		require.NotNil(t, proof)

		// Verify proof structure
		assert.Equal(t, *tx1Hash, proof.TxID)
		assert.Equal(t, uint32(100), proof.BlockHeight)
		assert.Equal(t, 0, proof.SubtreeIndex)
		assert.Equal(t, 1, proof.TxIndexInSubtree)
		assert.Equal(t, *subtreeRoot, proof.SubtreeRoot)
		assert.Equal(t, *subtreeRoot, proof.MerkleRoot)

		// Should have one hash in subtree proof (the coinbase)
		assert.Len(t, proof.SubtreeProof, 1)
		assert.Equal(t, *coinbaseHash, proof.SubtreeProof[0])

		// Should have no block proof for single subtree
		assert.Len(t, proof.BlockProof, 0)
	})

	t.Run("transaction in multiple subtree block", func(t *testing.T) {
		// Create test transaction hashes
		coinbaseHash, _ := chainhash.NewHashFromStr("abc1234567890123456789012345678901234567890123456789012345678901")
		tx1Hash, _ := chainhash.NewHashFromStr("1111111111111111111111111111111111111111111111111111111111111111")
		tx2Hash, _ := chainhash.NewHashFromStr("2222222222222222222222222222222222222222222222222222222222222222")
		tx3Hash, _ := chainhash.NewHashFromStr("3333333333333333333333333333333333333333333333333333333333333333")

		// Create first subtree with coinbase and tx1
		st1, err := subtree.NewTreeByLeafCount(2)
		require.NoError(t, err)
		st1.Nodes = []subtree.Node{
			{Hash: *coinbaseHash},
			{Hash: *tx1Hash},
		}
		subtreeRoot1 := st1.RootHash()

		// Create second subtree with tx2 and tx3
		st2, err := subtree.NewTreeByLeafCount(2)
		require.NoError(t, err)
		st2.Nodes = []subtree.Node{
			{Hash: *tx2Hash},
			{Hash: *tx3Hash},
		}
		subtreeRoot2 := st2.RootHash()

		// Calculate block merkle root from two subtrees
		combined := append(subtreeRoot1.CloneBytes(), subtreeRoot2.CloneBytes()...)
		merkleRoot := chainhash.DoubleHashH(combined)

		// Create mock block
		block := &model.Block{
			Subtrees: []*chainhash.Hash{subtreeRoot1, subtreeRoot2},
		}

		// Create mock block header
		blockHeader := &model.BlockHeader{
			HashMerkleRoot: &merkleRoot,
		}

		// Setup mock repository for tx2
		mock := &MockMerkleProofConstructor{
			txMeta: &TxMetaData{
				BlockIDs:     []uint32{1},
				BlockHeights: []uint32{100},
				SubtreeIdxs:  []int{1}, // tx2 is in second subtree
			},
			block:       block,
			blockHeader: blockHeader,
			subtrees: map[string]*subtree.Subtree{
				subtreeRoot1.String(): st1,
				subtreeRoot2.String(): st2,
			},
		}

		// Construct proof for tx2
		proof, err := ConstructMerkleProof(tx2Hash, mock)
		require.NoError(t, err)
		require.NotNil(t, proof)

		// Verify proof structure
		assert.Equal(t, *tx2Hash, proof.TxID)
		assert.Equal(t, uint32(100), proof.BlockHeight)
		assert.Equal(t, 1, proof.SubtreeIndex)
		assert.Equal(t, 0, proof.TxIndexInSubtree)
		assert.Equal(t, *subtreeRoot2, proof.SubtreeRoot)
		assert.Equal(t, merkleRoot, proof.MerkleRoot)

		// Should have one hash in subtree proof (tx3)
		assert.Len(t, proof.SubtreeProof, 1)
		assert.Equal(t, *tx3Hash, proof.SubtreeProof[0])

		// Should have one hash in block proof (subtreeRoot1)
		assert.Len(t, proof.BlockProof, 1)
		assert.Equal(t, *subtreeRoot1, proof.BlockProof[0])
	})

	t.Run("coinbase transaction proof", func(t *testing.T) {
		// Create test transaction hashes
		coinbaseHash, _ := chainhash.NewHashFromStr("abc1234567890123456789012345678901234567890123456789012345678901")
		tx1Hash, _ := chainhash.NewHashFromStr("1111111111111111111111111111111111111111111111111111111111111111")

		// Create a subtree with two transactions
		st, err := subtree.NewTreeByLeafCount(2)
		require.NoError(t, err)
		st.Nodes = []subtree.Node{
			{Hash: *coinbaseHash},
			{Hash: *tx1Hash},
		}

		// Calculate subtree root
		subtreeRoot := st.RootHash()

		// Create mock block
		block := &model.Block{
			Subtrees: []*chainhash.Hash{subtreeRoot},
		}

		// Create mock block header
		blockHeader := &model.BlockHeader{
			HashMerkleRoot: subtreeRoot,
		}

		// Setup mock repository for coinbase
		mock := &MockMerkleProofConstructor{
			txMeta: &TxMetaData{
				BlockIDs:     []uint32{1},
				BlockHeights: []uint32{100},
				SubtreeIdxs:  []int{0},
			},
			block:       block,
			blockHeader: blockHeader,
			subtrees: map[string]*subtree.Subtree{
				subtreeRoot.String(): st,
			},
		}

		// Construct proof for coinbase
		proof, err := ConstructMerkleProof(coinbaseHash, mock)
		require.NoError(t, err)
		require.NotNil(t, proof)

		// Verify proof structure
		assert.Equal(t, *coinbaseHash, proof.TxID)
		assert.Equal(t, 0, proof.SubtreeIndex)
		assert.Equal(t, 0, proof.TxIndexInSubtree) // Coinbase is always at position 0

		// Should have one hash in subtree proof (tx1)
		assert.Len(t, proof.SubtreeProof, 1)
		assert.Equal(t, *tx1Hash, proof.SubtreeProof[0])
	})
}

func TestVerifyMerkleProof(t *testing.T) {
	t.Run("verify valid proof for single subtree", func(t *testing.T) {
		// Create test transaction hashes
		coinbaseHash, _ := chainhash.NewHashFromStr("abc1234567890123456789012345678901234567890123456789012345678901")
		tx1Hash, _ := chainhash.NewHashFromStr("1111111111111111111111111111111111111111111111111111111111111111")

		// Calculate subtree root manually
		combined := append(coinbaseHash.CloneBytes(), tx1Hash.CloneBytes()...)
		subtreeRoot := chainhash.DoubleHashH(combined)

		// Create proof for tx1
		blockHash, _ := chainhash.NewHashFromStr("deadbeef000000000000000000000000000000000000000000000000000000")
		proof := &MerkleProof{
			TxID:             *tx1Hash,
			BlockHash:        *blockHash,
			BlockHeight:      100,
			MerkleRoot:       subtreeRoot,
			SubtreeIndex:     0,
			TxIndexInSubtree: 1,
			SubtreeRoot:      subtreeRoot,
			SubtreeProof:     []chainhash.Hash{*coinbaseHash},
			BlockProof:       []chainhash.Hash{},
			Flags:            []int{0}, // coinbase is on the left
		}

		// Verify the proof
		valid, returnedBlockHash, err := VerifyMerkleProof(proof)
		require.NoError(t, err)
		assert.True(t, valid)
		assert.Equal(t, blockHash, returnedBlockHash)
	})

	t.Run("verify valid proof for multiple subtrees", func(t *testing.T) {
		// Create test transaction hashes
		tx1Hash, _ := chainhash.NewHashFromStr("1111111111111111111111111111111111111111111111111111111111111111")
		tx2Hash, _ := chainhash.NewHashFromStr("2222222222222222222222222222222222222222222222222222222222222222")

		// Calculate first subtree root (single tx)
		subtreeRoot1 := tx1Hash // Single tx in subtree, root is the tx itself

		// Calculate second subtree root (single tx)
		subtreeRoot2 := tx2Hash

		// Calculate block merkle root
		combined := append(subtreeRoot1.CloneBytes(), subtreeRoot2.CloneBytes()...)
		merkleRoot := chainhash.DoubleHashH(combined)

		// Create proof for tx2
		blockHash, _ := chainhash.NewHashFromStr("deadbeef000000000000000000000000000000000000000000000000000000")
		proof := &MerkleProof{
			TxID:             *tx2Hash,
			BlockHash:        *blockHash,
			BlockHeight:      100,
			MerkleRoot:       merkleRoot,
			SubtreeIndex:     1,
			TxIndexInSubtree: 0,
			SubtreeRoot:      *subtreeRoot2,
			SubtreeProof:     []chainhash.Hash{}, // Single tx in subtree, no proof needed
			BlockProof:       []chainhash.Hash{*subtreeRoot1},
			Flags:            []int{0}, // subtreeRoot1 is on the left
		}

		// Verify the proof
		valid, returnedBlockHash, err := VerifyMerkleProof(proof)
		require.NoError(t, err)
		assert.True(t, valid)
		assert.Equal(t, blockHash, returnedBlockHash)
	})

	t.Run("verify invalid proof - wrong merkle root", func(t *testing.T) {
		// Create test transaction hashes
		tx1Hash, _ := chainhash.NewHashFromStr("1111111111111111111111111111111111111111111111111111111111111111")
		wrongHash, _ := chainhash.NewHashFromStr("9999999999999999999999999999999999999999999999999999999999999999")

		// Create proof with wrong merkle root
		blockHash, _ := chainhash.NewHashFromStr("deadbeef000000000000000000000000000000000000000000000000000000")
		proof := &MerkleProof{
			TxID:             *tx1Hash,
			BlockHash:        *blockHash,
			BlockHeight:      100,
			MerkleRoot:       *wrongHash, // Wrong merkle root
			SubtreeIndex:     0,
			TxIndexInSubtree: 0,
			SubtreeRoot:      *tx1Hash,
			SubtreeProof:     []chainhash.Hash{},
			BlockProof:       []chainhash.Hash{},
			Flags:            []int{},
		}

		// Verify the proof - should fail
		valid, returnedBlockHash, err := VerifyMerkleProof(proof)
		require.NoError(t, err)
		assert.False(t, valid)
		assert.Nil(t, returnedBlockHash)
	})

	t.Run("verify coinbase proof", func(t *testing.T) {
		// Create test transaction hashes
		coinbaseHash, _ := chainhash.NewHashFromStr("abc1234567890123456789012345678901234567890123456789012345678901")
		tx1Hash, _ := chainhash.NewHashFromStr("1111111111111111111111111111111111111111111111111111111111111111")

		// Calculate subtree root manually
		combined := append(coinbaseHash.CloneBytes(), tx1Hash.CloneBytes()...)
		subtreeRoot := chainhash.DoubleHashH(combined)

		// Create proof for coinbase
		blockHash, _ := chainhash.NewHashFromStr("deadbeef000000000000000000000000000000000000000000000000000000")
		proof := &MerkleProof{
			TxID:             *coinbaseHash,
			BlockHash:        *blockHash,
			BlockHeight:      100,
			MerkleRoot:       subtreeRoot,
			SubtreeIndex:     0,
			TxIndexInSubtree: 0, // Coinbase is always at position 0
			SubtreeRoot:      subtreeRoot,
			SubtreeProof:     []chainhash.Hash{*tx1Hash},
			BlockProof:       []chainhash.Hash{},
			Flags:            []int{1}, // tx1 is on the right
		}

		// Verify the proof using coinbase-specific function
		valid, returnedBlockHash, err := VerifyMerkleProofForCoinbase(proof)
		require.NoError(t, err)
		assert.True(t, valid)
		assert.Equal(t, blockHash, returnedBlockHash)
	})

	t.Run("verify invalid coinbase proof - wrong position", func(t *testing.T) {
		// Create test transaction hashes
		coinbaseHash, _ := chainhash.NewHashFromStr("abc1234567890123456789012345678901234567890123456789012345678901")

		// Create proof with wrong position for coinbase
		blockHash, _ := chainhash.NewHashFromStr("deadbeef000000000000000000000000000000000000000000000000000000")
		proof := &MerkleProof{
			TxID:             *coinbaseHash,
			BlockHash:        *blockHash,
			BlockHeight:      100,
			MerkleRoot:       *coinbaseHash,
			SubtreeIndex:     1, // Wrong! Coinbase must be in subtree 0
			TxIndexInSubtree: 0,
			SubtreeRoot:      *coinbaseHash,
			SubtreeProof:     []chainhash.Hash{},
			BlockProof:       []chainhash.Hash{},
			Flags:            []int{},
		}

		// Verify the proof - should fail
		valid, _, err := VerifyMerkleProofForCoinbase(proof)
		assert.Error(t, err)
		assert.False(t, valid)
		assert.Contains(t, err.Error(), "invalid coinbase position")
	})
}

func TestGenerateBlockMerkleProof(t *testing.T) {
	t.Run("single subtree", func(t *testing.T) {
		subtreeHash, _ := chainhash.NewHashFromStr("1111111111111111111111111111111111111111111111111111111111111111")
		subtrees := []*chainhash.Hash{subtreeHash}

		proof, flags, err := generateBlockMerkleProof(subtrees, 0)
		require.NoError(t, err)

		// Single subtree should have empty proof
		assert.Len(t, proof, 0)
		assert.Len(t, flags, 0)
	})

	t.Run("two subtrees", func(t *testing.T) {
		subtreeHash1, _ := chainhash.NewHashFromStr("1111111111111111111111111111111111111111111111111111111111111111")
		subtreeHash2, _ := chainhash.NewHashFromStr("2222222222222222222222222222222222222222222222222222222222222222")
		subtrees := []*chainhash.Hash{subtreeHash1, subtreeHash2}

		// Get proof for first subtree
		proof, flags, err := generateBlockMerkleProof(subtrees, 0)
		require.NoError(t, err)

		assert.Len(t, proof, 1)
		assert.Equal(t, *subtreeHash2, *proof[0])
		assert.Len(t, flags, 1)
		assert.Equal(t, 1, flags[0]) // subtreeHash2 is on the right

		// Get proof for second subtree
		proof, flags, err = generateBlockMerkleProof(subtrees, 1)
		require.NoError(t, err)

		assert.Len(t, proof, 1)
		assert.Equal(t, *subtreeHash1, *proof[0])
		assert.Len(t, flags, 1)
		assert.Equal(t, 0, flags[0]) // subtreeHash1 is on the left
	})

	t.Run("four subtrees", func(t *testing.T) {
		subtreeHash1, _ := chainhash.NewHashFromStr("1111111111111111111111111111111111111111111111111111111111111111")
		subtreeHash2, _ := chainhash.NewHashFromStr("2222222222222222222222222222222222222222222222222222222222222222")
		subtreeHash3, _ := chainhash.NewHashFromStr("3333333333333333333333333333333333333333333333333333333333333333")
		subtreeHash4, _ := chainhash.NewHashFromStr("4444444444444444444444444444444444444444444444444444444444444444")
		subtrees := []*chainhash.Hash{subtreeHash1, subtreeHash2, subtreeHash3, subtreeHash4}

		// Get proof for third subtree (index 2)
		proof, flags, err := generateBlockMerkleProof(subtrees, 2)
		require.NoError(t, err)

		// Should have 2 proofs (one at each level)
		assert.Len(t, proof, 2)
		assert.Len(t, flags, 2)

		// First proof should be sibling at same level (subtreeHash4)
		assert.Equal(t, *subtreeHash4, *proof[0])
		assert.Equal(t, 1, flags[0]) // subtreeHash4 is on the right

		// Second proof should be the hash of the first pair
		combined := append(subtreeHash1.CloneBytes(), subtreeHash2.CloneBytes()...)
		expectedHash := chainhash.DoubleHashH(combined)
		assert.Equal(t, expectedHash, *proof[1])
		assert.Equal(t, 0, flags[1]) // First pair is on the left
	})

	t.Run("odd number of subtrees", func(t *testing.T) {
		subtreeHash1, _ := chainhash.NewHashFromStr("1111111111111111111111111111111111111111111111111111111111111111")
		subtreeHash2, _ := chainhash.NewHashFromStr("2222222222222222222222222222222222222222222222222222222222222222")
		subtreeHash3, _ := chainhash.NewHashFromStr("3333333333333333333333333333333333333333333333333333333333333333")
		subtrees := []*chainhash.Hash{subtreeHash1, subtreeHash2, subtreeHash3}

		// Get proof for third subtree (index 2) - odd node
		proof, flags, err := generateBlockMerkleProof(subtrees, 2)
		require.NoError(t, err)

		// Should have 2 proofs
		assert.Len(t, proof, 2)
		assert.Len(t, flags, 2)

		// First proof should be itself (duplicated for odd node)
		assert.Equal(t, *subtreeHash3, *proof[0])
		assert.Equal(t, 1, flags[0])
	})
}

func TestMerkleProofWithRealTransaction(t *testing.T) {
	t.Run("construct and verify proof for real transaction", func(t *testing.T) {
		// Create simple test transactions using hashes instead of parsing invalid hex
		txHash, _ := chainhash.NewHashFromStr("abc1234567890123456789012345678901234567890123456789012345678901")
		tx2Hash, _ := chainhash.NewHashFromStr("def1234567890123456789012345678901234567890123456789012345678901")

		// Create a subtree with both transactions
		st, err := subtree.NewTreeByLeafCount(2)
		require.NoError(t, err)
		st.Nodes = []subtree.Node{
			{Hash: *txHash},
			{Hash: *tx2Hash},
		}

		subtreeRoot := st.RootHash()

		// Create mock block
		block := &model.Block{
			Subtrees: []*chainhash.Hash{subtreeRoot},
		}

		// Create mock block header
		blockHeader := &model.BlockHeader{
			HashMerkleRoot: subtreeRoot,
		}

		// Setup mock repository
		mock := &MockMerkleProofConstructor{
			txMeta: &TxMetaData{
				BlockIDs:     []uint32{1},
				BlockHeights: []uint32{100},
				SubtreeIdxs:  []int{0},
			},
			block:       block,
			blockHeader: blockHeader,
			subtrees: map[string]*subtree.Subtree{
				subtreeRoot.String(): st,
			},
		}

		// Construct proof
		proof, err := ConstructMerkleProof(txHash, mock)
		require.NoError(t, err)
		require.NotNil(t, proof)

		// Verify the proof
		valid, blockHash, err := VerifyMerkleProof(proof)
		require.NoError(t, err)
		assert.True(t, valid)
		assert.NotNil(t, blockHash)
	})
}
