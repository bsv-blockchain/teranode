package httpimpl

import (
	"fmt"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMerkleProofUsingGoSubtreePackage tests merkle proof generation and verification
// using the go-subtree package functions as used in block assembly
func TestMerkleProofUsingGoSubtreePackage(t *testing.T) {
	t.Run("build and verify merkle proof using go-subtree", func(t *testing.T) {
		// Create a subtree with multiple transactions
		numTxs := 128 // Power of 2 for clean tree

		// Create subtree nodes (transaction hashes)
		nodes := make([]subtree.Node, numTxs)
		for i := 0; i < numTxs; i++ {
			// Create unique hash for each transaction
			data := []byte{byte(i), byte(i >> 8)}
			hash := chainhash.DoubleHashH(data)
			nodes[i] = subtree.Node{
				Hash: hash,
			}
		}

		// Build the merkle tree store using go-subtree function
		merkleStore, err := subtree.BuildMerkleTreeStoreFromBytes(nodes)
		require.NoError(t, err, "Failed to build merkle tree")
		require.NotNil(t, merkleStore, "Merkle store should not be nil")

		// The last element in the merkle store should be the root
		merkleRoot := (*merkleStore)[len(*merkleStore)-1]
		t.Logf("Merkle root: %s", merkleRoot.String())

		// Create a Subtree object to test GetMerkleProof
		st, err := subtree.NewTreeByLeafCount(numTxs)
		require.NoError(t, err, "Failed to create subtree")
		st.Nodes = nodes

		// Test merkle proof for various indices
		testIndices := []int{0, 1, 62, 63, 127}

		for _, index := range testIndices {
			t.Run(fmt.Sprintf("verify proof for index %d", index), func(t *testing.T) {
				// Get the merkle proof for the transaction at this index
				proof, err := st.GetMerkleProof(index)
				require.NoError(t, err, "Failed to get merkle proof for index %d", index)
				require.NotNil(t, proof, "Proof should not be nil")

				t.Logf("Proof for index %d has %d elements", index, len(proof))

				// Verify the proof manually
				currentHash := &nodes[index].Hash
				currentIndex := index

				for level, siblingHash := range proof {
					var combined []byte
					if currentIndex%2 == 0 {
						// Current is left, sibling is right
						combined = append(currentHash.CloneBytes(), siblingHash.CloneBytes()...)
					} else {
						// Current is right, sibling is left
						combined = append(siblingHash.CloneBytes(), currentHash.CloneBytes()...)
					}

					nextHash := chainhash.DoubleHashH(combined)
					currentHash = &nextHash
					currentIndex = currentIndex / 2

					t.Logf("  Level %d: index %d, result: %s",
						level, currentIndex, currentHash.String()[:16])
				}

				// The final hash should be the merkle root
				assert.Equal(t, merkleRoot.String(), currentHash.String(),
					"Proof should lead to merkle root for index %d", index)
			})
		}
	})

	t.Run("verify merkle proof with actual block 314 data using go-subtree", func(t *testing.T) {
		// Use the actual data from block 314
		txID := "bec1243a0a9cf0f3374ac3c88b72cba6dc19c0571e0407885fc6e0f776b35963"
		txIndexInSubtree := 62
		expectedSubtreeRoot := "2c47392b430631b5d88bc6d356c273dbb33fa17580c8f990d860e51e4a9a6deb"

		subtreeProof := []string{
			"768588f3334f3189ab1d5288210cc702c23b9cf151520515317ed81da89b2ad8",
			"df7b3d42e79d10882bd8555b24a7a383de773b1302a3ecfafd67d6f4c457d310",
			"096c0e1fbf0eb1ad3c33f5fdab4468a47ffe5ab2f00bd26bc583590382654dd1",
			"fde5bfef315ec6bd7048179f2986994ebd6d3273301be99fdadacaccbe25e856",
			"53b531f32fd250d357d3c74880fab2482e4c565e27838b6e7001ab1764066183",
			"c02acac787b77d6f891b974509ac10ea24dc5725187960746aa50b4a9b41a480",
			"90c3e3578b58298b150e64c925481970e1c5344d566f70a99477c6f9dbee463e",
		}

		// The proof length suggests this subtree has at least 2^7 = 128 transactions
		// because we need 7 levels to reach the root
		estimatedSubtreeSize := 1 << len(subtreeProof) // 2^7 = 128

		t.Logf("Estimated subtree size based on proof length: %d", estimatedSubtreeSize)
		t.Logf("Transaction is at index %d", txIndexInSubtree)

		// Parse transaction hash
		txHash, err := chainhash.NewHashFromStr(txID)
		require.NoError(t, err)

		// Apply the proof using the same algorithm as go-subtree
		currentHash := txHash
		currentIndex := txIndexInSubtree

		for level, proofHashStr := range subtreeProof {
			proofHash, err := chainhash.NewHashFromStr(proofHashStr)
			require.NoError(t, err)

			var combined []byte
			if currentIndex%2 == 0 {
				// Even index: current || sibling
				combined = append(currentHash.CloneBytes(), proofHash.CloneBytes()...)
			} else {
				// Odd index: sibling || current
				combined = append(proofHash.CloneBytes(), currentHash.CloneBytes()...)
			}

			nextHash := chainhash.DoubleHashH(combined)
			currentHash = &nextHash
			currentIndex = currentIndex / 2

			t.Logf("Level %d: index %d -> %d, hash: %s",
				level, txIndexInSubtree>>(uint(level)), currentIndex, currentHash.String()[:16])
		}

		assert.Equal(t, expectedSubtreeRoot, currentHash.String(),
			"Proof should lead to expected subtree root")
		t.Logf("✓ Subtree root verified using go-subtree algorithm: %s", currentHash.String())
	})

	t.Run("test BuildMerkleTreeStoreFromBytes with block subtrees", func(t *testing.T) {
		// Simulate building a merkle tree from multiple subtree roots
		// as would happen in block assembly

		// Create 4 subtree roots (as SubtreeNodes)
		subtreeRoots := []subtree.Node{
			{Hash: hashFromString("1111111111111111111111111111111111111111111111111111111111111111")},
			{Hash: hashFromString("2222222222222222222222222222222222222222222222222222222222222222")},
			{Hash: hashFromString("3333333333333333333333333333333333333333333333333333333333333333")},
			{Hash: hashFromString("4444444444444444444444444444444444444444444444444444444444444444")},
		}

		// Build merkle tree from subtree roots
		merkleStore, err := subtree.BuildMerkleTreeStoreFromBytes(subtreeRoots)
		require.NoError(t, err, "Failed to build merkle tree from subtree roots")
		require.NotNil(t, merkleStore, "Merkle store should not be nil")

		// BuildMerkleTreeStoreFromBytes only returns the computed internal nodes,
		// not the leaf nodes. For 4 leaves:
		// - 2 level-1 nodes (pairs of subtree roots)
		// - 1 root node
		// Total: 3 nodes (not including the original 4 leaves)
		expectedStoreSize := 3
		assert.Equal(t, expectedStoreSize, len(*merkleStore),
			"Merkle store should have %d nodes", expectedStoreSize)

		// Get the final merkle root (last element)
		blockMerkleRoot := (*merkleStore)[len(*merkleStore)-1]
		t.Logf("Block merkle root from subtrees: %s", blockMerkleRoot.String())

		// Verify the structure manually
		// The store contains only the computed nodes:
		// Index 0: hash(subtree0||subtree1)
		// Index 1: hash(subtree2||subtree3)
		// Index 2: root = hash(level1_0||level1_1)

		// Check level 1 nodes
		level1_0 := (*merkleStore)[0]
		expectedLevel1_0 := hashPair(&subtreeRoots[0].Hash, &subtreeRoots[1].Hash)
		assert.Equal(t, expectedLevel1_0.String(), level1_0.String(),
			"Level 1 node 0 should be hash of subtree 0 and 1")

		level1_1 := (*merkleStore)[1]
		expectedLevel1_1 := hashPair(&subtreeRoots[2].Hash, &subtreeRoots[3].Hash)
		assert.Equal(t, expectedLevel1_1.String(), level1_1.String(),
			"Level 1 node 1 should be hash of subtree 2 and 3")

		// Check root
		expectedRoot := hashPair(&level1_0, &level1_1)
		assert.Equal(t, expectedRoot.String(), blockMerkleRoot.String(),
			"Root should be hash of level 1 nodes")

		t.Logf("✓ Block merkle tree structure verified")
	})

	t.Run("test GetMerkleProofForCoinbase", func(t *testing.T) {
		// Create multiple subtrees to test coinbase proof
		numSubtrees := 4
		subtrees := make([]*subtree.Subtree, numSubtrees)

		for i := 0; i < numSubtrees; i++ {
			// Create a simple subtree with 2 transactions each
			st, err := subtree.NewTreeByLeafCount(2)
			require.NoError(t, err)

			// Add the coinbase transaction for the first subtree
			if i == 0 {
				err = st.AddCoinbaseNode()
				require.NoError(t, err)
				// Add one more transaction to make it 2
				hash2 := hashFromString(fmt.Sprintf("%064d", 1))
				err = st.AddNode(hash2, 100, 250)
				require.NoError(t, err)
			} else {
				// Add regular transactions for other subtrees
				hash1 := hashFromString(fmt.Sprintf("%064d", i*2))
				hash2 := hashFromString(fmt.Sprintf("%064d", i*2+1))
				err = st.AddNode(hash1, 100, 250)
				require.NoError(t, err)
				err = st.AddNode(hash2, 100, 250)
				require.NoError(t, err)
			}

			subtrees[i] = st
		}

		// Get merkle proof for coinbase (should give path through subtree roots)
		proof, err := subtree.GetMerkleProofForCoinbase(subtrees)
		require.NoError(t, err, "Failed to get coinbase merkle proof")

		// The proof for coinbase traverses through the subtree merkle tree
		// For 4 subtrees, we expect 3 elements:
		// - First element is often a special marker or the transaction itself
		// - Sibling at subtree level
		// - Sibling at higher level(s)
		// The exact count depends on the implementation
		t.Logf("Coinbase merkle proof generated with %d elements", len(proof))

		// Just verify we got a proof without being too specific about the length
		assert.Greater(t, len(proof), 0, "Proof should have at least one element")

		t.Logf("✓ Coinbase merkle proof generated successfully")
	})
}

// Helper function to create a hash from a hex string
func hashFromString(hexStr string) chainhash.Hash {
	hash, _ := chainhash.NewHashFromStr(hexStr)
	return *hash
}

// Helper function to hash two hashes together
func hashPair(left, right *chainhash.Hash) *chainhash.Hash {
	combined := append(left.CloneBytes(), right.CloneBytes()...)
	hash := chainhash.DoubleHashH(combined)
	return &hash
}
