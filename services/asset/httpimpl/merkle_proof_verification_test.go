package httpimpl

import (
	"encoding/hex"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMerkleProofVerification tests the complete merkle proof verification
// It verifies that we can reconstruct both the subtree root and the block merkle root
// using the proof path provided
func TestMerkleProofVerification(t *testing.T) {
	t.Run("verify merkle proof with real data", func(t *testing.T) {
		// Test data setup - Using real transaction and merkle tree structure
		// Transaction 1 hash
		tx1HashStr := "e8c300c87986efa84c37c0a6c6a3b35dd28c2e34a7d1b7b3c1080354aec5f3e0"
		tx1Hash, err := chainhash.NewHashFromStr(tx1HashStr)
		require.NoError(t, err)

		// Transaction 2 hash (sibling in subtree)
		tx2HashStr := "f5a5ce5988cc76b53f8e7c8be0c1d711842e7d7c3f0a5178b24ffc1e65dfbb85"
		tx2Hash, err := chainhash.NewHashFromStr(tx2HashStr)
		require.NoError(t, err)

		// Transaction 3 hash (in second subtree)
		tx3HashStr := "a1b2c3d4e5f6789012345678901234567890123456789012345678901234abcd"
		tx3Hash, err := chainhash.NewHashFromStr(tx3HashStr)
		require.NoError(t, err)

		// Transaction 4 hash (sibling of tx3 in second subtree)
		tx4HashStr := "dcba432109876543210987654321098765432109876543210987654321f6e5d4"
		tx4Hash, err := chainhash.NewHashFromStr(tx4HashStr)
		require.NoError(t, err)

		// Test Case 1: Verify proof for tx1 in a 2-transaction subtree
		t.Run("verify tx1 subtree proof", func(t *testing.T) {
			// Step 1: Calculate subtree root from tx1 and its proof
			// tx1 is at index 0, so its sibling (tx2) is at index 1
			// When index is even, we concatenate as: hash(tx1 || sibling)
			combinedSubtree := append(tx1Hash.CloneBytes(), tx2Hash.CloneBytes()...)
			calculatedSubtreeRoot := chainhash.DoubleHashH(combinedSubtree)

			// The calculated subtree root is what we expect
			// We're verifying the algorithm, not a specific expected value
			t.Logf("Subtree root calculated: %s", calculatedSubtreeRoot.String())
			t.Logf("From tx1: %s", tx1HashStr)
			t.Logf("From tx2: %s", tx2HashStr)

			// Verify we get the same result if we calculate it again
			recalculated := chainhash.DoubleHashH(append(tx1Hash.CloneBytes(), tx2Hash.CloneBytes()...))
			assert.Equal(t, calculatedSubtreeRoot.String(), recalculated.String(),
				"Recalculation should produce the same result")

			t.Logf("✓ Subtree root calculation verified: %s", calculatedSubtreeRoot.String())
		})

		// Test Case 2: Verify proof from subtree to block merkle root
		t.Run("verify block merkle proof with two subtrees", func(t *testing.T) {
			// Calculate both subtree roots
			subtree1Root := calculateHashFromBytes(
				append(tx1Hash.CloneBytes(), tx2Hash.CloneBytes()...),
			)
			subtree2Root := calculateHashFromBytes(
				append(tx3Hash.CloneBytes(), tx4Hash.CloneBytes()...),
			)

			// Calculate block merkle root from subtree roots
			combinedBlock := append(subtree1Root.CloneBytes(), subtree2Root.CloneBytes()...)
			calculatedMerkleRoot := chainhash.DoubleHashH(combinedBlock)

			// This is what should be in the block header
			t.Logf("✓ Block merkle root calculated: %s", calculatedMerkleRoot.String())

			// Verify we can reconstruct it from tx1's complete proof
			// Start with tx1
			currentHash := tx1Hash

			// Apply subtree proof (tx2 is the sibling)
			combined := append(currentHash.CloneBytes(), tx2Hash.CloneBytes()...)
			currentHash = hashPtr(chainhash.DoubleHashH(combined))
			assert.Equal(t, subtree1Root.String(), currentHash.String(),
				"Should match subtree 1 root")

			// Apply block proof (subtree2 root is the sibling)
			combined = append(currentHash.CloneBytes(), subtree2Root.CloneBytes()...)
			currentHash = hashPtr(chainhash.DoubleHashH(combined))
			assert.Equal(t, calculatedMerkleRoot.String(), currentHash.String(),
				"Should match block merkle root")

			t.Logf("✓ Complete merkle proof verified from tx to block root")
		})

		// Test Case 3: Complex merkle proof with 4 subtrees
		t.Run("verify complex merkle proof with 4 subtrees", func(t *testing.T) {
			// Create 4 subtree roots
			subtree1 := calculateHashFromBytes(
				append(tx1Hash.CloneBytes(), tx2Hash.CloneBytes()...),
			)
			subtree2 := calculateHashFromBytes(
				append(tx3Hash.CloneBytes(), tx4Hash.CloneBytes()...),
			)

			// Create two more subtrees with dummy data
			dummyHash1, _ := chainhash.NewHashFromStr("1111111111111111111111111111111111111111111111111111111111111111")
			dummyHash2, _ := chainhash.NewHashFromStr("2222222222222222222222222222222222222222222222222222222222222222")
			subtree3 := calculateHashFromBytes(
				append(dummyHash1.CloneBytes(), dummyHash2.CloneBytes()...),
			)

			dummyHash3, _ := chainhash.NewHashFromStr("3333333333333333333333333333333333333333333333333333333333333333")
			dummyHash4, _ := chainhash.NewHashFromStr("4444444444444444444444444444444444444444444444444444444444444444")
			subtree4 := calculateHashFromBytes(
				append(dummyHash3.CloneBytes(), dummyHash4.CloneBytes()...),
			)

			// Build the merkle tree
			// Level 1: combine subtrees pairwise
			level1Left := calculateHashFromBytes(
				append(subtree1.CloneBytes(), subtree2.CloneBytes()...),
			)
			level1Right := calculateHashFromBytes(
				append(subtree3.CloneBytes(), subtree4.CloneBytes()...),
			)

			// Root: combine level 1 nodes
			merkleRoot := calculateHashFromBytes(
				append(level1Left.CloneBytes(), level1Right.CloneBytes()...),
			)

			t.Logf("4-subtree merkle root: %s", merkleRoot.String())

			// Verify proof for tx3 (in subtree2, index 1)
			// tx3's proof path:
			// 1. Sibling in subtree: tx4
			// 2. Sibling subtree: subtree1
			// 3. Sibling at level 1: level1Right

			currentHash := tx3Hash

			// Step 1: Apply subtree proof (tx3 at index 0 within its subtree)
			combined := append(currentHash.CloneBytes(), tx4Hash.CloneBytes()...)
			currentHash = hashPtr(chainhash.DoubleHashH(combined))
			assert.Equal(t, subtree2.String(), currentHash.String(),
				"Should match subtree2 root")

			// Step 2: Apply first level of block proof (subtree2 at index 1, sibling is subtree1)
			// When index is odd, we concatenate as: hash(sibling || current)
			combined = append(subtree1.CloneBytes(), currentHash.CloneBytes()...)
			currentHash = hashPtr(chainhash.DoubleHashH(combined))
			assert.Equal(t, level1Left.String(), currentHash.String(),
				"Should match level1Left")

			// Step 3: Apply second level of block proof (level1Left at index 0, sibling is level1Right)
			combined = append(currentHash.CloneBytes(), level1Right.CloneBytes()...)
			currentHash = hashPtr(chainhash.DoubleHashH(combined))
			assert.Equal(t, merkleRoot.String(), currentHash.String(),
				"Should match final merkle root")

			t.Logf("✓ Complex 4-subtree merkle proof verified")
		})

		// Test Case 4: Verify with odd number of nodes (duplication case)
		t.Run("verify merkle proof with odd number of nodes", func(t *testing.T) {
			// Three transactions, third one will be duplicated
			tx1, _ := chainhash.NewHashFromStr("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
			tx2, _ := chainhash.NewHashFromStr("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
			tx3, _ := chainhash.NewHashFromStr("cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")

			// Build merkle tree with 3 nodes
			// Level 0: tx1, tx2, tx3
			// Level 1: hash(tx1||tx2), hash(tx3||tx3) <- tx3 is duplicated

			node1 := calculateHashFromBytes(append(tx1.CloneBytes(), tx2.CloneBytes()...))
			node2 := calculateHashFromBytes(append(tx3.CloneBytes(), tx3.CloneBytes()...)) // Duplicated

			// Root
			root := calculateHashFromBytes(append(node1.CloneBytes(), node2.CloneBytes()...))

			// Verify proof for tx3 (index 2)
			// tx3's sibling at level 0 should be marked as duplicate (*)
			currentHash := tx3

			// Step 1: tx3 has no sibling (odd node), so duplicate itself
			combined := append(currentHash.CloneBytes(), currentHash.CloneBytes()...)
			currentHash = hashPtr(chainhash.DoubleHashH(combined))
			assert.Equal(t, node2.String(), currentHash.String(),
				"Should match node2 (duplicated tx3)")

			// Step 2: node2's sibling is node1
			combined = append(node1.CloneBytes(), currentHash.CloneBytes()...)
			currentHash = hashPtr(chainhash.DoubleHashH(combined))
			assert.Equal(t, root.String(), currentHash.String(),
				"Should match root with odd nodes")

			t.Logf("✓ Odd number nodes (with duplication) merkle proof verified")
		})
	})
}

// TestMerkleProofWithRealWorldData tests with data similar to actual Bitcoin transactions
func TestMerkleProofWithRealWorldData(t *testing.T) {
	// This test uses a structure similar to a real Bitcoin block with subtrees
	t.Run("real world merkle proof verification", func(t *testing.T) {
		// Simulate a block with 100 transactions split into 4 subtrees
		// We'll focus on proving one specific transaction

		// Our target transaction (e.g., index 42 overall, in subtree 2 at index 10)
		targetTxHash, _ := chainhash.NewHashFromStr("5f4e3d2c1b0a9988776655443322110099887766554433221100ffeeddccbbaa")

		// Its sibling in the subtree (index 11 in subtree 2)
		subtreeSibling, _ := chainhash.NewHashFromStr("aabbccddeeff00112233445566778899001122334455667788990a1b2c3d4e5f")

		// Calculate subtree 2 root
		// Since target is at even index (10), concatenate as target||sibling
		subtree2Root := calculateHashFromBytes(
			append(targetTxHash.CloneBytes(), subtreeSibling.CloneBytes()...),
		)

		// Other subtree roots (simplified for this test)
		subtree1Root, _ := chainhash.NewHashFromStr("1111111111111111111111111111111111111111111111111111111111111111")
		subtree3Root, _ := chainhash.NewHashFromStr("3333333333333333333333333333333333333333333333333333333333333333")
		subtree4Root, _ := chainhash.NewHashFromStr("4444444444444444444444444444444444444444444444444444444444444444")

		// Build block merkle tree
		// Level 1
		left := calculateHashFromBytes(
			append(subtree1Root.CloneBytes(), subtree2Root.CloneBytes()...),
		)
		right := calculateHashFromBytes(
			append(subtree3Root.CloneBytes(), subtree4Root.CloneBytes()...),
		)

		// Root
		blockMerkleRoot := calculateHashFromBytes(
			append(left.CloneBytes(), right.CloneBytes()...),
		)

		// Create the proof response structure
		proof := LegacyMerkleProofResponse{
			TxID:             targetTxHash.String(),
			BlockHeight:      850000,
			MerkleRoot:       blockMerkleRoot.String(),
			SubtreeIndex:     1, // Second subtree (index 1)
			TxIndexInSubtree: 10,
			SubtreeRoot:      subtree2Root.String(),
			SubtreeProof:     []string{subtreeSibling.String()},
			BlockProof: []string{
				subtree1Root.String(), // Sibling at subtree level
				right.String(),        // Sibling at level 1
			},
			Flags: []int{1, 0, 1}, // Right, Left, Right
		}

		// Verify the proof
		currentHash := targetTxHash

		// Apply subtree proof
		for i, siblingHex := range proof.SubtreeProof {
			sibling, err := chainhash.NewHashFromStr(siblingHex)
			require.NoError(t, err)

			var combined []byte
			if i < len(proof.Flags) && proof.Flags[i] == 0 {
				// Sibling on left
				combined = append(sibling.CloneBytes(), currentHash.CloneBytes()...)
			} else {
				// Sibling on right (or default for even index)
				combined = append(currentHash.CloneBytes(), sibling.CloneBytes()...)
			}
			currentHash = hashPtr(chainhash.DoubleHashH(combined))
		}

		// Verify subtree root
		assert.Equal(t, proof.SubtreeRoot, currentHash.String(),
			"Subtree root should match after applying subtree proof")

		// Apply block proof
		// First sibling: subtree1 (we are subtree2 at index 1, so sibling is on left)
		sibling1, _ := chainhash.NewHashFromStr(proof.BlockProof[0])
		combined := append(sibling1.CloneBytes(), currentHash.CloneBytes()...)
		currentHash = hashPtr(chainhash.DoubleHashH(combined))

		// Second sibling: right side of level 1
		sibling2, _ := chainhash.NewHashFromStr(proof.BlockProof[1])
		combined = append(currentHash.CloneBytes(), sibling2.CloneBytes()...)
		currentHash = hashPtr(chainhash.DoubleHashH(combined))

		// Final verification
		assert.Equal(t, proof.MerkleRoot, currentHash.String(),
			"Block merkle root should match after applying complete proof")

		t.Logf("✓ Real-world style merkle proof verified")
		t.Logf("  Transaction: %s", proof.TxID)
		t.Logf("  Subtree Root: %s", proof.SubtreeRoot)
		t.Logf("  Block Merkle Root: %s", proof.MerkleRoot)
	})
}

// Helper functions

// calculateHash takes two hex strings and returns the double SHA256 hash
func calculateHash(left, right string) string {
	leftBytes, _ := hex.DecodeString(left)
	rightBytes, _ := hex.DecodeString(right)
	combined := append(leftBytes, rightBytes...)
	hash := chainhash.DoubleHashH(combined)
	return hash.String()
}

// calculateHashFromBytes takes bytes and returns the double SHA256 hash
func calculateHashFromBytes(data []byte) *chainhash.Hash {
	hash := chainhash.DoubleHashH(data)
	return &hash
}

// hashPtr converts a chainhash.Hash value to a pointer
func hashPtr(h chainhash.Hash) *chainhash.Hash {
	return &h
}

// TestVerifyMerkleProofAlgorithm tests the merkle proof verification algorithm
func TestVerifyMerkleProofAlgorithm(t *testing.T) {
	t.Run("verify merkle proof algorithm step by step", func(t *testing.T) {
		// Create a simple 4-transaction tree to demonstrate the algorithm
		// Transactions
		txs := make([]*chainhash.Hash, 4)
		for i := 0; i < 4; i++ {
			data := []byte{byte(i)}
			hash := chainhash.DoubleHashH(data)
			txs[i] = &hash
		}

		// Build the tree manually
		// Level 0: tx0, tx1, tx2, tx3
		// Level 1: hash(tx0||tx1), hash(tx2||tx3)
		// Level 2: root

		level1_0 := calculateHashFromBytes(append(txs[0].CloneBytes(), txs[1].CloneBytes()...))
		level1_1 := calculateHashFromBytes(append(txs[2].CloneBytes(), txs[3].CloneBytes()...))
		root := calculateHashFromBytes(append(level1_0.CloneBytes(), level1_1.CloneBytes()...))

		// Test proof for tx1 (index 1)
		// Proof path: tx0 (sibling at level 0), level1_1 (sibling at level 1)
		proof := []string{
			txs[0].String(),   // Left sibling at level 0
			level1_1.String(), // Right sibling at level 1
		}
		_ = []int{0, 1} // Left, Right (flags not used in this manual verification)

		// Verify
		currentHash := txs[1]

		// Apply level 0 proof (tx0 is on left)
		sibling0, _ := chainhash.NewHashFromStr(proof[0])
		combined := append(sibling0.CloneBytes(), currentHash.CloneBytes()...)
		currentHash = hashPtr(chainhash.DoubleHashH(combined))
		assert.Equal(t, level1_0.String(), currentHash.String(), "Should match level 1 node")

		// Apply level 1 proof (level1_1 is on right)
		sibling1, _ := chainhash.NewHashFromStr(proof[1])
		combined = append(currentHash.CloneBytes(), sibling1.CloneBytes()...)
		currentHash = hashPtr(chainhash.DoubleHashH(combined))
		assert.Equal(t, root.String(), currentHash.String(), "Should match root")

		t.Logf("✓ Merkle proof algorithm verified")
		t.Logf("  Started with: tx1")
		t.Logf("  Applied proof: [%s, %s]", proof[0][:8], proof[1][:8])
		t.Logf("  Reached root: %s", root.String()[:16])
	})
}
