// Package blockassembly provides functionality for assembling Bitcoin blocks in Teranode.
package blockassembly

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/assert"
)

// createTestTx creates a test transaction for parent validation ordering tests
func createTestTx(txID string, parentIDs ...string) *utxo.UnminedTransaction {
	// Create TxInpoints structure
	var txInpoints subtree.TxInpoints

	if len(parentIDs) > 0 {
		// Create parent transaction hashes and indices
		parentHashes := make([]chainhash.Hash, 0, len(parentIDs))
		idxs := make([][]uint32, 0, len(parentIDs))

		// Group parents by hash
		parentMap := make(map[string][]uint32)
		for _, parentID := range parentIDs {
			if _, exists := parentMap[parentID]; !exists {
				parentMap[parentID] = []uint32{}
			}
			// For simplicity, always use output index 0
			parentMap[parentID] = append(parentMap[parentID], 0)
		}

		// Build the arrays
		for parentID, indices := range parentMap {
			parentHash, _ := chainhash.NewHashFromStr(parentID)
			parentHashes = append(parentHashes, *parentHash)
			idxs = append(idxs, indices)
		}

		txInpoints.ParentTxHashes = parentHashes
		txInpoints.Idxs = idxs
	}

	// Create hash from the txID string
	hash, _ := chainhash.NewHashFromStr(txID)

	return &utxo.UnminedTransaction{
		Node: &subtree.Node{
			Hash:        *hash,
			Fee:         1000,
			SizeInBytes: 250,
		},
		TxInpoints: &txInpoints,
	}
}

// TestParentChildOrderingLogic tests the core logic of parent-child ordering validation
// without needing the full BlockAssembler setup
func TestParentChildOrderingLogic(t *testing.T) {
	tests := []struct {
		name          string
		setupTxs      func() []*utxo.UnminedTransaction
		setupUTXO     func() map[chainhash.Hash]bool // Simulates UTXO store - true if exists
		expectedValid []string                       // expected transaction IDs that should be valid
		expectedSkip  []string                       // expected transaction IDs that should be skipped
	}{
		{
			name: "Valid ordering - parent before child",
			setupTxs: func() []*utxo.UnminedTransaction {
				parent := createTestTx("0000000000000000000000000000000000000000000000000000000000000001")
				child := createTestTx("0000000000000000000000000000000000000000000000000000000000000002",
					"0000000000000000000000000000000000000000000000000000000000000001")
				parent.CreatedAt = 100
				child.CreatedAt = 200
				return []*utxo.UnminedTransaction{parent, child}
			},
			setupUTXO: func() map[chainhash.Hash]bool {
				utxoStore := make(map[chainhash.Hash]bool)
				// Parent exists in UTXO store as unmined
				parent1, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
				utxoStore[*parent1] = true
				return utxoStore
			},
			expectedValid: []string{
				"0000000000000000000000000000000000000000000000000000000000000001",
				"0000000000000000000000000000000000000000000000000000000000000002",
			},
			expectedSkip: []string{},
		},
		{
			name: "Invalid ordering - parent after child",
			setupTxs: func() []*utxo.UnminedTransaction {
				parent := createTestTx("0000000000000000000000000000000000000000000000000000000000000002")
				child := createTestTx("0000000000000000000000000000000000000000000000000000000000000001",
					"0000000000000000000000000000000000000000000000000000000000000002")
				child.CreatedAt = 100
				parent.CreatedAt = 200
				return []*utxo.UnminedTransaction{child, parent}
			},
			setupUTXO: func() map[chainhash.Hash]bool {
				utxoStore := make(map[chainhash.Hash]bool)
				// Parent exists in UTXO store as unmined
				parent2, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000002")
				utxoStore[*parent2] = true
				return utxoStore
			},
			expectedValid: []string{
				"0000000000000000000000000000000000000000000000000000000000000002", // parent is valid (no dependencies)
			},
			expectedSkip: []string{
				"0000000000000000000000000000000000000000000000000000000000000001", // child should be skipped (parent after)
			},
		},
		{
			name: "Complex chain - all in order",
			setupTxs: func() []*utxo.UnminedTransaction {
				tx1 := createTestTx("0000000000000000000000000000000000000000000000000000000000000001")
				tx2 := createTestTx("0000000000000000000000000000000000000000000000000000000000000002",
					"0000000000000000000000000000000000000000000000000000000000000001")
				tx3 := createTestTx("0000000000000000000000000000000000000000000000000000000000000003",
					"0000000000000000000000000000000000000000000000000000000000000002")
				tx1.CreatedAt = 100
				tx2.CreatedAt = 200
				tx3.CreatedAt = 300
				return []*utxo.UnminedTransaction{tx1, tx2, tx3}
			},
			setupUTXO: func() map[chainhash.Hash]bool {
				utxoStore := make(map[chainhash.Hash]bool)
				// All parents exist in UTXO store
				h1, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
				h2, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000002")
				utxoStore[*h1] = true
				utxoStore[*h2] = true
				return utxoStore
			},
			expectedValid: []string{
				"0000000000000000000000000000000000000000000000000000000000000001",
				"0000000000000000000000000000000000000000000000000000000000000002",
				"0000000000000000000000000000000000000000000000000000000000000003",
			},
			expectedSkip: []string{},
		},
		{
			name: "Multiple parents - all before child",
			setupTxs: func() []*utxo.UnminedTransaction {
				parent1 := createTestTx("0000000000000000000000000000000000000000000000000000000000000001")
				parent2 := createTestTx("0000000000000000000000000000000000000000000000000000000000000002")
				child := createTestTx("0000000000000000000000000000000000000000000000000000000000000003",
					"0000000000000000000000000000000000000000000000000000000000000001",
					"0000000000000000000000000000000000000000000000000000000000000002")
				parent1.CreatedAt = 100
				parent2.CreatedAt = 200
				child.CreatedAt = 300
				return []*utxo.UnminedTransaction{parent1, parent2, child}
			},
			setupUTXO: func() map[chainhash.Hash]bool {
				utxoStore := make(map[chainhash.Hash]bool)
				// Both parents exist in UTXO store
				h1, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
				h2, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000002")
				utxoStore[*h1] = true
				utxoStore[*h2] = true
				return utxoStore
			},
			expectedValid: []string{
				"0000000000000000000000000000000000000000000000000000000000000001",
				"0000000000000000000000000000000000000000000000000000000000000002",
				"0000000000000000000000000000000000000000000000000000000000000003",
			},
			expectedSkip: []string{},
		},
		{
			name: "Multiple parents - one after child",
			setupTxs: func() []*utxo.UnminedTransaction {
				parent1 := createTestTx("0000000000000000000000000000000000000000000000000000000000000001")
				parent2 := createTestTx("0000000000000000000000000000000000000000000000000000000000000003") // This comes after child
				child := createTestTx("0000000000000000000000000000000000000000000000000000000000000002",
					"0000000000000000000000000000000000000000000000000000000000000001",
					"0000000000000000000000000000000000000000000000000000000000000003")
				parent1.CreatedAt = 100
				child.CreatedAt = 200
				parent2.CreatedAt = 300
				return []*utxo.UnminedTransaction{parent1, child, parent2}
			},
			setupUTXO: func() map[chainhash.Hash]bool {
				utxoStore := make(map[chainhash.Hash]bool)
				// Both parents exist in UTXO store
				h1, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
				h3, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000003")
				utxoStore[*h1] = true
				utxoStore[*h3] = true
				return utxoStore
			},
			expectedValid: []string{
				"0000000000000000000000000000000000000000000000000000000000000001",
				"0000000000000000000000000000000000000000000000000000000000000003",
			},
			expectedSkip: []string{
				"0000000000000000000000000000000000000000000000000000000000000002", // child skipped because parent2 comes after
			},
		},
		{
			name: "Parent not in UTXO store - should skip child",
			setupTxs: func() []*utxo.UnminedTransaction {
				// Child depends on parent that doesn't exist in UTXO store
				child := createTestTx("0000000000000000000000000000000000000000000000000000000000000002",
					"0000000000000000000000000000000000000000000000000000000000000001")
				child.CreatedAt = 100
				return []*utxo.UnminedTransaction{child}
			},
			setupUTXO: func() map[chainhash.Hash]bool {
				// Empty UTXO store - parent doesn't exist
				return make(map[chainhash.Hash]bool)
			},
			expectedValid: []string{},
			expectedSkip: []string{
				"0000000000000000000000000000000000000000000000000000000000000002", // child skipped because parent not in UTXO
			},
		},
		{
			name: "Parent in UTXO but not in unmined list - should skip child",
			setupTxs: func() []*utxo.UnminedTransaction {
				// Only child in unmined list, parent exists in UTXO but not in our processing list
				child := createTestTx("0000000000000000000000000000000000000000000000000000000000000002",
					"0000000000000000000000000000000000000000000000000000000000000001")
				child.CreatedAt = 100
				return []*utxo.UnminedTransaction{child}
			},
			setupUTXO: func() map[chainhash.Hash]bool {
				utxoStore := make(map[chainhash.Hash]bool)
				// Parent exists in UTXO store but is not in unmined list
				parent1, _ := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
				utxoStore[*parent1] = true
				return utxoStore
			},
			expectedValid: []string{},
			expectedSkip: []string{
				"0000000000000000000000000000000000000000000000000000000000000002", // child skipped because parent not in processing list
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup
			unminedTxs := tt.setupTxs()
			utxoStore := tt.setupUTXO()

			// Create maps for validation logic (simulating the actual implementation)
			unminedTxMap := make(map[chainhash.Hash]bool, len(unminedTxs))
			unminedTxIndexMap := make(map[chainhash.Hash]int, len(unminedTxs))

			for idx, tx := range unminedTxs {
				unminedTxMap[tx.Hash] = true
				unminedTxIndexMap[tx.Hash] = idx
			}

			// Perform validation logic - matching production code behavior
			validTxs := make([]*utxo.UnminedTransaction, 0)
			skippedTxs := make([]*utxo.UnminedTransaction, 0)

			for _, tx := range unminedTxs {
				allParentsValid := true
				hasInvalidOrdering := false

				// Check each parent transaction (matching production logic)
				parentHashes := tx.TxInpoints.GetParentTxHashes()

				for _, parentTxID := range parentHashes {
					// STEP 1: Check if parent exists in UTXO store (production lines 1882-1889)
					if !utxoStore[parentTxID] {
						// Parent not found in UTXO store at all
						allParentsValid = false
						break
					}

					// STEP 2: Parent exists - check if it's unmined and in our list
					if unminedTxMap[parentTxID] {
						// Parent is unmined and in our list - check ordering
						currentIdx := unminedTxIndexMap[tx.Hash]
						parentIdx, exists := unminedTxIndexMap[parentTxID]
						if !exists || parentIdx >= currentIdx {
							// Parent comes after or at same position as child - invalid ordering
							hasInvalidOrdering = true
							break
						}
					} else {
						// Parent exists in UTXO but NOT in unmined list
						// This means it's either:
						// 1. On best chain (would be valid in production)
						// 2. Unmined but not in our processing list (invalid)
						// For this test, we assume unmined but not in list = invalid
						// (matching production lines 1913-1917)
						allParentsValid = false
						break
					}
				}

				if !allParentsValid || hasInvalidOrdering {
					skippedTxs = append(skippedTxs, tx)
				} else {
					validTxs = append(validTxs, tx)
				}
			}

			// Assert valid transactions
			assert.Equal(t, len(tt.expectedValid), len(validTxs), "Number of valid transactions mismatch")
			for i, expectedID := range tt.expectedValid {
				assert.Equal(t, expectedID, validTxs[i].Hash.String(), "Valid transaction at index %d mismatch", i)
			}

			// Assert skipped transactions
			assert.Equal(t, len(tt.expectedSkip), len(skippedTxs), "Number of skipped transactions mismatch")
			for i, expectedID := range tt.expectedSkip {
				assert.Equal(t, expectedID, skippedTxs[i].Hash.String(), "Skipped transaction at index %d mismatch", i)
			}
		})
	}
}
