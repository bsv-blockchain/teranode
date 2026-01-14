package subtreeprocessor

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupTestSubtreeProcessor creates a test SubtreeProcessor with common configuration
func setupTestSubtreeProcessor(t *testing.T) *SubtreeProcessor {
	t.Helper()

	// Setup UTXO store
	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)

	utxoStore, err := sql.New(context.Background(), ulogger.TestLogger{}, test.CreateBaseTestSettings(t), utxoStoreURL)
	require.NoError(t, err)

	// Setup blob store
	blobStore := blob_memory.New()

	// Setup settings with small subtrees to trigger multiple subtrees
	settings := test.CreateBaseTestSettings(t)
	settings.BlockAssembly.InitialMerkleItemsPerSubtree = 4

	// Setup new subtree channel
	newSubtreeChan := make(chan NewSubtreeRequest, 10)
	go func() {
		for newSubtreeRequest := range newSubtreeChan {
			if newSubtreeRequest.ErrChan != nil {
				newSubtreeRequest.ErrChan <- nil
			}
		}
	}()
	t.Cleanup(func() {
		close(newSubtreeChan)
	})

	// Create SubtreeProcessor
	stp, err := NewSubtreeProcessor(t.Context(), ulogger.TestLogger{}, settings, blobStore, nil, utxoStore, newSubtreeChan)
	require.NoError(t, err)

	// Start processing and ensure cleanup
	stp.Start(t.Context())
	t.Cleanup(func() {
		stp.Stop(context.Background())
	})

	return stp
}

// TestReorgDuplicateTransactionBug demonstrates the bug where during a reorg,
// if a block contains the same transaction in multiple subtrees, that transaction
// gets added multiple times to the block assembly.
func TestReorgDuplicateTransactionBug(t *testing.T) {
	t.Run("ReorgWithDuplicateTransactionAcrossSubtrees", func(t *testing.T) {
		// Setup using helper function
		stp := setupTestSubtreeProcessor(t)

		// Create a transaction that will appear in multiple subtrees
		duplicateTxHash := chainhash.HashH([]byte("duplicate_tx_in_reorg"))
		duplicateNode := subtreepkg.Node{
			Hash:        duplicateTxHash,
			Fee:         1000,
			SizeInBytes: 250,
		}

		// Create some unique transactions for variety
		uniqueTx1 := subtreepkg.Node{
			Hash:        chainhash.HashH([]byte("unique_1")),
			Fee:         500,
			SizeInBytes: 200,
		}
		uniqueTx2 := subtreepkg.Node{
			Hash:        chainhash.HashH([]byte("unique_2")),
			Fee:         600,
			SizeInBytes: 180,
		}

		// Create subtree 1 with the duplicate transaction
		subtree1, err := subtreepkg.NewTreeByLeafCount(4)
		require.NoError(t, err)
		err = subtree1.AddCoinbaseNode()
		require.NoError(t, err)
		err = subtree1.AddSubtreeNode(duplicateNode) // First occurrence
		require.NoError(t, err)
		err = subtree1.AddSubtreeNode(uniqueTx1)
		require.NoError(t, err)

		// Create subtree 2 also with the duplicate transaction
		subtree2, err := subtreepkg.NewTreeByLeafCount(4)
		require.NoError(t, err)
		err = subtree2.AddCoinbaseNode()
		require.NoError(t, err)
		err = subtree2.AddSubtreeNode(duplicateNode) // Second occurrence - DUPLICATE!
		require.NoError(t, err)
		err = subtree2.AddSubtreeNode(uniqueTx2)
		require.NoError(t, err)

		// Create subtree 3 also with the duplicate transaction
		subtree3, err := subtreepkg.NewTreeByLeafCount(4)
		require.NoError(t, err)
		err = subtree3.AddCoinbaseNode()
		require.NoError(t, err)
		err = subtree3.AddSubtreeNode(duplicateNode) // Third occurrence - DUPLICATE!
		require.NoError(t, err)
		// Add filler to make it look more realistic
		fillerHash := chainhash.HashH([]byte("filler"))
		err = subtree3.AddSubtreeNode(subtreepkg.Node{Hash: fillerHash, Fee: 300, SizeInBytes: 150})
		require.NoError(t, err)

		// Simulate the state during a reorg where we're processing our own block
		// that contains these subtrees
		chainedSubtrees := []*subtreepkg.Subtree{subtree1, subtree2, subtree3}

		// Create a fresh current subtree (as would happen during reorg)
		currentSubtree, err := subtreepkg.NewTreeByLeafCount(4)
		require.NoError(t, err)
		err = currentSubtree.AddCoinbaseNode()
		require.NoError(t, err)

		// Create currentTxMap that contains all the transactions
		// This simulates the state where these transactions were in the mempool
		currentTxMap := NewSplitTxInpointsMap(256)

		// Pre-populate currentTxMap with all transactions (simulating they were in mempool)
		// Use a common parent hash instead of self-reference
		parentHash := chainhash.HashH([]byte("parent_tx"))
		for _, subtree := range chainedSubtrees {
			for _, node := range subtree.Nodes {
				if !node.Hash.Equal(*subtreepkg.CoinbasePlaceholderHash) {
					currentTxMap.Set(node.Hash, &subtreepkg.TxInpoints{
						ParentTxHashes: []chainhash.Hash{parentHash}, // Use common parent instead of self-reference
					})
				}
			}
		}

		// Create a mock block that would contain these subtrees
		prevBlockHash := chainhash.HashH([]byte("prev_block"))
		mockBlock := &model.Block{
			Header: &model.BlockHeader{
				HashPrevBlock: &prevBlockHash,
				Version:       1,
				Timestamp:     1234567890,
			},
			CoinbaseTx: &bt.Tx{}, // Simple coinbase
			Subtrees:   []*chainhash.Hash{},
		}

		// Count how many times the duplicate transaction appears initially
		initialCount := countTransactionInSubtreesForTest(stp, duplicateTxHash)
		assert.Equal(t, 0, initialCount, "Should start with 0 occurrences in block assembly")

		// Process the block - this simulates processing during a reorg
		// where we're rebuilding the block assembly from our own mined block
		err = stp.processOwnBlockNodes(
			context.Background(),
			mockBlock,
			chainedSubtrees,
			currentSubtree,
			currentTxMap,
			true, // skipNotification
		)
		require.NoError(t, err)

		// Count how many times the duplicate transaction appears after processing
		finalCount := countTransactionInSubtreesForTest(stp, duplicateTxHash)

		// THE BUG: The duplicate transaction should only appear once,
		// but due to the bug it appears multiple times (once for each subtree)
		t.Logf("Duplicate transaction %s appears %d times in block assembly",
			duplicateTxHash.String()[:8], finalCount)

		// This assertion SHOULD pass but WILL FAIL due to the bug
		// The transaction should only be added once, not 3 times
		assert.Equal(t, 1, finalCount,
			"Transaction should only appear ONCE in block assembly, but appears %d times due to bug",
			finalCount)

		// Also verify that unique transactions appear only once each
		unique1Count := countTransactionInSubtreesForTest(stp, uniqueTx1.Hash)
		unique2Count := countTransactionInSubtreesForTest(stp, uniqueTx2.Hash)

		assert.Equal(t, 1, unique1Count, "Unique transaction 1 should appear once")
		assert.Equal(t, 1, unique2Count, "Unique transaction 2 should appear once")

		// Log the issue
		if finalCount > 1 {
			t.Logf("\n🔴 BUG CONFIRMED: Duplicate transaction added %d times during reorg!", finalCount)
			t.Log("  This happens because addNode() doesn't check for duplicates when parents != nil")
			t.Log("  Location: SubtreeProcessor.go, addNode() function")
			t.Log("  When processing own blocks during reorg, each subtree's occurrence gets added")
			t.Log("  This leads to duplicate transactions in block assembly!")
		}
	})
}

// countTransactionInSubtreesForTest counts how many times a transaction appears in the subtrees
// This version uses proper synchronization to avoid race conditions
func countTransactionInSubtreesForTest(stp *SubtreeProcessor, txHash chainhash.Hash) int {
	count := 0

	// Check chained subtrees
	for _, subtree := range stp.chainedSubtrees {
		for _, node := range subtree.Nodes {
			if node.Hash.Equal(txHash) {
				count++
			}
		}
	}

	// Check current subtree
	if currentSubtree := stp.currentSubtree.Load(); currentSubtree != nil {
		for _, node := range currentSubtree.Nodes {
			if node.Hash.Equal(txHash) {
				count++
			}
		}
	}

	return count
}
