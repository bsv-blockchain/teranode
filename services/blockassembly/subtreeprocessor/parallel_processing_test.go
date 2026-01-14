package subtreeprocessor

import (
	"crypto/rand"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/stretchr/testify/require"
)

// generateTestHash generates a random hash for testing
func generateTestHash(t *testing.T) chainhash.Hash {
	t.Helper()
	var hash chainhash.Hash
	_, err := rand.Read(hash[:])
	require.NoError(t, err)
	return hash
}

// TestParallelGetAndSetIfNotExistsConcurrency tests that the parallel processing
// correctly handles a large number of nodes without race conditions.
func TestParallelGetAndSetIfNotExistsConcurrency(t *testing.T) {
	stp, cleanup := setupSubtreeProcessorForBench(t, 1024)
	defer cleanup()

	// Create large node set (10k transactions) to trigger parallel path
	const txCount = 10_000
	nodes := make([]subtreepkg.Node, txCount)
	currentTxMap := NewSplitTxInpointsMap(splitMapBuckets)

	// Populate currentTxMap with parent data
	for i := 0; i < txCount; i++ {
		hash := generateTestHash(t)
		nodes[i] = subtreepkg.Node{Hash: hash, Fee: uint64(i), SizeInBytes: 250}
		currentTxMap.Set(hash, &subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{}})
	}

	// Allocate wasInserted
	wasInserted := make([]bool, len(nodes))

	// Run parallel Get+SetIfNotExists
	err := stp.parallelGetAndSetIfNotExists(nodes, currentTxMap, 0, 375, wasInserted)
	require.NoError(t, err)

	// Verify all nodes were processed and marked as inserted
	validCount := 0
	for idx, node := range nodes {
		if wasInserted[idx] {
			validCount++
			// Verify it's in stp.currentTxMap
			_, found := stp.currentTxMap.Get(node.Hash)
			require.True(t, found, "Node should be in currentTxMap")
		}
	}

	require.Equal(t, txCount, validCount, "All nodes should be marked as inserted")
	require.Equal(t, txCount, stp.currentTxMap.Length(), "All nodes should be in currentTxMap")
}

// TestParallelGetAndSetIfNotExistsDuplicates tests that duplicates are correctly
// handled and only the first occurrence is marked as inserted.
func TestParallelGetAndSetIfNotExistsDuplicates(t *testing.T) {
	stp, cleanup := setupSubtreeProcessorForBench(t, 1024)
	defer cleanup()

	// Create nodes with intentional duplicates
	const uniqueCount = 1000
	const duplicationFactor = 5
	uniqueHashes := make([]chainhash.Hash, uniqueCount)
	for i := range uniqueHashes {
		uniqueHashes[i] = generateTestHash(t)
	}

	// Create nodes with duplicates (5x duplication)
	totalNodes := uniqueCount * duplicationFactor
	nodes := make([]subtreepkg.Node, totalNodes)
	currentTxMap := NewSplitTxInpointsMap(splitMapBuckets)

	for i := 0; i < totalNodes; i++ {
		// create duplicates by cycling through unique hashes
		hash := uniqueHashes[i%uniqueCount] // nolint:gosec
		nodes[i] = subtreepkg.Node{Hash: hash, Fee: uint64(i), SizeInBytes: 250}
		currentTxMap.Set(hash, &subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{}})
	}

	// Allocate wasInserted
	wasInserted := make([]bool, len(nodes))

	// Run parallel Get+SetIfNotExists
	err := stp.parallelGetAndSetIfNotExists(nodes, currentTxMap, 0, 375, wasInserted)
	require.NoError(t, err)

	// Verify only uniqueCount entries in stp.currentTxMap
	require.Equal(t, uniqueCount, stp.currentTxMap.Length(), "Only unique nodes should be in currentTxMap")

	// Count how many were marked as inserted
	insertedCount := 0
	for _, inserted := range wasInserted {
		if inserted {
			insertedCount++
		}
	}

	// Due to parallel execution, the exact count of "first" insertions may vary,
	// but it should be exactly uniqueCount (one per unique hash)
	require.Equal(t, uniqueCount, insertedCount, "Exactly one insertion per unique hash")
}

// TestParallelGetAndSetIfNotExistsErrorHandling tests that errors are properly
// propagated when a node is not found in currentTxMap.
func TestParallelGetAndSetIfNotExistsErrorHandling(t *testing.T) {
	stp, cleanup := setupSubtreeProcessorForBench(t, 1024)
	defer cleanup()

	// Create nodes
	const txCount = 1000
	nodes := make([]subtreepkg.Node, txCount)
	currentTxMap := NewSplitTxInpointsMap(splitMapBuckets)

	// Populate currentTxMap with only HALF of them (simulate missing data)
	for i := 0; i < txCount; i++ {
		hash := generateTestHash(t)
		nodes[i] = subtreepkg.Node{Hash: hash, Fee: uint64(i), SizeInBytes: 250}

		if i < txCount/2 {
			// Only add first half to currentTxMap
			currentTxMap.Set(hash, &subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{}})
		}
	}

	// Allocate wasInserted
	wasInserted := make([]bool, len(nodes))

	// Run parallel Get+SetIfNotExists - should fail when it hits missing node
	err := stp.parallelGetAndSetIfNotExists(nodes, currentTxMap, 0, 375, wasInserted)

	// Verify function returns error (does not silently fail)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found in currentTxMap")
}

// TestParallelGetAndSetIfNotExistsCoinbasePlaceholder tests that coinbase
// placeholder nodes are correctly skipped.
func TestParallelGetAndSetIfNotExistsCoinbasePlaceholder(t *testing.T) {
	stp, cleanup := setupSubtreeProcessorForBench(t, 1024)
	defer cleanup()

	// Create nodes with some coinbase placeholders
	const txCount = 100
	nodes := make([]subtreepkg.Node, txCount)
	currentTxMap := NewSplitTxInpointsMap(splitMapBuckets)

	coinbasePlaceholderIndices := []int{0, 50, 99} // Indices with coinbase placeholder

	for i := 0; i < txCount; i++ {
		var hash chainhash.Hash
		isCoinbase := false
		for _, idx := range coinbasePlaceholderIndices {
			if i == idx {
				hash = *subtreepkg.CoinbasePlaceholderHash
				isCoinbase = true
				break
			}
		}
		if !isCoinbase {
			hash = generateTestHash(t)
			currentTxMap.Set(hash, &subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{}})
		}
		nodes[i] = subtreepkg.Node{Hash: hash, Fee: uint64(i), SizeInBytes: 250}
	}

	wasInserted := make([]bool, len(nodes))

	err := stp.parallelGetAndSetIfNotExists(nodes, currentTxMap, 0, 375, wasInserted)
	require.NoError(t, err)

	// Verify coinbase placeholders were not inserted
	for _, idx := range coinbasePlaceholderIndices {
		require.False(t, wasInserted[idx], "Coinbase placeholder should not be marked as inserted")
	}

	// Verify non-coinbase nodes were inserted
	expectedInserts := txCount - len(coinbasePlaceholderIndices)
	insertedCount := 0
	for _, inserted := range wasInserted {
		if inserted {
			insertedCount++
		}
	}
	require.Equal(t, expectedInserts, insertedCount)
}

// TestParallelGetAndSetIfNotExistsRemoveMap tests that nodes in the removeMap
// are correctly skipped and removed.
func TestParallelGetAndSetIfNotExistsRemoveMap(t *testing.T) {
	stp, cleanup := setupSubtreeProcessorForBench(t, 1024)
	defer cleanup()

	const txCount = 100
	const removeCount = 20
	nodes := make([]subtreepkg.Node, txCount)
	currentTxMap := NewSplitTxInpointsMap(splitMapBuckets)

	// First removeCount nodes will be in removeMap
	for i := 0; i < txCount; i++ {
		hash := generateTestHash(t)
		nodes[i] = subtreepkg.Node{Hash: hash, Fee: uint64(i), SizeInBytes: 250}
		currentTxMap.Set(hash, &subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{}})

		if i < removeCount {
			err := stp.removeMap.Put(hash, 1)
			require.NoError(t, err)
		}
	}

	removeMapLength := stp.removeMap.Length()
	require.Equal(t, removeCount, removeMapLength)

	wasInserted := make([]bool, len(nodes))

	err := stp.parallelGetAndSetIfNotExists(nodes, currentTxMap, removeMapLength, 375, wasInserted)
	require.NoError(t, err)

	// Verify nodes in removeMap were not inserted
	for i := 0; i < removeCount; i++ {
		require.False(t, wasInserted[i], "Node in removeMap should not be marked as inserted")
	}

	// Verify other nodes were inserted
	for i := removeCount; i < txCount; i++ {
		require.True(t, wasInserted[i], "Node not in removeMap should be marked as inserted")
	}

	// Verify removeMap nodes were deleted
	for i := 0; i < removeCount; i++ {
		require.False(t, stp.removeMap.Exists(nodes[i].Hash), "Node should be removed from removeMap")
	}
}

// TestParallelGetAndSetIfNotExistsEmptyNodes tests the edge case of empty node slice.
func TestParallelGetAndSetIfNotExistsEmptyNodes(t *testing.T) {
	stp, cleanup := setupSubtreeProcessorForBench(t, 1024)
	defer cleanup()

	nodes := []subtreepkg.Node{}
	currentTxMap := NewSplitTxInpointsMap(splitMapBuckets)
	wasInserted := make([]bool, 0)

	err := stp.parallelGetAndSetIfNotExists(nodes, currentTxMap, 0, 375, wasInserted)
	require.NoError(t, err)
}

// TestProcessOwnBlockSubtreeNodesParallelPath tests that the parallel path
// in processOwnBlockSubtreeNodes works correctly.
func TestProcessOwnBlockSubtreeNodesParallelPath(t *testing.T) {
	stp, cleanup := setupSubtreeProcessorForBench(t, 1024)
	defer cleanup()

	// Set threshold low to trigger parallel path
	stp.settings.BlockAssembly.ParallelSetIfNotExistsThreshold = 100

	const txCount = 200 // Above threshold
	nodes := make([]subtreepkg.Node, txCount)
	currentTxMap := NewSplitTxInpointsMap(splitMapBuckets)

	for i := 0; i < txCount; i++ {
		hash := generateTestHash(t)
		nodes[i] = subtreepkg.Node{Hash: hash, Fee: uint64(i), SizeInBytes: 250}
		currentTxMap.Set(hash, &subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{}})
	}

	block := &model.Block{
		Header: blockHeader,
	}

	err := stp.processOwnBlockSubtreeNodes(block, nodes, currentTxMap, 0, nil, true)
	require.NoError(t, err)

	// Verify all nodes were added to currentTxMap
	require.Equal(t, txCount, stp.currentTxMap.Length())
}

// TestProcessOwnBlockSubtreeNodesSequentialPath tests that the sequential path
// in processOwnBlockSubtreeNodes still works correctly for small node sets.
func TestProcessOwnBlockSubtreeNodesSequentialPath(t *testing.T) {
	stp, cleanup := setupSubtreeProcessorForBench(t, 1024)
	defer cleanup()

	// Set threshold high to use sequential path
	stp.settings.BlockAssembly.ParallelSetIfNotExistsThreshold = 10000

	const txCount = 50 // Below threshold
	nodes := make([]subtreepkg.Node, txCount)
	currentTxMap := NewSplitTxInpointsMap(splitMapBuckets)

	for i := 0; i < txCount; i++ {
		hash := generateTestHash(t)
		nodes[i] = subtreepkg.Node{Hash: hash, Fee: uint64(i), SizeInBytes: 250}
		currentTxMap.Set(hash, &subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{}})
	}

	block := &model.Block{
		Header: blockHeader,
	}

	err := stp.processOwnBlockSubtreeNodes(block, nodes, currentTxMap, 0, nil, true)
	require.NoError(t, err)

	// Verify all nodes were added to currentTxMap
	require.Equal(t, txCount, stp.currentTxMap.Length())
}
